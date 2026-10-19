"""Ordinary Movie records through the existing incremental production Core.

Only raw, bounded record bytes enter Core. Ray submission and waiting run in
the existing asynchronous transport; prepared ObjectRefs go to the model actor.
The synchronous model kernel and its actor owner are shared with the static
reference. This module never calls that reference's iterator or scheduler.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
import hashlib
import json
import os
import threading
import time
from typing import Any, Callable, Iterator

from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.models import EndpointSnapshot, TopologySnapshot
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import (
    OfferedTask, SessionLimits, SessionSpec, SessionTimeouts, State, TaskInfo,
)
from src.scheduling.core.session_policy import SessionPolicies
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.runtime.async_backend import BoundedAsyncBackend, ConfirmedTaskFailure
from src.scheduling.submission_control.admission import StaticAdmissionController
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

from .lotus_records_actor_owner import RecordsActorOwner, _ModelActor


@dataclass(frozen=True)
class CoreRecordsCapacity:
    """Per-row logical storage, bounded actions, and poll interval; not RSS caps."""

    item_input_bytes: int = 2 * 1024 * 1024
    item_result_bytes: int = 2 * 1024 * 1024
    metadata_bytes: int = 2048
    step_actions: int = 16
    poll_interval_s: float = 0.01

    def __post_init__(self):
        for name in ("item_input_bytes", "item_result_bytes", "metadata_bytes", "step_actions"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(name + " must be a positive integer")
        import math
        if (type(self.poll_interval_s) not in (int, float)
                or not math.isfinite(self.poll_interval_s) or self.poll_interval_s <= 0):
            raise ValueError("poll_interval_s must be finite and positive")

    def limits(self, resources):
        maximum = resources.max_active_batches
        return SessionLimits(
            held_tasks=maximum, input_bytes=maximum * self.item_input_bytes,
            result_bytes=maximum * self.item_result_bytes,
            active_requests=maximum, active_work=maximum, offer_tasks=1,
            item_input_bytes=self.item_input_bytes, item_result_bytes=self.item_result_bytes,
            metadata_bytes=self.metadata_bytes, step_actions=self.step_actions,
            wait_timeout_s=resources.query_timeout_s, poll_interval_s=self.poll_interval_s,
            timeouts=SessionTimeouts(),
        )


def _encode(value, maximum):
    encoded = bytearray()
    encoder = json.JSONEncoder(ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    for piece in encoder.iterencode(value):
        piece = piece.encode("utf-8")
        if len(encoded) + len(piece) > maximum:
            raise ValueError("records bytes exceed the reserved item capacity")
        encoded.extend(piece)
    return bytes(encoded)


def _check(deadline, cancelled=None):
    if cancelled is not None and cancelled():
        raise InterruptedError("Movie Q3 Core records cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("Movie Q3 Core records deadline expired")


def _capacity_failure(value):
    """Keep identity and known usage when a full result cannot fit its reservation."""
    occurrence = identity(value["query_id"], value["row_id"],
                          value["source_position"], value["call_index"])
    return dict(
        occurrence, status="unknown" if value["status"] == "unknown" else "failed",
        text=None, prompt_tokens=value.get("prompt_tokens"),
        completion_tokens=value.get("completion_tokens"), total_tokens=value.get("total_tokens"),
        submission_confirmed=value.get("submission_confirmed", value["status"] == "succeeded"),
        error_type="ResultCapacityExceeded", result_fields_omitted=True,
    )


def _check_result_capacity(occurrence, maximum):
    # Real token counts are lengths of finite Python sequences. Reserve their
    # widest supported representation before allowing this call to be sent.
    value = dict(occurrence, status="unknown", prompt_tokens=(1 << 64) - 1,
                 completion_tokens=(1 << 64) - 1, total_tokens=(1 << 65) - 2)
    _encode(_capacity_failure(value), maximum)


class _CoreModelActor(_ModelActor):
    def run_core(self, rows, deadline, maximum):
        result = self.run(rows, deadline)
        if type(result) is not list or len(result) != 1:
            raise RuntimeError("single-row model completion is unconfirmed")
        value = result[0]
        occurrence = identity(rows[0]["query_id"], rows[0]["row_id"],
                              rows[0]["source_position"], rows[0]["call_index"])
        audit = CallLedger([occurrence], 1)
        audit.record(value)
        # The driver and Core receive only bounded bytes. The small failure
        # representation was checked before dispatch and retains known billing.
        try:
            return _encode(value, maximum)
        except ValueError:
            return _encode(_capacity_failure(value), maximum)


class _ActorOwner(RecordsActorOwner):
    """Reuse only actor allocation and shutdown, with a bounded result method."""

    close_deadline = None

    def _model_actor_type(self):
        return _CoreModelActor

    def wait_ready(self, ref, deadline, cancelled=None):
        if self.close_deadline is not None:
            deadline = min(deadline, self.close_deadline)
        return super().wait_ready(ref, deadline, cancelled)


class _RecordTransport:
    """Bounded Ray I/O and failure receipts; Core remains the sole capacity owner."""

    def __init__(self, actors, maximum, notify):
        self.actors = actors
        self.deadline = None
        self._next_prepare = 0
        self._refs = {}
        self._outcomes = {}
        self._lock = threading.Lock()
        self.backend = BoundedAsyncBackend(
            self._execute, max_tasks=maximum, notify=notify, isolate_failures=True,
        )

    async def _execute(self, task, endpoint):
        _check(self.deadline)
        row = json.loads(task.task.payload)
        actor = self.actors.prepare_actors[self._next_prepare % len(self.actors.prepare_actors)]
        self._next_prepare += 1
        # All potentially blocking Ray calls happen on the transport thread.
        prepared = actor.prepare.remote([row], self.deadline)
        with self._lock:
            self._refs[task.key] = (prepared,)
        result_ref = self.actors.model_actor.run_core.remote(
            prepared, self.deadline, task.task.max_result_bytes,
        )
        with self._lock:
            self._refs[task.key] = (prepared, result_ref)
        result = await asyncio.wrap_future(result_ref.future())
        if type(result) is not bytes or len(result) > task.task.max_result_bytes:
            raise ValueError("model completion exceeded its byte reservation")
        value = json.loads(result)
        expected = {key: row[key] for key in identity(
            row["query_id"], row["row_id"], row["source_position"], row["call_index"],
        )}
        # Association is checked before a successful backend terminal can free input.
        audit = CallLedger([expected], 1)
        audit.record(value)
        with self._lock:
            self._outcomes[task.key] = result
            if value["status"] != "unknown":
                self._refs.pop(task.key, None)
        if value["status"] == "unknown":
            raise RuntimeError("local model work has no confirmed terminal")
        if value["status"] == "failed":
            raise ConfirmedTaskFailure(task.key, {"exception_type": "ModelCallFailed"})
        return result

    def acknowledge(self, key):
        with self._lock:
            self._outcomes.pop(key, None)

    def failure_receipts(self):
        with self._lock:
            return tuple((key, value) for key, value in self._outcomes.items())

    def take_receipts(self, session_id):
        with self._lock:
            keys = tuple(key for key in self._outcomes if key.session_id == session_id)
            return tuple(self._outcomes.pop(key) for key in keys)

    def retained_refs(self):
        with self._lock:
            return len(self._refs)


class _CoreRows(Iterator[dict[str, Any]]):
    def __init__(self, owner, source_rows, expected, query_id, deadline, cancelled):
        self.owner, self.source_rows = owner, iter(source_rows)
        self.expected = {row["call_id"]: row for row in expected}
        self.query_id, self.deadline, self.cancelled = query_id, deadline, cancelled
        self.session = owner.core.open(SessionSpec(
            query_id, "movie-q3-filter", "lotus-records-local-model-v1", work_unit="model_calls",
        ))
        self.received, self.offered = set(), set()
        self.sequence = 0
        self.pending = None
        self.source_eof = self.finished = self.source_closed = False
        self._failure = None
        self.unconsumed_call_results = []
        self.last_close_report = None
        self.peak_usage = asdict(owner.core.capacity.usage())

    def __iter__(self):
        return self

    def _observe(self):
        for name, value in asdict(self.owner.core.capacity.usage()).items():
            self.peak_usage[name] = max(value, self.peak_usage[name])

    def _fill(self):
        for _ in range(self.owner.capacity.step_actions):
            _check(self.deadline, self.cancelled)
            if self.source_eof:
                return
            if self.pending is None:
                if self.owner.core.capacity.usage().held_tasks >= self.session.limits.held_tasks:
                    return
                try:
                    raw = dict(next(self.source_rows))
                except StopIteration:
                    self.source_eof = True
                    self.session.seal()
                    return
                occurrence = identity(self.query_id, raw["row_id"], raw["source_position"])
                call_id = occurrence["call_id"]
                if occurrence != self.expected.get(call_id) or call_id in self.offered:
                    raise ValueError("Core source returned an undeclared or duplicate occurrence")
                if type(raw["reviewText"]) is not str:
                    raise ValueError("Core records reviewText must be text")
                payload = _encode(dict(occurrence, reviewText=raw["reviewText"]),
                                  self.owner.capacity.item_input_bytes)
                work = WorkDescriptor((StageWork("model", 1, "model_calls"),),
                                      "model", "request-count-not-token-cost-v1")
                self.pending = OfferedTask(
                    self.sequence, payload, 1, self.owner.capacity.item_result_bytes,
                    info=TaskInfo(hashlib.sha256(call_id.encode()).hexdigest(),
                                  occurrence["source_position"], "filter", work),
                )
            offered = self.session.offer((self.pending,))
            if offered.status == "REJECTED":
                raise ValueError("Core rejected raw record: " + offered.reason)
            if not offered.accepted_prefix_count:
                return
            self.offered.add(json.loads(self.pending.payload)["call_id"])
            self.sequence += 1
            self.pending = None
            self._observe()

    def _value(self, payload):
        value = json.loads(payload)
        expected = self.expected.get(value.get("call_id"))
        if expected is None or value["call_id"] in self.received:
            raise ValueError("Core returned an undeclared or duplicate call")
        audit = CallLedger([expected], 1)
        audit.record(value)
        self.received.add(value["call_id"])
        return value

    def __next__(self):
        self.owner._control()
        if self.finished:
            raise StopIteration
        try:
            while True:
                if self._failure is not None:
                    # Receipts from a failed tick retain known usage independently
                    # of Core's rule to discard unpublished success deliveries.
                    for key, payload in self.owner.transport.failure_receipts():
                        self.owner.transport.acknowledge(key)
                        if json.loads(payload).get("call_id") not in self.received:
                            return self._value(payload)
                    raise self._failure
                _check(self.deadline, self.cancelled)
                self._fill()
                _check(self.deadline, self.cancelled)
                progress = self.session.advance(1)
                self._observe()
                if progress.state in (State.FAILED, State.CANCELLED):
                    self._failure = RuntimeError("Core records execution failed: " + str(progress.error))
                    continue
                if progress.deliveries:
                    delivery = progress.deliveries[0]
                    try:
                        value = self._value(delivery.result)
                        self.owner.transport.acknowledge(delivery.key)
                    finally:
                        self.session.release((delivery.lease_id,))
                    return value
                if progress.state == State.FINISHED:
                    if self.received != set(self.expected):
                        raise ValueError("Core records omitted expected calls")
                    report = self.close()
                    if report["cleanup_errors"]:
                        raise RuntimeError("Core source cleanup is unconfirmed")
                    raise StopIteration
                if not progress.has_immediate_work:
                    wait = min(self.owner.capacity.poll_interval_s,
                               max(0, self.deadline - time.monotonic()))
                    if progress.next_deadline is not None:
                        wait = min(wait, max(0, progress.next_deadline - time.monotonic()))
                    self.owner.core.wake.wait(progress.generation, wait)
        except StopIteration:
            raise
        except BaseException as error:
            report = self.close()
            if report["status"] != "closed":
                error.add_note("Core query cleanup remains unconfirmed")
            raise

    def close(self):
        self.owner._control()
        self.finished = True
        self.pending = None
        errors = []
        if not self.source_closed:
            try:
                close = getattr(self.source_rows, "close", None)
                if close is not None:
                    close()
                self.source_closed = True
            except BaseException as error:
                errors.append("source_iterator_close: " + type(error).__name__)
        core_report = self.session.close()
        self.owner.core.reap(self.owner.capacity.step_actions)
        self.collect_receipts()
        usage = self.owner.core.capacity.usage()
        self.last_close_report = dict(
            status="closed" if self.source_closed and not usage.held_tasks and not errors else "unknown",
            cleanup_scope="core_query_input_and_results", source_closed=self.source_closed,
            received_occurrences=len(self.received), expected_occurrences=len(self.expected),
            core_close=asdict(core_report), core_usage=asdict(usage), peak_core_usage=self.peak_usage,
            cleanup_errors=errors, unconsumed_call_results=list(self.unconsumed_call_results),
        )
        self.owner.last_query_report = self.last_close_report
        return self.last_close_report

    def collect_receipts(self):
        for payload in self.owner.transport.take_receipts(self.session.session_id):
            row = json.loads(payload)
            if row["call_id"] not in self.received:
                self.unconsumed_call_results.append(row)


class SemLoomRecordsCore:
    """One single-control-thread Core, one local model owner, consecutive queries."""

    def __init__(self, method: FilterMethod, engine: EngineSpec, generation: GenerationSpec,
                 resources: RecordsResources, *, capacity: CoreRecordsCapacity | None = None):
        self.method = FilterMethod.from_dict(asdict(method))
        self.engine = EngineSpec.from_dict(asdict(engine))
        self.generation = GenerationSpec.from_dict(asdict(generation))
        self.resources = RecordsResources(**asdict(resources))
        if resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1:
            raise ValueError("Core records currently requires prepare_batch_rows=model_batch_rows=1")
        self.capacity = CoreRecordsCapacity() if capacity is None else capacity
        if type(self.capacity) is not CoreRecordsCapacity:
            raise TypeError("Core records capacity must be CoreRecordsCapacity")
        self._thread = threading.get_ident()
        self._actors = _ActorOwner(self.method, self.engine, self.generation, self.resources)
        self.core = self.transport = self._active = None
        self._closing = False
        self._deadline = None
        self.last_query_report = self.last_close_report = None

    def _control(self):
        if threading.get_ident() != self._thread:
            raise RuntimeError("Core records owner requires its single control thread")

    def evaluate(self, source, *, query_id, cancelled: Callable[[], bool] | None = None):
        self._control()
        if self._closing or (self.core is not None and
                             (self.core.error or self.core.capacity.usage().held_tasks)):
            raise RuntimeError("Core records has closing or unconfirmed model ownership")
        if self._active is not None and not self._active.finished:
            raise RuntimeError("Core records already owns an active query")
        if self._active is not None:
            # A cancelled flow can settle after its iterator closes. Preserve
            # its compact receipts before a new query replaces that flow.
            self._active.close()
        deadline = time.monotonic() + self.resources.query_timeout_s
        self._deadline = deadline
        from src.semantic_methods.lotus.engine import validate_filter_source, validate_generation
        source.preflight(self.method)
        validate_generation(self.method, self.generation)
        validate_filter_source()
        self.engine.validate_driver_environment()
        identity(query_id, "preflight", 0)
        expected = source.expected(query_id)
        CallLedger(expected, max(1, len(expected)))
        for occurrence in expected:
            _check_result_capacity(occurrence, self.capacity.item_result_bytes)
        # Core's fixed-size session identity is checked before actors are allocated.
        spec = SessionSpec(query_id, "movie-q3-filter", "lotus-records-local-model-v1")
        _check(deadline, cancelled)
        import daft
        import ray
        from daft.runners import _get_runner, get_or_create_runner
        from src.infrastructure.daft_runtime import configure_daft_runner
        if ray.__version__ != "2.56.1" or not ray.is_initialized():
            raise RuntimeError("Core records requires caller-owned Ray 2.56.1")
        if daft.__version__ != "0.7.21":
            raise RuntimeError("Core records requires Daft 0.7.21 Native")
        if _get_runner() is None:
            if os.environ.get("DAFT_RUNNER") not in (None, "native"):
                raise RuntimeError("Core records requires an explicit Daft Native configuration")
            configure_daft_runner("native")
        if get_or_create_runner().name != "native":
            raise RuntimeError("Core records requires Daft 0.7.21 Native")
        frame = daft.read_csv(str(source.csv_path), infer_schema=False, schema={
            "row_id": daft.DataType.string(), "source_position": daft.DataType.int64(),
            "reviewId": daft.DataType.string(), "id": daft.DataType.string(),
            "reviewText": daft.DataType.string(),
        }).filter(daft.col("id") == "taken_3").select("row_id", "source_position", "reviewText")
        try:
            if expected:
                self._actors.start(min(deadline, time.monotonic() + self.resources.startup_timeout_s),
                                   cancelled)
            else:
                self._actors.ray = ray
            if self.core is None:
                limits = self.capacity.limits(self.resources)
                endpoint = EndpointSnapshot("local-model", "local://model", "local", "0",
                                            True, 0, 0, None, time.monotonic())
                policies = SessionPolicies(StaticAdmissionController(limits.active_requests),
                                           RoundRobinEndpointRouter(),
                                           TopologySnapshot((endpoint,), time.monotonic()), "local")
                self.transport = _RecordTransport(self._actors, limits.held_tasks, lambda: self.core.wake.notify())
                self.core = SessionEngine(limits, self.transport.backend, policies)
            self.transport.deadline = deadline
            self._active = _CoreRows(self, frame.iter_rows(results_buffer_size=1), expected,
                                     spec.job_id, deadline, cancelled)
        except BaseException as error:
            error.add_note("Core startup cleanup: " + str(self.close()))
            raise
        return self._active

    def close(self, *, timeout=None):
        self._control()
        self._closing = True
        errors = []
        if self._active is not None:
            try:
                errors.extend(self._active.close()["cleanup_errors"])
            except BaseException as error:
                errors.append("core_query_close: " + type(error).__name__)
        # The first closure shares the query's absolute deadline. An explicit
        # subsequent timeout grants only a new cleanup attempt, never new work.
        if timeout is not None:
            import math
            if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("cleanup timeout must be finite and positive")
            self._actors.close_deadline = time.monotonic() + timeout
        else:
            self._actors.close_deadline = self._deadline
        actor_report = self._actors.close()
        errors.extend(actor_report["cleanup_errors"])
        if self.core is not None:
            self.core.reap(self.capacity.step_actions)
        if self._active is not None:
            self._active.collect_receipts()
            if self.last_query_report is not None:
                self.last_query_report["unconsumed_call_results"] = list(self._active.unconsumed_call_results)
                self.last_query_report["core_usage"] = asdict(self.core.capacity.usage())
        transport_closed = True
        if self.transport is not None:
            transport_closed = self.transport.backend.close(timeout=0.05)
        usage = None if self.core is None else asdict(self.core.capacity.usage())
        self.last_close_report = dict(
            implementation="semloom_records_core", production_core_connected=self.core is not None,
            status="closed" if actor_report["status"] == "closed" and transport_closed
            and (usage is None or not usage["held_tasks"]) and not errors else "unknown",
            cleanup_scope="core_transport_actor_and_engine_api",
            model_owner=actor_report, gpu_exit_confirmed=actor_report["gpu_exit_confirmed"],
            core_usage=usage, transport_closed=transport_closed,
            retained_remote_inputs=0 if self.transport is None else self.transport.retained_refs(),
            last_query_report=self.last_query_report, cleanup_errors=errors,
            work_unit="model_calls", model_batch_mode="finite_synchronous_single_row",
            native_engine_abort_supported=False,
        )
        return self.last_close_report
