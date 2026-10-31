"""LOTUS text Filter records through the existing incremental production Core.

Only raw, bounded record bytes enter Core. Ray submission and waiting run in
the existing asynchronous transport; prepared ObjectRefs go to the model actor.
The synchronous model kernel and its actor owner are shared with the static
reference. This module never calls that reference's iterator or scheduler.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass, replace
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
    OfferedTask, SessionLimits, SessionSpec, SessionTimeouts, State, TaskInfo, TaskKey,
)
from src.scheduling.core.session_policy import SessionPolicies
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.runtime.async_backend import BoundedAsyncBackend, ConfirmedTaskFailure
from src.scheduling.submission_control.admission import StaticAdmissionController, DynamicAdmissionGate
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordScan, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS

from .lotus_records_actor_owner import RecordsActorOwner, _ModelActor
from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig, ResourceFeedbackController
from src.scheduling.runtime.observations import ServiceMetricsSnapshot, NonBlockingMetricsObservationProvider


def expand_core_profile(profile):
    """Retain named legacy experiment settings; ordinary defaults stay fixed."""
    if type(profile) is not str:
        return profile
    if profile not in ("delivered-work-scan", "computed-work-local-pairs"):
        raise ValueError("unknown Core query feedback profile")
    admission = dict(kind="resource_feedback", search_mode="geometric",
        poll_interval_s=0.1, window_s=0.25, min_samples=16, min_remaining=64,
        exploration_s=10.0, max_changes=8, allow_exploration=True,
        min_gain=0.03, work_tolerance=0.03, max_candidate_attempts=2,
        memory_floor_bytes=0)
    if profile == "computed-work-local-pairs":
        admission.update(work_basis="computed", work_comparison="local_pairs")
    return dict(mode="async", max_inflight=8, stage_timing=True, admission=admission)


def resolve_core_profile(engine, resources, profile=None, *, rolling_refill=False):
    """Resolve an explicit execution profile, without probing or allocating hardware.

    Auto is a bound on caller-declared capacity, not a throughput tuner. Device
    allocation and machine preflight remain the caller's existing responsibility.
    """
    profile = {} if profile is None else expand_core_profile(profile)
    if type(profile) is not dict or set(profile) - {"mode", "max_inflight", "stage_timing", "admission"}:
        raise ValueError("Core profile accepts mode, max_inflight, stage_timing and optional admission only")
    mode = profile.get("mode", "sync")
    request_feedback = rolling_refill and profile.get('admission') is not None
    requested = profile.get("max_inflight", 1 if request_feedback else resources.max_active_batches)
    timing = profile.get("stage_timing", False)
    if mode not in ("sync", "async") or type(timing) is not bool:
        raise ValueError("Core requires sync/async mode and boolean stage_timing")
    if requested == "auto":
        if mode != "async":
            raise ValueError("automatic in-flight capacity requires the existing async owner")
        maximum = resources.max_active_batches
    elif type(requested) is not int or requested < 1 or requested > resources.max_active_batches:
        raise ValueError("Core in-flight limit exceeds its declared request capacity")
    else:
        maximum = requested
    result = dict(requested=dict(profile), mode=mode, max_inflight=maximum, stage_timing=timing,
                capacity_source="Core request budget"
                if requested == "auto" else "explicit Core request budget",
                declared_request_budget=resources.max_active_batches,
                engine_max_num_seqs=engine.max_num_seqs,
                model_batch_rows=1,
                model_batch_mode="independent_final_results" if mode == "async" else "finite_synchronous_single_row",
                performance_tuned=False)
    admission = profile.get("admission")
    if admission is not None:
        if mode != "async" or type(admission) is not dict or admission.get("kind") != "resource_feedback":
            raise ValueError("resource feedback requires explicit async Core admission")
        gpu_ids = admission.get("gpu_ids", ())
        if not isinstance(gpu_ids, (list, tuple)) or any(not isinstance(v, str) or not v.startswith("GPU-") for v in gpu_ids):
            raise ValueError("feedback GPU IDs must be physical UUIDs when supplied")
        options = {k: v for k, v in admission.items() if k not in ("kind", "gpu_ids")}
        if request_feedback:
            from src.scheduling.submission_control.stage_work import default_supply_feedback_options
            if gpu_ids:
                raise ValueError('rolling request feedback uses native stats; physical GPU UUID sampling is not connected')
            if maximum != 1:
                raise ValueError('rolling request feedback starts at one without a historical search hint')
            config = replace(default_supply_feedback_options(), **options)
            if config.poll_interval_s > config.window_s:
                raise ValueError('request feedback poll interval must fit the measurement window')
        else:
            config = ResourceFeedbackConfig(**options)
        result.update(owner_max_inflight=resources.max_active_batches,
            initial_window=maximum, admission=dict(kind="resource_feedback", gpu_ids=list(gpu_ids), **asdict(config)))
    return result


def resolve_record_batching(resolved, prepared_batching=None, batch_concurrency=None, batch_feedback=None,
                            rolling_refill=False):
    """Validate the optional fixed token batch path before allocating an owner."""
    if type(rolling_refill) is not bool:
        raise ValueError('rolling_refill must be boolean')
    if prepared_batching is None:
        if batch_concurrency is not None or batch_feedback is not None or rolling_refill:
            raise ValueError('batch controls require prepared_batching')
        return None
    from src.semantic_methods.lotus.prepared_batching import PreparedBatching
    if (type(prepared_batching) is not PreparedBatching or prepared_batching.policy != 'token_budget'
            or prepared_batching.max_rows is not None):
        raise ValueError('record batching requires token_budget with max_rows=None')
    if resolved['mode'] != 'async' or ('admission' in resolved and not rolling_refill) or resolved['stage_timing']:
        raise ValueError('record batching requires async execution; admission requires rolling_refill, stage_timing is unsupported')
    requests = resolved.get('owner_max_inflight', resolved['max_inflight'])
    if rolling_refill and (batch_concurrency is not None or batch_feedback is not None):
        raise ValueError('rolling_refill uses request R, not batch_concurrency or batch_feedback')
    concurrency = None if rolling_refill else 1 if batch_concurrency is None else batch_concurrency
    if not rolling_refill and (type(concurrency) is not int or not 1 <= concurrency <= requests):
        raise ValueError('batch_concurrency must fit the request grant R')
    feedback = {}
    if batch_feedback is not None:
        from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
        if (type(batch_feedback) is not SupplyFeedbackConfig or concurrency != 1
                or batch_feedback.max_batches > requests):
            raise ValueError('batch feedback requires SupplyFeedbackConfig, initial one and a ceiling within R')
        feedback['feedback'] = asdict(batch_feedback)
    if rolling_refill:
        feedback.update(rolling_refill=True, C_request=resolved['max_inflight'], admission_unit='single_model_requests')
        if 'admission' in resolved:
            if resolved['max_inflight'] != 1:
                raise ValueError('rolling request feedback starts at one')
            feedback['request_feedback'] = {k: v for k, v in resolved['admission'].items()
                                            if k not in ('kind', 'gpu_ids')}
    return dict(prepared_batching=asdict(prepared_batching), batch_concurrency=concurrency,
        request_limit=requests, B=prepared_batching.token_budget, C_batch=concurrency, R=requests,
        row_cap=None, W=None, batch_unit='application transport batch with unsettled compute members',
        request_unit=('single complete model request; soft C_request, independent resident R'
                      if 'request_feedback' in feedback else
                      'single complete model request; also the inherited resident-row limit'), **feedback)


def create_records_core(method, engine, generation, resources, *, profile=None, async_limits=None,
                        prepared_batching=None, batch_concurrency=None, batch_feedback=None, rolling_refill=False):
    """Select existing ownership; both Movie lifecycles use this same factory."""
    resolved = resolve_core_profile(engine, resources, profile, rolling_refill=rolling_refill)
    batching = resolve_record_batching(resolved, prepared_batching, batch_concurrency, batch_feedback, rolling_refill)
    effective = replace(resources, max_active_batches=resolved.get("owner_max_inflight", resolved["max_inflight"]))
    if batching is not None:
        from .lotus_records_batches import PreparedRecordsCore
        return PreparedRecordsCore(method, engine, generation, effective, resolved, batching, async_limits)
    options = {}
    actors = None
    if resolved["mode"] == "async":
        from .lotus_records_async_actor import AsyncRecordsActorOwner
        actors = options["actor_owner"] = AsyncRecordsActorOwner(
            method, engine, generation, effective, limits=async_limits,
        )
    elif async_limits is not None:
        raise ValueError("async_limits requires async Core execution")
    if "admission" in resolved:
        options["admission"] = resolved["admission"]
    if resolved["stage_timing"]:
        options["stage_timing"] = True
    try:
        owner = SemLoomRecordsCore(method, engine, generation, effective, **options)
    except BaseException as error:
        if actors is not None:
            try:
                actors.close()
            except BaseException as cleanup:
                error.add_note("unadopted async actor close also failed: " + type(cleanup).__name__)
        raise
    owner.execution_profile = resolved
    return owner


class _StageTimings:
    """Bounded aggregate host intervals, never an inference or GPU profiler."""

    def __init__(self):
        self.lock = threading.Lock()
        self.values = {}
        self.errors = 0

    @contextmanager
    def measure(self, name):
        started = None
        try:
            started = time.monotonic_ns()
        except Exception:
            self.errors += 1
        try:
            yield
        finally:
            if started is not None:
                try:
                    elapsed = time.monotonic_ns() - started
                    with self.lock:
                        value = self.values.setdefault(name, dict(count=0, total_ns=0, max_ns=0))
                        value["count"] += 1
                        value["total_ns"] += elapsed
                        value["max_ns"] = max(value["max_ns"], elapsed)
                except Exception:
                    self.errors += 1

    def snapshot(self):
        with self.lock:
            return dict(scope="host intervals; concurrent totals overlap and are not query wall time",
                        stages={name: dict(value) for name, value in self.values.items()},
                        timing_errors=self.errors, prepare_worker_compute_ns=None,
                        wait_scope="preparation dependency, model actor/IPC queue and model completion",
                        pure_GPU_compute_ns=None)


def _measure(timings, name):
    return nullcontext() if timings is None else timings.measure(name)


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
        raise InterruptedError("LOTUS Core records cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("LOTUS Core records deadline expired")


def _capacity_failure(value):
    """Keep identity and known usage when a full result cannot fit its reservation."""
    occurrence = {name:value[name] for name in IDENTITY_FIELDS}
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

    def __init__(self, actors, maximum, notify, *, finalize=None):
        self.actors = actors
        self.maximum = maximum
        self.deadline = None
        self._next_prepare = 0
        self._refs = {}
        self._outcomes = {}
        self._requests = {}
        self._consumed_receipts = set()
        self._lock = threading.Lock()
        self.timings = None
        self._query_binding = None
        self._retirement = None
        self.backend = BoundedAsyncBackend(
            self._execute, max_tasks=maximum, notify=notify, isolate_failures=True,
            cancel_pending=actors.cancel_core, finalize=finalize,
        )

    def bind_query(self, session_id, prepare_method):
        # Query methods are immutable; an executing coroutine captures its own
        # binding before awaiting preparation/model completion.
        self._query_binding = session_id, prepare_method

    def _submit_task(self, task, timings, prepare_method):
        row = json.loads(task.task.payload)
        expected = {key: row[key] for key in identity(
            row["query_id"], row["row_id"], row["source_position"], row["call_index"],
        )}
        with self._lock:
            self._requests[task.key] = expected, task.task.max_result_bytes
        actor = self.actors.prepare_actors[self._next_prepare % len(self.actors.prepare_actors)]
        self._next_prepare += 1
        # All potentially blocking Ray calls happen on the transport thread.
        with _measure(timings, "prepare_submit"):
            prepared = (actor.prepare.remote([row], self.deadline) if prepare_method is None
                        else actor.prepare.remote([row], self.deadline, asdict(prepare_method)))
        with self._lock:
            self._refs[task.key] = (prepared,)
        with _measure(timings, "model_submit"):
            result_ref = self.actors.submit_core(
                prepared, self.deadline, task.task.max_result_bytes, task.key,
            )
        with self._lock:
            self._refs[task.key] = (prepared, result_ref)
        return result_ref

    def _audit_result(self, expected, value):
        CallLedger([expected], 1).record(value)

    def _result_transferred(self, task, value):
        """Optional adapter sink, after retention and before the actor ack."""

    async def _execute(self, task, endpoint):
        timings = self.timings  # Keep late work attached to its original query.
        binding = self._query_binding
        if binding is not None and binding[0] != task.key.session_id:
            raise ValueError("Core task belongs to another query method binding")
        prepare_method = None if binding is None else binding[1]
        _check(self.deadline)
        result_ref = self._submit_task(task, timings, prepare_method)
        with self._lock:
            expected = self._requests[task.key][0]
        with _measure(timings, "dependency_and_model_wait"):
            result = await asyncio.wrap_future(result_ref.future())
        with _measure(timings, "transport_result_processing"):
            if type(result) is not bytes or len(result) > task.task.max_result_bytes:
                raise ValueError("model completion exceeded its byte reservation")
            value = json.loads(result)
            # Association is checked before a successful backend terminal can free input.
            self._audit_result(expected, value)
            with self._lock:
                self._outcomes[task.key] = result
                if value["status"] != "unknown":
                    self._refs.pop(task.key, None)
            self._result_transferred(task, value)
            # Transfer before the actor may discard its copy; unknown still owns refs.
            self.actors.acknowledge_core(task.key)
        if value["status"] == "unknown":
            raise RuntimeError("local model work has no confirmed terminal")
        if value["status"] == "failed":
            raise ConfirmedTaskFailure(task.key, {"exception_type": "ModelCallFailed"})
        return result

    def acknowledge(self, key):
        with self._lock:
            self._outcomes.pop(key, None)
            if key not in self._refs:
                self._requests.pop(key, None)
            else:
                self._consumed_receipts.add(key)

    def recover_actor_receipts(self, receipts):
        """Transfer only registered task receipts; this never ends Core borrowing."""
        if type(receipts) is not list:
            return ["actor_close_receipts: TypeError"]
        errors = []
        for receipt in receipts:
            try:
                pair = receipt["request_key"]
                if (type(pair) not in (list, tuple) or len(pair) != 2
                        or any(type(item) is not int or not 0 <= item < 1 << 64 for item in pair)):
                    raise ValueError("actor close receipt requires a complete task key")
                key = TaskKey(*pair)
                with self._lock:
                    request = None if key in self._consumed_receipts else self._requests.get(key)
                # Already consumed/foreign sessions are not inferred from query_id.
                if request is None:
                    continue
                expected, maximum = request
                value = receipt["result"]
                self._audit_result(expected, value)
                try:
                    encoded = _encode(value, maximum)
                except ValueError:
                    encoded = _encode(_capacity_failure(value), maximum)
                with self._lock:
                    if key not in self._requests or key in self._consumed_receipts:
                        continue
                    previous = self._outcomes.get(key)
                    if previous is not None and json.loads(previous) != json.loads(encoded):
                        raise ValueError("actor close receipt conflicts with an existing outcome")
                    self._outcomes[key] = encoded
            except BaseException as error:
                errors.append("actor_close_receipt: " + type(error).__name__)
        return errors

    def failure_receipts(self):
        with self._lock:
            return tuple((key, value) for key, value in self._outcomes.items())

    def take_receipts(self, session_id):
        with self._lock:
            keys = tuple(key for key in self._outcomes if key.session_id == session_id)
            for key in keys:
                if key not in self._refs:
                    self._requests.pop(key, None)
                else:
                    self._consumed_receipts.add(key)
            return tuple(self._outcomes.pop(key) for key in keys)

    def retire_producer(self, proof, keys):
        """Release only exact registered borrowing; call receipts remain unchanged."""
        if self._retirement is not None:
            return self._retirement
        ready = self.actors.model_ready or {}
        if (type(proof) is not dict or proof.get("schema") != "semloom.async_actor_quiescence.v1"
                or proof.get("confirmed") is not True or proof.get("input_gate_closed") is not True
                or proof.get("active_readers") != 0):
            raise ValueError("actor producer quiescence is unconfirmed")
        ipc = proof.get("ipc")
        if (type(ipc) is not dict or ipc.get("schema") != "semloom.async_producer_quiescence.v1"
                or ipc.get("confirmed") is not True
                or type(ready.get("producer_owner_uid")) is not str
                or ipc.get("owner_uid") != ready["producer_owner_uid"]
                or ipc.get("engine_uid") != ready.get("engine_uid")):
            raise ValueError("producer quiescence changed model ownership")
        requests = proof.get("requests")
        if type(requests) is not list or len(requests) > self.maximum:
            raise ValueError("producer quiescence request set is invalid")
        certified = {}
        for item in requests:
            pair = item.get("request_key") if type(item) is dict else None
            if (type(pair) is not list or len(pair) != 2
                    or any(type(value) is not int or not 0 <= value < 1 << 64 for value in pair)):
                raise ValueError("producer quiescence requires full task keys")
            key = TaskKey(*pair)
            if key in certified:
                raise ValueError("duplicate producer retirement key")
            certified[key] = item
        with self._lock:
            if any(key not in certified or key not in self._requests
                    or certified[key].get("call_id") != self._requests[key][0]["call_id"] for key in keys):
                raise ValueError("producer quiescence omits or changes owned borrowing")
        retired = self.backend.retire_producer(keys)
        with self._lock:
            for key in keys:
                self._refs.pop(key, None)
                self._consumed_receipts.discard(key)
                if key not in self._outcomes:
                    self._requests.pop(key, None)
        self._retirement = dict(confirmed=True, scope="stopped model producer and local I/O borrowing",
            request_keys=[[key.session_id, key.sequence] for key in keys], retired_backend_slots=retired,
            producer_quiescence=proof, outcome_confirmation=False)
        return self._retirement

    def retained_refs(self):
        with self._lock:
            return len(self._refs)


class _CoreRows(Iterator[dict[str, Any]]):
    def __init__(self, owner, source_rows, expected, query_id, deadline, cancelled,
                 prepare_method=None, session_limits=None, execution_profile=None):
        self.owner, self.source_rows = owner, iter(source_rows)
        self.expected = {row["call_id"]: row for row in expected}
        self.query_id, self.deadline, self.cancelled = query_id, deadline, cancelled
        self.session = owner.core.open(SessionSpec(
            query_id, "lotus-records-filter", "lotus-records-local-model-v1", work_unit="model_calls",
        ), limits=session_limits)
        self._execution_profile = dict(owner.execution_profile if execution_profile is None else execution_profile)
        owner.transport.bind_query(self.session.session_id, prepare_method)
        self.received, self.offered = set(), set()
        self.completed_work = (0, 0, 0)
        self.completed_effective_work = (0, 0, 0)
        self.sequence = 0
        self.pending = None
        self.source_eof = self.finished = self.source_closed = False
        self._failure = None
        self.unconsumed_call_results = []
        self.last_close_report = None
        self.peak_usage = asdict(owner.core.capacity.usage())
        self.timings = owner.transport.timings
        if owner._feedback is not None:
            owner._feedback.stream = self

    def __iter__(self):
        return self

    @property
    def production_core_connected(self):
        return isinstance(self.owner.core, SessionEngine)

    @property
    def execution_profile(self):
        return dict(self._execution_profile)

    def _observe(self):
        if self.owner._feedback is not None:
            with _measure(self.timings, "feedback_progress"):
                self.owner._feedback.observe(self)
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
                if any(type(value) is not str for name, value in raw.items()
                       if name not in ("row_id", "source_position")):
                    raise ValueError("Core records payload columns must be text")
                payload = _encode(dict(raw, **occurrence),
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
        _, input_work, output_work = self.completed_work
        if (value["status"] != "succeeded" or value.get("prompt_tokens") is None
                or value.get("completion_tokens") is None or input_work is None):
            input_work = output_work = None
        else:
            input_work += value["prompt_tokens"]
            output_work += value["completion_tokens"]
        self.completed_work = (len(self.received) + 1, input_work, output_work)
        if self.owner._feedback is not None and self.owner._feedback.options.work_basis == "computed":
            _, computed, cached = self.completed_effective_work
            new_computed, new_cached = value.get("prompt_computed_tokens"), value.get("prompt_cached_tokens")
            valid = (value["status"] == "succeeded" and computed is not None
                and type(new_computed) is int and type(new_cached) is int
                and min(new_computed, new_cached) >= 0
                and new_computed + new_cached == value.get("prompt_tokens"))
            self.completed_effective_work = (len(self.received) + 1,
                computed + new_computed if valid else None, cached + new_cached if valid else None)
        self.received.add(value["call_id"])
        if self.owner._feedback is not None:
            self.owner._feedback.observe(self)
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
                with _measure(self.timings, "source_offer"):
                    self._fill()
                _check(self.deadline, self.cancelled)
                with _measure(self.timings, "control_advance"):
                    progress = self.session.advance(1)
                self._observe()
                if progress.state in (State.FAILED, State.CANCELLED):
                    self._failure = RuntimeError("Core records execution failed: " + str(progress.error))
                    continue
                if progress.deliveries:
                    delivery = progress.deliveries[0]
                    try:
                        with _measure(self.timings, "delivery_processing"):
                            value = self._value(delivery.result)
                            self.owner.transport.acknowledge(delivery.key)
                    finally:
                        with _measure(self.timings, "lease_release"):
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
                    with _measure(self.timings, "control_wait"):
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
        if self.timings is not None:
            self.last_close_report["stage_timings"] = self.timings.snapshot()
        if self.owner._feedback is not None:
            self.last_close_report["admission_feedback"] = self.owner._feedback.finish(self)
        self.owner.last_query_report = self.last_close_report
        return self.last_close_report

    def collect_receipts(self):
        for payload in self.owner.transport.take_receipts(self.session.session_id):
            row = json.loads(payload)
            if row["call_id"] not in self.received and row not in self.unconsumed_call_results:
                self.unconsumed_call_results.append(row)


def close_prepared_core(actors, transport, core, capacity, *, deadline, timeout=None):
    """Stop the existing producer, recover receipts, then retire proved borrowing.

    The caller first closes its source/MethodDriver consumer. A new timeout
    permits cleanup only; unknown results are never upgraded by retirement.
    """
    errors = []
    # The first closure shares the query's absolute deadline. An explicit
    # subsequent timeout grants only a new cleanup attempt, never new work.
    if timeout is not None:
        import math
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("cleanup timeout must be finite and positive")
        actors.close_deadline = time.monotonic() + timeout
    else:
        actors.close_deadline = deadline
    actor_report = actors.close()
    errors.extend(actor_report["cleanup_errors"])
    if transport is not None:
        errors.extend(transport.recover_actor_receipts(
            actor_report.get("unconsumed_core_results", [])))
    if core is not None:
        core.reap(capacity.step_actions)
    transport_closed = True
    retirement = None
    if transport is not None:
        transport_closed = (transport.backend.close(timeout=0.05, wait_pending=True)
                            if getattr(transport, 'supports_batch_submit', False)
                            else transport.backend.close(timeout=0.05))
        proof = actor_report.get("producer_quiescence")
        if transport_closed and proof is not None:
            try:
                core.reap(capacity.step_actions)
                keys = tuple(record.key for record in core.capacity.records.values() if record.compute)
                retirement = transport.retire_producer(proof, keys)
                for _ in range(1 + len(keys) // capacity.step_actions):
                    core.reap(capacity.step_actions)
            except Exception as error:
                errors.append("producer_retirement: " + type(error).__name__)
    return dict(model_owner=actor_report, transport_closed=transport_closed,
                producer_retirement=retirement, cleanup_errors=errors)


class SemLoomRecordsCore:
    """One model owner, with the constructor method as each query's default."""

    def __init__(self, method: FilterMethod, engine: EngineSpec, generation: GenerationSpec,
                 resources: RecordsResources, *, capacity: CoreRecordsCapacity | None = None,
                 actor_owner: RecordsActorOwner | None = None, stage_timing: bool = False, admission=None):
        self.method = FilterMethod.from_dict(asdict(method))
        self.engine = EngineSpec.from_dict(asdict(engine))
        self.generation = GenerationSpec.from_dict(asdict(generation))
        self.resources = RecordsResources(**asdict(resources))
        if type(stage_timing) is not bool:
            raise ValueError("stage_timing must be boolean")
        self.stage_timing = stage_timing
        if resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1:
            raise ValueError("Core records currently requires prepare_batch_rows=model_batch_rows=1")
        self.capacity = CoreRecordsCapacity() if capacity is None else capacity
        if type(self.capacity) is not CoreRecordsCapacity:
            raise TypeError("Core records capacity must be CoreRecordsCapacity")
        if actor_owner is not None:
            if not isinstance(actor_owner, RecordsActorOwner):
                raise TypeError("actor_owner must be RecordsActorOwner")
            for name in ("method", "engine", "generation", "resources"):
                if getattr(actor_owner, name) != getattr(self, name):
                    raise ValueError("actor_owner " + name + " differs from the Core configuration")
        self._thread = threading.get_ident()
        self._actors = (actor_owner if actor_owner is not None else
                        _ActorOwner(self.method, self.engine, self.generation, self.resources))
        self.execution_profile = resolve_core_profile(
            self.engine, self.resources, dict(stage_timing=stage_timing))
        actual_mode = self._actors.execution_metadata()["model_batch_mode"]
        self.execution_profile.update(
            mode="async" if actual_mode == "independent_final_results" else "sync",
            model_batch_mode=actual_mode)
        self._feedback = None
        if admission is not None:
            configured = resolve_core_profile(self.engine, self.resources,
                dict(mode=self.execution_profile["mode"], admission=admission))
            self._actors.limits = replace(self._actors.limits,
                feedback_interval_s=configured["admission"]["poll_interval_s"])
            self._feedback = _CoreFeedback(self, configured["admission"])
            self.execution_profile.update(configured)
        self.core = self.transport = self._active = None
        self._closing = False
        self._deadline = None
        self.last_query_report = self.last_close_report = None

    @property
    def production_core_connected(self):
        return isinstance(self.core, SessionEngine)

    def execution_metadata(self):
        return dict(self._actors.execution_metadata(), execution_profile=dict(self.execution_profile))

    def _control(self):
        if threading.get_ident() != self._thread:
            raise RuntimeError("Core records owner requires its single control thread")

    def start(self):
        """Load the existing model owner without reading or executing a query."""
        self._control()
        if self._closing or self.core is not None and (
                self.core.error or self.core.capacity.usage().held_tasks):
            raise RuntimeError("Core records has closing or unconfirmed model ownership")
        if self._active is not None and not self._active.finished:
            raise RuntimeError("Core records already owns an active query")
        deadline = time.monotonic() + self.resources.startup_timeout_s
        self._deadline = deadline
        return self._actors.start(deadline)

    def evaluate(self, source, *, query_id, method: FilterMethod | None = None,
                 cancelled: Callable[[], bool] | None = None, max_inflight: int | None = None,
                 allow_exploration: bool | None = None, search_hint: dict | None = None):
        self._control()
        if self._closing or (self.core is not None and
                             (self.core.error or self.core.capacity.usage().held_tasks)):
            raise RuntimeError("Core records has closing or unconfirmed model ownership")
        if self._active is not None and not self._active.finished:
            raise RuntimeError("Core records already owns an active query")
        query_limits, query_profile = None, dict(self.execution_profile)
        if search_hint is not None and (self._feedback is None or
                self._feedback.base_options.search_mode != "geometric"):
            raise ValueError("search_hint requires geometric resource feedback")
        if allow_exploration is not None:
            if self._feedback is None or type(allow_exploration) is not bool:
                raise ValueError("per-query exploration requires resource feedback and a boolean")
            query_profile["admission"] = dict(query_profile["admission"], allow_exploration=allow_exploration)
        if max_inflight is not None:
            if (query_profile["mode"] != "async" or type(max_inflight) is not int
                    or not 1 <= max_inflight <= query_profile.get("owner_max_inflight", query_profile["max_inflight"])):
                raise ValueError("query in-flight must fit the existing async model owner")
            if self._feedback is None:
                query_limits = self.capacity.limits(replace(self.resources, max_active_batches=max_inflight))
            query_profile.update(owner_max_inflight=query_profile.get("owner_max_inflight", query_profile["max_inflight"]),
                max_inflight=max_inflight, capacity_source="per-query initial soft window" if self._feedback is not None
                else "per-query limit within existing owner; model capacity unchanged")
            if self._feedback is not None:
                query_profile["initial_window"] = max_inflight
        if self._active is not None:
            # A cancelled flow can settle after its iterator closes. Preserve
            # its compact receipts before a new query replaces that flow.
            self._active.close()
        deadline = time.monotonic() + self.resources.query_timeout_s
        self._deadline = deadline
        from src.semantic_methods.lotus.engine import bind_filter_method, validate_generation
        query_method = self.method if method is None else FilterMethod.from_dict(asdict(method))
        if query_method.model_name != self.method.model_name:
            raise ValueError("a resident Core query must retain the same model identity")
        source.preflight(query_method)
        scan = source.scan_spec(query_method)
        if not isinstance(scan, RecordScan):
            raise TypeError("source.scan_spec(method) must return RecordScan")
        scan.describe()
        validate_generation(query_method, self.generation)
        query_method = bind_filter_method(query_method)
        self._actors.method = bind_filter_method(self.method)
        if not set(query_method.resolved_input_columns).issubset(scan.payload_columns):
            raise ValueError("record scan omits Filter input columns")
        self.engine.validate_driver_environment()
        identity(query_id, "preflight", 0)
        expected = source.expected(query_id)
        CallLedger(expected, max(1, len(expected)))
        for occurrence in expected:
            _check_result_capacity(occurrence, self.capacity.item_result_bytes)
        # Core's fixed-size session identity is checked before actors are allocated.
        spec = SessionSpec(query_id, "lotus-records-filter", "lotus-records-local-model-v1")
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
        types = {"string": daft.DataType.string(), "int64": daft.DataType.int64()}
        frame = daft.read_csv(str(source.csv_path), infer_schema=False, schema={
            name: types[dtype] for name, dtype in scan.csv_schema
        })
        if scan.predicate is not None:
            column, value = scan.predicate
            frame = frame.filter(daft.col(column) == value)
        frame = frame.select("row_id", "source_position", *scan.payload_columns)
        try:
            if expected:
                self._actors.start(min(deadline, time.monotonic() + self.resources.startup_timeout_s),
                                   cancelled)
            else:
                self._actors.ray = ray
            if self._feedback is not None:
                self._feedback.begin(len(expected), query_profile["max_inflight"],
                                     allow_exploration=allow_exploration,search_hint=search_hint)
            if self.core is None:
                limits = self.capacity.limits(self.resources)
                endpoint = EndpointSnapshot("local-model", "local://model", "local", "0",
                                            True, 0, 0, None, time.monotonic())
                policies = SessionPolicies(self._feedback.gate if self._feedback is not None
                                           else StaticAdmissionController(limits.active_requests),
                                           RoundRobinEndpointRouter(),
                                           TopologySnapshot((endpoint,), time.monotonic()), "local")
                self.transport = _RecordTransport(self._actors, limits.held_tasks, lambda: self.core.wake.notify())
                self.core = SessionEngine(limits, self.transport.backend, policies)
            self.transport.deadline = deadline
            self.transport.timings = _StageTimings() if self.stage_timing else None
            self._active = _CoreRows(self, frame.iter_rows(results_buffer_size=1), expected,
                                     spec.job_id, deadline, cancelled,
                                     None if query_method == self._actors.method else query_method,
                                     query_limits, query_profile)
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
        if self._feedback is not None:
            self._feedback.stop()
        closed = close_prepared_core(self._actors, self.transport, self.core, self.capacity,
                                     deadline=self._deadline, timeout=timeout)
        actor_report = closed["model_owner"]
        transport_closed, retirement = closed["transport_closed"], closed["producer_retirement"]
        errors.extend(closed["cleanup_errors"])
        if self._active is not None:
            self._active.collect_receipts()
            if self.last_query_report is not None:
                self.last_query_report["unconsumed_call_results"] = list(self._active.unconsumed_call_results)
                self.last_query_report["core_usage"] = asdict(self.core.capacity.usage())
                if self._active.timings is not None:
                    self.last_query_report["stage_timings"] = self._active.timings.snapshot()
        usage = None if self.core is None else asdict(self.core.capacity.usage())
        self.last_close_report = {
            **self._actors.execution_metadata(),
            "implementation": "semloom_records_core", "production_core_connected": self.production_core_connected,
            "status": "closed" if actor_report["status"] == "closed" and transport_closed
            and (usage is None or not usage["held_tasks"]) and not errors else "unknown",
            "cleanup_scope": "core_transport_actor_and_engine_api",
            "model_owner": actor_report, "gpu_exit_confirmed": actor_report["gpu_exit_confirmed"],
            "core_usage": usage, "transport_closed": transport_closed,
            "retained_remote_inputs": 0 if self.transport is None else self.transport.retained_refs(),
            "producer_retirement": retirement,
            "last_query_report": self.last_query_report, "cleanup_errors": errors,
            "execution_profile": dict(self.execution_profile),
            "feedback_sampler_stopped": self._feedback is None or self._feedback.provider is None
                or not self._feedback.provider.is_running,
            "work_unit": "model_calls",
        }
        return self.last_close_report


class _CoreFeedback:
    """Query-scoped observation; Core remains the only admission/credit owner."""
    def __init__(self, owner, admission):
        self.owner = owner
        self.options = ResourceFeedbackConfig(**{k: v for k, v in admission.items() if k not in ("kind", "gpu_ids")})
        self.base_options = self.options
        self.gpu_ids = admission.get("gpu_ids", ())
        self.gate = self.provider = self.stream = self.ref = self.future = None
        self.controller = None
        self.progress = (0, 0, 0, False)
        self.completed_work = (0, None, None)
        self.completed_effective_work = (0, None, None)
        self.transition_keys = set()
        self.generation = 0
        self.ref_deadline = None
        self.ref_expired = False
        self.samples = self.snapshot_requests = self.sample_errors = self.peak_native_running = 0

    def begin(self, total, initial, *, allow_exploration=None, search_hint=None):
        self.options = (self.base_options if allow_exploration is None else
                        replace(self.base_options, allow_exploration=allow_exploration))
        self.generation += 1
        self.started = time.monotonic()
        self.stream = None
        self.progress = (0, total, 0, False)
        self.completed_work = (0, None, None)
        self.completed_effective_work = (0, None, None)
        self.transition_keys.clear()
        self.samples = self.snapshot_requests = self.sample_errors = self.peak_native_running = 0
        self.controller = ResourceFeedbackController(self.owner.resources.max_active_batches, initial, self.options)
        self.controller.reset(initial, total, search_hint=search_hint)
        if self.provider is None:
            self.provider = NonBlockingMetricsObservationProvider(self.sample,
                poll_interval_s=self.options.poll_interval_s,
                # Delivery already consumes up to two poll periods; allow one
                # more for the gate's scheduled consumption of the source frame.
                stale_after_s=max(.5, self.options.poll_interval_s * 3), close_timeout_s=.1)
        else:
            self.provider.poll_interval_s = self.options.poll_interval_s
            self.provider.resume()
        if self.gate is None:
            self.gate = DynamicAdmissionGate(self.controller, self.provider, trace_sink=self.changed,
                min_update_interval_s=self.options.poll_interval_s)
        else:
            self.gate.controller, self.gate.observation_provider, self.gate.limit = self.controller, self.provider, initial
            self.gate.reset_cadence()
        self.last_window = initial

    def sample(self):
        timings = getattr(getattr(self.owner, "transport", None), "timings", None)
        with _measure(timings, "feedback_poll"):
            return self._sample()

    def _observe_native(self,snapshot,value,received_at_s,*,accepted,reason):
        audit=getattr(self.provider,"observe_native",None)
        if callable(audit):
            try:audit(snapshot,session_id=value.get("session_id"),received_at_s=received_at_s,
                      accepted=accepted,reason=reason)
            except Exception:pass

    def _sample(self):
        generation, stream = self.generation, self.stream
        if stream is None:
            return None
        try:
            if self.ref is None:
                self.ref = self.owner._actors.model_actor.snapshot.remote()
                self.snapshot_requests += 1
                self.future = self.ref.future()
                self.ref_deadline = time.monotonic() + max(.1, self.options.poll_interval_s * 2)
                self.ref_expired = False
            future = self.future
            if not future.done():
                if not self.ref_expired and time.monotonic() >= self.ref_deadline:
                    self.ref_expired = True
                    try:
                        self.owner._actors.ray.cancel(self.ref, force=False)
                    except Exception:
                        self.sample_errors += 1
                return None
            if self.ref_expired:
                self.ref = self.future = None
                return None
            value = future.result()
            try:received_at_s = time.monotonic()
            except Exception:received_at_s = None
            if generation != self.generation:
                return None
            self.ref = self.future = None
            ipc = value.get("ipc") or {}
            snapshot = ipc.get("native_feedback")
            if snapshot is None:
                self._observe_native(None,value,received_at_s,accepted=False,reason="native_frame_missing")
                return None
            expected = self.owner._actors.model_ready["engine_uid"]
            reason = ("engine_uid_mismatch" if snapshot.get("engine_uid") != expected else
                      "session_mismatch" if value.get("session_id") != stream.session.session_id else
                      "source_predates_query" if snapshot.get("observed_at_s", -1) < self.started else "RPC_sample_valid")
            self._observe_native(snapshot,value,received_at_s,accepted=reason=="RPC_sample_valid",reason=reason)
            if reason != "RPC_sample_valid":
                return None
            completed, pending, transition, backpressure = self.progress
            work_count, input_work, output_work = self.completed_work
            if work_count != completed:
                input_work = output_work = None
            effective_count, computed_work, cached_work = self.completed_effective_work
            if effective_count != completed:
                computed_work = cached_work = None
            pressure = None
            if self.gpu_ids:
                from src.observability.metrics.resources import gpu_metadata
                gpu = gpu_metadata(self.gpu_ids, use_nvml=True)
                if gpu["gpu_memory_used_mib"] and gpu["gpu_memory_total_mib"]:
                    free = (float(gpu["gpu_memory_total_mib"]) - float(gpu["gpu_memory_used_mib"])) * 1024**2
                    pressure = free < self.options.memory_floor_bytes if self.options.memory_floor_bytes else None
            if generation != self.generation:
                return None
            self.samples += 1
            self.peak_native_running = max(self.peak_native_running, snapshot["running"])
            return ServiceMetricsSnapshot(snapshot["running"], snapshot["waiting"], snapshot["kv_usage"],
                source_uid=snapshot["engine_uid"], source_session_id=value["session_id"], received_at_s=received_at_s,
                completed_tokens_total=snapshot["generation_tokens_total"],
                source_sequence=snapshot["sequence"], source_observed_at_s=snapshot["observed_at_s"],
                completed_items_total=completed, pending_items=pending,
                preemptions_total=snapshot["preemptions_total"], memory_pressure=pressure,
                transition_pending=transition, consumer_backpressure=backpressure,
                completed_input_work_total=input_work, completed_output_work_total=output_work,
                completed_computed_input_work_total=computed_work,
                completed_cached_input_work_total=cached_work)
        except Exception:
            if generation == self.generation:
                self.ref = self.future = None
                self.sample_errors += 1
            return None
        finally:
            if generation == self.generation and self.provider is not None:
                self.provider.poll_interval_s = (self.options.poll_interval_s if self.ref is not None
                    else self.controller.sampling_interval_s(self.progress[1]))

    def observe(self, stream):
        if self.stream is not stream:
            return
        if self.transition_keys:
            records = self.owner.core.capacity.records
            self.transition_keys = {key for key in self.transition_keys
                                    if key in records and records[key].compute}
        records = tuple(r for r in self.owner.core.capacity.records.values()
                        if r.key.session_id == stream.session.session_id)
        unsubmitted = len(stream.expected) - len(stream.offered) + sum(r.phase == "QUEUED" for r in records)
        ready = sum(r.phase in ("READY", "LEASED") for r in records)
        self.progress = (len(stream.received), unsubmitted, len(self.transition_keys),
                         ready >= self.controller.current_window)
        self.completed_work = stream.completed_work
        self.completed_effective_work = stream.completed_effective_work

    def changed(self, event):
        if event.window != self.last_window:
            self.last_window = event.window
            self.transition_keys = {r.key for r in self.owner.core.capacity.records.values() if r.compute}
            self.observe(self.stream)

    def pause(self):
        self.generation += 1
        if self.provider is not None:
            self.provider.pause()
        if self.ref is not None and not self.ref_expired:
            self.ref_expired = True
            try:
                self.owner._actors.ray.cancel(self.ref, force=False)
            except Exception:
                self.sample_errors += 1

    def stop(self):
        self.pause()
        if self.provider is not None:
            self.provider.close()

    def finish(self, stream):
        if self.stream is not stream:
            return getattr(stream, "_admission_report", None)
        self.pause()
        stream._admission_report = dict(self.controller.report(), samples=self.samples,
            snapshot_requests=self.snapshot_requests, exploration_enabled=self.options.allow_exploration,
            exploration_poll_interval_s=self.options.poll_interval_s,
            protection_poll_interval_s=max(self.options.poll_interval_s,self.options.window_s),
            source_stale_after_s=self.provider.stale_after_s,
            sample_errors=self.sample_errors, peak_native_running=self.peak_native_running,
            sampling_paused=True, sampler_scope="model_owner", sampler_stopped=not self.provider.is_running)
        self.stream = None
        return stream._admission_report
