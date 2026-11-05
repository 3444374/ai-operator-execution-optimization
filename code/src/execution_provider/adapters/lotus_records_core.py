"""LOTUS text Filter records through the existing incremental production Core.

Only raw, bounded record bytes enter Core. Ray submission and waiting run in
the existing asynchronous transport; prepared ObjectRefs go to the model actor.
The synchronous model kernel and its actor owner are shared with the static
reference. This module never calls that reference's iterator or scheduler.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, replace
import hashlib
import json
import os
import threading
import time
from typing import Any, Callable, Iterator

from src.infrastructure.json_content import json_content_equal
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.models import EndpointSnapshot, TopologySnapshot
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import (
    OfferedTask, SessionSpec, State, TaskInfo, TaskKey as TaskKey,
)
from src.scheduling.core.session_policy import SessionPolicies
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.runtime.async_backend import (
    BoundedAsyncBackend as BoundedAsyncBackend, ConfirmedTaskFailure as ConfirmedTaskFailure,
)
from src.scheduling.submission_control.admission import StaticAdmissionController
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordScan, RecordsResources, identity
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec

from .lotus_records_actor_owner import RecordsActorOwner, _deadline as _actor_deadline
from .records_capacity import (
    CoreRecordsCapacity, RecordsCapacityBudget as RecordsCapacityBudget, resolve_records_async_limits,
)
from .records_model_adapter import (
    _ActorOwner, _CoreModelActor as _CoreModelActor, _capacity_failure as _capacity_failure,
    _check_result_capacity, _encode, _measure,
)
from .records_profile import (
    resolve_core_profile, resolve_record_batching,
    resolve_records_runtime,
)
from .records_transport import (
    _await_result as _await_result, _check, _RecordTransport, close_prepared_core,
)
from .records_runtime_identity import observe_ray_runtime


def create_records_core(method, engine, generation, resources=None, *, profile=None, async_limits=None,
                        prepared_batching=None, batch_concurrency=None, batch_feedback=None, rolling_refill=False,
                        resident=False, experimental_skip_token_packing=False, capacity=None,
                        interleaved_preparation=False, event_driven_preparation=False,
                        service_budget=None, service_allocation_bytes=None, service_delivery=None):
    """Select existing ownership; both Movie lifecycles use this same factory."""
    if service_budget is not None:
        from .records_service_budget import create_service_records_core
        return create_service_records_core(method, engine, generation, resources,
            service_budget, service_allocation_bytes, capacity=capacity, profile=profile,
            async_limits=async_limits, prepared_batching=prepared_batching, service_delivery=service_delivery,
            batch_concurrency=batch_concurrency, batch_feedback=batch_feedback,
            rolling_refill=rolling_refill, resident=resident,
            experimental_skip_token_packing=experimental_skip_token_packing,
            interleaved_preparation=interleaved_preparation,
            event_driven_preparation=event_driven_preparation)
    if resources is None:
        raise TypeError('records resources or an explicit service payload pool are required')
    if service_delivery is not None:
        raise ValueError('service_delivery requires an actual service_budget')
    if service_allocation_bytes is not None:
        raise ValueError('service_allocation_bytes requires a service payload pool')
    if type(interleaved_preparation) is not bool or type(event_driven_preparation) is not bool:
        raise TypeError('preparation options must be boolean')
    if (interleaved_preparation or event_driven_preparation) and prepared_batching is None:
        raise ValueError('interleaved preparation requires fixed rolling token requests')
    configured = resolve_records_runtime(engine, resources, profile, capacity=capacity, async_limits=async_limits,
        prepared_batching=prepared_batching, rolling_refill=rolling_refill)
    capacity, resolved, async_limits = configured.capacity, configured.profile, configured.async_limits
    batching = resolve_record_batching(resolved, prepared_batching, batch_concurrency, batch_feedback, rolling_refill,
                                      experimental_skip_token_packing)
    effective = replace(resources, max_active_batches=resolved.get("owner_max_inflight", resolved["max_inflight"]))
    if batching is not None:
        if capacity.budget is not None:
            capacity.method_capacity(effective, capacity.method_limits(1))
        from .lotus_records_batches import PreparedRecordsCore
        return PreparedRecordsCore(method, engine, generation, effective, resolved, batching, async_limits,
                                   resident=resident, capacity=capacity,
                                   **(dict(interleaved_preparation=interleaved_preparation,
                                       event_driven_preparation=event_driven_preparation)
                                      if interleaved_preparation or event_driven_preparation else {}))
    options = dict(capacity=capacity)
    actors = None
    if resolved["mode"] == "async":
        from .lotus_records_async_actor import AsyncRecordsActorOwner
        actors = options["actor_owner"] = AsyncRecordsActorOwner(
            method, engine, generation, effective, limits=async_limits,
            **({} if capacity.budget is None else dict(capacity=capacity)),
        )
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
        if self.owner._feedback is not None and self.owner._feedback.uses_computed_work:
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

    def completed_model_work(self):
        """Combine delivered totals with validated retained receipts, without consuming them."""
        completed, input_work, output_work = self.completed_work
        effective_count, computed, cached = self.completed_effective_work
        work = dict(completed_items_total=completed, completed_input_work_total=input_work,
            completed_output_work_total=output_work,
            completed_computed_input_work_total=computed if effective_count == completed else None,
            completed_cached_input_work_total=cached if effective_count == completed else None)
        fields = (('completed_input_work_total', 'prompt_tokens'),
                  ('completed_output_work_total', 'completion_tokens'),
                  ('completed_computed_input_work_total', 'prompt_computed_tokens'),
                  ('completed_cached_input_work_total', 'prompt_cached_tokens'))
        for key, payload in self.owner.transport.failure_receipts():
            if key.session_id != self.session.session_id:
                continue
            value = json.loads(payload)
            if value['call_id'] in self.received:
                continue
            if value['status'] != 'succeeded':
                work.update({target: None for target, _ in fields})
                continue
            work['completed_items_total'] += 1
            computed, cached = value.get('prompt_computed_tokens'), value.get('prompt_cached_tokens')
            if (type(computed) is not int or type(cached) is not int or min(computed, cached) < 0
                    or computed + cached != value.get('prompt_tokens')):
                value['prompt_computed_tokens'] = value['prompt_cached_tokens'] = None
            for target, source in fields:
                item = value.get(source)
                work[target] = (work[target]+item if work[target] is not None
                                and type(item) is int and item >= 0 else None)
        return work

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
        if self.capacity.budget is not None and self.resources.max_active_batches != self.capacity.budget.active_requests:
            raise ValueError('owner resources must match the declared hard request budget')
        if actor_owner is not None:
            if not isinstance(actor_owner, RecordsActorOwner):
                raise TypeError("actor_owner must be RecordsActorOwner")
            for name in ("method", "engine", "generation", "resources"):
                if getattr(actor_owner, name) != getattr(self, name):
                    raise ValueError("actor_owner " + name + " differs from the Core configuration")
        if self.capacity.budget is not None and (actor_owner is None or
                actor_owner.execution_metadata()['model_batch_mode'] != 'independent_final_results'):
            raise ValueError('explicit resource budgets require an async actor owner')
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
            from .records_request_feedback import CoreResourceRequestFeedback
            self._feedback = CoreResourceRequestFeedback(self, configured["admission"])
            self.execution_profile.update(configured)
        self.core = self.transport = self._active = None
        self._closing = False
        self._deadline = None
        self.last_query_report = self.last_close_report = None
        self._service_allocation = None
        self._model_runtime = None
        self._model_runtime_capture_attempted = False

    @property
    def production_core_connected(self):
        return isinstance(self.core, SessionEngine)

    def execution_metadata(self):
        value = dict(self._actors.execution_metadata(), execution_profile=dict(self.execution_profile))
        if self._service_allocation is not None:
            value['service_allocation'] = self._service_allocation.describe()
        return value

    @property
    def model_ready(self):
        """Observe the existing model receipt without allocating an owner."""
        return deepcopy(self._actors.model_ready)

    def _control(self):
        if threading.get_ident() != self._thread:
            raise RuntimeError("Core records owner requires its single control thread")

    def bind_model_runtime(self, observed=None):
        """Bind once to an existing connection; an unavailable first read stays unknown."""
        self._control()
        if observed is not None and (type(observed) is not tuple or len(observed) != 2
                or type(observed[1]) is not dict or set(observed[1]) != {'job_id', 'node_id'}
                or any(type(value) is not str or not value.strip() for value in observed[1].values())):
            raise ValueError('model runtime requires the observed module, job and node identity')
        if self._model_runtime_capture_attempted:
            original = self._model_runtime
            if observed is not None and (original is None or observed[0] is not original[0]
                    or observed[1] != original[1]):
                raise RuntimeError('model runtime cannot be rebound after its first observation')
            try:
                self._check_model_runtime()
            except RuntimeError as error:
                allocation = self._service_allocation
                if allocation is not None and allocation._runtime_identity is not None:
                    raise ValueError('payload allocation cannot replace its original Ray runtime identity') from error
                raise
            return
        if observed is None:
            import sys
            current = observe_ray_runtime(sys.modules.get('ray'))
        else:
            current = observe_ray_runtime(observed[0])
            if current is None or current[0] is not observed[0] or current[1] != observed[1]:
                raise RuntimeError('supplied model runtime identity is unavailable or changed')
        self._model_runtime_capture_attempted = True
        self._model_runtime = None if current is None else (current[0], dict(current[1]))

    def _check_model_runtime(self):
        original = self._model_runtime
        if original is None:
            return  # Preserve existing callers which supplied no observable runtime identity.
        current = observe_ray_runtime(original[0])
        if current is None or current[0] is not original[0] or current[1] != original[1]:
            raise RuntimeError('original model Ray runtime is unavailable or changed')

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
        self.bind_model_runtime()
        ready = self._actors.start(deadline)
        self._check_model_runtime()
        if self._service_allocation is not None:
            self._service_allocation.observe_ready(self)
        return ready

    def evaluate(self, source, *, query_id, method: FilterMethod | None = None,
                 cancelled: Callable[[], bool] | None = None, max_inflight: int | None = None,
                 allow_exploration: bool | None = None):
        self._control()
        if self._closing or (self.core is not None and
                             (self.core.error or self.core.capacity.usage().held_tasks)):
            raise RuntimeError("Core records has closing or unconfirmed model ownership")
        if self._active is not None and not self._active.finished:
            raise RuntimeError("Core records already owns an active query")
        query_limits, query_profile = None, dict(self.execution_profile)
        if allow_exploration is not None:
            if self._feedback is None or type(allow_exploration) is not bool:
                raise ValueError("per-query exploration requires resource feedback and a boolean")
            query_profile["admission"] = dict(query_profile["admission"], allow_exploration=allow_exploration)
        if max_inflight is not None:
            if (query_profile["mode"] != "async" or type(max_inflight) is not int
                    or not 1 <= max_inflight <= query_profile.get("owner_max_inflight", query_profile["max_inflight"])):
                raise ValueError("query in-flight must fit the existing async model owner")
            if self._feedback is not None and max_inflight != 1:
                raise ValueError('resource requests start at one without cross-query hints')
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
            if self._service_allocation is not None and self._active.unconsumed_call_results:
                raise RuntimeError('take unconsumed results before replacing their service query')
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
                self.bind_model_runtime()
                self._actors.start(min(deadline, time.monotonic() + self.resources.startup_timeout_s),
                                   cancelled)
                self._check_model_runtime()
                if self._service_allocation is not None:
                    self._service_allocation.observe_ready(self)
            else:
                self._actors.ray = ray
            if self._feedback is not None:
                self._feedback.begin(len(expected), query_profile["max_inflight"],
                                     allow_exploration=allow_exploration)
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
            if self.capacity.budget is not None and self._feedback is None:
                self.core.policies = replace(self.core.policies,
                    admission=StaticAdmissionController(query_profile['max_inflight']))
            self.transport.deadline = deadline
            self.transport.timings = _StageTimings() if self.stage_timing else None
            self._active = _CoreRows(self, frame.iter_rows(results_buffer_size=1), expected,
                                     spec.job_id, deadline, cancelled,
                                     None if query_method == self._actors.method else query_method,
                                     query_limits, query_profile)
        except BaseException as error:
            try:
                error.add_note("Core startup cleanup: " + str(self.close()))
            except BaseException as cleanup:
                error.add_note('Core startup cleanup failed: ' + type(cleanup).__name__ + ': ' + str(cleanup))
            raise
        return self._active

    def cleanup_confirmed(self):
        """Read original terminal proof; local retained results still need acceptance."""
        self._control()
        report = self.last_close_report or {}
        if type(report) is not dict:
            return False
        actor = report.get('model_owner') or {}
        actual = self._actors.last_close_report or {}
        if type(actor) is not dict or type(actual) is not dict:
            return False
        if (not self._closing or report.get('status') != 'closed' or report.get('cleanup_errors')
                or actor.get('status') != 'closed' or actor.get('cleanup_errors')
                or actual.get('status') != 'closed' or actual.get('cleanup_errors')
                or actor.get('actor_kill_requests_confirmed') is not True
                or actual.get('actor_kill_requests_confirmed') is not True
                or report.get('transport_closed') is not True or report.get('retained_remote_inputs') != 0):
            return False
        try:
            ready = self._actors.model_ready
            if actor.get('model_ready') != ready or actual.get('model_ready') != ready:
                return False
            allocation = self._service_allocation
            if allocation is not None and allocation._identity is not None and any(
                    (ready or {}).get(name) != value for name, value in allocation._identity.items()):
                return False
            if self._actors.model_actor is not None:
                if (actual.get('engine_close_report') or {}).get('engine_api_shutdown_confirmed') is not True:
                    return False
            elif self._actors._start_failed:
                return False
            if self.core is not None and any(asdict(self.core.capacity.usage()).values()):
                return False
            if self.transport is not None and (self.transport.retained_refs()
                    or self.transport.backend._closed is not True or self.transport.backend._thread.is_alive()):
                return False
            progress = getattr(self._actors, '_data_rpc_progress', None)
            if callable(progress):
                rpc = progress()
                if rpc.pending or rpc.inflight or rpc.batch_submits_pending or rpc.unconfirmed:
                    return False
            if self._feedback is not None:
                proof = getattr(self._feedback, 'snapshot_rpc_report', None)
                if not callable(proof) or proof().get('snapshot_rpc_terminal_evidence_complete') is not True:
                    return False
            return True
        except Exception:
            return False

    def close(self, *, timeout=None):
        self._control()
        if timeout is not None:
            _actor_deadline(timeout)
        if self.cleanup_confirmed():
            return self.last_close_report
        actors = self._actors
        if (actors.ray is not None or actors.model_actor is not None
                or actors.prepare_actors or actors.model_ready is not None):
            self._check_model_runtime()
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
        feedback_close = None
        snapshot_report = getattr(self._feedback, 'snapshot_rpc_report', None)
        if callable(snapshot_report):
            try:
                feedback_close = snapshot_report()
                if feedback_close.get('snapshot_rpc_terminal_evidence_complete') is not True:
                    errors.append('feedback_snapshot_rpc_terminal_unconfirmed')
            except BaseException as error:
                errors.append('feedback_snapshot_report: ' + type(error).__name__)
                feedback_close = dict(snapshot_rpc_terminal_evidence_complete=False,
                                      report_error_type=type(error).__name__)
        if self._active is not None:
            self._active.collect_receipts()
            if self.last_query_report is not None:
                if self._service_allocation is not None:
                    self.last_query_report = deepcopy(self.last_query_report)
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
        if feedback_close is not None:
            self.last_close_report['feedback_close'] = feedback_close
        if self._service_allocation is not None:
            self._service_allocation.settle(self)
            self.last_close_report['service_allocation'] = self._service_allocation.describe()
        return self.last_close_report

    def take_unconsumed_results(self, *, expected=None):
        """Transfer the closed query's retained receipts once, without parsing them."""
        self._control()
        if self._active is None:
            if expected is not None and (type(expected) is not list or expected):
                raise ValueError('retained receipts changed before consumer acceptance')
            return []
        if not self._active.finished:
            raise RuntimeError('close the query before taking its retained receipts')
        self._active.collect_receipts()
        taken = deepcopy(self._active.unconsumed_call_results)
        if expected is not None and (type(expected) is not list or not json_content_equal(taken, expected)):
            raise ValueError('retained receipts changed before consumer acceptance')
        query = deepcopy(self.last_query_report)
        if query is not None:
            query['unconsumed_call_results'] = []
        report = None if self.last_close_report is None else dict(self.last_close_report,
            last_query_report=query)
        self._active.unconsumed_call_results = []
        self.last_query_report = self._active.last_close_report = query
        self.last_close_report = report
        if self._service_allocation is not None and report is not None:
            self._service_allocation.settle(self)
            report['service_allocation'] = self._service_allocation.describe()
        return taken
