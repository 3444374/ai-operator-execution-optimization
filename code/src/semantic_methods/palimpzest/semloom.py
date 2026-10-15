"""Query-thread Core progress over the existing complete NativeCall HTTP kernel."""

from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import threading
import time

from src.baselines.text.frameworks import native_calls
from src.execution_provider.adapters import full_response, native_call_transport, native_tasks, ray_map_transport
from src.execution_provider.adapters.native_call_transport import NativeCallTransport
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.scheduling.core.session_contract import State
from .backend import PalimpzestAdapterError


_SOURCE_HASHES = {
    native_calls: "ec02723d9d997d120ea66429b1edaa87709c7028e37aa1339e6f0a6bc3ffbb6f",
    full_response: "ef0bdfeb99bd5aab1c1b4002ee1f61469ee270966437d71eac66b2ddbd99ebde",
    native_tasks: "afd3e07dd71eac8bcd358a042876860074846ebd628c868fb86822bb47e5206b",
    native_call_transport: "85e3b91991a87388405f70881b12cf58142261d5a00f80088c2aa1b0458c5a70",
    ray_map_transport: "600ed26f6e1bc6f3868b97ba6f0f786e613f4e1956a635d85e0bf275e489df4f",
}


def validate_native_call_sources():
    for module, expected in _SOURCE_HASHES.items():
        if hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() != expected:
            raise PalimpzestAdapterError("PZ complete-call dependency changed: " + module.__name__)


def _positive_timeout(value):
    if type(value) not in (int, float) or not 0 < value <= 120:
        raise ValueError("PZ Core progress requires a finite deadline at most 120 seconds")


class SemLoomProgress:
    """One query Job and persistent operator flows, all driven on the query thread.

    Exhausting a ready group never seals these flows. Core owns admission and
    execution; this adapter associates complete calls and releases result leases.
    Ray uses the existing Daft payload materializer and physical HTTP actors.
    physical=None is an explicit local diagnostic with the same complete-call kernel.
    """

    backend = "semloom"
    finite_batches = False

    def __init__(self, model_config, *, query_id, operator_ids, limits, max_active_requests,
                 observer=None, physical: RayMapConfig | None = None):
        from .query_execution import QueryExecutionLimits
        if type(limits) is not QueryExecutionLimits:
            raise TypeError("PZ Core progress requires QueryExecutionLimits")
        if (type(operator_ids) is not tuple or not operator_ids or len(set(operator_ids)) != len(operator_ids)
                or any(type(value) is not str or not value or len(value.encode()) > 256 for value in operator_ids)
                or type(query_id) is not str or not query_id or len(query_id.encode()) > 256):
            raise ValueError("PZ Core progress requires finite declared query/operator identities")
        if type(max_active_requests) is not int or max_active_requests < 1:
            raise ValueError("SemLoom needs its deployment's positive request capacity")
        if physical is not None and (type(physical) is not RayMapConfig
                or physical.response_mode != 'native-call-full' or physical.payload_backend != 'daft'
                or physical.preparation is not None):
            raise ValueError("PZ SemLoom physical execution requires explicit native-call-full Daft/Ray configuration")
        validate_native_call_sources()
        self.limits, self.query_id = limits, query_id
        self.physical, self._physical_identity, self._runtime_closed = physical, None, False
        self.max_batch_rows, self.max_batch_bytes = limits.max_batch_rows, limits.max_batch_bytes
        self._thread = threading.get_ident()
        self.transport = NativeCallTransport(model_config, max_active_requests, observer)
        def observe(event):
            if event.get('event') == 'ray_worker_pool':
                self._physical_identity = event
            if observer is not None:
                observer(event)
        local_transport = dict(transport_factory=lambda *_args: self.transport) if physical is None else {}
        self.execution = native_tasks.build_native_execution(model_config, physical=physical,
            **local_transport, observer=observer if physical is None else observe,
            max_tasks=limits.max_pending_calls, max_active_requests=max_active_requests,
            input_bytes=limits.max_input_bytes, result_bytes=limits.max_retained_response_bytes)
        self.job = None
        self.flows = {}
        try:
            self.job = native_tasks.NativeQueryJob(self.execution, query_id, flow_count=len(operator_ids))
            for operator in operator_ids:
                self.flows[operator] = self.job.open_session(operator)
        except BaseException:
            for flow in self.flows.values():
                flow.close()
            if self.job is not None:
                self.job.close()
            self.execution.close()
            raise
        self._next = dict.fromkeys(operator_ids, 0)
        self._expected, self._deadlines = {}, {}
        self._active = self._closed = self._failed = self._error_sent = False
        self._cancelled = None
        self._cursor = self._batches = self._received = self._submitted = 0
        self.last_close_report = None

    def _check_thread(self):
        if threading.get_ident() != self._thread:
            raise PalimpzestAdapterError("PZ Core progress belongs to its original query thread")

    @property
    def active(self):
        return self._active

    @property
    def accepts_ready(self):
        return not self._closed and not self._failed

    def start(self, calls, *, identities, cancelled, timeout_s):
        self._check_thread()
        _positive_timeout(timeout_s)
        if self._closed or self._failed:
            raise PalimpzestAdapterError("PZ Core progress cannot accept another ready snapshot")
        calls = tuple(calls)
        if (not calls or any(type(c) is not native_calls.NativeCall for c in calls)
                or type(identities) is not dict or len(calls) > self.max_batch_rows
                or len({c.call_id for c in calls}) != len(calls)
                or set(identities) != {c.call_id for c in calls}
                or sum(len(c.payload) for c in calls) > self.max_batch_bytes):
            raise ValueError("PZ ready snapshot exceeds its finite complete-call contract")
        prepared, wire_bytes = [], 0
        counts = dict(self._next)
        outstanding_ids = {value.call_id for value in self._expected.values()}
        for call in calls:
            if call.call_id in outstanding_ids:
                raise ValueError("PZ ready call is already owned by Core")
            self.transport.validate_call(call)
            identity = identities[call.call_id]
            if type(identity) is not dict:
                raise ValueError("PZ ready snapshot requires original occurrence metadata")
            operator = identity.get("physical_operator")
            if (identity.get("query_id") != self.query_id or operator not in self.flows
                    or identity.get("input_occurrence") != call.row_id
                    or identity.get("generation_step") != 0 or identity.get("attempt") != 0
                    or call.call_id != "pz:" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()):
                raise ValueError("PZ ready snapshot changed its original occurrence identity")
            wire = native_calls._call_wire(call).encode()
            wire_bytes += len(wire)
            flow = self.flows[operator]
            if (len(call.payload) > self.limits.max_request_bytes or len(wire) > flow.limits.item_input_bytes
                    or call.max_response_bytes > min(self.limits.max_response_bytes, flow.limits.item_result_bytes)):
                raise ValueError("PZ complete-call wire or response exceeds the opened Core item capacity")
            sequence = counts[operator]
            counts[operator] += 1
            task = native_tasks.prepare_native_task(wire, sequence, row_sequence=call.row_id,
                call_id=call.call_id, stage_id="model", max_result_bytes=call.max_response_bytes)
            prepared.append((operator, task, call))
        if wire_bytes > self.limits.max_input_bytes:
            raise ValueError("PZ ready snapshot exceeds its complete-wire input allowance")
        # No offer or request occurs until every item passed the snapshot preflight.
        if cancelled and cancelled():
            raise InterruptedError("PZ Core query cancelled before offer")
        self._cancelled = cancelled
        accepted, deadline = 0, time.monotonic() + timeout_s
        for operator, task, call in prepared:
            result = self.flows[operator].offer((task,))
            if result.status not in ("ACCEPTED", "BACKPRESSURE"):
                raise PalimpzestAdapterError("PZ Core rejected an offered complete call")
            if not result.accepted_prefix_count:
                break
            self._expected[(operator, task.sequence)] = call
            self._deadlines[call.call_id] = deadline
            self._submitted += 1
            self._next[operator] += 1
            accepted += 1
        self._active = bool(self._expected)
        self._batches += 1
        return accepted

    def _failure(self, error):
        self._failed = True
        self.job.request_cancel()
        if not self._error_sent:
            self._error_sent = True
            return "error", error
        return None

    def poll(self, timeout_s):
        self._check_thread()
        if type(timeout_s) not in (int, float) or not 0 <= timeout_s <= 120:
            raise ValueError("PZ Core poll requires a bounded nonnegative timeout")
        if not self._active:
            return None
        end = time.monotonic() + timeout_s
        while True:
            if self._cancelled and self._cancelled():
                return self._failure(InterruptedError("PZ Core query cancelled"))
            deadline = min(self._deadlines.values())
            if time.monotonic() >= deadline:
                return self._failure(TimeoutError("PZ Core ready snapshot deadline exceeded"))
            generation = self.execution.engine.wake.generation
            operators = tuple(self.flows)
            for _ in operators:
                operator = operators[self._cursor % len(operators)]
                self._cursor += 1
                flow = self.flows[operator]
                progress = flow.advance(1)
                if progress.deliveries:
                    delivery = progress.deliveries[0]
                    primary = None
                    try:
                        call = self._expected.pop((operator, delivery.key.sequence))
                        if (delivery.status != "completed" or delivery.info is None
                                or (delivery.info.call_id, delivery.info.row_sequence) != (call.call_id, call.row_id)):
                            raise PalimpzestAdapterError("PZ Core completion changed its occurrence identity")
                        response = full_response.decode_full_response(delivery.result)
                        outcome = native_calls.NativeCallOutcome(call.call_id, call.row_id, response)
                        self._deadlines.pop(call.call_id)
                        self._active = bool(self._expected)
                        self._received += 1
                    except BaseException as error:
                        primary = error
                        return self._failure(error)
                    finally:
                        try:
                            flow.release((delivery.lease_id,))
                        except BaseException as cleanup:
                            if primary is None:
                                raise
                            primary.add_note("PZ Core result release also failed: " + type(cleanup).__name__)
                    return "outcome", outcome
                if progress.state in (State.FAILED, State.CANCELLED):
                    return self._failure(PalimpzestAdapterError(progress.error or "PZ Core flow stopped"))
            remaining = min(end, deadline) - time.monotonic()
            if remaining <= 0:
                return None
            self.execution.engine.wake.wait(generation, min(remaining, 0.01))

    def finish(self, *, method_complete, timeout_s):
        self._check_thread()
        if type(timeout_s) not in (int, float) or not 0 <= timeout_s <= 120:
            raise ValueError("PZ Core finish requires a bounded nonnegative timeout")
        if self._closed:
            return self.last_close_report
        complete = method_complete and not self._failed and not self._expected
        errors = []
        if complete:
            for flow in self.flows.values():
                try:
                    flow.end_input()
                    flow.advance(1)
                except BaseException as error:
                    errors.append(error)
                    complete = False
        if not complete:
            self.job.request_cancel()
        for flow in self.flows.values():
            try:
                flow.close(clean=complete)
            except BaseException as error:
                errors.append(error)
        try:
            self.job.close()
        except BaseException as error:
            errors.append(error)
        deadline = time.monotonic() + timeout_s
        while (self.execution.engine.capacity.records or self.execution.engine.jobs.jobs) and time.monotonic() < deadline:
            generation = self.execution.engine.wake.generation
            try:
                progress = self.execution.engine.advance()
            except BaseException as error:
                errors.append(error)
                break
            if not progress.has_immediate_work:
                self.execution.engine.wake.wait(generation, min(0.01, max(0, deadline-time.monotonic())))
        settled = not self.execution.engine.capacity.records and not self.execution.engine.jobs.jobs
        runtime_closed = False
        if settled:
            try:
                runtime_closed = self.execution.close(max(0.001, deadline-time.monotonic()))
            except BaseException as error:
                errors.append(error)
        self._active = False
        self._expected.clear()
        self._deadlines.clear()
        self._closed = settled and runtime_closed and not errors
        self._runtime_closed = runtime_closed
        self.last_close_report = dict(self.snapshot(), method_complete=complete,
            runtime_closed=runtime_closed, clean=self._closed, unconfirmed_work=not settled or not runtime_closed,
            cleanup_errors=[type(error).__name__ for error in errors])
        if errors:
            for error in errors[1:]:
                errors[0].add_note("PZ Core cleanup also failed: " + type(error).__name__)
            raise errors[0]
        return self.last_close_report

    def close(self, timeout_s=5.0):
        return self.finish(method_complete=False, timeout_s=timeout_s)

    def snapshot(self):
        return dict(backend="semloom", scheduler_owner="SemLoom SessionEngine",
            physical_backend="local complete NativeCall transport" if self.physical is None else
                "Daft payload batches + Ray HTTP workers + complete NativeCall transport",
            ray_backend=self.physical is not None,
            physical_config=asdict(self.physical) if self.physical is not None else None,
            physical_identity=self._physical_identity,
            native_call_transport=native_call_transport.native_call_transport_identity(),
            finite_batches=False, accepts_ready=self.accepts_ready,
            configuration_mode="fixed-profile", work_unit="request-count", calibrated_gpu_work=False,
            declared_request_capacity=self.transport.max_active_requests,
            batches=self._batches, calls_submitted=self._submitted, outcomes_received=self._received,
            active=self._active, closed=self._closed, pending_calls=len(self._expected),
            core_usage=asdict(self.execution.engine.capacity.usage()),
            registered_jobs=len(self.execution.engine.jobs.jobs),
            http_pool_closed=self.transport.closed if self.physical is None else None,
            physical_transport_closed=self._runtime_closed if self.physical is not None else None,
            worker_lifecycle=("local diagnostic" if self.physical is None else
                "caller pool" if self.physical.worker_pool else "execution owner"),
            raw_request_limit_bytes=self.limits.max_request_bytes, core_wire_item_limit_bytes=MAX_FRAME_BYTES,
            remote_http_stopped="unavailable", actual_posts="separate gateway ledger required",
            redirect_qualification="controlled nonredirect profile; follow_redirects=False; arbitrary redirects unsupported")
