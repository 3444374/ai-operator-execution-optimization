"""Opt-in Core model actor over the existing bounded private AsyncLLM IPC.

Core owns admission, ordering and query state. This threaded actor only hands
off prepared calls and retains each result until its transport acknowledges it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import threading
import time

from src.semantic_methods.lotus.records import CallLedger
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import (
    EngineSpec, GenerationSpec, IDENTITY_FIELDS, _prepared, result_record,
)

from .lotus_records_actor_owner import _deadline, _runtime_env
from .lotus_records_core import _ActorOwner, _capacity_failure, _check_result_capacity, _encode


def _request_key(value):
    if (not isinstance(value, (tuple, list)) or len(value) != 2
            or any(type(item) is not int or not 0 <= item < (1 << 64) for item in value)):
        raise ValueError("Core request key must contain two uint64 integers")
    return tuple(value)


@dataclass
class _Call:
    identity: dict
    ticket: str | None = None
    value: dict | None = None
    confirmed: bool = False
    delivered: bool = False
    aborted: bool = False
    abort_queued: bool = False


class AsyncCoreModelActor:
    """Bounded threaded actor. Only the private child imports the model runtime."""

    def __init__(self, engine, generation, model_batch_rows, limits):
        if type(model_batch_rows) is not int or model_batch_rows != 1:
            raise ValueError("Async Core model actor requires model_batch_rows=1")
        self.spec = EngineSpec.from_dict(engine)
        self.generation = GenerationSpec.from_dict(generation)
        self.limits = AsyncEngineLimits(**limits)
        self.engine = None
        self._condition = threading.Condition()
        self._close_lock = threading.Lock()
        self._start_attempted = self._start_failed = self._closing = self._unhealthy = False
        self._held, self._tickets, self._early_cancels = {}, {}, set()
        self._session = -1
        self._cancel_session = -1
        self.last_close_report = None

    def _process_type(self):
        return AsyncEngineProcess

    def ready(self, deadline):
        _deadline(deadline)
        with self._condition:
            if self._closing or self._start_failed:
                raise RuntimeError("Async actor startup or cleanup is unconfirmed")
            if self.engine is not None:
                return self._ready_report()
            if self._start_attempted:
                raise RuntimeError("Async actor startup is already owned")
            if time.monotonic() >= deadline:
                raise TimeoutError("Async actor startup deadline expired")
            self._start_attempted = True
        try:
            engine = self._process_type()(self.spec, self.generation, limits=self.limits,
                startup_timeout_s=max(0.001, deadline - time.monotonic()))
        except BaseException as error:
            with self._condition:
                self.engine = getattr(error, "async_engine_process", None)
                self._start_failed = True
                self._condition.notify_all()
            raise
        with self._condition:
            self.engine = engine
            self._condition.notify_all()
            cancelled = self._closing or time.monotonic() >= deadline
        if cancelled:
            self.close(1)
            raise TimeoutError("Async actor startup ended outside its open lifetime")
        return self._ready_report()

    def _ready_report(self):
        return dict(self.engine.ready_report, model_batch_mode="independent_final_results",
            model_batch_rows=1, limits=asdict(self.limits), physical_gpu_exit_confirmed=None)

    def snapshot(self):
        with self._condition:
            return dict(held_requests=len(self._held),
                reserved_result_bytes=len(self._held) * self.limits.item_result_bytes,
                delivered_results=sum(item.delivered for item in self._held.values()),
                unknown_requests=sum(item.value is not None and not item.confirmed
                                     for item in self._held.values()),
                early_cancellations=len(self._early_cancels), session_id=self._session,
                closing=self._closing, unhealthy=self._unhealthy,
                ipc=None if self.engine is None else self.engine.snapshot())

    def _receipt(self, identity, status, error_type, *, expired=False):
        now = time.monotonic()
        value = result_record(identity,
            engine_uid=(self.engine.ready_report or {}).get("engine_uid", "async-actor-unstarted")
                if self.engine is not None else "async-actor-unstarted",
            start=now, end=now, status=status, submitted=False, error_type=error_type)
        value.update(engine_terminal_confirmed=status != "unknown", deadline_expired=expired)
        return value

    @staticmethod
    def _bounded(value, maximum):
        try:
            return _encode(value, maximum)
        except ValueError:
            return _encode(_capacity_failure(value), maximum)

    def _collect(self):
        # Called under the actor condition; one reader transfers each IPC event.
        events = self.engine.poll(self.limits.result_slots)
        for event in events:
            key = self._tickets.get(event.request_id)
            if key is None:
                self._unhealthy = True
                raise RuntimeError("Async IPC result has no Core request owner")
            call = self._held[key]
            value = event.result
            if value is None:
                value = self._receipt(call.identity, "unknown", event.error_type or "EngineOutcomeUnknown")
            audit = CallLedger([call.identity], 1)
            audit.record(value)
            call.value, call.confirmed = value, event.engine_terminal_confirmed
            if not event.engine_terminal_confirmed:
                self._unhealthy = True
        if events:
            self._condition.notify_all()

    def _submit(self, key, call, row, deadline):
        try:
            submitted = self.engine.submit(row, deadline_monotonic=deadline)
        except (TypeError, ValueError) as error:
            call.value = self._receipt(call.identity, "failed", type(error).__name__)
            call.confirmed = True
        else:
            if submitted.status == "queued":
                call.ticket = submitted.request_id
                self._tickets[call.ticket] = key
            elif submitted.status != "backpressure":
                call.value = self._receipt(call.identity, "failed", "AsyncInputNotAccepted")
                call.confirmed = True

    def run_core(self, rows, deadline, maximum, request_key):
        _deadline(deadline)
        key = _request_key(request_key)
        if type(rows) is not list or len(rows) != 1:
            raise ValueError("Async Core model actor requires one prepared occurrence")
        identities, _ = _prepared(rows)
        occurrence = {name: identities[0][name] for name in IDENTITY_FIELDS}
        _check_result_capacity(occurrence, maximum)
        with self._condition:
            slots = min(self.limits.max_inflight, self.limits.result_slots,
                        self.limits.result_bytes // self.limits.item_result_bytes)
            # The Core releases a known terminal before its asynchronous ack
            # necessarily reaches this threaded actor. A following session can
            # already be valid; keep its finite handoff waiting for that ack.
            while len(self._held) >= slots or (key[0] > self._session and self._held):
                if self._closing or self._unhealthy or time.monotonic() >= deadline:
                    return self._bounded(self._receipt(occurrence, "failed", "CapacityUnavailable",
                        expired=time.monotonic() >= deadline), maximum)
                self._condition.wait(min(0.01, max(0, deadline - time.monotonic())))
            if self._closing or self._unhealthy or self.engine is None:
                return self._bounded(self._receipt(occurrence, "failed", "AsyncOwnerUnavailable"), maximum)
            if key in self._held:
                raise RuntimeError("Core request key is already owned")
            if key[0] < self._session:
                return self._bounded(self._receipt(occurrence, "failed", "StaleCoreSession"), maximum)
            if key[0] > self._session:
                self._session = key[0]
                self._cancel_session = max(self._cancel_session, key[0])
                self._early_cancels = {item for item in self._early_cancels if item[0] >= key[0]}
            call = self._held[key] = _Call(occurrence, aborted=key in self._early_cancels)
            self._early_cancels.discard(key)
            expired = time.monotonic() >= deadline
            if call.aborted or expired:
                call.value = self._receipt(occurrence, "failed",
                    "DeadlineExceeded" if expired else "CancelledBeforeSubmission", expired=expired)
                call.confirmed = True
            else:
                self._submit(key, call, rows[0], deadline)
        while True:
            with self._condition:
                self._collect()
                expired = time.monotonic() >= deadline
                if call.value is None and call.ticket is None:
                    if call.aborted or expired or self._closing or self._unhealthy:
                        call.value = self._receipt(occurrence, "failed",
                            "DeadlineExceeded" if expired else "CancelledBeforeSubmission", expired=expired)
                        call.confirmed = True
                    else:
                        # NOT_ACCEPTED carries no model ownership. Retry only
                        # this finite handoff after a capacity change; never
                        # submit an accepted call a second time.
                        self._submit(key, call, rows[0], deadline)
                if call.value is not None:
                    # Keep this bounded receipt until the Core transport has
                    # actually received it, including failure/unknown usage.
                    encoded = self._bounded(call.value, maximum)
                    call.delivered = True
                    self._condition.notify_all()
                    return encoded
                if expired:
                    call.aborted = True
                if call.aborted and not call.abort_queued:
                    call.abort_queued = self.engine.abort(call.ticket)
                # The Core controller owns the query deadline and invokes close.
                # Keep reading until an actual receipt or child-death unknown;
                # timing out this waiter alone would lose a late known usage.
                broken = self.engine.snapshot()["broken"] is not None
                if broken:
                    # IPC.wait deliberately returns immediately on broken.
                    # Other requests can still deliver known late receipts.
                    self._condition.wait(0.01)
            if not broken:
                self.engine.wait(0.01)

    def abort_core(self, request_key):
        key = _request_key(request_key)
        with self._condition:
            if self._closing or key[0] < self._session:
                return dict(requested=False, engine_terminal_confirmed=False)
            call = self._held.get(key)
            if call is not None:
                if call.confirmed:
                    return dict(requested=False, engine_terminal_confirmed=False)
                call.aborted = True
                requested = call.ticket is None or self.engine.abort(call.ticket)
                call.abort_queued = call.ticket is not None and requested
            else:
                # Cancels can overtake the next session's first prepared call.
                # Track that epoch separately so a late old cancel cannot fill
                # its bounded early-intent set or block an already owned call.
                if key[0] < self._cancel_session:
                    return dict(requested=False, engine_terminal_confirmed=False)
                if key[0] > self._cancel_session:
                    self._cancel_session = key[0]
                    self._early_cancels.clear()
                requested = key in self._early_cancels or len(self._early_cancels) < self.limits.max_inflight
                if requested:
                    self._early_cancels.add(key)
            return dict(requested=requested, engine_terminal_confirmed=False)

    def acknowledge_core(self, request_key):
        key = _request_key(request_key)
        with self._condition:
            call = self._held.get(key)
            if call is None or not call.delivered or not call.confirmed:
                return False
            del self._held[key]
            if call.ticket is not None:
                self._tickets.pop(call.ticket, None)
            self._condition.notify_all()
            return True

    def close(self, timeout):
        _deadline(timeout)
        if not self._close_lock.acquire(blocking=False):
            return dict(status="unknown", engine_api_shutdown_confirmed=False,
                        gpu_exit_confirmed=None, cleanup_errors=["close_already_pending"])
        try:
            with self._condition:
                self._closing = True
                self._condition.notify_all()
                engine = self.engine
            if engine is None:
                confirmed = not self._start_attempted
                report = dict(status="closed" if confirmed else "unknown",
                    engine_api_shutdown_confirmed=confirmed, gpu_exit_confirmed=None,
                    cleanup_errors=[] if confirmed else ["startup_cleanup_unconfirmed"],
                    unconsumed_call_results=[])
            else:
                ipc = engine.close(timeout=timeout)
                with self._condition:
                    self._collect()
                    receipts = [dict(call.value) for call in self._held.values() if call.value is not None]
                settled = ipc.get("status") == "closed"
                report = dict(status="closed" if settled else "unknown",
                    cleanup_scope="async_engine_api_direct_child_and_ipc",
                    engine_api_shutdown_confirmed=settled,
                    gpu_exit_confirmed=None, descendants_exit_confirmed=None,
                    ipc_close=ipc, unconsumed_call_results=receipts,
                    cleanup_errors=[] if settled else ["async_ipc_cleanup_unconfirmed"])
            self.last_close_report = report
            return report
        finally:
            self._close_lock.release()


class AsyncRecordsActorOwner(_ActorOwner):
    """Opt-in actor allocation hooks; the existing Core owns all query state."""

    model_batch_mode = "independent_final_results"
    native_engine_abort_supported = True

    def __init__(self, method, engine, generation, resources, *, limits=None):
        super().__init__(method, engine, generation, resources)
        if resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1:
            raise ValueError("Async Core requires prepare_batch_rows=model_batch_rows=1")
        maximum = resources.max_active_batches
        self.limits = AsyncEngineLimits(max_inflight=maximum, result_slots=maximum,
            command_slots=maximum, input_bytes=maximum * 2 * 1024**2,
            result_bytes=maximum * 2 * 1024**2, control_slots=max(16, maximum * 2)) if limits is None else limits
        if type(self.limits) is not AsyncEngineLimits or self.limits.max_inflight != maximum:
            raise ValueError("Async actor input capacity must match the Core request limit")
        if self.limits.result_slots < maximum:
            raise ValueError("Async actor needs one reserved result per Core request")

    def _model_actor_type(self):
        return AsyncCoreModelActor

    def execution_metadata(self):
        return dict(model_batch_mode=self.model_batch_mode,
                    native_engine_abort_supported=self.native_engine_abort_supported)

    def _model_actor_arguments(self):
        return (asdict(self.engine), asdict(self.generation), self.resources.model_batch_rows,
                asdict(self.limits))

    def _model_actor_options(self):
        return dict(num_cpus=1, num_gpus=1, max_restarts=0, max_task_retries=0,
            max_concurrency=self.limits.max_inflight + 2,
            runtime_env=_runtime_env(self.engine.driver_python))

    def submit_core(self, prepared, deadline, maximum, key):
        return self.model_actor.run_core.remote(prepared, deadline, maximum,
                                                (key.session_id, key.sequence))

    def cancel_core(self, key):
        if self.model_actor is not None:
            return self.model_actor.abort_core.remote((key.session_id, key.sequence))

    def acknowledge_core(self, key):
        if self.model_actor is not None:
            return self.model_actor.acknowledge_core.remote((key.session_id, key.sequence))

    def close(self):
        report = super().close()
        report["unconsumed_call_results"] = (report.get("engine_close_report") or {}).get(
            "unconsumed_call_results", [])
        return report
