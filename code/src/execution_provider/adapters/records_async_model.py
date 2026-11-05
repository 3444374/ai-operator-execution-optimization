"""Threaded model-side calls, receipts and private async-engine ownership.

The driver-side Ray owner lives in lotus_records_async_actor. This actor keeps
each prepared call and result until the existing transport acknowledges it.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import threading
import time

from src.semantic_methods.lotus.records import CallLedger
from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_protocol import (
    EngineSpec, GenerationSpec, IDENTITY_FIELDS, _prepared, result_record,
)

from .lotus_records_actor_owner import _deadline
from .records_model_adapter import _capacity_failure, _check_result_capacity, _encode


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
    chain_call: bool = False
    batch_key: tuple | None = None
    poll_exposed: bool = False
    max_result_bytes: int | None = None
    deadline: float | None = None


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
        self._resource_lock = threading.Lock()
        self._resource_sources = None
        self._start_attempted = self._start_failed = self._closing = self._unhealthy = False
        self._held, self._tickets, self._early_cancels = {}, {}, set()
        self._session = -1
        self._cancel_session = -1
        self._readers = 0
        self.last_close_report = None
        self._closed_pending = None

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
            producer_owner_uid=self.engine.snapshot()["owner_uid"],
            model_batch_rows=1, limits=asdict(self.limits), physical_gpu_exit_confirmed=None)

    def snapshot(self, *, observe_resources=False):
        if type(observe_resources) is not bool:
            raise TypeError('observe_resources must be boolean')
        with self._condition:
            value = dict(held_requests=len(self._held),
                reserved_result_bytes=len(self._held) * self.limits.item_result_bytes,
                delivered_results=sum(item.delivered for item in self._held.values()),
                unknown_requests=sum(item.value is not None and not item.confirmed
                                     for item in self._held.values()),
                early_cancellations=len(self._early_cancels), session_id=self._session,
                closing=self._closing, unhealthy=self._unhealthy,
                active_readers=self._readers,
                ipc=None if self.engine is None else self.engine.snapshot())
            if self.engine is not None:
                from .records_native_source import native_sample_age
                ipc = value['ipc']
                age = native_sample_age(ipc.get('native_feedback') if isinstance(ipc, dict) else None,
                    self.engine.ready_report, captured_at_s=time.monotonic())
                if age is not None:
                    value['native_sample_age'] = age
        if observe_resources:
            # The same owned RPC reads the model actor's allocated device, not
            # a driver's potentially different host. Never hold the model's
            # condition while optional NVML/host readers execute.
            value['resources'] = self._resource_snapshot(value['session_id'])
        return value

    def _resource_snapshot(self, session_id):
        import os
        from .records_native_source import PRIVATE_CLOCK
        started = time.monotonic()
        try:
            from src.observability.records_telemetry import MetricSources
            with self._resource_lock:
                if self._resource_sources is None:
                    identity = (self.engine.ready_report or {}).get('native_gpu_identity') or {}
                    binding = identity.get('gpu_uuids')
                    self._resource_sources = MetricSources.from_cuda_visibility(
                        **({} if binding is None else dict(assigned_gpu_uuids=binding)))
                sample = self._resource_sources()
                devices = self._resource_sources.gpu_ids
            return dict(engine_uid=(self.engine.ready_report or {}).get('engine_uid'),
                session_id=session_id, source_elapsed_s=time.monotonic()-started,
                actor_pid=os.getpid(), captured_at_s=time.monotonic(), clock_domain=PRIVATE_CLOCK,
                gpu_uuids=list(devices), sample=sample,
                status='observed_or_partial',
                scope='model actor GPU UUID; cgroup CPU per effective quota or affinity host context; no request capacity estimate')
        except Exception as error:
            return dict(status='unavailable', error_type=type(error).__name__,
                        source_elapsed_s=time.monotonic()-started)

    def drain_query(self, deadline):
        """Prove the previous query released its slots without stopping the model."""
        _deadline(deadline)
        with self._condition:
            while True:
                value = self.snapshot()
                ipc = value['ipc']
                clean = (not value['held_requests'] and not value['active_readers']
                    and not value['early_cancellations'] and ipc is not None
                    and not any(ipc[name] for name in ('held_requests', 'active_requests',
                        'input_bytes', 'reserved_result_bytes', 'ready_results',
                        'queued_commands', 'queued_controls', 'unknown_requests')))
                healthy = (not value['closing'] and not value['unhealthy'] and ipc is not None
                           and not ipc['broken'] and not ipc['closing'] and ipc['child_alive'])
                if clean or not healthy or time.monotonic() >= deadline:
                    return dict(status='retained' if clean and healthy else 'unknown',
                        cleanup_scope='query_slots_drained_model_retained', snapshot=value,
                        model_ready=self._ready_report(), model_retained=clean and healthy,
                        cleanup_errors=[] if clean and healthy else ['resident_query_drain_unconfirmed'])
                self._condition.wait(min(.01, max(0, deadline-time.monotonic())))

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

    def _bounded_call_result(self, value, maximum):
        maximum = (self.limits.item_result_bytes if maximum is None
                   else min(self.limits.item_result_bytes, maximum))
        return json.loads(self._bounded(value, maximum))

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
            if call.chain_call:
                from .lotus_chain_token_budget import audit_chain_call
                audit_chain_call(call.identity, value)
            else:
                CallLedger([call.identity], 1).record(value)
            call.value, call.confirmed = value, event.engine_terminal_confirmed
            if not event.engine_terminal_confirmed:
                self._unhealthy = True
        if events:
            self._condition.notify_all()

    def _submit(self, key, call, row, deadline, generation=None):
        try:
            options = {} if generation is None else {"generation": generation}
            submitted = self.engine.submit(row, deadline_monotonic=deadline, **options)
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
        return self._serve_core(rows, deadline, maximum, request_key)

    def run_prepared_core(self, rows, deadline, maximum, request_key, generation, token_cost=None):
        generation = self._prepared_generation(rows, generation, token_cost)
        return self._serve_core(rows, deadline, maximum, request_key, generation)

    def _prepared_generation(self, rows, generation, token_cost):
        generation = GenerationSpec.from_dict(generation)
        from .lotus_chain_token_budget import audit_chain_call
        if type(rows) is not list or len(rows) != 1:
            raise ValueError("prepared Core calls need one occurrence")
        audit_chain_call(rows[0])
        if token_cost is not None:
            from .lotus_chain_token_budget import verify_cost
            verify_cost(rows[0], generation, token_cost, engine=self.spec, model_ready=self.engine.ready_report)
        return generation

    def try_submit_core(self, rows, deadline, maximum, request_key):
        return self._try_core(rows, deadline, maximum, request_key)

    def try_submit_prepared_core(self, rows, deadline, maximum, request_key, generation, token_cost=None):
        generation = self._prepared_generation(rows, generation, token_cost)
        return self._try_core(rows, deadline, maximum, request_key, generation)

    def _try_core(self, rows, deadline, maximum, request_key, generation=None):
        with self._condition:
            self._readers += 1
        try:
            call, rejected = self._start_core(rows, deadline, maximum, request_key, generation, wait=False)
            if rejected is not None:
                return dict(status='rejected', result=json.loads(rejected))
            if call is None:
                return dict(status='backpressure')
            return dict(status='accepted')
        finally:
            with self._condition:
                self._readers -= 1
                self._condition.notify_all()

    def submit_prepared_batch(self, payload):
        return self._accept_prepared_batch(payload, wait=True)

    def try_submit_prepared_batch(self, payload):
        return self._accept_prepared_batch(payload, wait=False)

    def _accept_prepared_batch(self, payload, *, wait):
        if type(payload) is not bytes or len(payload) > self.limits.input_bytes:
            raise ValueError('bulk RPC exceeds its byte allowance')
        packet = json.loads(payload)
        with self._condition:
            self._readers += 1
        try:
            return self._submit_prepared_batch(packet['calls'], packet['batch_key'], packet['deadline'], wait=wait)
        finally:
            with self._condition:
                self._readers -= 1
                self._condition.notify_all()

    def _submit_prepared_batch(self, calls, batch_key, deadline, *, wait=True):
        """One data RPC/IPC handoff. Results are polled and acknowledged per key."""
        from .lotus_chain_token_budget import audit_chain_call
        _deadline(deadline)
        batch_key = _request_key(batch_key)
        if type(calls) is not list or not 0 < len(calls) <= self.limits.max_inflight:
            raise ValueError('bulk prepared handoff must fit the request grant')
        keys = tuple(_request_key(c['request_key']) for c in calls)
        if len(set(keys)) != len(keys) or any(k[0] != batch_key[0] for k in keys):
            raise ValueError('batch request keys must be distinct in one session')
        generation = GenerationSpec.from_dict(calls[0]['generation'])
        compatible = set()
        for call in calls:
            audit_chain_call(call['row'])
            _check_result_capacity(call['row'], call['maximum'])
            if GenerationSpec.from_dict(call['generation']) != generation:
                raise ValueError('batch generation differs between members')
            compatible.add(tuple(call['row'][name] for name in ('query_id', 'algorithm_stage', 'model_role')))
        if len(compatible) != 1:
            raise ValueError('batch members changed stage compatibility')
        with self._condition:
            slots = min(self.limits.max_inflight, self.limits.result_slots,
                        self.limits.result_bytes//self.limits.item_result_bytes)
            while (len(self._held)+len(calls) > slots or batch_key[0] > self._session and self._held):
                if self._closing or self._unhealthy or time.monotonic() >= deadline:
                    break
                if not wait:
                    return dict(status='backpressure')
                self._condition.wait(min(.01, max(0, deadline-time.monotonic())))
            expired = time.monotonic() >= deadline
            if self._closing or self._unhealthy or self.engine is None or expired or batch_key[0] < self._session:
                return dict(status='rejected', results=[dict(request_key=list(key), result=self._bounded_call_result(
                    self._receipt({n: call['row'][n] for n in IDENTITY_FIELDS}, 'failed', 'BatchNotSubmitted',
                                  expired=expired), call['maximum'])) for key, call in zip(keys, calls)])
            if any(key in self._held for key in keys):
                raise ValueError('batch repeats an owned Core key')
            while True:
                live = [(key, call) for key, call in zip(keys, calls) if key not in self._early_cancels]
                submitted = (self.engine.submit_batch([c['row'] for _, c in live], generation=generation,
                    deadline_monotonic=deadline) if live else None)
                if submitted is None or submitted.status != 'backpressure':
                    break
                if not wait:
                    return dict(status='backpressure')
                if self._closing or self._unhealthy or time.monotonic() >= deadline:
                    break
                self._condition.wait(min(.01, max(0, deadline-time.monotonic())))
            if submitted is not None and submitted.status != 'queued':
                return dict(status='rejected', results=[dict(request_key=list(key), result=self._bounded_call_result(
                    self._receipt({n: call['row'][n] for n in IDENTITY_FIELDS}, 'failed', 'BatchNotSubmitted'),
                    call['maximum'])) for key, call in zip(keys, calls)])
            self._session = batch_key[0]
            self._cancel_session = max(self._cancel_session, self._session)
            self._early_cancels = {key for key in self._early_cancels if key[0] >= self._session}
            tickets = {} if submitted is None else dict(zip((key for key, _ in live), submitted.request_ids, strict=True))
            for key, call in zip(keys, calls):
                identity = {name: call['row'][name] for name in IDENTITY_FIELDS}
                held = self._held[key] = _Call(identity, chain_call=True, batch_key=batch_key,
                                             max_result_bytes=call['maximum'], deadline=deadline)
                if key not in tickets:
                    held.value = self._receipt(identity, 'failed', 'CancelledBeforeSubmission')
                    held.confirmed = True
                else:
                    held.ticket = tickets[key]
                    self._tickets[held.ticket] = key
                    if key in self._early_cancels:
                        held.aborted = True
                        held.abort_queued = self.engine.abort(held.ticket)
                self._early_cancels.discard(key)
            self._condition.notify_all()
            return dict(status='accepted')

    def poll_prepared_batch(self, batch_key):
        batch_key = _request_key(batch_key)
        with self._condition:
            if self.engine is not None and not self._closing:
                self._collect()
                # Nonblocking data calls leave abort retries and late terminal
                # collection with this existing per-member poll/ACK frontier.
                for call in self._held.values():
                    if call.batch_key != batch_key or call.confirmed or call.ticket is None:
                        continue
                    if call.deadline is not None and time.monotonic() >= call.deadline:
                        call.aborted = True
                    if call.aborted and not call.abort_queued:
                        call.abort_queued = self.engine.abort(call.ticket)
            if self._closing and self.last_close_report is not None:
                available = [(tuple(v['request_key']), v['result']) for v in
                    self.last_close_report.get('unconsumed_core_results', [])
                    if tuple(v.get('batch_key', ())) == batch_key and tuple(v['request_key']) in self._closed_pending]
            else:
                available = [(key, call.value) for key, call in self._held.items()
                             if call.batch_key == batch_key and call.value is not None and not call.delivered]
            results, exposed = [], []
            maximum = self.limits.item_result_bytes + self.limits.control_frame_bytes
            closed = self.last_close_report is not None
            empty = _encode(dict(results=[], closed=closed, more_results=False), maximum)
            if not available:
                return empty
            used = len(empty)
            for key, value in available:
                call = self._held.get(key)
                value = self._bounded_call_result(value, None if call is None else call.max_result_bytes)
                candidate = dict(request_key=list(key), result=value)
                try:
                    # Count the exact member and comma once. False reserves the
                    # same larger flag spelling used by the original page probe.
                    size = len(_encode(candidate, maximum-used-(1 if results else 0)))
                except ValueError:
                    if not results:
                        raise
                    break
                used += size + (1 if results else 0)
                results.append(candidate)
                exposed.append(key)
            payload = _encode(dict(results=results, closed=closed, more_results=len(results)<len(available)), maximum)
            for key in exposed:
                if key in self._held:
                    self._held[key].poll_exposed = True
                if self._closed_pending is not None and key in self._closed_pending:
                    self._closed_pending[key] = True
            return payload

    def _serve_core(self, rows, deadline, maximum, request_key, generation=None):
        with self._condition:
            self._readers += 1
        try:
            return self._run_core(rows, deadline, maximum, request_key, generation)
        finally:
            with self._condition:
                self._readers -= 1
                self._condition.notify_all()

    def _run_core(self, rows, deadline, maximum, request_key, generation=None):
        call, rejected = self._start_core(rows, deadline, maximum, request_key, generation, wait=True)
        if rejected is not None:
            return rejected
        key = _request_key(request_key)
        occurrence = call.identity
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
                        self._submit(key, call, rows[0], deadline, generation)
                if call.value is not None:
                    encoded = self._bounded(call.value, maximum)
                    call.delivered = True
                    self._condition.notify_all()
                    return encoded
                if expired:
                    call.aborted = True
                if call.aborted and not call.abort_queued:
                    call.abort_queued = self.engine.abort(call.ticket)
                broken = self.engine.snapshot()["broken"] is not None
                if broken:
                    self._condition.wait(0.01)
            if not broken:
                self.engine.wait(0.01)

    def _start_core(self, rows, deadline, maximum, request_key, generation, *, wait):
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
                    return None, self._bounded(self._receipt(occurrence, "failed", "CapacityUnavailable",
                        expired=time.monotonic() >= deadline), maximum)
                if not wait:
                    return None, None
                self._condition.wait(min(0.01, max(0, deadline - time.monotonic())))
            if self._closing or self._unhealthy or self.engine is None:
                return None, self._bounded(self._receipt(occurrence, "failed", "AsyncOwnerUnavailable"), maximum)
            if key in self._held:
                raise RuntimeError("Core request key is already owned")
            if key[0] < self._session:
                return None, self._bounded(self._receipt(occurrence, "failed", "StaleCoreSession"), maximum)
            if key[0] > self._session:
                self._session = key[0]
                self._cancel_session = max(self._cancel_session, key[0])
                self._early_cancels = {item for item in self._early_cancels if item[0] >= key[0]}
            call = self._held[key] = _Call(occurrence, aborted=key in self._early_cancels,
                chain_call=generation is not None, batch_key=None if wait else key,
                max_result_bytes=None if wait else maximum, deadline=None if wait else deadline)
            expired = time.monotonic() >= deadline
            if call.aborted or expired:
                call.value = self._receipt(occurrence, "failed",
                    "DeadlineExceeded" if expired else "CancelledBeforeSubmission", expired=expired)
                call.confirmed = True
            else:
                self._submit(key, call, rows[0], deadline, generation)
            if not wait and call.value is None and call.ticket is None:
                # Explicit NOT_ACCEPTED: no model work owns this key yet.
                del self._held[key]
                return None, None
            self._early_cancels.discard(key)
            return call, None

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
            closed_ack = self._closed_pending is not None and self._closed_pending.get(key) is True
            if closed_ack:
                del self._closed_pending[key]
            if call is None:
                return closed_ack
            if call.batch_key is not None and call.poll_exposed:
                # Only the consumer ACK establishes delivery of a bulk reply.
                # A lost/failed RPC remains readable without resubmitting work.
                call.delivered = True
            if not call.delivered or not call.confirmed:
                return False
            del self._held[key]
            if call.ticket is not None:
                self._tickets.pop(call.ticket, None)
            self._condition.notify_all()
            return True

    def close(self, timeout):
        _deadline(timeout)
        deadline = time.monotonic() + timeout
        if not self._close_lock.acquire(blocking=False):
            return dict(status="unknown", engine_api_shutdown_confirmed=False,
                        gpu_exit_confirmed=None, cleanup_errors=["close_already_pending"])
        try:
            with self._condition:
                if (self.last_close_report or {}).get("producer_quiescence") is not None:
                    return self.last_close_report
                if self._closed_pending is None:
                    # Delivery metadata only, at most the existing request grant.
                    # Keep the close audit snapshot intact after individual ACKs,
                    # including when producer proof clears the borrowing ledger.
                    self._closed_pending = {key: call.poll_exposed for key, call in self._held.items()
                        if call.batch_key is not None and not call.delivered}
                self._closing = True
                self._condition.notify_all()
                engine = self.engine
                handoff_errors = []
                if engine is not None:
                    self._collect()
                pending = [(key, call) for key, call in self._held.items() if call.value is None]
                adopted = [list(key) for key, call in pending if not call.aborted]
                for key, call in pending:
                    call.aborted = True
                    if call.ticket is None:
                        call.value = self._receipt(call.identity, "failed", "CancelledBeforeSubmission")
                        call.confirmed = True
                # A Core cancel RPC can enter after close. Adopt its still
                # owned work here, and hand off bounded abort controls before
                # native shutdown. Bulk polling stops at the closing gate, so
                # close must collect terminal races while abort is unavailable.
                while engine is not None:
                    self._collect()
                    waiting = False
                    for key, call in pending:
                        if call.value is None and call.ticket is not None and not call.abort_queued:
                            try:
                                call.abort_queued = engine.abort(call.ticket)
                            except Exception as error:
                                handoff_errors.append(type(error).__name__)
                                break
                            waiting = waiting or not call.abort_queued
                    if handoff_errors or not waiting or time.monotonic() >= deadline:
                        break
                    self._condition.wait(min(0.01, max(0, deadline - time.monotonic())))
                handoff = dict(scope="parent_control_queue_only",
                    adopted_request_keys=adopted,
                    queued_request_keys=[list(key) for key, call in pending if call.abort_queued],
                    unqueued_request_keys=[list(key) for key, call in pending
                        if call.value is None and call.ticket is not None and not call.abort_queued],
                    errors=handoff_errors,
                    engine_terminal_confirmed=False)
            if engine is None:
                confirmed = not self._start_attempted
                report = dict(status="closed" if confirmed else "unknown",
                    engine_api_shutdown_confirmed=confirmed, gpu_exit_confirmed=None,
                    cleanup_errors=[] if confirmed else ["startup_cleanup_unconfirmed"],
                    unconsumed_call_results=[], unconsumed_core_results=[])
            else:
                ipc = engine.close(timeout=max(0.001, deadline - time.monotonic()))
                with self._condition:
                    self._collect()
                    self._condition.notify_all()
                    while self._readers and time.monotonic() < deadline:
                        self._condition.wait(min(0.01, max(0, deadline - time.monotonic())))
                    receipts = [dict(call.value) for call in self._held.values() if call.value is not None]
                    keyed = [dict(request_key=list(key),
                                  result=self._bounded_call_result(call.value, call.max_result_bytes),
                                  **(dict(batch_key=list(call.batch_key)) if call.batch_key is not None else {}))
                             for key, call in self._held.items() if call.value is not None]
                    retained = {tuple(v['request_key']): v for v in
                                (self.last_close_report or {}).get('unconsumed_core_results', [])}
                    for receipt in keyed:
                        key = tuple(receipt['request_key'])
                        if key in retained and retained[key] != receipt:
                            raise ValueError('close retry changed an existing audited receipt')
                        retained[key] = receipt
                    keyed = list(retained.values())
                    receipts = [dict(v['result']) for v in keyed]
                settled = ipc.get("status") == "closed"
                report = dict(status="closed" if settled else "unknown",
                    cleanup_scope="async_engine_api_direct_child_and_ipc",
                    engine_api_shutdown_confirmed=settled,
                    gpu_exit_confirmed=None, descendants_exit_confirmed=None,
                    ipc_close=ipc, unconsumed_call_results=receipts,
                    unconsumed_core_results=keyed,
                    cleanup_errors=[] if settled else ["async_ipc_cleanup_unconfirmed"])
                proof = ipc.get("producer_quiescence")
                with self._condition:
                    if (settled and not handoff_errors and self._readers == 0
                            and type(proof) is dict and proof.get("confirmed") is True
                            and proof.get("schema") == "semloom.async_producer_quiescence.v1"
                            and proof.get("owner_uid") == engine.snapshot().get("owner_uid")
                            and proof.get("engine_uid") == (engine.ready_report or {}).get("engine_uid")
                            and all(call.value is not None for call in self._held.values())):
                        issued = proof.get("issued_submissions")
                        if (type(issued) is not int or issued < 0
                                or issued != engine.snapshot().get("issued_submissions")):
                            raise ValueError("producer close has no issued input bound")
                        requests = []
                        for key, call in self._held.items():
                            if call.ticket is not None:
                                prefix, _, number = call.ticket.rpartition(":")
                                if (prefix != proof["owner_uid"] or not number.isdecimal()
                                        or int(number) >= issued):
                                    raise ValueError("producer close changed its Core request set")
                            requests.append(dict(request_key=list(key), call_id=call.identity["call_id"],
                                                 request_id=call.ticket))
                        report["producer_quiescence"] = dict(
                            schema="semloom.async_actor_quiescence.v1", confirmed=True,
                            ipc=proof, input_gate_closed=True, active_readers=0, requests=requests,
                            outcome_confirmation=False)
                        self._held.clear()
                        self._tickets.clear()
                        self._early_cancels.clear()
                    elif self._readers:
                        report.update(status="unknown", engine_api_shutdown_confirmed=False)
                        report["cleanup_errors"].append("async_actor_readers_still_active")
            report["abort_handoff"] = handoff
            if handoff_errors:
                report.update(status="unknown", engine_api_shutdown_confirmed=False)
                report["cleanup_errors"].extend("abort_handoff: " + error for error in handoff_errors)
            self.last_close_report = report
            return report
        finally:
            self._close_lock.release()
