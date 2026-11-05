"""Records I/O, receipt transfer and producer cleanup shared by query hosts.

This adapter owns the existing bounded transport. Core remains responsible for
capacity, and the caller retains query, source and method lifecycles.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict
import inspect
import json
import threading
import time

from src.scheduling.core.session_contract import TaskKey
from src.scheduling.runtime.async_backend import BoundedAsyncBackend, ConfirmedTaskFailure
from src.semantic_methods.lotus.records import CallLedger, identity

from .records_model_adapter import _capacity_failure, _encode, _measure


async def _await_result(value):
    """Wait in the existing transport loop, never in a Ray data thread."""
    return await value if inspect.isawaitable(value) else await asyncio.wrap_future(value.future())


def _check(deadline, cancelled=None):
    if cancelled is not None and cancelled():
        raise InterruptedError("LOTUS Core records cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("LOTUS Core records deadline expired")


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
        if getattr(self.actors, 'nonblocking_core_handoff', False):
            result_ref = self.actors.submit_core(
                prepared, self.deadline, task.task.max_result_bytes, task.key, timings=timings)
        else:
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

    def _acknowledge_result(self, task):
        self.actors.acknowledge_core(task.key)

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
            result = await _await_result(result_ref)
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
            self._acknowledge_result(task)
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


def close_prepared_core(actors, transport, core, capacity, *, deadline, timeout=None, retain_model=False):
    """Close query transport, retaining only a proved drained resident producer.

    Otherwise stop the producer, recover receipts, and retire proved borrowing.
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
    if retain_model:
        remaining = max(0., actors.close_deadline - time.monotonic())
        transport_closed = transport is None or transport.backend.close(timeout=remaining, wait_pending=True)
        if core is not None:
            core.reap(capacity.step_actions)
        if (not transport_closed or transport is not None and transport.retained_refs()
                or core is not None and any(asdict(core.capacity.usage()).values())):
            raise RuntimeError('resident query still owns transport or Core work')
        actor_report = actors.drain_query(actors.close_deadline)
        return dict(model_owner=actor_report, transport_closed=transport_closed,
                    producer_retirement=None, cleanup_errors=actor_report['cleanup_errors'])
    transport_closed = transport is None
    drain_before_release = transport is not None and getattr(transport, '_batch_poll_ack', False)

    def drain_receipts(report):
        nonlocal transport_closed
        if transport is not None:
            errors.extend(transport.recover_actor_receipts(
                (report or {}).get("unconsumed_core_results", [])))
        if core is not None:
            core.reap(capacity.step_actions)
        if transport is not None:
            remaining = max(0., actors.close_deadline - time.monotonic())
            transport_closed = transport.backend.close(timeout=remaining, wait_pending=True)

    try:
        if (transport is not None and getattr(transport, '_batch_poll_ack', False)
                and core is not None and not any(record.compute for record in core.capacity.records.values())):
            # Known local results may still owe their final batched ACK. Drain
            # it before killing the producer; unfinished/unknown work still
            # needs the original producer-first cancellation/recovery below.
            drain_before_release = False
            transport_closed = transport.backend.close(
                timeout=max(0., actors.close_deadline-time.monotonic()), wait_pending=True)
            drain_before_release = not transport_closed
    finally:
        actor_report = (actors.close(before_actor_release=drain_receipts)
                        if drain_before_release else actors.close())
    errors[:0] = actor_report["cleanup_errors"]
    if not drain_before_release:
        drain_receipts(actor_report)
    retirement = None
    if transport is not None:
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
