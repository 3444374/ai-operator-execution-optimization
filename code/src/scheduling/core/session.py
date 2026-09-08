"""Bounded, single-control-thread sessions with incremental delivery and remote draining.

Only a nonblocking backend and local bounded policies/sink may be supplied. This
module never waits for producer input or remote work and retains no result history.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from typing import Callable

from .session_capacity import SessionCapacity, TaskRecord
from .session_contract import (
    Acceptance,
    AdvanceResult,
    BackendTask,
    CleanupReport,
    CloseReport,
    Delivery,
    IncrementalBackend,
    LeaseId,
    OfferedTask,
    OfferResult,
    SessionLimits,
    SessionSpec,
    State,
    Submission,
    TaskKey,
    Terminal,
)
from .session_policy import IncrementalCreditPolicy, SessionPolicies

UINT64_MAX = (1 << 64) - 1
TERMINAL_STATES = (State.CANCELLED, State.FAILED, State.FINISHED)


class WakeSignal:
    """A generation check and wait share one lock, so notifications cannot be lost."""

    def __init__(self):
        self._condition = threading.Condition()
        self._generation = 0

    @property
    def generation(self) -> int:
        with self._condition:
            return self._generation

    def notify(self) -> None:
        with self._condition:
            self._generation += 1
            self._condition.notify_all()

    def wait(self, generation: int, timeout: float | None) -> int:
        with self._condition:
            if self._generation == generation:
                self._condition.wait(timeout)
            return self._generation


class SessionEngine:
    """Own service capacity across consecutive sessions, including uncertain work."""

    def __init__(
        self,
        limits: SessionLimits,
        backend: IncrementalBackend,
        policies: SessionPolicies,
        *,
        clock: Callable[[], float] = time.monotonic,
        credit: IncrementalCreditPolicy | None = None,
        sink: Callable[[str, TaskKey], None] | None = None,
    ):
        if credit is not None and not getattr(credit, "incremental_safe", False):
            raise ValueError("credit must qualify local bounded incremental operation")
        self.capacity = SessionCapacity(limits)
        self.backend, self.policies, self.clock = backend, policies, clock
        self.credit, self.sink = credit, sink
        self.wake = WakeSignal()
        self.error: str | None = None
        self._session: SchedulingSession | None = None
        self._next_session = 0
        self._next_ready = 0
        self._owner = threading.get_ident()
        self._busy = False
        self._retired_jobs: set[str] = set()

    @contextmanager
    def _operation(self):
        if threading.get_ident() != self._owner or self._busy:
            raise RuntimeError("session operations require one non-reentrant control thread")
        self._busy = True
        try:
            yield
        finally:
            self._busy = False

    def open(self, spec: SessionSpec, limits: SessionLimits | None = None) -> SchedulingSession:
        with self._operation():
            if type(spec) is not SessionSpec:
                raise ValueError("invalid session specification")
            if self._session is not None or self.error:
                raise RuntimeError("engine is occupied or failed")
            limits = limits or self.capacity.limits
            for name, value in vars(limits).items():
                if not name.endswith("_s") and value > getattr(self.capacity.limits, name):
                    raise ValueError("session limits exceed engine limits")
            if self._next_session > UINT64_MAX:
                raise OverflowError("session identity exhausted")
            if spec.job_id in self._retired_jobs:
                raise ValueError("job still owns remote work")
            session = SchedulingSession(self, self._next_session, spec, limits)
            self._next_session += 1
            self._session = session
            return session

    def _fault(self, reason: str) -> None:
        self.error = self.error or reason
        if self._session:
            self._session._fail(self.error)
        self.wake.notify()

    def _emit(self, kind: str, key: TaskKey) -> None:
        if self.sink is not None and not self.error:
            try:
                self.sink(kind, key)
            except Exception:
                self._fault("observation sink failed")

    def _finish_retired(self) -> None:
        for job in tuple(self._retired_jobs):
            if any(r.spec.job_id == job for r in self.capacity.records.values()):
                continue
            try:
                if self.credit:
                    self.credit.finish_job(job)
                    self.credit.forget_finished_job(job)
            except Exception:
                self._fault("credit finish failed")
            finally:
                self._retired_jobs.remove(job)

    def _terminal(self, event: Terminal) -> None:
        record = self.capacity.records.get(event.key)
        if (
            record is None
            or not record.compute
            or record.handle != event.handle
            or event.status not in ("completed", "failed", "cancelled")
        ):
            self._fault("backend terminal identity conflict")
            return
        # The authoritative terminal, including a malformed bounded result, ends
        # remote borrowing. No other path releases an accepted compute reservation.
        record.compute = False
        if record.credit:
            record.credit = False
            try:
                self.credit.release(self._request_id(record), job_id=record.spec.job_id)
            except Exception:
                self._fault("credit release failed")
        valid = (
            type(event.result) is bytes
            and type(event.metadata) is bytes
            and len(event.result) <= record.task.max_result_bytes
            and len(event.metadata) <= self.capacity.limits.metadata_bytes
        )
        session = self._session
        owned = session is not None and session.session_id == event.key.session_id
        if (
            owned
            and type(event.metadata) is bytes
            and len(event.metadata) > session.limits.metadata_bytes
        ):
            valid = False
        if not valid:
            self._fault("backend result violates bounds")
        if owned and session.state not in TERMINAL_STATES and valid and event.status == "completed":
            record.task = replace(record.task, payload=b"", metadata=b"")
            record.result, record.result_metadata = event.result, event.metadata
            record.phase = "READY"
            record.ready_order = self._next_ready
            self._next_ready += 1
            record.since = self.clock()
        else:
            del self.capacity.records[event.key]
            if owned and session.state not in TERMINAL_STATES:
                session._fail("backend task failed")
        self._emit("terminal", event.key)
        self.wake.notify()

    @staticmethod
    def _request_id(record: TaskRecord) -> str:
        return f"{record.key.session_id}:{record.key.sequence}"

    def _poll(self, maximum: int) -> int:
        handles = tuple((r.key, r.handle) for r in self.capacity.records.values() if r.compute)
        if maximum <= 0:
            return 0
        try:
            events = self.backend.poll(handles, maximum)
        except Exception:
            self._fault("backend poll failed")
            return 0
        if type(events) is not tuple or len(events) > maximum:
            self._fault("backend poll violates event bound")
            return 0
        for event in events:
            if (
                type(event) is not Terminal
                or type(event.key) is not TaskKey
                or type(event.key.session_id) is not int
                or type(event.key.sequence) is not int
                or not 0 <= event.key.session_id <= UINT64_MAX
                or not 0 <= event.key.sequence <= UINT64_MAX
            ):
                self._fault("backend terminal invalid")
            else:
                self._terminal(event)
        self._finish_retired()
        return len(events)

    def reap(self, max_events: int) -> CleanupReport:
        with self._operation():
            if (
                type(max_events) is not int
                or not 0 < max_events <= self.capacity.limits.step_actions
            ):
                raise ValueError("invalid reap event bound")
            count = self._poll(max_events)
            return CleanupReport(count, self.capacity.usage(), self.error)


class SchedulingSession:
    """One flow owns only accepted tasks and outstanding delivery leases."""

    def __init__(
        self, engine: SessionEngine, session_id: int, spec: SessionSpec, limits: SessionLimits
    ):
        self.engine, self.session_id, self.spec, self.limits = engine, session_id, spec, limits
        self.state = State.OPEN
        self.error: str | None = None
        self._next_sequence = 0
        self._next_lease = 0
        self._cancel = threading.Event()
        self._closed: CloseReport | None = None
        self._retry_at = 0.0

    def _records(self) -> tuple[TaskRecord, ...]:
        return tuple(
            r for r in self.engine.capacity.records.values() if r.key.session_id == self.session_id
        )

    def _check_open_handle(self) -> None:
        if self._closed is not None:
            raise RuntimeError("session is closed")

    def _fail(self, reason: str) -> None:
        self.error = self.error or reason
        self.state = State.FAILED

    def request_cancel(self) -> None:
        """The only session mutation available to another thread is a cancellation flag."""
        self._cancel.set()
        self.engine.wake.notify()

    def _validate_batch(self, tasks: tuple[OfferedTask, ...] | list[OfferedTask]) -> bool:
        if type(tasks) not in (tuple, list) or len(tasks) > self.limits.offer_tasks:
            return False
        for offset, task in enumerate(tasks):
            if type(task) is not OfferedTask or type(task.sequence) is not int:
                return False
            if (
                task.sequence != self._next_sequence + offset
                or not 0 <= task.sequence <= UINT64_MAX
            ):
                return False
            if type(task.payload) is not bytes or type(task.metadata) is not bytes:
                return False
            if type(task.estimated_work) is not int or type(task.max_result_bytes) is not int:
                return False
            if (
                not 0 < task.estimated_work <= self.limits.active_work
                or not 0 < task.max_result_bytes
            ):
                return False
            if (
                len(task.payload) > min(self.limits.input_bytes, self.limits.item_input_bytes)
                or task.max_result_bytes
                > min(self.limits.result_bytes, self.limits.item_result_bytes)
                or len(task.metadata) > self.limits.metadata_bytes
            ):
                return False
        return True

    def offer(self, tasks: tuple[OfferedTask, ...] | list[OfferedTask]) -> OfferResult:
        with self.engine._operation():
            self._check_open_handle()
            wake = self.engine.wake
            if self.state != State.OPEN or self._cancel.is_set():
                return OfferResult(0, "REJECTED", "session not open", wake.generation)
            if not self._validate_batch(tasks):
                return OfferResult(0, "REJECTED", "invalid batch", wake.generation)
            # Prepare against a private copy; no user callback or dispatch occurs here.
            capacity = self.engine.capacity
            candidate = dict(capacity.records)
            total, local = capacity.usage(), capacity.usage(self.session_id)
            accepted = 0
            now = self.engine.clock()
            for task in tasks:
                if not capacity.fits(total, capacity.limits, task) or not capacity.fits(
                    local, self.limits, task
                ):
                    break
                key = TaskKey(self.session_id, task.sequence)
                candidate[key] = TaskRecord(key, self.spec, task, now)
                delta = dict(
                    held_tasks=local.held_tasks + 1,
                    input_bytes=local.input_bytes + len(task.payload),
                    result_bytes=local.result_bytes + task.max_result_bytes,
                )
                local = replace(local, **delta)
                total = replace(
                    total,
                    held_tasks=total.held_tasks + 1,
                    input_bytes=total.input_bytes + len(task.payload),
                    result_bytes=total.result_bytes + task.max_result_bytes,
                )
                accepted += 1
            status = "ACCEPTED" if accepted == len(tasks) else "BACKPRESSURE"
            result = OfferResult(accepted, status, "", wake.generation + 1)
            if self._cancel.is_set():
                return OfferResult(0, "REJECTED", "cancel requested", wake.generation)
            capacity.records = candidate
            self._next_sequence += accepted
            wake.notify()
            return result

    def seal(self) -> None:
        with self.engine._operation():
            self._check_open_handle()
            if self.state == State.OPEN:
                self.state = State.DRAINING
                self.engine.wake.notify()

    def cancel(self, reason: str = "cancelled") -> None:
        with self.engine._operation():
            self._check_open_handle()
            if self.state not in TERMINAL_STATES:
                self.state = State.CANCELLED
            self.request_cancel()

    def _cleanup(self, budget: int) -> int:
        used = 0
        for record in self._records():
            if used >= budget:
                break
            if record.phase == "LEASED":
                continue
            if not record.compute:
                del self.engine.capacity.records[record.key]
                used += 1
            elif not record.cancel_sent:
                record.cancel_sent = True
                record.phase = "UNCERTAIN"
                try:
                    self.engine.backend.request_cancel(record.key, record.handle)
                except Exception:
                    pass  # Neither an exception nor a cancellation request is a terminal.
                used += 1
        return used

    def _expired(self, now: float) -> None:
        for r in self._records():
            if (
                r.phase in ("QUEUED", "INFLIGHT", "LEASED")
                and now >= r.since + self.limits.wait_timeout_s
            ):
                self._fail(
                    {
                        "QUEUED": "capacity wait timed out",
                        "INFLIGHT": "backend wait timed out",
                        "LEASED": "consumer release timed out",
                    }[r.phase]
                )
                return

    def _dispatch(self, record: TaskRecord, now: float) -> bool:
        engine = self.engine
        if not engine.capacity.can_dispatch(record, self.limits):
            return False
        endpoint = engine.policies.select(
            record, tuple(r for r in engine.capacity.records.values() if r.compute), now
        )
        if endpoint is None:
            return False
        record.endpoint = endpoint
        if engine.credit:
            record.credit = engine.credit.try_acquire(
                request_id=engine._request_id(record),
                job_id=self.spec.job_id,
                endpoint_id=endpoint,
                estimated_work=record.task.estimated_work,
            )
            if not record.credit:
                # A denied waiter is transient; no hidden unbounded ready queue remains.
                engine.credit.cancel_waiter(engine._request_id(record), job_id=self.spec.job_id)
                return False
        if self._cancel.is_set():
            if record.credit:
                record.credit = False
                engine.credit.release(engine._request_id(record), job_id=self.spec.job_id)
            self.state = State.CANCELLED
            return False
        queued_since = record.since
        record.compute = True
        record.phase = "INFLIGHT"
        record.since = now
        try:
            outcome = engine.backend.try_submit(
                BackendTask(record.key, self.spec, record.task), endpoint
            )
        except Exception:
            outcome = Submission(Acceptance.UNKNOWN)
        if (
            type(outcome) is not Submission
            or type(outcome.acceptance) is not Acceptance
            or (outcome.acceptance == Acceptance.NOT_ACCEPTED and outcome.handle is not None)
            or (
                outcome.handle is not None
                and (
                    type(outcome.handle) is not str
                    or not outcome.handle
                    or len(outcome.handle.encode()) > self.limits.metadata_bytes
                )
            )
        ):
            outcome = Submission(Acceptance.UNKNOWN)
        if outcome.acceptance == Acceptance.NOT_ACCEPTED:
            record.compute = False
            record.phase = "QUEUED"
            record.since = queued_since
            if record.credit:
                record.credit = False
                engine.credit.release(engine._request_id(record), job_id=self.spec.job_id)
            return False
        record.handle = outcome.handle
        if outcome.acceptance == Acceptance.UNKNOWN or outcome.handle is None:
            record.phase = "UNCERTAIN"
            self._fail("backend acceptance unknown")
        engine._emit("submitted", record.key)
        return True

    def advance(self, max_deliveries: int) -> AdvanceResult:
        with self.engine._operation():
            self._check_open_handle()
            if type(max_deliveries) is not int or not 0 < max_deliveries <= self.limits.held_tasks:
                raise ValueError("invalid delivery bound")
            now = self.engine.clock()
            budget = self.limits.step_actions
            try:
                if self._cancel.is_set() and self.state not in TERMINAL_STATES:
                    self.state = State.CANCELLED
                if self.state not in TERMINAL_STATES:
                    self._expired(now)
                # One bounded poll per call; terminal, dispatch and delivery actions
                # share the step budget. Empty polls never consume local progress.
                budget -= self.engine._poll(budget)
                if self.state in (State.CANCELLED, State.FAILED):
                    self._cleanup(budget)
                elif now >= self._retry_at:
                    while budget and self.state not in TERMINAL_STATES:
                        queued = tuple(r for r in self._records() if r.phase == "QUEUED")
                        if not queued:
                            break
                        budget -= 1
                        record = self.engine.policies.select_task(queued)
                        if record is None or not self._dispatch(record, now):
                            self._retry_at = now + self.limits.poll_interval_s
                            break
                return self._deliver(max_deliveries, now, budget)
            except Exception:
                self._fail("session advancement failed")
                self._cleanup(max(0, budget))
                raise

    def _deliver(self, maximum: int, now: float, budget: int) -> AdvanceResult:
        records = self._records()
        if self._cancel.is_set() and self.state not in TERMINAL_STATES:
            self.state = State.CANCELLED
        ready = (
            tuple(sorted((r for r in records if r.phase == "READY"), key=lambda r: r.ready_order))
            if self.state not in TERMINAL_STATES
            else ()
        )
        selected = ready[: min(maximum, max(0, budget))]
        deliveries = tuple(
            Delivery(
                r.key, r.result, r.result_metadata, LeaseId(self.session_id, self._next_lease + i)
            )
            for i, r in enumerate(selected)
        )
        queued = tuple(r for r in records if r.phase == "QUEUED")
        cleanup = self.state in (State.CANCELLED, State.FAILED) and any(
            r.phase != "LEASED" and (not r.compute or not r.cancel_sent) for r in records
        )
        immediate = bool(
            len(ready) > len(selected)
            or cleanup
            or (
                queued
                and now >= self._retry_at
                and self.engine.capacity.can_dispatch(queued[0], self.limits)
            )
        )
        if self.state == State.DRAINING and not records:
            self.state = State.FINISHED
        reason = None
        if not immediate and not deliveries:
            reason = (
                "WAIT_BACKEND"
                if any(r.compute for r in records)
                else "WAIT_RELEASE"
                if any(r.phase == "LEASED" for r in records)
                else "WAIT_CAPACITY"
                if queued
                else "NEED_INPUT"
                if self.state == State.OPEN
                else None
            )
        deadlines = [
            r.since + self.limits.wait_timeout_s
            for r in records
            if r.phase in ("QUEUED", "INFLIGHT", "LEASED") and self.state not in TERMINAL_STATES
        ]
        if any(r.compute for r in self.engine.capacity.records.values()) or queued:
            deadlines.append(now + self.limits.poll_interval_s)
        if selected:
            deadlines.append(now + self.limits.wait_timeout_s)
        result = AdvanceResult(
            deliveries,
            self.state,
            immediate,
            reason,
            self.engine.wake.generation,
            min(deadlines, default=None),
            self.error,
        )
        next_lease = self._next_lease + len(selected)
        transfers = tuple(zip(selected, deliveries))
        # No backend, policy, clock or sink call may occur after this transfer.
        try:
            for record, delivery in transfers:
                record.phase, record.lease, record.since = "LEASED", delivery.lease_id.ordinal, now
        except BaseException:
            for record in selected:
                record.phase, record.lease = "READY", None
            raise
        self._next_lease = next_lease
        return result

    def release(self, leases: tuple[LeaseId, ...] | list[LeaseId]) -> None:
        with self.engine._operation():
            self._check_open_handle()
            if type(leases) not in (tuple, list) or len(leases) > self.limits.held_tasks:
                raise ValueError("invalid release batch")
            for lease in leases:
                if (
                    type(lease) is not LeaseId
                    or type(lease.session_id) is not int
                    or lease.session_id != self.session_id
                    or type(lease.ordinal) is not int
                    or not 0 <= lease.ordinal < self._next_lease
                ):
                    raise ValueError("unknown lease")
            ordinals = {lease.ordinal for lease in leases}
            for record in self._records():
                if record.phase == "LEASED" and record.lease in ordinals:
                    del self.engine.capacity.records[record.key]
            self.engine.wake.notify()

    def close(self) -> CloseReport:
        with self.engine._operation():
            if self._closed is not None:
                return self._closed
            if self.state not in TERMINAL_STATES:
                self.state = State.CANCELLED
            self._cleanup(self.limits.held_tasks)
            records = self._records()
            usage = self.engine.capacity.usage(self.session_id)
            report = CloseReport(
                "WAITING_FOR_RELEASE" if any(r.phase == "LEASED" for r in records) else "CLOSED",
                sum(r.compute for r in records),
                usage,
                self.error,
            )
            if report.status == "CLOSED":
                self._closed = report
                self.engine._session = None
                self.engine._retired_jobs.add(self.spec.job_id)
                self.engine._finish_retired()
            return report
