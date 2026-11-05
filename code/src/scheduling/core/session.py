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
    BackendTask,
    CleanupReport,
    IncrementalBackend,
    SessionLimits,
    SessionSpec,
    State,
    TaskKey,
    TaskPreparation,
    Terminal,
    Uncertain,
    UINT64_MAX,
    TERMINAL_STATES,
)
from .session_policy import IncrementalCreditPolicy, SessionPolicies
from .session_dispatch import _dispatch, _next_record
from .session_jobs import (
    JobBudget,
    JobHandle,
    JobRegistry,
    ReadyJob,
    JobSelectionHistory,
    FlowChoice,
    round_robin_flow,
)

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
        max_jobs: int = 1,
        observe_capacity_blocks: bool = False,
        preparation: TaskPreparation | None = None,
        choose_flow: Callable[
            [tuple[ReadyJob, ...], JobSelectionHistory], FlowChoice
        ] = round_robin_flow,
    ):
        if credit is not None and not getattr(credit, "incremental_safe", False):
            raise ValueError("credit must qualify local bounded incremental operation")
        if type(observe_capacity_blocks) is not bool:
            raise ValueError('capacity block observation must be boolean')
        self.observe_capacity_blocks = observe_capacity_blocks
        self.capacity = SessionCapacity(limits)
        self.backend, self.policies, self.clock = backend, policies, clock
        self.preparation = preparation
        self._last_preparation_reap = None
        self.credit, self.sink = credit, sink
        self.wake = WakeSignal()
        self.error: str | None = None
        self._sessions: dict[int, SchedulingSession] = {}
        self.jobs = JobRegistry(self.capacity, max_jobs)
        self.choose_flow = choose_flow
        self._last_job = None
        self._last_flow = {}
        self._next_session = 0
        self._next_ready = 0
        self._owner = threading.get_ident()
        self._busy = False
        # Legacy IDs remain until their session and borrowed work are gone.
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

    def register_job(self, label: str, budget: JobBudget) -> JobHandle:
        with self._operation():
            if self.error or any(s.job is None for s in self._sessions.values()):
                raise RuntimeError("engine unavailable for Job registration")
            if any(
                r.spec.job_id not in self.capacity.job_limits
                for r in self.capacity.records.values()
            ):
                raise RuntimeError("legacy work still owns capacity")
            return self.jobs.register(label, budget)

    def close_job(self, job: JobHandle) -> None:
        with self._operation():
            registered = self.jobs.require(job)
            registered.closing = True
            for session in self._sessions.values():
                if session.job is job:
                    session.request_cancel()
            self._finish_retired()
            self.wake.notify()

    def advance(self, max_actions: int | None = None) -> CleanupReport:
        """One owner tick: settle globally, then grant bounded opportunities by Job."""
        with self._operation():
            maximum = self.capacity.limits.step_actions if max_actions is None else max_actions
            if type(maximum) is not int or not 0 < maximum <= self.capacity.limits.step_actions:
                raise ValueError("invalid global action bound")
            if any(s.job is None for s in self._sessions.values()):
                raise RuntimeError("legacy session owns its own advance loop")
            now = self.clock()
            for session in tuple(self._sessions.values()):
                if session._cancel.is_set() and session.state not in TERMINAL_STATES:
                    session.state = State.CANCELLED
                if session.state not in TERMINAL_STATES:
                    session._expired(now)
            polled = self._poll(maximum, reap_preparation=False)
            used = polled
            for session in tuple(self._sessions.values()):
                if session.state in TERMINAL_STATES:
                    # With preparation, notify every retiring flow before
                    # retrying local inputs through the shared reap cursor.
                    used += session._cleanup(max(0, maximum - used),
                                             cancel_only=self.preparation is not None)
            if self.preparation is not None:
                used += self._reap_preparation(maximum - used)
                for session in tuple(self._sessions.values()):
                    remaining = max(0, maximum - used)
                    prepare_actions = remaining // 2
                    if remaining == 1 and not self._prepared_candidates(session):
                        prepare_actions = 1
                    used += self._prepare_inputs(session, prepare_actions)
            while used < maximum and not self.error:
                try:
                    selected = self._select_job_flow(now)
                except Exception:
                    self._fault("Job selection failed")
                    break
                if selected is None:
                    break
                job_id, session = selected
                used += 1
                self._last_job = job_id
                self._last_flow[job_id] = session.session_id
                try:
                    queued = self._prepared_candidates(session)
                    session._capacity_waiting = None
                    record = _next_record(session, queued)
                    if record is None or not _dispatch(session, record, now):
                        if session._capacity_waiting is None:
                            session._retry_at = now + session.limits.poll_interval_s
                except Exception:
                    session._fail("session advancement failed")
                    used += session._cleanup(max(0, maximum - used))
            self._finish_retired()
            return self._progress(used, self.clock(), poll_full=polled == maximum)

    def _progress(self, used, now, *, poll_full=False):
        """Describe the owner's next tick without invoking stateful policies again."""
        # A full bounded poll may leave completed events behind. An empty next poll clears the hint.
        immediate, queued, consumer, deadlines = poll_full, False, False, []
        for session in self._sessions.values():
            records = session._records()
            terminal = session.state in TERMINAL_STATES
            if terminal:
                immediate |= any(
                    r.phase != "LEASED" and (
                        (not r.compute and not r.preparation_held)
                        or (r.compute and not r.cancel_sent))
                    for r in records
                )
                continue
            candidates = []
            for record in records:
                duration = session.limits.phase_timeout(record.phase)
                if duration is not None:
                    deadlines.append(record.since + duration)
                consumer |= record.phase in ("READY", "LEASED")
                if record.phase != "QUEUED":
                    continue
                queued = True
                if self._is_prepared(record):
                    candidates.append(record)
            if candidates and session._dispatch_enabled:
                if now < session._retry_at:
                    deadlines.append(session._retry_at)
                elif (not self.error and session._capacity_ready()
                      and self.capacity.any_dispatchable(candidates, session.limits)):
                    immediate = True
        remote = any(r.compute for r in self.capacity.records.values())
        if remote or queued or any(r.preparation_held for r in self.capacity.records.values()):
            deadlines.append(now + self.capacity.limits.poll_interval_s)
        reason = None if immediate else (
            "WAIT_BACKEND" if remote else "WAIT_CAPACITY" if queued
            else "WAIT_CONSUMER" if consumer else "NEED_INPUT"
        )
        return CleanupReport(used, self.capacity.usage(), self.error, bool(immediate),
                             min(deadlines, default=None), reason)

    def _select_job_flow(self, now):
        ready = {}
        for session in self._sessions.values():
            if (
                session.state in TERMINAL_STATES
                or not session._dispatch_enabled
                or now < session._retry_at
                or not session._capacity_ready()
                or session._cancel.is_set()
            ):
                continue
            records = session._records()
            if self.capacity.any_dispatchable(
                (r for r in records if r.phase == 'QUEUED' and self._is_prepared(r)), session.limits
            ):
                ready.setdefault(session.spec.job_id, []).append(session.session_id)
            elif self.observe_capacity_blocks:
                blocked = next((r for r in records if r.phase == "QUEUED"), None)
                if blocked is not None:
                    self._emit("flow_capacity_blocked", blocked.key)
        if not ready:
            return None
        candidates = tuple(ReadyJob(job, tuple(flows)) for job, flows in ready.items())
        choice = self.choose_flow(
            candidates, JobSelectionHistory(self._last_job, tuple(self._last_flow.items()))
        )
        # Policies select from eligible identities; they cannot create work or bypass capacity.
        if (
            type(choice) is not FlowChoice
            or type(choice.session_id) is not int
            or choice.session_id not in ready.get(choice.job_id, ())
        ):
            raise ValueError("Job policy selected an ineligible flow")
        return choice.job_id, self._sessions[choice.session_id]

    def _prepared_candidates(self, session):
        return tuple(r for r in session._records() if r.phase == "QUEUED" and self._is_prepared(r))

    def _is_prepared(self, record):
        if self.preparation is None:
            return True
        if not record.preparation_held:
            return False
        try:
            ready = self.preparation.is_ready(record.key)
            if type(ready) is not bool:
                raise ValueError('invalid preparation readiness')
            return ready
        except Exception:
            self._fault('preparation readiness unknown')
            return False

    def _prepare_inputs(self, session, maximum):
        if (self.preparation is None or not maximum or self.error
                or session.state in TERMINAL_STATES or session._cancel.is_set()
                or not session._dispatch_enabled):
            return 0
        records = tuple(r for r in session._records()
                        if r.phase == "QUEUED" and not r.preparation_held)[:maximum]
        if not records:
            return 0
        # Borrowing happens before compute admission. Keep the input reservation
        # if the preparation port raises or returns an ambiguous acceptance.
        for record in records:
            record.preparation_held = True
        try:
            accepted = self.preparation.try_prepare(tuple(
                BackendTask(r.key, r.spec, r.task, r.member) for r in records))
            if type(accepted) is not int or not 0 <= accepted <= len(records):
                raise ValueError("invalid preparation acceptance prefix")
        except Exception:
            session._fail("preparation acceptance unknown")
            return len(records)
        for record in records[accepted:]:
            record.preparation_held = False
        for record in records[:accepted]:
            self._emit("preparation_borrowed", record.key)
        return accepted

    def _release_preparation(self, record):
        if not record.preparation_held:
            return True
        try:
            released = self.preparation.release(record.key)
            if type(released) is not bool:
                raise ValueError("invalid preparation release")
        except Exception:
            self._fault("preparation release unknown")
            return False
        if released:
            record.preparation_held = False
            self._emit("preparation_released", record.key)
        return released

    def open(
        self,
        spec: SessionSpec,
        limits: SessionLimits | None = None,
        *,
        job: JobHandle | None = None,
    ) -> SchedulingSession:
        with self._operation():
            if type(spec) is not SessionSpec:
                raise ValueError("invalid session specification")
            if self.error or (job is None and (self._sessions or self.jobs.jobs)):
                raise RuntimeError("engine is occupied or failed")
            if job is not None:
                registered = self.jobs.require(job, joining=True)
                if any(s.job is None for s in self._sessions.values()):
                    raise RuntimeError("cannot mix legacy and registered sessions")
                if (
                    sum(s.job is job for s in self._sessions.values())
                    >= registered.budget.max_sessions
                ):
                    raise ValueError("Job session capacity exhausted")
                spec = replace(spec, job_id=job.job_id)
            limits = limits or self.capacity.limits
            for name, value in vars(limits).items():
                if (
                    name != "timeouts"
                    and not name.endswith("_s")
                    and value > getattr(self.capacity.limits, name)
                ):
                    raise ValueError("session limits exceed engine limits")
            if self._next_session > UINT64_MAX:
                raise OverflowError("session identity exhausted")
            if spec.job_id in self._retired_jobs:
                raise ValueError("job still owns remote work")
            session = SchedulingSession(self, self._next_session, spec, limits)
            self._next_session += 1
            session.job = job
            self._sessions[session.session_id] = session
            return session

    def _fault(self, reason: str) -> None:
        self.error = self.error or reason
        for session in self._sessions.values():
            session._fail(self.error)
        self.wake.notify()

    def _emit(self, kind: str, key: TaskKey) -> None:
        if self.sink is not None and not self.error:
            try:
                self.sink(kind, key)
            except Exception:
                self._fault("observation sink failed")

    def _finish_retired(self) -> None:
        closing = {handle.job_id: handle for handle, registered in self.jobs.jobs.items()
                   if registered.closing}
        retiring = self._retired_jobs.union(closing)
        if not retiring:
            return
        live_jobs = {session.spec.job_id for session in self._sessions.values()}
        retiring.difference_update(live_jobs)
        if not retiring:
            return
        held_jobs = {record.spec.job_id for record in self.capacity.records.values()}
        for job in retiring:
            if job in held_jobs:
                continue
            handle = closing.get(job)
            try:
                if self.credit:
                    self.credit.finish_job(job)
                    self.credit.forget_finished_job(job)
            except Exception:
                self._fault("credit finish failed")
            finally:
                if handle is None:
                    self._retired_jobs.remove(job)
                else:
                    self.jobs.finish(handle)
                    self._last_flow.pop(job, None)

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
        preparation_released = self._release_preparation(record)
        valid = (
            type(event.result) is bytes
            and type(event.metadata) is bytes
            and len(event.result) <= record.task.max_result_bytes
            and len(event.metadata) <= self.capacity.limits.metadata_bytes
        )
        session = self._sessions.get(event.key.session_id)
        owned = session is not None
        if (
            owned
            and type(event.metadata) is bytes
            and len(event.metadata) > session.limits.metadata_bytes
        ):
            valid = False
        if not valid:
            if record.spec.job_id in self.capacity.job_limits:
                if owned:
                    session._fail("backend result violates bounds")
            else:
                self._fault("backend result violates bounds")
        if owned and session.state not in TERMINAL_STATES and valid and event.status == "completed":
            if preparation_released:
                record.task = replace(record.task, payload=b"", metadata=b"")
            record.result, record.result_metadata = event.result, event.metadata
            record.phase = "READY"
            record.ready_order = self._next_ready
            self._next_ready += 1
            record.since = self.clock()
        else:
            if preparation_released:
                del self.capacity.records[event.key]
            if owned and session.state not in TERMINAL_STATES:
                session._fail("backend task failed")
        self._emit("terminal", event.key)
        self.wake.notify()

    def _uncertain(self, event: Uncertain) -> None:
        record = self.capacity.records.get(event.key)
        if (
            record is None
            or not record.compute
            or record.handle != event.handle
            or event.code not in ("MODEL_UNAVAILABLE", "MODEL_TIMEOUT")
        ):
            self._fault("backend uncertainty identity conflict")
            return
        record.phase = "UNCERTAIN"
        session = self._sessions.get(event.key.session_id)
        if session is not None:
            session._fail(event.code)
        self._emit("uncertain", event.key)
        self.wake.notify()

    @staticmethod
    def _request_id(record: TaskRecord) -> str:
        return f"{record.key.session_id}:{record.key.sequence}"

    def _poll(self, maximum: int, *, reap_preparation: bool = True) -> int:
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
                type(event) not in (Terminal, Uncertain)
                or type(event.key) is not TaskKey
                or type(event.key.session_id) is not int
                or type(event.key.sequence) is not int
                or not 0 <= event.key.session_id <= UINT64_MAX
                or not 0 <= event.key.sequence <= UINT64_MAX
            ):
                self._fault("backend terminal invalid")
            elif type(event) is Uncertain:
                self._uncertain(event)
            else:
                self._terminal(event)
        count = len(events)
        if reap_preparation:
            count += self._reap_preparation(maximum - count)
        self._finish_retired()
        return count

    def _reap_preparation(self, maximum: int) -> int:
        if self.preparation is None or maximum <= 0:
            return 0
        # Closed consumers may leave local preparation borrowing an input,
        # just as accepted remote work can outlive its session handle.
        candidates = []
        for record in self.capacity.records.values():
            session = self._sessions.get(record.key.session_id)
            if (not record.compute and record.phase != 'LEASED' and (
                    session is None or session.state in TERMINAL_STATES
                    or (record.phase == 'READY' and record.preparation_held))):
                candidates.append(record)
        # A retained early input must not repeatedly hide later confirmed ones.
        start = next((i + 1 for i, r in enumerate(candidates)
                      if r.key == self._last_preparation_reap), 0)
        selected = (candidates[start:] + candidates[:start])[:maximum]
        for record in selected:
            self._last_preparation_reap = record.key
            session = self._sessions.get(record.key.session_id)
            if self._release_preparation(record):
                if session is None or session.state in TERMINAL_STATES:
                    del self.capacity.records[record.key]
                else:
                    record.task = replace(record.task, payload=b'', metadata=b'')
        self._finish_retired()
        return len(selected)

    def reap(self, max_events: int) -> CleanupReport:
        with self._operation():
            if (
                type(max_events) is not int
                or not 0 < max_events <= self.capacity.limits.step_actions
            ):
                raise ValueError("invalid reap event bound")
            count = self._poll(max_events)
            return CleanupReport(count, self.capacity.usage(), self.error)


# Both classes are defined before linking the service owner and its flow.
from .session_flow import SchedulingSession
