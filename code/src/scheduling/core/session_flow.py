"""One flow owns intake, delivery leases, cancellation and consumer close."""

from __future__ import annotations

import threading
from dataclasses import replace

from .session_capacity import TaskRecord
from .session_contract import (
    AdvanceResult, CloseReport, Delivery, LeaseId, OfferedTask, OfferResult,
    SessionLimits, SessionSpec, State, TaskKey, Usage, UINT64_MAX, TERMINAL_STATES,
)
from .session_dispatch import _dispatch, _next_record
from .session_jobs import JobHandle
from .task_info import _TaskInfoChecks, validate_task_info

class SchedulingSession:
    """One flow owns accepted tasks, delivery leases and bounded pending metadata checks."""

    def __init__(
        self, engine: SessionEngine, session_id: int, spec: SessionSpec, limits: SessionLimits
    ):
        self.engine, self.session_id, self.spec, self.limits = engine, session_id, spec, limits
        self.state = State.OPEN
        self.error: str | None = None
        self._next_sequence = 0
        self._next_lease = 0
        self._next_batch = 0
        self._pending_members: tuple[TaskKey, ...] = ()
        self._cancel = threading.Event()
        self._closed: CloseReport | None = None
        self._retry_at = 0.0
        self._capacity_waiting: TaskKey | None = None
        self.job: JobHandle | None = None
        self._dispatch_enabled = True
        self._info_checks = _TaskInfoChecks()

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
        self._info_checks.clear()

    def request_cancel(self) -> None:
        """The only session mutation available to another thread is a cancellation flag."""
        self._cancel.set()
        self.engine.wake.notify()

    def _validate_batch(self, tasks: tuple[OfferedTask, ...] | list[OfferedTask]) -> bool:
        if type(tasks) not in (tuple, list) or len(tasks) > self.limits.offer_tasks:
            return False
        self._info_checks.begin()
        empty_usage = Usage()
        for offset, task in enumerate(tasks):
            if type(task) is not OfferedTask or type(task.sequence) is not int:
                return False
            if (
                task.sequence != self._next_sequence + offset
                or not 0 <= task.sequence <= UINT64_MAX
            ):
                return False
            try:
                spec = self.spec.resolve(task.profile_name)
                validate_task_info(task, spec, self.limits.metadata_bytes, self._info_checks)
            except ValueError:
                return False
            job_budget = self.engine.capacity.job_limits.get(self.spec.job_id)
            if type(task.estimated_work) is not int or type(task.max_result_bytes) is not int:
                return False
            if job_budget is not None and task.estimated_work > job_budget.active_work:
                return False
            if type(task.payload) is not bytes or type(task.metadata) is not bytes:
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
            if job_budget is not None and not self.engine.capacity.fits(empty_usage, job_budget, task):
                return False
        return True

    def offer(self, tasks: tuple[OfferedTask, ...] | list[OfferedTask]) -> OfferResult:
        with self.engine._operation():
            self._check_open_handle()
            wake = self.engine.wake
            if self.state != State.OPEN or self._cancel.is_set():
                self._info_checks.clear()
                return OfferResult(0, "REJECTED", "session not open", wake.generation)
            if not self._validate_batch(tasks):
                self._info_checks.clear()
                return OfferResult(0, "REJECTED", "invalid batch", wake.generation)
            # Prepare against a private copy; no user callback or dispatch occurs here.
            capacity = self.engine.capacity
            candidate = dict(capacity.records)
            total, local = capacity.usage(), capacity.usage(self.session_id)
            job_usage = capacity.usage(job_id=self.spec.job_id)
            job_budget = capacity.job_limits.get(self.spec.job_id)
            accepted = 0
            reason = ""
            now = self.engine.clock()
            for task in tasks:
                bounds = [("global", total, capacity.limits), ("session", local, self.limits)]
                if job_budget is not None:
                    bounds.append(("job", job_usage, job_budget))
                reason = next((f"{scope}_{block}" for scope, usage, bound in bounds
                               if (block := capacity.storage_block(usage, bound, task)) is not None), "")
                if reason:
                    break
                key = TaskKey(self.session_id, task.sequence)
                candidate[key] = TaskRecord(key, self.spec.resolve(task.profile_name), task, now)
                total, local, job_usage = (
                    replace(usage,
                            held_tasks=usage.held_tasks + 1,
                            input_bytes=usage.input_bytes + len(task.payload),
                            result_bytes=usage.result_bytes + task.max_result_bytes)
                    for usage in (total, local, job_usage)
                )
                accepted += 1
            status = "ACCEPTED" if accepted == len(tasks) else "BACKPRESSURE"
            result = OfferResult(accepted, status, reason, wake.generation + bool(accepted))
            if self._cancel.is_set():
                self._info_checks.clear()
                return OfferResult(0, "REJECTED", "cancel requested", wake.generation)
            capacity.records = candidate
            self._next_sequence += accepted
            self._info_checks.retain(self._next_sequence)
            if accepted:
                wake.notify()
            return result

    def seal(self) -> None:
        with self.engine._operation():
            self._check_open_handle()
            if self.state == State.OPEN:
                self.state = State.DRAINING
                self._info_checks.clear()
                self.engine.wake.notify()

    def cancel(self, reason: str = "cancelled") -> None:
        with self.engine._operation():
            self._check_open_handle()
            if self.state not in TERMINAL_STATES:
                self.state = State.CANCELLED
            self._info_checks.clear()
            self.request_cancel()

    def _cleanup(self, budget: int, *, cancel_only: bool = False) -> int:
        self._info_checks.clear()
        self._pending_members = ()
        used = 0
        records = self._records()
        if cancel_only:
            records = tuple(r for r in records if r.compute and not r.cancel_sent)
        elif self.engine.preparation is not None:
            # Notify unsent transports before retrying local input borrowing.
            cancellations = tuple(r for r in records if r.compute and not r.cancel_sent)
            local = tuple(r for r in records if not r.compute and r.phase != "LEASED")
            start = next((i + 1 for i, r in enumerate(local)
                          if r.key == self.engine._last_preparation_reap), 0)
            records = cancellations + local[start:] + local[:start]
        for record in records:
            if used >= budget:
                break
            if record.phase == "LEASED":
                continue
            if not record.compute:
                if record.preparation_held:
                    self.engine._last_preparation_reap = record.key
                if self.engine._release_preparation(record):
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
            duration = self.limits.phase_timeout(r.phase)
            if duration is not None and now >= r.since + duration:
                self._fail(
                    {
                        "QUEUED": "capacity wait timed out",
                        "INFLIGHT": "backend wait timed out",
                        "LEASED": "consumer release timed out",
                    }[r.phase]
                )
                return

    def _capacity_ready(self) -> bool:
        """Recheck the blocked member without rerunning selection or backend policy."""
        if self._capacity_waiting is None:
            return True
        record = self.engine.capacity.records.get(self._capacity_waiting)
        return record is None or self.engine.capacity.can_dispatch(record, self.limits)


    def advance(self, max_deliveries: int) -> AdvanceResult:
        with self.engine._operation():
            self._check_open_handle()
            if type(max_deliveries) is not int or not 0 < max_deliveries <= self.limits.held_tasks:
                raise ValueError("invalid delivery bound")
            now = self.engine.clock()
            budget = self.limits.step_actions
            if self.job is not None:
                # Registered flows cannot acquire more dispatch opportunities by polling faster.
                return self._deliver(max_deliveries, now, budget)
            try:
                if self._cancel.is_set() and self.state not in TERMINAL_STATES:
                    self.state = State.CANCELLED
                if self.state not in TERMINAL_STATES:
                    self._expired(now)
                # One bounded poll per call; terminal, dispatch and delivery actions
                # share the step budget. Empty polls never consume local progress.
                budget -= self.engine._poll(budget, reap_preparation=False)
                if self.state in (State.CANCELLED, State.FAILED):
                    budget -= self._cleanup(budget)
                if self.engine.preparation is not None:
                    budget -= self.engine._reap_preparation(budget)
                    prepare_actions = budget // 2
                    if budget == 1 and not self.engine._prepared_candidates(self):
                        prepare_actions = 1
                    budget -= self.engine._prepare_inputs(self, prepare_actions)
                if (self.state not in TERMINAL_STATES and now >= self._retry_at
                        and self._capacity_ready()):
                    while budget and self.state not in TERMINAL_STATES:
                        queued = self.engine._prepared_candidates(self)
                        if not queued:
                            break
                        budget -= 1
                        self._capacity_waiting = None
                        record = _next_record(self, queued)
                        if record is None or not _dispatch(self, record, now):
                            if self._capacity_waiting is None:
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
            tuple(sorted((r for r in records if r.phase == "READY" and not r.preparation_held),
                         key=lambda r: r.ready_order))
            if self.state not in TERMINAL_STATES
            else ()
        )
        selected = ready[: min(maximum, max(0, budget))]
        deliveries = tuple(
            Delivery(
                r.key,
                r.result,
                r.result_metadata,
                LeaseId(self.session_id, self._next_lease + i),
                info=r.task.info,
                member=r.member,
            )
            for i, r in enumerate(selected)
        )
        queued = tuple(r for r in records if r.phase == "QUEUED")
        cleanup = self.state in (State.CANCELLED, State.FAILED) and any(
            r.phase != "LEASED" and (
                (not r.compute and not r.preparation_held) or (r.compute and not r.cancel_sent))
            for r in records
        )
        immediate = bool(
            len(ready) > len(selected)
            or cleanup
            or (
                self._dispatch_enabled
                and queued
                and now >= self._retry_at
                and self._capacity_ready()
                # A reordered member can fit even when the input-order head cannot.
                and self.engine.capacity.any_dispatchable(
                    queued if self.engine.preparation is None else self.engine._prepared_candidates(self), self.limits)
            )
        )
        if self.state == State.DRAINING and not records:
            self.state = State.FINISHED
        reason = None
        if not immediate and not deliveries:
            reason = (
                "WAIT_BACKEND"
                if any(r.compute or r.preparation_held for r in records)
                else "WAIT_RELEASE"
                if any(r.phase == "LEASED" for r in records)
                else "WAIT_CAPACITY"
                if queued
                else "NEED_INPUT"
                if self.state == State.OPEN
                else None
            )
        deadlines = [
            r.since + duration
            for r in records
            if self.state not in TERMINAL_STATES
            and (duration := self.limits.phase_timeout(r.phase)) is not None
        ]
        consumer_timeout = self.limits.phase_timeout("LEASED")
        if selected and consumer_timeout is not None:
            # Include the leases about to transfer, without publishing ownership early.
            deadlines.append(now + consumer_timeout)
        if any(r.compute or r.preparation_held for r in self.engine.capacity.records.values()) or queued:
            deadlines.append(now + self.limits.poll_interval_s)
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
            if not leases:
                # No capacity changed. Publishing a wake here makes a caller's
                # no-delivery progress generation stale before its backend wait.
                return
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
                    self.engine._emit("released", record.key)
            self.engine.wake.notify()

    def set_dispatch_enabled(self, enabled: bool) -> None:
        """Gate dispatch while a producer is filling its accepted organization window."""
        with self.engine._operation():
            self._check_open_handle()
            if type(enabled) is not bool:
                raise ValueError("dispatch gate must be boolean")
            self._dispatch_enabled = enabled
            self.engine.wake.notify()

    def fail(self, reason: str) -> None:
        """Fail this flow without releasing uncertain work or touching another Job."""
        with self.engine._operation():
            self._check_open_handle()
            if type(reason) is not str or not reason:
                raise ValueError("failure reason must be nonempty text")
            self._fail(reason)
            self.engine.wake.notify()

    def close_consumer(self, *, clean: bool = False) -> CloseReport:
        """Close after the consumer has stopped using every delivery, including unpublished ones.

        The caller must join/stop its sender first. Ordinary close keeps delivery leases.
        Remote work remains charged until an authoritative terminal arrives.
        """
        with self.engine._operation():
            if self._closed is not None:
                return self._closed
            for record in self._records():
                if record.phase == "LEASED":
                    del self.engine.capacity.records[record.key]
            if clean and not self._records() and self.state not in TERMINAL_STATES:
                self.state = State.FINISHED
            report = self._close()
            self.engine.wake.notify()  # Released leases may unblock another producer or Job.
            return report

    def close(self) -> CloseReport:
        with self.engine._operation():
            return self._close()

    def _close(self) -> CloseReport:
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
            self.engine._sessions.pop(self.session_id, None)
            if self.job is None:
                self.engine._retired_jobs.add(self.spec.job_id)
            self.engine._finish_retired()
        return report


from .session import SessionEngine
