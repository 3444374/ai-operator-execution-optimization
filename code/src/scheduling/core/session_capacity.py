"""One bounded table owns input, result and compute reservations across sessions."""

from __future__ import annotations

from dataclasses import dataclass

from .session_contract import BatchMember, OfferedTask, SessionLimits, SessionSpec, TaskKey, Usage


@dataclass
class TaskRecord:
    key: TaskKey
    spec: SessionSpec
    task: OfferedTask
    since: float
    phase: str = "QUEUED"
    endpoint: str | None = None
    handle: str | None = None
    compute: bool = False
    credit: bool = False
    cancel_sent: bool = False
    result: bytes = b""
    result_metadata: bytes = b""
    lease: int | None = None
    ready_order: int = 0
    member: BatchMember | None = None
    preparation_held: bool = False


class SessionCapacity:
    """Derive counters from live records, avoiding a second independently settled ledger."""

    def __init__(self, limits: SessionLimits):
        self.limits = limits
        self.records: dict[TaskKey, TaskRecord] = {}
        self.job_limits = {}

    def usage(self, session_id: int | None = None, *, job_id: str | None = None) -> Usage:
        held = input_bytes = result_bytes = active_requests = active_work = 0
        for record in self.records.values():
            if session_id is not None and record.key.session_id != session_id:
                continue
            if job_id is not None and record.spec.job_id != job_id:
                continue
            held += 1
            input_bytes += len(record.task.payload)
            result_bytes += record.task.max_result_bytes
            if record.compute:
                active_requests += 1
                active_work += record.task.estimated_work
        return Usage(held, input_bytes, result_bytes, active_requests, active_work)

    @staticmethod
    def fits(usage: Usage, limits: SessionLimits, task: OfferedTask) -> bool:
        return SessionCapacity.storage_block(usage, limits, task) is None

    @staticmethod
    def storage_block(usage, limits, task):
        for name, amount in (("held_tasks", 1), ("input_bytes", len(task.payload)),
                             ("result_bytes", task.max_result_bytes)):
            if getattr(usage, name) + amount > getattr(limits, name):
                return name
        return None

    def can_dispatch(self, record: TaskRecord, limits: SessionLimits) -> bool:
        return self.any_dispatchable((record,), limits)

    def any_dispatchable(self, records, limits: SessionLimits) -> bool:
        """One read-only candidate scan; reuse derived usage only within this call.

        The owner cannot mutate task state during this scan. No summary survives
        a return, submission, terminal, cancellation or subsequent owner tick.
        """
        global_bound = (self.usage(), self.limits)
        if global_bound[0].active_requests >= self.limits.active_requests:
            return False
        scoped = {}
        for record in records:
            if global_bound[0].active_work + record.task.estimated_work > self.limits.active_work:
                continue
            key = (record.key.session_id, record.spec.job_id)
            if key not in scoped:
                bounds = [global_bound, (self.usage(record.key.session_id), limits)]
                if record.spec.job_id in self.job_limits:
                    bounds.append((self.usage(job_id=record.spec.job_id),
                                   self.job_limits[record.spec.job_id]))
                scoped[key] = bounds
            if all(usage.active_requests < bound.active_requests
                   and usage.active_work + record.task.estimated_work <= bound.active_work
                   for usage, bound in scoped[key]):
                return True
        return False
