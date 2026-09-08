"""One bounded table owns input, result and compute reservations across sessions."""

from __future__ import annotations

from dataclasses import dataclass

from .session_contract import OfferedTask, SessionLimits, SessionSpec, TaskKey, Usage


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


class SessionCapacity:
    """Derive counters from live records, avoiding a second independently settled ledger."""

    def __init__(self, limits: SessionLimits):
        self.limits = limits
        self.records: dict[TaskKey, TaskRecord] = {}

    def usage(self, session_id: int | None = None) -> Usage:
        records = tuple(
            r for r in self.records.values() if session_id is None or r.key.session_id == session_id
        )
        return Usage(
            len(records),
            sum(len(r.task.payload) for r in records),
            sum(r.task.max_result_bytes for r in records),
            sum(r.compute for r in records),
            sum(r.task.estimated_work for r in records if r.compute),
        )

    @staticmethod
    def fits(usage: Usage, limits: SessionLimits, task: OfferedTask) -> bool:
        return (
            usage.held_tasks + 1 <= limits.held_tasks
            and usage.input_bytes + len(task.payload) <= limits.input_bytes
            and usage.result_bytes + task.max_result_bytes <= limits.result_bytes
        )

    def can_dispatch(self, record: TaskRecord, limits: SessionLimits) -> bool:
        for usage, bound in (
            (self.usage(), self.limits),
            (self.usage(record.key.session_id), limits),
        ):
            if (
                usage.active_requests >= bound.active_requests
                or usage.active_work + record.task.estimated_work > bound.active_work
            ):
                return False
        return True
