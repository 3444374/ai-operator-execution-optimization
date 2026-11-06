"""Trusted Job registration and storage grants over the existing task capacity table."""

from dataclasses import dataclass


@dataclass(frozen=True)
class JobBudget:
    held_tasks: int
    input_bytes: int
    result_bytes: int
    active_requests: int
    active_work: int
    max_sessions: int = 1

    def __post_init__(self):
        if any(type(value) is not int or value <= 0 for value in vars(self).values()):
            raise ValueError("Job budgets must be positive integers")


@dataclass(frozen=True, eq=False)
class JobHandle:
    """Process-local capability: copying its visible fields does not grant membership."""

    job_id: str
    label: str


@dataclass
class RegisteredJob:
    handle: JobHandle
    budget: JobBudget
    closing: bool = False


class JobRegistry:
    def __init__(self, capacity, maximum):
        if type(maximum) is not int or maximum <= 0:
            raise ValueError("invalid Job count")
        self.capacity, self.maximum = capacity, maximum
        self.jobs: dict[JobHandle, RegisteredJob] = {}
        self.sequence = 0

    def register(self, label, budget):
        if type(label) is not str or not 0 < len(label.encode()) <= 256:
            raise ValueError("invalid Job label")
        if type(budget) is not JobBudget:
            raise ValueError("invalid Job budget")
        if len(self.jobs) >= self.maximum:
            raise ValueError("Job capacity exhausted")
        for field in ("held_tasks", "input_bytes", "result_bytes"):
            reserved = sum(getattr(job.budget, field) for job in self.jobs.values())
            if reserved + getattr(budget, field) > getattr(self.capacity.limits, field):
                raise ValueError("Job storage grants exceed service capacity")
        for field in ("active_requests", "active_work"):
            if getattr(budget, field) > getattr(self.capacity.limits, field):
                raise ValueError("Job compute limit exceeds service capacity")
        if self.sequence >= 2**64:
            raise OverflowError("Job identity exhausted")
        handle = JobHandle(f"registered-{self.sequence}", label)
        self.sequence += 1
        self.jobs[handle] = RegisteredJob(handle, budget)
        self.capacity.job_limits[handle.job_id] = budget
        return handle

    def require(self, handle, *, joining=False):
        if type(handle) is not JobHandle or handle not in self.jobs:
            raise ValueError("unregistered Job capability")
        job = self.jobs[handle]
        if joining and job.closing:
            raise ValueError("Job is closing")
        return job

    def finish(self, handle):
        job = self.require(handle)
        del self.capacity.job_limits[job.handle.job_id]
        del self.jobs[handle]


def equal_share_job_budget(engine):
    """Initial resource policy: reserve storage shares; do not borrow idle grants."""
    limits, count = engine.capacity.limits, engine.jobs.maximum
    budget = JobBudget(
        held_tasks=limits.held_tasks // count,
        input_bytes=limits.input_bytes // count,
        result_bytes=limits.result_bytes // count,
        active_requests=max(1, limits.active_requests // count),
        active_work=max(1, limits.active_work // count),
    )
    if (
        budget.input_bytes < limits.item_input_bytes
        or budget.result_bytes < limits.item_result_bytes
    ):
        raise ValueError("each Job needs storage for one bounded request and result")
    return budget


@dataclass(frozen=True)
class ReadyJob:
    job_id: str
    session_ids: tuple[int, ...]


@dataclass(frozen=True)
class JobSelectionHistory:
    last_job: str | None
    last_sessions: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class FlowChoice:
    job_id: str
    session_id: int


def round_robin_flow(ready: tuple[ReadyJob, ...], history: JobSelectionHistory) -> FlowChoice:
    """One opportunity per Job, then rotate within that Job's ready flows."""

    def after(values, previous):
        return (
            values[(values.index(previous) + 1) % len(values)] if previous in values else values[0]
        )

    job_id = after(tuple(job.job_id for job in ready), history.last_job)
    job = next(job for job in ready if job.job_id == job_id)
    session_id = after(job.session_ids, dict(history.last_sessions).get(job_id))
    return FlowChoice(job_id, session_id)
