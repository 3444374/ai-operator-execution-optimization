"""Immutable single-flow inputs and bounded backend events; no PG or vendor objects."""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from enum import Enum
from typing import Protocol, get_args

from .models import OperatorName
from ...planning.work import WorkDescriptor


MAX_TASK_PROFILES = 32


class State(str, Enum):
    OPEN = "OPEN"
    DRAINING = "DRAINING"
    FINISHED = "FINISHED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class Acceptance(str, Enum):
    ACCEPTED = "ACCEPTED"
    NOT_ACCEPTED = "NOT_ACCEPTED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class SessionTimeouts:
    """Optional phase durations; None leaves that phase to the caller's lifecycle."""

    queue_s: float | None = None
    backend_s: float | None = None
    consumer_s: float | None = None

    def __post_init__(self):
        for name, value in vars(self).items():
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be finite and positive or None")


@dataclass(frozen=True)
class SessionLimits:
    held_tasks: int
    input_bytes: int
    result_bytes: int
    active_requests: int
    active_work: int
    offer_tasks: int
    item_input_bytes: int
    item_result_bytes: int
    metadata_bytes: int
    step_actions: int
    wait_timeout_s: float
    poll_interval_s: float
    timeouts: SessionTimeouts | None = None

    def __post_init__(self):
        for name, value in vars(self).items():
            if name == "timeouts":
                if value is not None and type(value) is not SessionTimeouts:
                    raise ValueError("timeouts must be SessionTimeouts or None")
                continue
            if name.endswith("_s"):
                if (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    or value <= 0
                ):
                    raise ValueError(f"{name} must be finite and positive")
            elif type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

    def phase_timeout(self, phase: str) -> float | None:
        field = {"QUEUED": "queue_s", "INFLIGHT": "backend_s", "LEASED": "consumer_s"}.get(phase)
        if field is None:
            return None
        # Existing callers keep their original all-phase timeout until migrated.
        return self.wait_timeout_s if self.timeouts is None else getattr(self.timeouts, field)


def _identity(value: str) -> None:
    if type(value) is not str or not value or len(value.encode()) > 256:
        raise ValueError("identity must contain 1..256 UTF-8 bytes")


@dataclass(frozen=True)
class TaskProfile:
    """A caller-authorized capability; all profiles share the session's work unit."""

    name: str
    capability: str
    operator: OperatorName = "ai_complete"

    def __post_init__(self):
        for value in (self.name, self.capability, self.operator):
            _identity(value)
        if self.operator not in get_args(OperatorName):
            raise ValueError("unknown scheduling operator")


@dataclass(frozen=True)
class SessionSpec:
    job_id: str
    flow_id: str
    capability: str
    operator: OperatorName = "ai_complete"
    work_unit: str = "work_units"
    task_profiles: tuple[TaskProfile, ...] = ()

    def __post_init__(self):
        for value in (self.job_id, self.flow_id, self.capability, self.operator, self.work_unit):
            _identity(value)
        if type(self.task_profiles) is not tuple or len(self.task_profiles) > MAX_TASK_PROFILES:
            raise ValueError("at most 32 immutable task profiles are allowed")
        if any(type(profile) is not TaskProfile for profile in self.task_profiles):
            raise ValueError("invalid task profile")
        if len({profile.name for profile in self.task_profiles}) != len(self.task_profiles):
            raise ValueError("duplicate task profile")
        if self.operator not in get_args(OperatorName):
            raise ValueError("unknown scheduling operator")

    def resolve(self, profile_name: str | None) -> SessionSpec:
        if profile_name is None:
            return self
        for profile in self.task_profiles:
            if type(profile_name) is str and profile.name == profile_name:
                return replace(self, capability=profile.capability, operator=profile.operator)
        raise ValueError("undeclared task profile")


@dataclass(frozen=True)
class TaskKey:
    session_id: int
    sequence: int


@dataclass(frozen=True)
class TaskInfo:
    """Logical identity survives regrouping; work describes an already authorized stage."""

    call_id: str
    row_sequence: int
    stage_id: str
    work: WorkDescriptor


@dataclass(frozen=True)
class BatchKey:
    session_id: int
    sequence: int


@dataclass(frozen=True)
class BatchMember:
    """An organization batch expands to size independent single-member requests."""

    batch_key: BatchKey
    index: int
    size: int


@dataclass(frozen=True)
class OfferedTask:
    sequence: int
    payload: bytes
    estimated_work: int
    max_result_bytes: int
    metadata: bytes = b""
    profile_name: str | None = None
    info: TaskInfo | None = None


@dataclass(frozen=True)
class BackendTask:
    key: TaskKey
    spec: SessionSpec
    task: OfferedTask
    member: BatchMember | None = None


@dataclass(frozen=True)
class Submission:
    acceptance: Acceptance
    handle: str | None = None


@dataclass(frozen=True)
class Terminal:
    key: TaskKey
    handle: str | None
    status: str
    result: bytes = b""
    metadata: bytes = b""


@dataclass(frozen=True)
class Uncertain:
    """Identified remote work without a terminal; its reservation must remain held."""

    key: TaskKey
    handle: str | None
    code: str = "MODEL_UNAVAILABLE"


class IncrementalBackend(Protocol):
    """All operations must be bounded and nonblocking, including buffer allocation.

    poll returns at most max_events identified terminal or uncertain events.
    Uncertain notices retain remote ownership; only terminals release it.
    Cancellation is only a request; only poll can confirm the end of remote ownership.
    The adapter must enforce task result/metadata bounds before buffering an event.
    """

    def try_submit(self, task: BackendTask, endpoint: str) -> Submission: ...
    def poll(
        self, handles: tuple[tuple[TaskKey, str | None], ...], max_events: int
    ) -> tuple[Terminal | Uncertain, ...]: ...
    def request_cancel(self, key: TaskKey, handle: str | None) -> None: ...


@dataclass(frozen=True)
class LeaseId:
    session_id: int
    ordinal: int


@dataclass(frozen=True)
class Delivery:
    key: TaskKey
    result: bytes
    metadata: bytes
    lease_id: LeaseId
    status: str = "completed"
    info: TaskInfo | None = None
    member: BatchMember | None = None


@dataclass(frozen=True)
class OfferResult:
    accepted_prefix_count: int
    status: str
    reason: str
    generation: int


@dataclass(frozen=True)
class AdvanceResult:
    deliveries: tuple[Delivery, ...]
    state: State
    has_immediate_work: bool
    blocked_reason: str | None
    generation: int
    next_deadline: float | None
    error: str | None


@dataclass(frozen=True)
class Usage:
    held_tasks: int = 0
    input_bytes: int = 0
    result_bytes: int = 0
    active_requests: int = 0
    active_work: int = 0


@dataclass(frozen=True)
class CloseReport:
    status: str
    uncertain_requests: int
    usage: Usage
    error: str | None


@dataclass(frozen=True)
class CleanupReport:
    events: int
    usage: Usage
    error: str | None
    has_immediate_work: bool = False
    next_deadline: float | None = None
    blocked_reason: str | None = None
