"""Immutable single-flow inputs and bounded backend events; no PG or vendor objects."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, get_args

from .models import OperatorName


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

    def __post_init__(self):
        for name, value in vars(self).items():
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


@dataclass(frozen=True)
class SessionSpec:
    job_id: str
    flow_id: str
    capability: str
    operator: OperatorName = "ai_complete"
    work_unit: str = "work_units"

    def __post_init__(self):
        for value in vars(self).values():
            if type(value) is not str or not value or len(value.encode()) > 256:
                raise ValueError("session identity must contain 1..256 UTF-8 bytes")
        if self.operator not in get_args(OperatorName):
            raise ValueError("unknown scheduling operator")


@dataclass(frozen=True)
class TaskKey:
    session_id: int
    sequence: int


@dataclass(frozen=True)
class OfferedTask:
    sequence: int
    payload: bytes
    estimated_work: int
    max_result_bytes: int
    metadata: bytes = b""


@dataclass(frozen=True)
class BackendTask:
    key: TaskKey
    spec: SessionSpec
    task: OfferedTask


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


class IncrementalBackend(Protocol):
    """All operations must be bounded and nonblocking, including buffer allocation.

    poll returns an exact tuple of at most max_events authoritative terminal events.
    Cancellation is only a request; only poll can confirm the end of remote ownership.
    The adapter must enforce task result/metadata bounds before buffering an event.
    """

    def try_submit(self, task: BackendTask, endpoint: str) -> Submission: ...
    def poll(
        self, handles: tuple[tuple[TaskKey, str | None], ...], max_events: int
    ) -> tuple[Terminal, ...]: ...
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
