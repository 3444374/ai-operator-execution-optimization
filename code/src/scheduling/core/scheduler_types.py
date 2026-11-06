"""Public configuration, results and adapter interface for synchronous scheduling."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Callable, Protocol

from .models import (
    CollectedSubmission,
    PayloadEnvelope,
    SubmissionCompletion,
    SubmissionLifecycleEvent,
)
from .execution import RecordedCompletion
from .policy_contracts import AdmissionPolicy


DEFAULT_SOURCE_POLL_S = 0.001


class SubmissionAdapter(Protocol):
    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object: ...

    def wait_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
    ) -> CollectedSubmission: ...

    def poll_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
    ) -> CollectedSubmission | None: ...


class SubmissionLifecycle(Protocol):
    """Submission and verified-completion handoff for an explicit execution adapter.

    collect_pending settles one actual pending result and returns True, or
    returns False when no submission can complete. It never fabricates a terminal.
    """

    def before_submit(self, envelope: PayloadEnvelope, endpoint_id: str,
                      collect_pending: Callable[[], bool]) -> None: ...
    def submit_failed(self, envelope: PayloadEnvelope) -> None: ...
    def completed(self, recorded: RecordedCompletion) -> None: ...
    def finish(self) -> None: ...


@dataclass(frozen=True)
class EndpointCapacityConfig:
    """Endpoint-local request, work, and policy capacity."""

    request_limit: int | None = None
    work_limit: int | None = None
    admission_by_endpoint: Mapping[str, AdmissionPolicy] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.request_limit is not None and self.request_limit <= 0:
            raise ValueError("per_endpoint_limit must be positive")
        if self.work_limit is not None and self.work_limit <= 0:
            raise ValueError("per_endpoint_work_limit must be positive")
        object.__setattr__(
            self,
            "admission_by_endpoint",
            dict(self.admission_by_endpoint),
        )


@dataclass(frozen=True)
class SchedulerConfig:
    """Self-validating scheduler settings, independent of engine adapters."""

    endpoint_capacity: EndpointCapacityConfig = field(default_factory=EndpointCapacityConfig)
    source_poll_interval_s: float = DEFAULT_SOURCE_POLL_S
    lifecycle: SubmissionLifecycle | None = None
    actual_work_extractor: Callable[[SubmissionCompletion], int | None] | None = None

    def __post_init__(self) -> None:
        if self.source_poll_interval_s <= 0:
            raise ValueError("source polling interval must be positive")


@dataclass(frozen=True)
class SchedulerResult:
    completions: tuple[SubmissionCompletion, ...]
    operator_invocations: int
    max_inflight_seen: int
    applied_limit: int
    bounded_wait_s: float
    avg_bounded_wait_s: float
    fanin_s: float
    submit_s: float
    max_active_work_per_endpoint_seen: int = 0
    max_ready_requests_seen: int = 0
    max_ready_work_seen: int = 0
    max_ready_payload_bytes_seen: int = 0
    ready_requests_transition_samples: tuple[int, ...] = ()
    ready_work_transition_samples: tuple[int, ...] = ()
    ready_payload_bytes_transition_samples: tuple[int, ...] = ()
    submission_events: tuple[SubmissionLifecycleEvent, ...] = ()
