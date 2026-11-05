"""Public configuration, results and adapter interface for synchronous scheduling."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Callable, Protocol

from .models import (
    CollectedSubmission,
    PayloadEnvelope,
    SubmissionCompletion,
    SubmissionLifecycleEvent,
)
from .policy_contracts import AdmissionPolicy, SharedCreditPolicy


DEFAULT_SHARED_CREDIT_POLL_S = 0.001


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
class ReadyWindowConfig:
    """Bounds pre-registered work waiting for shared credit."""

    request_limit: int = 1
    work_limit: int | None = None
    payload_bytes_limit: int | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.request_limit, int)
            or isinstance(self.request_limit, bool)
            or self.request_limit <= 0
        ):
            raise ValueError("shared_ready_request_limit must be positive")
        if self.work_limit is not None and (
            not isinstance(self.work_limit, int)
            or isinstance(self.work_limit, bool)
            or self.work_limit <= 0
        ):
            raise ValueError("shared_ready_work_limit must be positive")
        if self.request_limit > 1 and self.work_limit is None:
            raise ValueError("a multi-candidate ready window requires a work limit")
        if self.payload_bytes_limit is not None and (
            not isinstance(self.payload_bytes_limit, int)
            or isinstance(self.payload_bytes_limit, bool)
            or self.payload_bytes_limit <= 0
        ):
            raise ValueError("shared_ready_payload_bytes_limit must be positive")


@dataclass(frozen=True)
class JobSchedulingContract:
    """Job metadata used by shared-credit fairness and SLO policies."""

    weight: int = 1
    priority: int = 0
    slo_target_s: float | None = None
    priority_window_s: float | None = None
    fairness_debt_cap: float | None = None

    def __post_init__(self) -> None:
        if self.weight <= 0:
            raise ValueError("job_weight must be positive")
        if (
            not isinstance(self.priority, int)
            or isinstance(self.priority, bool)
            or self.priority < 0
        ):
            raise ValueError("job_priority must be a non-negative integer")
        for value, name in (
            (self.slo_target_s, "job_slo_target_s"),
            (self.priority_window_s, "job_priority_window_s"),
            (self.fairness_debt_cap, "job_fairness_debt_cap"),
        ):
            if value is not None and (not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be finite and positive")
        if self.priority_window_s is not None and self.slo_target_s is None:
            raise ValueError("a priority window requires a Job SLO target")


@dataclass(frozen=True)
class SharedCreditConfig:
    """Shared capacity coordinator and its bounded-wait behavior."""

    policy: SharedCreditPolicy | None = None
    poll_interval_s: float = DEFAULT_SHARED_CREDIT_POLL_S
    acquire_timeout_s: float | None = None
    ready_window: ReadyWindowConfig = field(default_factory=ReadyWindowConfig)
    job: JobSchedulingContract = field(default_factory=JobSchedulingContract)

    def __post_init__(self) -> None:
        if self.poll_interval_s <= 0:
            raise ValueError("shared_credit_poll_s must be positive")
        if self.acquire_timeout_s is not None and (
            not isinstance(self.acquire_timeout_s, (int, float))
            or isinstance(self.acquire_timeout_s, bool)
            or not math.isfinite(self.acquire_timeout_s)
            or self.acquire_timeout_s <= 0
        ):
            raise ValueError("shared credit acquire timeout must be positive")


@dataclass(frozen=True)
class SchedulerConfig:
    """Self-validating scheduler settings, independent of engine adapters."""

    endpoint_capacity: EndpointCapacityConfig = field(default_factory=EndpointCapacityConfig)
    shared_credit: SharedCreditConfig = field(default_factory=SharedCreditConfig)
    actual_work_extractor: Callable[[SubmissionCompletion], int | None] | None = None


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
