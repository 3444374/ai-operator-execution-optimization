"""Typed, engine-independent scheduling data models."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal


OperatorName = Literal["ai_complete", "ai_embed", "ai_classify"]


@dataclass(frozen=True)
class BatchRequest:
    request_id: str
    job_id: str
    operator: OperatorName
    row_count: int
    prompt_tokens: int
    estimated_output_tokens: int
    prefix_key: str
    first_arrival_s: float
    oldest_arrival_s: float
    payload_id: str

    def __post_init__(self) -> None:
        if self.row_count <= 0:
            raise ValueError("row_count must be positive")
        if self.prompt_tokens < 0 or self.estimated_output_tokens < 0:
            raise ValueError("token counts must be non-negative")
        if not self.request_id or not self.job_id or not self.payload_id:
            raise ValueError("request_id, job_id, and payload_id must be non-empty")

    @property
    def estimated_total_tokens(self) -> int:
        return self.prompt_tokens + self.estimated_output_tokens


@dataclass(frozen=True)
class PayloadEnvelope:
    request: BatchRequest
    payload: object


@dataclass(frozen=True)
class EndpointSnapshot:
    endpoint_id: str
    url: str
    pool_id: str
    gpu_id: str
    healthy: bool
    running: int
    waiting: int
    kv_usage: float | None
    observed_at_s: float

    def __post_init__(self) -> None:
        if not self.endpoint_id or not self.url or not self.pool_id:
            raise ValueError("endpoint_id, url, and pool_id must be non-empty")
        if self.running < 0 or self.waiting < 0:
            raise ValueError("running and waiting must be non-negative")
        if self.kv_usage is not None and not 0.0 <= self.kv_usage <= 1.0:
            raise ValueError("kv_usage must be between 0 and 1")


@dataclass(frozen=True)
class TopologySnapshot:
    endpoints: tuple[EndpointSnapshot, ...]
    observed_at_s: float

    def __post_init__(self) -> None:
        endpoint_ids = [endpoint.endpoint_id for endpoint in self.endpoints]
        if len(endpoint_ids) != len(set(endpoint_ids)):
            raise ValueError("endpoint_id values must be unique")


@dataclass(frozen=True)
class AdmissionDecision:
    allowed: bool
    limit: int
    action: str
    reason: str


@dataclass(frozen=True)
class AdmissionObservation:
    observed_at_s: float
    fresh: bool
    inflight: int
    running: int | None
    waiting: int | None
    kv_usage: float | None
    sample_age_s: float | None = None
    hol_age_s: float | None = None

    def __post_init__(self) -> None:
        if not math.isfinite(self.observed_at_s) or self.observed_at_s < 0:
            raise ValueError("observed_at_s must be finite and non-negative")
        if self.sample_age_s is not None and (
            not math.isfinite(self.sample_age_s) or self.sample_age_s < 0
        ):
            raise ValueError("sample_age_s must be finite and non-negative when present")
        if self.hol_age_s is not None and (
            not math.isfinite(self.hol_age_s) or self.hol_age_s < 0
        ):
            raise ValueError("hol_age_s must be finite and non-negative when present")
        if self.inflight < 0:
            raise ValueError("inflight must be non-negative")
        if (
            self.running is not None
            and self.running < 0
            or self.waiting is not None
            and self.waiting < 0
        ):
            raise ValueError("running and waiting must be non-negative when present")
        if self.kv_usage is not None and not 0.0 <= self.kv_usage <= 1.0:
            raise ValueError("kv_usage must be between 0 and 1 when present")

    @property
    def has_service_metrics(self) -> bool:
        return (
            self.running is not None
            and self.waiting is not None
            and self.kv_usage is not None
        )


@dataclass(frozen=True)
class ControlDiagnostics:
    smoothed_running: float | None = None
    smoothed_waiting: float | None = None
    smoothed_kv_usage: float | None = None
    error: float | None = None
    integral_error: float | None = None
    derivative_error: float | None = None
    selected_arm: int | None = None
    arm_scores: tuple[tuple[int, float], ...] = ()


@dataclass(frozen=True)
class WindowDecision:
    window: int
    action: str
    reason: str
    diagnostics: ControlDiagnostics = field(default_factory=ControlDiagnostics)

    def __post_init__(self) -> None:
        if self.window <= 0:
            raise ValueError("window must be positive")
        if not self.action or not self.reason:
            raise ValueError("action and reason must be non-empty")


@dataclass(frozen=True)
class RoutingDecision:
    endpoint_id: str
    pool_id: str
    reason: str


@dataclass(frozen=True)
class PoolRoutingDecision:
    pool_id: str
    reason: str

    def __post_init__(self) -> None:
        if not self.pool_id or not self.reason:
            raise ValueError("pool_id and reason must be non-empty")


@dataclass(frozen=True)
class SubmissionCompletion:
    request_id: str
    status: Literal["completed", "failed"]
    result: object | None = None
    error: str = ""


@dataclass(frozen=True)
class SubmissionLifecycleEvent:
    submission_id: str
    pool_id: str
    endpoint_id: str
    gpu_id: str
    submit_epoch_s: float
    completion_epoch_s: float
    status: Literal["completed", "failed"]
    error: str = ""

    def __post_init__(self) -> None:
        if not self.submission_id or not self.pool_id or not self.endpoint_id:
            raise ValueError(
                "submission_id, pool_id, and endpoint_id must be non-empty"
            )
        if (
            not math.isfinite(self.submit_epoch_s)
            or not math.isfinite(self.completion_epoch_s)
            or self.submit_epoch_s < 0
            or self.completion_epoch_s < self.submit_epoch_s
        ):
            raise ValueError("submission lifecycle timestamps are invalid")


@dataclass(frozen=True)
class CollectedSubmission:
    handle: object
    completion: SubmissionCompletion
    wait_s: float
    result_s: float

    def __post_init__(self) -> None:
        if self.wait_s < 0 or self.result_s < 0:
            raise ValueError("collection timings must be non-negative")
