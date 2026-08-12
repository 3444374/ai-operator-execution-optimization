"""Typed, engine-independent scheduling data models."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Literal

from ...planning.work import WorkDescriptor


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
    planning_batch_id: str = ""
    service_quantum_index: int = -1
    service_quantum_oversized: bool = False
    preferred_endpoint_id: str = ""
    work_units: int | None = None
    work_unit: str = "tokens"
    work_descriptor: WorkDescriptor | None = None
    oldest_arrival_epoch_s: float | None = None

    def __post_init__(self) -> None:
        if self.row_count <= 0:
            raise ValueError("row_count must be positive")
        if self.prompt_tokens < 0 or self.estimated_output_tokens < 0:
            raise ValueError("token counts must be non-negative")
        if self.work_units is not None and (
            not isinstance(self.work_units, int)
            or isinstance(self.work_units, bool)
            or self.work_units < 0
        ):
            raise ValueError("work_units must be a non-negative integer when present")
        if not isinstance(self.work_unit, str) or not self.work_unit:
            raise ValueError("work_unit must be a non-empty string")
        if self.work_descriptor is not None:
            primary = self.work_descriptor.primary
            if self.work_units is not None and self.work_units != primary.units:
                raise ValueError(
                    "legacy work_units must match descriptor primary work"
                )
            if self.work_unit != primary.unit:
                raise ValueError(
                    "legacy work_unit must match descriptor primary unit"
                )
        if not self.request_id or not self.job_id or not self.payload_id:
            raise ValueError("request_id, job_id, and payload_id must be non-empty")
        if self.oldest_arrival_epoch_s is not None and (
            not math.isfinite(self.oldest_arrival_epoch_s)
            or self.oldest_arrival_epoch_s < 0
        ):
            raise ValueError(
                "oldest_arrival_epoch_s must be finite and non-negative"
            )

    @property
    def estimated_total_tokens(self) -> int:
        return self.prompt_tokens + self.estimated_output_tokens

    @property
    def estimated_work_units(self) -> int:
        """Return modality-neutral admission work with token compatibility."""
        if self.work_descriptor is not None:
            return self.work_descriptor.primary.units
        if self.work_units is not None:
            return self.work_units
        return self.estimated_total_tokens

    def estimated_stage_work(self, stage: str) -> int | None:
        """Return one staged estimate without exposing modality internals."""
        if self.work_descriptor is None:
            return None
        item = self.work_descriptor.for_stage(stage)
        return None if item is None else item.units


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
    estimated_active_work: int = 0
    service_rate_tokens_s: float | None = None
    available: bool = True
    service_rate_work_units_s: float | None = None

    def __post_init__(self) -> None:
        if not self.endpoint_id or not self.url or not self.pool_id:
            raise ValueError("endpoint_id, url, and pool_id must be non-empty")
        if self.running < 0 or self.waiting < 0:
            raise ValueError("running and waiting must be non-negative")
        if self.kv_usage is not None and not 0.0 <= self.kv_usage <= 1.0:
            raise ValueError("kv_usage must be between 0 and 1")
        if self.estimated_active_work < 0:
            raise ValueError("estimated_active_work must be non-negative")
        if self.service_rate_tokens_s is not None and (
            not math.isfinite(self.service_rate_tokens_s)
            or self.service_rate_tokens_s <= 0
        ):
            raise ValueError(
                "service_rate_tokens_s must be finite and positive when present"
            )
        if self.service_rate_work_units_s is not None and (
            not math.isfinite(self.service_rate_work_units_s)
            or self.service_rate_work_units_s <= 0
        ):
            raise ValueError(
                "service_rate_work_units_s must be finite and positive when present"
            )

    @property
    def effective_service_rate_work_units_s(self) -> float | None:
        """Prefer a neutral work rate while preserving text-only callers."""
        return self.service_rate_work_units_s or self.service_rate_tokens_s


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
    service_rate_tokens_s_per_endpoint: float | None = None
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
        if self.service_rate_tokens_s_per_endpoint is not None and (
            not math.isfinite(self.service_rate_tokens_s_per_endpoint)
            or self.service_rate_tokens_s_per_endpoint < 0
        ):
            raise ValueError(
                "service_rate_tokens_s_per_endpoint must be finite and "
                "non-negative when present"
            )

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
    planning_batch_id: str = ""
    service_quantum_index: int = -1
    service_quantum_oversized: bool = False
    actor_worker_id: str = ""
    actor_worker_index: int = -1
    actor_worker_pid: int = 0

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
    actor_worker_id: str = ""
    actor_worker_index: int = -1
    actor_worker_pid: int = 0

    def __post_init__(self) -> None:
        if self.wait_s < 0 or self.result_s < 0:
            raise ValueError("collection timings must be non-negative")
