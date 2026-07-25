"""Typed, engine-independent scheduling data models."""

from __future__ import annotations

from dataclasses import dataclass
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
class RoutingDecision:
    endpoint_id: str
    pool_id: str
    reason: str


@dataclass(frozen=True)
class SubmissionCompletion:
    request_id: str
    status: Literal["completed", "failed"]
    result: object | None = None
    error: str = ""
