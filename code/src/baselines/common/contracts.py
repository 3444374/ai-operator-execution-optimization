"""Immutable request and result records shared by all baseline adapters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


RequestStatus = Literal["completed", "failed"]


@dataclass(frozen=True)
class ChatRequest:
    """One complete model request with a precomputed endpoint assignment."""

    doc_id: int
    prompt: str
    arrival_time_s: float
    prompt_tokens: int
    max_output_tokens: int
    estimated_output_tokens: int
    source_row_hash: str
    endpoint_index: int

    @property
    def estimated_work(self) -> int:
        return self.prompt_tokens + self.estimated_output_tokens

    @property
    def messages(self) -> tuple[dict[str, str], ...]:
        return ({"role": "user", "content": self.prompt},)


@dataclass(frozen=True)
class BaselineRequestResult:
    """Normalized per-request evidence emitted by a baseline adapter."""

    doc_id: int
    endpoint_index: int
    status: RequestStatus
    error: str | None
    submitted_at_s: float
    started_at_s: float
    completed_at_s: float
    input_tokens: int
    output_tokens: int
    output_text: str | None
    finish_reason: str | None

    @property
    def latency_s(self) -> float:
        return self.completed_at_s - self.submitted_at_s


@dataclass(frozen=True)
class ManifestMetadata:
    """Digest metadata for exact workload identity checks."""

    row_count: int
    sha256: str
