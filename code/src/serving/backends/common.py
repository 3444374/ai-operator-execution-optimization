"""Shared backend types, validation, and readiness contracts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Literal


EmbeddingBackendName = Literal["fake", "compatible_http", "http_openai"]

CompletionBackendName = Literal["fake", "compatible_http", "http_openai", "ollama"]

CompletionPromptFormat = Literal["raw", "chatml"]

CompletionProtocol = Literal["completions", "chat_completions"]

@dataclass(frozen=True)
class CompletionEndpointResult:
    outputs: list[str]
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    output_token_counts: list[int | None]
    finish_reasons: list[str | None]
    http_request_start_epoch_s: float
    http_response_headers_epoch_s: float
    http_response_body_epoch_s: float
    http_headers_wait_s: float
    http_body_read_s: float

class _ReadyActor:
    """Side-effect-free readiness contract for explicit Ray startup barriers."""

    def ready(self) -> dict[str, object]:
        return {
            "actor_worker_pid": os.getpid(),
            "actor_type": type(self).__name__,
        }

def normalize_embedding_backend(name: EmbeddingBackendName) -> Literal["fake", "compatible_http"]:
    if name == "fake":
        return "fake"
    if name in {"compatible_http", "http_openai"}:
        return "compatible_http"
    raise ValueError(f"Unknown embedding backend: {name}")

def normalize_completion_backend(name: CompletionBackendName) -> Literal["fake", "compatible_http", "ollama"]:
    if name == "fake":
        return "fake"
    if name in {"compatible_http", "http_openai"}:
        return "compatible_http"
    if name == "ollama":
        return "ollama"
    raise ValueError(f"Unknown completion backend: {name}")

def text_token_count(text: str) -> int:
    return max(1, len(text.split()))

def model_request_wall_time(results: list[dict]) -> float:
    starts = [float(result["service_start_epoch_s"]) for result in results if "service_start_epoch_s" in result]
    ends = [float(result["service_end_epoch_s"]) for result in results if "service_end_epoch_s" in result]
    if not starts or not ends:
        return 0.0
    return max(ends) - min(starts)
