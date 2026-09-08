"""Shared request, completion and adapter contracts, independent of transport loops."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol

from .generation_profile import GenerationProfile


@dataclass(frozen=True)
class Completion:
    """Unmodified model text and metadata, without transport or parser behavior."""

    raw_output: str
    response_model_id: str
    prompt_tokens: int
    output_tokens: int
    finish_reason: str


@dataclass(frozen=True)
class CompletionRequest:
    """One validated semantic task passed to a completion adapter."""

    semantic_payload_digest: str
    model_id: str
    canonical_messages: tuple[dict[str, str], ...]
    generation_constraints: Mapping[str, object]
    generation_profile: GenerationProfile | None = None
    protocol_version: int = 3


class CompletionAdapterError(Exception):
    """A redacted error code returned by a completion adapter."""

    def __init__(self, code: str, *, remote_outcome_unknown: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.remote_outcome_unknown = remote_outcome_unknown


class CompletionAdapter(Protocol):
    """Query-independent adapter used by the shared semantic session runner."""

    model_id: str | None

    def execution_id_for(self, protocol_version: int) -> str | None:
        """Return an explicit supported identity, or None to reject the version."""

    def complete(self, request: CompletionRequest) -> Completion:
        """Return one raw completion or raise a redacted adapter error."""
