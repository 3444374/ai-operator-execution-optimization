"""Deterministic completions for the versioned semantic wire contracts."""

from __future__ import annotations

from collections.abc import Mapping

from ..wire.v3 import GOLDEN_EXECUTION_ID
from ..wire.v4 import GOLDEN_EXECUTION_ID as CHOICE_EXECUTION_ID
from ..wire.v5 import GOLDEN_EXECUTION_ID as MAP_EXECUTION_ID
from .semantic_session import (
    CompletionAdapterError,
    Completion,
    CompletionRequest,
)


class GoldenCompletionAdapter:
    """Return test-owned raw outputs keyed by semantic payload digest."""

    execution_id = GOLDEN_EXECUTION_ID
    choice_execution_id = CHOICE_EXECUTION_ID
    model_id = None

    def __init__(self, fixtures: Mapping[str, str | Completion]) -> None:
        self._fixtures = fixtures

    def execution_id_for(self, protocol_version: int) -> str | None:
        return {3: self.execution_id, 4: self.choice_execution_id,
                5: MAP_EXECUTION_ID}.get(protocol_version)

    def complete(self, request: CompletionRequest) -> Completion:
        raw_output = self._fixtures.get(request.semantic_payload_digest)
        if raw_output is None:
            raise CompletionAdapterError("GOLDEN_FIXTURE_MISSING")
        if request.protocol_version == 5:
            if not isinstance(raw_output, Completion):
                raise CompletionAdapterError("GOLDEN_FIXTURE_INVALID")
            return raw_output
        if not isinstance(raw_output, str):
            raise CompletionAdapterError("GOLDEN_FIXTURE_INVALID")
        return Completion(
            raw_output=raw_output,
            response_model_id=request.model_id,
            prompt_tokens=0,
            output_tokens=1,
            finish_reason="stop",
        )



__all__ = ["GoldenCompletionAdapter"]
