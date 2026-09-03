"""Deterministic completions for the versioned semantic wire contracts."""

from __future__ import annotations

import socket
from collections.abc import Mapping

from ..wire.v3 import GOLDEN_EXECUTION_ID
from ..wire.v4 import GOLDEN_EXECUTION_ID as CHOICE_EXECUTION_ID
from ..wire.v5 import GOLDEN_EXECUTION_ID as MAP_EXECUTION_ID
from .semantic_session import (
    CompletionAdapterError,
    Completion,
    CompletionRequest,
    run_v3_session,
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


def run_golden_session(
    connection: socket.socket,
    fixtures: Mapping[str, str],
    *,
    open_message: dict[str, object] | None = None,
    response_delay_ms: int = 0,
    tamper_evidence_digest: bool = False,
    disconnect_on_task: bool = False,
    completion_fixture: str | None = None,
) -> None:
    """Compatibility entry point for one deterministic wire-v3 session."""
    run_v3_session(
        connection,
        GoldenCompletionAdapter(fixtures),
        open_message=open_message,
        response_delay_ms=response_delay_ms,
        tamper_evidence_digest=tamper_evidence_digest,
        disconnect_on_task=disconnect_on_task,
        completion_fixture=completion_fixture,
    )


__all__ = ["GoldenCompletionAdapter", "run_golden_session"]
