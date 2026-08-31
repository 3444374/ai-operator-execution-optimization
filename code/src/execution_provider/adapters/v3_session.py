"""Shared value interface for version-3 completion adapters and sessions."""

from __future__ import annotations

import socket
import time
from dataclasses import dataclass
from typing import Mapping, Protocol

from ..wire.framing import ProtocolError, encode_frame, read_frame
from ..wire.v3 import (
    MAX_FRAME_BYTES,
    MAX_INFLIGHT_TASKS,
    MAX_INPUT_BYTES,
    PROTOCOL_VERSION,
    build_error_message,
    completion_evidence_digest,
    validate_open,
    validate_task,
)


@dataclass(frozen=True)
class V3CompletionRequest:
    """One validated wire-v3 task passed to a completion adapter."""

    semantic_payload_digest: str
    model_id: str
    canonical_messages: tuple[dict[str, str], ...]
    generation_constraints: Mapping[str, object]


@dataclass(frozen=True)
class V3Completion:
    """Raw model result returned for PostgreSQL-side validation and parsing."""

    raw_output: str
    response_model_id: str
    prompt_tokens: int
    output_tokens: int
    finish_reason: str


class CompletionAdapterError(Exception):
    """A redacted version-3 error code returned by a completion adapter."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class CompletionAdapter(Protocol):
    """Query-independent adapter used by the shared wire-v3 session runner."""

    execution_id: str
    model_id: str | None

    def complete(self, request: V3CompletionRequest) -> V3Completion:
        """Return one raw completion or raise a redacted adapter error."""


def run_v3_session(
    connection: socket.socket,
    adapter: CompletionAdapter,
    *,
    open_message: dict[str, object] | None = None,
    response_delay_ms: int = 0,
    tamper_evidence_digest: bool = False,
    disconnect_on_task: bool = False,
    completion_fixture: str | None = None,
) -> None:
    """Serve one strict synchronous wire-v3 session through an adapter."""
    error_sequence: str | None = None
    try:
        opened = open_message if open_message is not None else read_frame(connection)
        if opened is None:
            return
        open_context = validate_open(
            opened,
            provider_execution_id=adapter.execution_id,
        )
        if adapter.model_id is not None and adapter.model_id != open_context.model_id:
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        if completion_fixture in ("v3-open-error", "v3-open-error-sequence"):
            _send_error(
                connection,
                "INVALID_OPEN",
                None,
                fault_fixture=completion_fixture,
            )
            return
        connection.sendall(
            encode_frame(
                {
                    "type": "opened",
                    "protocol_version": PROTOCOL_VERSION,
                    "semantic_spec_digest": open_context.semantic_spec_digest,
                    "physical_algorithm_digest": open_context.physical_algorithm_digest,
                    "provider_execution_digest": open_context.provider_execution_digest,
                    "max_inflight_tasks": MAX_INFLIGHT_TASKS,
                    "max_frame_bytes": MAX_FRAME_BYTES,
                    "max_input_bytes": MAX_INPUT_BYTES,
                }
            )
        )

        expected_sequence = 0
        while True:
            task = read_frame(connection)
            if task is None:
                return
            error_sequence = _valid_sequence_or_none(task.get("sequence"))
            sequence, payload_digest = validate_task(
                task,
                expected_sequence=expected_sequence,
                open_context=open_context,
            )
            error_sequence = str(sequence)
            if disconnect_on_task:
                return
            if response_delay_ms > 0:
                time.sleep(response_delay_ms / 1000)
            if completion_fixture is not None and completion_fixture.startswith(
                "v3-error-"
            ):
                _send_error(
                    connection,
                    "GOLDEN_FIXTURE_MISSING",
                    error_sequence,
                    fault_fixture=completion_fixture,
                )
                return
            result = adapter.complete(
                V3CompletionRequest(
                    semantic_payload_digest=payload_digest,
                    model_id=open_context.model_id,
                    canonical_messages=tuple(
                        dict(message) for message in task["canonical_messages"]
                    ),
                    generation_constraints=dict(opened["generation_constraints"]),
                )
            )
            evidence_digest = completion_evidence_digest(
                semantic_spec_sha256=open_context.semantic_spec_digest,
                physical_algorithm_sha256=open_context.physical_algorithm_digest,
                provider_execution_sha256=open_context.provider_execution_digest,
                semantic_payload_sha256=payload_digest,
                sequence=sequence,
                raw_output=result.raw_output,
                finish_reason=result.finish_reason,
                response_model_id=result.response_model_id,
                prompt_tokens=result.prompt_tokens,
                output_tokens=result.output_tokens,
            )
            if tamper_evidence_digest:
                evidence_digest = "0" * 64
            completion: dict[str, object] = {
                "type": "completion",
                "protocol_version": PROTOCOL_VERSION,
                "sequence": str(sequence),
                "semantic_spec_digest": open_context.semantic_spec_digest,
                "physical_algorithm_digest": open_context.physical_algorithm_digest,
                "provider_execution_digest": open_context.provider_execution_digest,
                "semantic_payload_digest": payload_digest,
                "raw_output": result.raw_output,
                "response_model_id": result.response_model_id,
                "prompt_tokens": str(result.prompt_tokens),
                "output_tokens": str(result.output_tokens),
                "finish_reason": result.finish_reason,
                "completion_evidence_digest": evidence_digest,
            }
            _apply_completion_fixture(completion, completion_fixture)
            connection.sendall(encode_frame(completion))
            expected_sequence += 1
            error_sequence = None
    except ProtocolError as failure:
        _send_error(connection, failure.code, error_sequence)
    except CompletionAdapterError as failure:
        _send_error(connection, failure.code, error_sequence)
    except (BrokenPipeError, ConnectionResetError, OSError):
        return
    except Exception:
        _send_error(connection, "GATEWAY_INTERNAL", error_sequence)
    finally:
        connection.close()


def _valid_sequence_or_none(value: object) -> str | None:
    if not isinstance(value, str) or (
        value != "0"
        and (
            not value.isascii()
            or not value.isdigit()
            or value[0] == "0"
        )
    ):
        return None
    if int(value) >= 2**64:
        return None
    return value


def _send_error(
    connection: socket.socket,
    code: str,
    sequence: str | None,
    *,
    fault_fixture: str | None = None,
) -> None:
    try:
        sequence_value = None if sequence is None else int(sequence)
        message = build_error_message(code, sequence=sequence_value)
        if fault_fixture == "v3-error-missing-field":
            del message["code"]
        elif fault_fixture == "v3-error-extra-field":
            message["future_field"] = True
        elif fault_fixture == "v3-error-sequence":
            message["sequence"] = None
        elif fault_fixture == "v3-error-code":
            message["code"] = "UNKNOWN_CODE"
        elif fault_fixture == "v3-open-error-sequence":
            message["sequence"] = "0"
        connection.sendall(encode_frame(message))
    except (BrokenPipeError, ConnectionResetError, OSError):
        pass


def _apply_completion_fixture(
    completion: dict[str, object],
    fixture: str | None,
) -> None:
    if fixture == "v3-model-mismatch":
        completion["response_model_id"] = "different-model"
    elif fixture == "v3-invalid-usage":
        completion["prompt_tokens"] = 0
    elif fixture == "v3-finish-reason":
        completion["finish_reason"] = "length"
    elif fixture == "v3-extra-field":
        completion["future_field"] = True


__all__ = [
    "CompletionAdapter",
    "CompletionAdapterError",
    "V3Completion",
    "V3CompletionRequest",
    "run_v3_session",
]
