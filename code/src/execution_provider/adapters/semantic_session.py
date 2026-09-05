"""Shared synchronous session loop; each codec owns its semantic wire checks."""

from __future__ import annotations

import copy
import socket
import time
from dataclasses import dataclass
from typing import Mapping, Protocol

from ..wire.framing import ProtocolError, encode_frame, read_frame
from ..wire import v3, v4, v5
from ..generation_profile import GenerationProfile
from ..completion import Completion


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

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class CompletionAdapter(Protocol):
    """Query-independent adapter used by the shared semantic session runner."""

    model_id: str | None

    def execution_id_for(self, protocol_version: int) -> str | None:
        """Return an explicit supported identity, or None to reject the version."""

    def complete(self, request: CompletionRequest) -> Completion:
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
    """Serve exactly wire v3; other versions are rejected."""
    _run_semantic_session(
        connection, adapter, wire_version=3,
        open_message=open_message,
        response_delay_ms=response_delay_ms,
        tamper_evidence_digest=tamper_evidence_digest,
        disconnect_on_task=disconnect_on_task,
        completion_fixture=completion_fixture,
    )


def run_v4_session(
    connection: socket.socket,
    adapter: CompletionAdapter,
    *,
    open_message: dict[str, object] | None = None,
    response_delay_ms: int = 0,
    tamper_evidence_digest: bool = False,
    disconnect_on_task: bool = False,
    completion_fixture: str | None = None,
) -> None:
    """Serve exactly wire v4; other versions are rejected."""
    _run_semantic_session(
        connection, adapter, wire_version=4,
        open_message=open_message,
        response_delay_ms=response_delay_ms,
        tamper_evidence_digest=tamper_evidence_digest,
        disconnect_on_task=disconnect_on_task,
        completion_fixture=completion_fixture,
    )


def run_v5_session(
    connection: socket.socket,
    adapter: CompletionAdapter,
    *,
    open_message: dict[str, object] | None = None,
    response_delay_ms: int = 0,
    tamper_evidence_digest: bool = False,
    disconnect_on_task: bool = False,
    completion_fixture: str | None = None,
) -> None:
    """Serve exactly wire v5; other versions are rejected."""
    _run_semantic_session(
        connection, adapter, wire_version=5,
        open_message=open_message,
        response_delay_ms=response_delay_ms,
        tamper_evidence_digest=tamper_evidence_digest,
        disconnect_on_task=disconnect_on_task,
        completion_fixture=completion_fixture,
    )


def _run_semantic_session(
    connection: socket.socket,
    adapter: CompletionAdapter,
    *,
    wire_version: int,
    open_message: dict[str, object] | None = None,
    response_delay_ms: int = 0,
    tamper_evidence_digest: bool = False,
    disconnect_on_task: bool = False,
    completion_fixture: str | None = None,
) -> None:
    """Serve one strict synchronous semantic session through an adapter."""
    codec = {3: v3, 4: v4, 5: v5}[wire_version]
    error_sequence: str | None = None
    open_context = None
    try:
        opened = open_message if open_message is not None else _read_semantic_frame(connection, wire_version)
        if opened is None:
            return
        identity_for = getattr(adapter, "execution_id_for", None)
        execution_id = identity_for(wire_version) if identity_for is not None else None
        if execution_id is None:
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        open_context = codec.validate_open(
            opened,
            provider_execution_id=execution_id,
        )
        if adapter.model_id is not None and adapter.model_id != open_context.model_id:
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        if completion_fixture in ("v3-open-error", "v3-open-error-sequence"):
            _send_error(
                connection,
                "INVALID_OPEN",
                None,
                fault_fixture=completion_fixture,
                codec=codec,
            )
            return
        connection.sendall(
            encode_frame(
                {
                    "type": "opened",
                    "protocol_version": codec.PROTOCOL_VERSION,
                    "semantic_spec_digest": open_context.semantic_spec_digest,
                    "physical_algorithm_digest": open_context.physical_algorithm_digest,
                    "provider_execution_digest": open_context.provider_execution_digest,
                    "max_inflight_tasks": codec.MAX_INFLIGHT_TASKS,
                    "max_frame_bytes": codec.MAX_FRAME_BYTES,
                    "max_input_bytes": codec.MAX_INPUT_BYTES,
                    **({"max_output_bytes": codec.MAX_OUTPUT_BYTES} if wire_version == 5 else {}),
                    **({"generation_profile_digest": open_context.generation_profile.digest}
                       if open_context.generation_profile is not None else {}),
                }
            )
        )

        expected_sequence = 0
        while True:
            if wire_version == 5:
                error_sequence = str(expected_sequence)
            task = _read_semantic_frame(connection, wire_version)
            if task is None:
                return
            sequence_text = task.get("sequence")
            if wire_version in (4, 5) and isinstance(sequence_text, str) and len(sequence_text) > 20:
                raise ProtocolError("INVALID_TASK")
            if wire_version != 5:
                error_sequence = _valid_sequence_or_none(sequence_text)
            sequence, payload_digest = codec.validate_task(
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
                    codec=codec,
                )
                return
            result = adapter.complete(
                CompletionRequest(
                    semantic_payload_digest=payload_digest,
                    model_id=open_context.model_id,
                    canonical_messages=tuple(
                        ({"role": message["role"], "content": message["content"]}
                         if wire_version == 5 else dict(message))
                        for message in task["canonical_messages"]
                    ),
                    generation_constraints=copy.deepcopy(opened["generation_constraints"]),
                    generation_profile=open_context.generation_profile,
                    protocol_version=wire_version,
                )
            )
            completion = codec.build_completion_message(
                open_context, sequence=sequence, payload_digest=payload_digest,
                completion=result,
            )
            if tamper_evidence_digest:
                completion["completion_evidence_digest"] = "0" * 64
            _apply_completion_fixture(completion, completion_fixture)
            connection.sendall(encode_frame(completion))
            expected_sequence += 1
            error_sequence = None
    except ProtocolError as failure:
        code = failure.code
        if wire_version in (4, 5) and code not in codec.ERROR_CODES:
            code = "INVALID_OPEN" if open_context is None else "INVALID_TASK"
        _send_error(connection, code, error_sequence, codec=codec)
    except CompletionAdapterError as failure:
        _send_error(connection, failure.code, error_sequence, codec=codec)
    except (BrokenPipeError, ConnectionResetError, OSError):
        return
    except Exception:
        _send_error(connection, "GATEWAY_INTERNAL", error_sequence, codec=codec)
    finally:
        connection.close()


def _read_semantic_frame(connection: socket.socket, wire_version: int) -> dict[str, object] | None:
    try:
        return read_frame(connection)
    except (ValueError, RecursionError):
        if wire_version != 5:
            raise
        # JSON integer conversion and nesting can exceed interpreter limits.
        raise ProtocolError("invalid_json") from None


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
    codec=v3,
) -> None:
    try:
        sequence_value = None if sequence is None else int(sequence)
        message = codec.build_error_message(code, sequence=sequence_value)
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
    "CompletionAdapter", "CompletionAdapterError", "Completion",
    "CompletionRequest", "run_v3_session", "run_v4_session", "run_v5_session",
]
