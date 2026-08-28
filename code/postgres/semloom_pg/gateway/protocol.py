"""Versioned, bounded wire contract for the SemLoom recording gateway."""

from __future__ import annotations

import hashlib
import json
import socket
import struct
import time
from typing import Any


PROTOCOL_VERSION = 2
MAX_FRAME_BYTES = 1024 * 1024
MAX_INPUT_BYTES = (MAX_FRAME_BYTES - 4096) // 6
MAX_INFLIGHT_TASKS = 1
RECORDING_PREFIX = "recorded:"
RECORDING_SPEC_ID = "semloom.recording.sem_map.text"
RECORDING_SPEC_VERSION = 1
RECORDING_ALGORITHM = "RECORDING"
UDS_EXECUTION_ID = "semloom.provider.recording.uds.v2"

_OPEN_FIELDS = {
    "type",
    "protocol_version",
    "semantic_spec_digest",
    "physical_algorithm_digest",
    "provider_execution_digest",
    "provider_execution_id",
    "operator_kind",
    "semantic_spec_id",
    "semantic_spec_version",
    "physical_algorithm",
    "null_policy",
    "error_policy",
    "input_type",
    "output_type",
}
_TASK_FIELDS = {
    "type",
    "protocol_version",
    "sequence",
    "semantic_spec_digest",
    "physical_algorithm_digest",
    "provider_execution_digest",
    "payload_digest",
    "is_null",
    "input",
}


class ProtocolError(Exception):
    """A fail-closed protocol violation safe to report without payload data."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def semantic_spec_digest() -> str:
    """Digest the SQL-visible semantics of the recording SemMap spec."""
    canonical = (
        b"semloom-semantic-spec-v1\0"
        + _canonical_text("SEM_MAP")
        + _canonical_text(RECORDING_SPEC_ID)
        + struct.pack("!I", RECORDING_SPEC_VERSION)
        + _canonical_text("PROPAGATE_NULL")
        + _canonical_text("FAIL_QUERY")
        + _canonical_text("text")
        + _canonical_text("text")
    )
    return hashlib.sha256(canonical).hexdigest()


def physical_algorithm_digest() -> str:
    """Digest the database-selected recording physical algorithm."""
    canonical = b"semloom-physical-algorithm-v1\0" + _canonical_text(
        RECORDING_ALGORITHM
    )
    return hashlib.sha256(canonical).hexdigest()


def provider_execution_digest() -> str:
    """Digest the concrete UDS recording execution profile."""
    canonical = b"semloom-provider-execution-v1\0" + _canonical_text(
        UDS_EXECUTION_ID
    )
    return hashlib.sha256(canonical).hexdigest()


def semantic_payload_digest(value: str | None) -> str:
    """Digest a nullable UTF-8 task payload without conflating NULL and empty text."""
    if value is not None and not isinstance(value, str):
        raise TypeError("semantic payload must be text or None")
    encoded = b"" if value is None else value.encode("utf-8")
    canonical = (
        b"semloom-payload-v1\0"
        + (b"\x01" if value is None else b"\x00")
        + struct.pack("!Q", len(encoded))
        + encoded
    )
    return hashlib.sha256(canonical).hexdigest()


def completion_evidence_digest(
    *,
    semantic_spec_sha256: str,
    physical_algorithm_sha256: str,
    provider_execution_sha256: str,
    payload_sha256: str,
    sequence: int,
    output: str | None,
) -> str:
    """Bind one terminal result to semantic, physical, and execution identities."""
    _require_sha256(semantic_spec_sha256, "invalid_semantic_spec_digest")
    _require_sha256(physical_algorithm_sha256, "invalid_physical_algorithm_digest")
    _require_sha256(provider_execution_sha256, "invalid_provider_execution_digest")
    _require_sha256(payload_sha256, "invalid_payload_digest")
    if type(sequence) is not int or sequence < 0:
        raise ValueError("sequence must be a non-negative integer")
    if output is not None and not isinstance(output, str):
        raise TypeError("completion output must be text or None")
    encoded = b"" if output is None else output.encode("utf-8")
    canonical = (
        b"semloom-completion-v2\0"
        + semantic_spec_sha256.encode("ascii")
        + physical_algorithm_sha256.encode("ascii")
        + provider_execution_sha256.encode("ascii")
        + payload_sha256.encode("ascii")
        + struct.pack("!Q", sequence)
        + (b"\x01" if output is None else b"\x00")
        + struct.pack("!Q", len(encoded))
        + encoded
    )
    return hashlib.sha256(canonical).hexdigest()


def encode_frame(message: dict[str, Any]) -> bytes:
    """Encode one canonical UTF-8 JSON frame with a four-byte big-endian length."""
    payload = json.dumps(
        message,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if not payload or len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError("invalid_frame_length")
    return struct.pack("!I", len(payload)) + payload


def read_frame(connection: socket.socket) -> dict[str, Any] | None:
    """Read one bounded frame; return None only for clean EOF between frames."""
    header = _read_exact(connection, 4, allow_initial_eof=True)
    if header is None:
        return None
    length = struct.unpack("!I", header)[0]
    if length == 0 or length > MAX_FRAME_BYTES:
        raise ProtocolError("invalid_frame_length")
    raw = _read_exact(connection, length, allow_initial_eof=False)
    assert raw is not None
    try:
        message = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError("invalid_json") from error
    if not isinstance(message, dict):
        raise ProtocolError("invalid_message")
    return message


def run_recording_session(
    connection: socket.socket,
    *,
    response_delay_ms: int = 0,
    tamper_evidence_digest: bool = False,
    disconnect_on_task: bool = False,
    completion_fixture: str | None = None,
) -> None:
    """Serve one query-scoped, single-inflight recording session."""
    try:
        opened = read_frame(connection)
        if opened is None:
            return
        semantic_sha256, physical_sha256, execution_sha256 = _validate_open(opened)
        connection.sendall(
            encode_frame(
                {
                    "type": "opened",
                    "protocol_version": PROTOCOL_VERSION,
                    "semantic_spec_digest": semantic_sha256,
                    "physical_algorithm_digest": physical_sha256,
                    "provider_execution_digest": execution_sha256,
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
            sequence, input_value, payload_sha256 = _validate_task(
                task,
                expected_sequence=expected_sequence,
                semantic_spec_sha256=semantic_sha256,
                physical_algorithm_sha256=physical_sha256,
                provider_execution_sha256=execution_sha256,
            )
            if disconnect_on_task:
                return
            if response_delay_ms > 0:
                time.sleep(response_delay_ms / 1000)
            output = RECORDING_PREFIX + input_value
            evidence_sha256 = completion_evidence_digest(
                semantic_spec_sha256=semantic_sha256,
                physical_algorithm_sha256=physical_sha256,
                provider_execution_sha256=execution_sha256,
                payload_sha256=payload_sha256,
                sequence=sequence,
                output=output,
            )
            if tamper_evidence_digest:
                evidence_sha256 = "0" * 64
            completion = {
                "type": "completion",
                "protocol_version": PROTOCOL_VERSION,
                "sequence": str(sequence),
                "semantic_spec_digest": semantic_sha256,
                "physical_algorithm_digest": physical_sha256,
                "provider_execution_digest": execution_sha256,
                "payload_digest": payload_sha256,
                "is_null": output is None,
                "output": output,
                "evidence_digest": evidence_sha256,
            }
            if completion_fixture is not None:
                _send_completion_fixture(connection, completion, completion_fixture)
                return
            connection.sendall(encode_frame(completion))
            expected_sequence += 1
    except ProtocolError as error:
        _send_error(connection, error.code)
    except (BrokenPipeError, ConnectionResetError, OSError):
        return
    finally:
        connection.close()


def _validate_open(message: dict[str, Any]) -> tuple[str, str, str]:
    if set(message) != _OPEN_FIELDS:
        raise ProtocolError("invalid_open_fields")
    if message["type"] != "open":
        raise ProtocolError("expected_open")
    if type(message["protocol_version"]) is not int or message["protocol_version"] != PROTOCOL_VERSION:
        raise ProtocolError("protocol_version_mismatch")
    if message["operator_kind"] != "SEM_MAP":
        raise ProtocolError("unsupported_operator_kind")
    if message["semantic_spec_id"] != RECORDING_SPEC_ID:
        raise ProtocolError("unsupported_semantic_spec")
    if (
        type(message["semantic_spec_version"]) is not int
        or message["semantic_spec_version"] != RECORDING_SPEC_VERSION
    ):
        raise ProtocolError("unsupported_semantic_spec_version")
    if message["physical_algorithm"] != RECORDING_ALGORITHM:
        raise ProtocolError("unsupported_physical_algorithm")
    if message["provider_execution_id"] != UDS_EXECUTION_ID:
        raise ProtocolError("unsupported_provider_execution")
    if message["null_policy"] != "PROPAGATE_NULL":
        raise ProtocolError("unsupported_null_policy")
    if message["error_policy"] != "FAIL_QUERY":
        raise ProtocolError("unsupported_error_policy")
    if message["input_type"] != "text" or message["output_type"] != "text":
        raise ProtocolError("unsupported_plan_type")
    semantic_sha256 = message["semantic_spec_digest"]
    physical_sha256 = message["physical_algorithm_digest"]
    execution_sha256 = message["provider_execution_digest"]
    _require_sha256(semantic_sha256, "invalid_semantic_spec_digest")
    _require_sha256(physical_sha256, "invalid_physical_algorithm_digest")
    _require_sha256(execution_sha256, "invalid_provider_execution_digest")
    if semantic_sha256 != semantic_spec_digest():
        raise ProtocolError("semantic_spec_digest_mismatch")
    if physical_sha256 != physical_algorithm_digest():
        raise ProtocolError("physical_algorithm_digest_mismatch")
    if execution_sha256 != provider_execution_digest():
        raise ProtocolError("provider_execution_digest_mismatch")
    return semantic_sha256, physical_sha256, execution_sha256


def _canonical_text(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("!I", len(encoded)) + encoded


def _validate_task(
    message: dict[str, Any],
    *,
    expected_sequence: int,
    semantic_spec_sha256: str,
    physical_algorithm_sha256: str,
    provider_execution_sha256: str,
) -> tuple[int, str, str]:
    if set(message) != _TASK_FIELDS:
        raise ProtocolError("invalid_task_fields")
    if message["type"] != "task":
        raise ProtocolError("expected_task")
    if message["protocol_version"] != PROTOCOL_VERSION:
        raise ProtocolError("protocol_version_mismatch")
    if message["semantic_spec_digest"] != semantic_spec_sha256:
        raise ProtocolError("semantic_spec_digest_mismatch")
    if message["physical_algorithm_digest"] != physical_algorithm_sha256:
        raise ProtocolError("physical_algorithm_digest_mismatch")
    if message["provider_execution_digest"] != provider_execution_sha256:
        raise ProtocolError("provider_execution_digest_mismatch")
    sequence_text = message["sequence"]
    if not isinstance(sequence_text, str) or (
        sequence_text != "0" and (not sequence_text.isascii() or not sequence_text.isdigit() or sequence_text[0] == "0")
    ):
        raise ProtocolError("invalid_sequence")
    sequence = int(sequence_text)
    if sequence != expected_sequence:
        raise ProtocolError("unexpected_sequence")
    is_null = message["is_null"]
    input_value = message["input"]
    if type(is_null) is not bool:
        raise ProtocolError("invalid_null_flag")
    if is_null:
        raise ProtocolError("null_task_not_allowed")
    if not isinstance(input_value, str):
        raise ProtocolError("invalid_input")
    if len(input_value.encode("utf-8")) > MAX_INPUT_BYTES:
        raise ProtocolError("input_too_large")
    payload_sha256 = message["payload_digest"]
    _require_sha256(payload_sha256, "invalid_payload_digest")
    if payload_sha256 != semantic_payload_digest(input_value):
        raise ProtocolError("payload_digest_mismatch")
    return sequence, input_value, payload_sha256


def _require_sha256(value: object, code: str) -> None:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ProtocolError(code)


def _read_exact(
    connection: socket.socket,
    length: int,
    *,
    allow_initial_eof: bool,
) -> bytes | None:
    chunks: list[bytes] = []
    received = 0
    while received < length:
        chunk = connection.recv(length - received)
        if not chunk:
            if allow_initial_eof and received == 0:
                return None
            raise ProtocolError("unexpected_eof")
        chunks.append(chunk)
        received += len(chunk)
    return b"".join(chunks)


def _send_error(connection: socket.socket, code: str) -> None:
    try:
        connection.sendall(encode_frame({"type": "error", "code": code}))
    except (BrokenPipeError, ConnectionResetError, OSError):
        pass


def _send_completion_fixture(
    connection: socket.socket,
    completion: dict[str, Any],
    fixture: str,
) -> None:
    """Send one deliberately invalid completion for PostgreSQL characterization tests."""
    if fixture == "malformed-json":
        _send_raw_frame(connection, b"{")
        return
    if fixture == "invalid-utf8":
        _send_raw_frame(connection, b"\xff")
        return
    if fixture == "non-object":
        _send_raw_frame(connection, b"[]")
        return
    if fixture == "error-message":
        connection.sendall(encode_frame({"type": "error", "code": "fixture_rejected"}))
        return
    if fixture == "raw-nul":
        valid_payload = encode_frame(completion)[4:]
        _send_raw_frame(connection, valid_payload + b"\x00trailing-garbage")
        return

    message = dict(completion)
    if fixture == "escaped-nul":
        message["output"] += "\x00provider-derived"
    elif fixture == "missing-field":
        del message["evidence_digest"]
    elif fixture == "extra-field":
        message["future_field"] = True
    elif fixture == "fractional-integer":
        message["protocol_version"] = 2.4
    elif fixture == "wrong-integer-type":
        message["protocol_version"] = "2"
    elif fixture == "integer-overflow":
        message["protocol_version"] = 10**100
    elif fixture == "identity-mismatch":
        message["payload_digest"] = "0" * 64
    else:
        raise ValueError(f"unknown completion fixture: {fixture}")
    connection.sendall(encode_frame(message))


def _send_raw_frame(connection: socket.socket, payload: bytes) -> None:
    connection.sendall(struct.pack("!I", len(payload)) + payload)
