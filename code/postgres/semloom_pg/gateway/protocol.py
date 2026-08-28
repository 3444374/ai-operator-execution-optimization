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

_OPEN_FIELDS = {
    "type",
    "protocol_version",
    "plan_digest",
    "mapped_column",
    "null_policy",
    "input_type",
    "output_type",
}
_TASK_FIELDS = {
    "type",
    "protocol_version",
    "sequence",
    "plan_digest",
    "payload_digest",
    "is_null",
    "input",
}


class ProtocolError(Exception):
    """A fail-closed protocol violation safe to report without payload data."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def plan_digest(*, mapped_column: int) -> str:
    """Return the v2 canonical digest for the supported text SemMap plan."""
    if type(mapped_column) is not int or mapped_column <= 0:
        raise ValueError("mapped_column must be a positive integer")
    canonical = (
        b"semloom-plan-v2\0"
        + struct.pack("!I", mapped_column)
        + b"SEM_MAP\0PROPAGATE_NULL\0text\0text"
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
    plan_sha256: str,
    payload_sha256: str,
    sequence: int,
    output: str | None,
) -> str:
    """Bind one terminal recording result to its plan, task, and sequence."""
    _require_sha256(plan_sha256, "invalid_plan_digest")
    _require_sha256(payload_sha256, "invalid_payload_digest")
    if type(sequence) is not int or sequence < 0:
        raise ValueError("sequence must be a non-negative integer")
    if output is not None and not isinstance(output, str):
        raise TypeError("completion output must be text or None")
    encoded = b"" if output is None else output.encode("utf-8")
    canonical = (
        b"semloom-completion-v1\0"
        + plan_sha256.encode("ascii")
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
) -> None:
    """Serve one query-scoped, single-inflight recording session."""
    try:
        opened = read_frame(connection)
        if opened is None:
            return
        mapped_column, plan_sha256 = _validate_open(opened)
        connection.sendall(
            encode_frame(
                {
                    "type": "opened",
                    "protocol_version": PROTOCOL_VERSION,
                    "plan_digest": plan_sha256,
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
                plan_sha256=plan_sha256,
            )
            if disconnect_on_task:
                return
            if response_delay_ms > 0:
                time.sleep(response_delay_ms / 1000)
            output = RECORDING_PREFIX + input_value
            evidence_sha256 = completion_evidence_digest(
                plan_sha256=plan_sha256,
                payload_sha256=payload_sha256,
                sequence=sequence,
                output=output,
            )
            if tamper_evidence_digest:
                evidence_sha256 = "0" * 64
            connection.sendall(
                encode_frame(
                    {
                        "type": "completion",
                        "protocol_version": PROTOCOL_VERSION,
                        "sequence": str(sequence),
                        "plan_digest": plan_sha256,
                        "payload_digest": payload_sha256,
                        "is_null": output is None,
                        "output": output,
                        "evidence_digest": evidence_sha256,
                    }
                )
            )
            expected_sequence += 1
    except ProtocolError as error:
        _send_error(connection, error.code)
    except (BrokenPipeError, ConnectionResetError, OSError):
        return
    finally:
        connection.close()


def _validate_open(message: dict[str, Any]) -> tuple[int, str]:
    if set(message) != _OPEN_FIELDS:
        raise ProtocolError("invalid_open_fields")
    if message["type"] != "open":
        raise ProtocolError("expected_open")
    if message["protocol_version"] != PROTOCOL_VERSION:
        raise ProtocolError("protocol_version_mismatch")
    mapped_column = message["mapped_column"]
    if type(mapped_column) is not int or mapped_column <= 0:
        raise ProtocolError("invalid_mapped_column")
    if message["null_policy"] != "PROPAGATE_NULL":
        raise ProtocolError("unsupported_null_policy")
    if message["input_type"] != "text" or message["output_type"] != "text":
        raise ProtocolError("unsupported_plan_type")
    plan_sha256 = message["plan_digest"]
    _require_sha256(plan_sha256, "invalid_plan_digest")
    if plan_sha256 != plan_digest(mapped_column=mapped_column):
        raise ProtocolError("plan_digest_mismatch")
    return mapped_column, plan_sha256


def _validate_task(
    message: dict[str, Any],
    *,
    expected_sequence: int,
    plan_sha256: str,
) -> tuple[int, str, str]:
    if set(message) != _TASK_FIELDS:
        raise ProtocolError("invalid_task_fields")
    if message["type"] != "task":
        raise ProtocolError("expected_task")
    if message["protocol_version"] != PROTOCOL_VERSION:
        raise ProtocolError("protocol_version_mismatch")
    if message["plan_digest"] != plan_sha256:
        raise ProtocolError("plan_digest_mismatch")
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
