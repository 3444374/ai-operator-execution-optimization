"""Deterministic recording adapter for the frozen version-2 gateway contract."""

from __future__ import annotations

import socket
import struct
import time
from typing import Any

from ..wire.v2 import (
    MAX_FRAME_BYTES,
    MAX_INFLIGHT_TASKS,
    MAX_INPUT_BYTES,
    PROTOCOL_VERSION,
    RECORDING_PREFIX,
    ProtocolError,
    _validate_open,
    _validate_task,
    completion_evidence_digest,
    encode_frame,
    read_frame,
)


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
        operator_kind, semantic_sha256, physical_sha256, execution_sha256 = (
            _validate_open(opened)
        )
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
            output = RECORDING_PREFIX + input_value if operator_kind == "SEM_MAP" else input_value
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
