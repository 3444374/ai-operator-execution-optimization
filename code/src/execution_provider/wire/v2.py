"""Frozen version-2 contract for the SemLoom recording gateway."""

from __future__ import annotations

import hashlib
import struct
from typing import Any

from .framing import MAX_FRAME_BYTES, ProtocolError, encode_frame, read_frame


PROTOCOL_VERSION = 2
MAX_INPUT_BYTES = (MAX_FRAME_BYTES - 4096) // 6
MAX_INFLIGHT_TASKS = 1
RECORDING_PREFIX = "recorded:"
RECORDING_SPEC_ID = "semloom.recording.sem_map.text"
FILTER_RECORDING_SPEC_ID = "semloom.recording.sem_filter.tristate"
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

__all__ = [
    "FILTER_RECORDING_SPEC_ID",
    "MAX_FRAME_BYTES",
    "MAX_INFLIGHT_TASKS",
    "MAX_INPUT_BYTES",
    "PROTOCOL_VERSION",
    "ProtocolError",
    "RECORDING_ALGORITHM",
    "RECORDING_PREFIX",
    "RECORDING_SPEC_ID",
    "RECORDING_SPEC_VERSION",
    "UDS_EXECUTION_ID",
    "completion_evidence_digest",
    "encode_frame",
    "filter_semantic_spec_digest",
    "physical_algorithm_digest",
    "provider_execution_digest",
    "read_frame",
    "semantic_payload_digest",
    "semantic_spec_digest",
]


def semantic_spec_digest() -> str:
    """Digest the SQL-visible semantics of the recording SemMap spec."""
    return _semantic_spec_digest(
        operator_kind="SEM_MAP",
        semantic_spec_id=RECORDING_SPEC_ID,
        output_type="text",
    )


def filter_semantic_spec_digest() -> str:
    """Digest the SQL-visible semantics of the recording SemFilter spec."""
    return _semantic_spec_digest(
        operator_kind="SEM_FILTER",
        semantic_spec_id=FILTER_RECORDING_SPEC_ID,
        output_type="tristate",
    )


def _semantic_spec_digest(
    *, operator_kind: str, semantic_spec_id: str, output_type: str
) -> str:
    canonical = (
        b"semloom-semantic-spec-v1\0"
        + _canonical_text(operator_kind)
        + _canonical_text(semantic_spec_id)
        + struct.pack("!I", RECORDING_SPEC_VERSION)
        + _canonical_text("PROPAGATE_NULL")
        + _canonical_text("FAIL_QUERY")
        + _canonical_text("text")
        + _canonical_text(output_type)
    )
    return hashlib.sha256(canonical).hexdigest()


def physical_algorithm_digest() -> str:
    """Digest the database-selected recording physical algorithm."""
    canonical = b"semloom-physical-algorithm-v1\0" + _canonical_text(RECORDING_ALGORITHM)
    return hashlib.sha256(canonical).hexdigest()


def provider_execution_digest() -> str:
    """Digest the concrete UDS recording execution profile."""
    canonical = b"semloom-provider-execution-v1\0" + _canonical_text(UDS_EXECUTION_ID)
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


def _validate_open(message: dict[str, Any]) -> tuple[str, str, str, str]:
    if set(message) != _OPEN_FIELDS:
        raise ProtocolError("invalid_open_fields")
    if message["type"] != "open":
        raise ProtocolError("expected_open")
    if (
        type(message["protocol_version"]) is not int
        or message["protocol_version"] != PROTOCOL_VERSION
    ):
        raise ProtocolError("protocol_version_mismatch")
    operator_kind = message["operator_kind"]
    semantic_spec_id = message["semantic_spec_id"]
    output_type = message["output_type"]
    if operator_kind == "SEM_MAP":
        expected_spec_id = RECORDING_SPEC_ID
        expected_output_type = "text"
        expected_semantic_digest = semantic_spec_digest()
    elif operator_kind == "SEM_FILTER":
        expected_spec_id = FILTER_RECORDING_SPEC_ID
        expected_output_type = "tristate"
        expected_semantic_digest = filter_semantic_spec_digest()
    else:
        raise ProtocolError("unsupported_operator_kind")
    if semantic_spec_id != expected_spec_id:
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
    if message["input_type"] != "text" or output_type != expected_output_type:
        raise ProtocolError("unsupported_plan_type")
    semantic_sha256 = message["semantic_spec_digest"]
    physical_sha256 = message["physical_algorithm_digest"]
    execution_sha256 = message["provider_execution_digest"]
    _require_sha256(semantic_sha256, "invalid_semantic_spec_digest")
    _require_sha256(physical_sha256, "invalid_physical_algorithm_digest")
    _require_sha256(execution_sha256, "invalid_provider_execution_digest")
    if semantic_sha256 != expected_semantic_digest:
        raise ProtocolError("semantic_spec_digest_mismatch")
    if physical_sha256 != physical_algorithm_digest():
        raise ProtocolError("physical_algorithm_digest_mismatch")
    if execution_sha256 != provider_execution_digest():
        raise ProtocolError("provider_execution_digest_mismatch")
    return operator_kind, semantic_sha256, physical_sha256, execution_sha256


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
        sequence_text != "0"
        and (
            not sequence_text.isascii()
            or not sequence_text.isdigit()
            or sequence_text[0] == "0"
        )
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
