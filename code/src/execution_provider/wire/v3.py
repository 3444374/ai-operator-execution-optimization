"""Version-3 synchronous exact SemFilter wire and digest contract."""

from __future__ import annotations

import hashlib
import json
import struct
from dataclasses import dataclass
from typing import Any

from .framing import MAX_FRAME_BYTES, ProtocolError


PROTOCOL_VERSION = 3
MAX_INPUT_BYTES = 163_840
MAX_INFLIGHT_TASKS = 1
PLAN_SCHEMA_VERSION = 2
SEMANTIC_SPEC_ID = "semloom.semantic.sem_filter.exact.v1"
SEMANTIC_SPEC_VERSION = 1
PROMPT_PROGRAM_ID = "semloom.sem_filter.exact_chat.v1"
PROMPT_PROGRAM_VERSION = 1
RESULT_PARSER_ID = "semloom.sem_filter.tristate_ascii.v1"
RESULT_PARSER_VERSION = 1
PHYSICAL_ALGORITHM = "MODEL_REFERENCE_SYNC_V1"
PHYSICAL_ROLE = "reference"
GOLDEN_EXECUTION_ID = "semloom.provider.golden.uds.v3"
SYSTEM_DIRECTIVE = (
    "Evaluate whether the input satisfies the instruction. Reply with exactly TRUE, "
    "FALSE, or UNKNOWN. Use UNKNOWN only when the input lacks enough information."
)
INSTRUCTION_SEPARATOR = "\nInstruction:\n"


def _canonical_text(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("!I", len(encoded)) + encoded


def _uint32(value: int) -> bytes:
    return struct.pack("!I", value)


def _uint64(value: int) -> bytes:
    return struct.pack("!Q", value)


PROMPT_PROGRAM_DIGEST = hashlib.sha256(
    b"semloom-prompt-program-v1\0"
    + _canonical_text(PROMPT_PROGRAM_ID)
    + _uint32(PROMPT_PROGRAM_VERSION)
    + _canonical_text("system")
    + _canonical_text("content")
    + _canonical_text(SYSTEM_DIRECTIVE)
    + _canonical_text(INSTRUCTION_SEPARATOR)
    + _canonical_text("user")
    + _canonical_text("content")
).hexdigest()

RESULT_PARSER_DIGEST = hashlib.sha256(
    b"semloom-result-parser-v1\0"
    + _canonical_text(RESULT_PARSER_ID)
    + _uint32(RESULT_PARSER_VERSION)
    + _canonical_text("TRUE")
    + _canonical_text("FALSE")
    + _canonical_text("UNKNOWN")
    + _canonical_text("exact-utf8-no-trim")
).hexdigest()

GENERATION_CONSTRAINTS = {
    "temperature": 0,
    "top_p": 1,
    "max_tokens": 8,
    "n": 1,
    "stream": False,
    "stop": ["\n"],
}

ERROR_CODES = frozenset(
    {
        "GATEWAY_INTERNAL",
        "GOLDEN_FIXTURE_INVALID",
        "GOLDEN_FIXTURE_MISSING",
        "INVALID_OPEN",
        "INVALID_TASK",
        "MODEL_REQUEST_REJECTED",
        "MODEL_RESPONSE_INVALID",
        "MODEL_TIMEOUT",
        "MODEL_UNAVAILABLE",
    }
)

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
    "physical_role",
    "prompt_program_digest",
    "result_parser_digest",
    "model_id",
    "generation_constraints",
    "null_policy",
    "error_policy",
    "order_policy",
    "input_type",
    "raw_output_type",
}
_TASK_FIELDS = {
    "type",
    "protocol_version",
    "sequence",
    "semantic_spec_digest",
    "physical_algorithm_digest",
    "provider_execution_digest",
    "semantic_payload_digest",
    "canonical_messages",
}
_MESSAGE_FIELDS = {"role", "content"}


@dataclass(frozen=True)
class SemanticFilterPlan:
    """Values from the PostgreSQL-owned exact SemFilter plan."""

    instruction: str
    model_id: str

    def __post_init__(self) -> None:
        if not isinstance(self.instruction, str):
            raise TypeError("instruction must be text")
        if not isinstance(self.model_id, str):
            raise TypeError("model_id must be text")
        instruction_length = len(self.instruction.encode("utf-8"))
        model_length = len(self.model_id.encode("utf-8"))
        if instruction_length == 0 or instruction_length > 4096:
            raise ValueError("instruction length is outside the plan contract")
        if model_length == 0 or model_length > 128:
            raise ValueError("model_id length is outside the plan contract")


@dataclass(frozen=True)
class OpenContext:
    semantic_spec_digest: str
    physical_algorithm_digest: str
    provider_execution_digest: str
    model_id: str


def canonical_messages(instruction: str, input_value: str) -> bytes:
    """Build the exact UTF-8 chat message bytes consumed by v3."""
    SemanticFilterPlan(instruction=instruction, model_id="validation")
    if not isinstance(input_value, str):
        raise TypeError("input_value must be text")
    if len(input_value.encode("utf-8")) > MAX_INPUT_BYTES:
        raise ValueError("input length is outside the wire contract")
    messages = [
        {
            "role": "system",
            "content": SYSTEM_DIRECTIVE + INSTRUCTION_SEPARATOR + instruction,
        },
        {"role": "user", "content": input_value},
    ]
    return json.dumps(
        messages,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def semantic_spec_digest(plan: SemanticFilterPlan) -> str:
    canonical = (
        b"semloom-semantic-spec-v2\0"
        + _uint32(PLAN_SCHEMA_VERSION)
        + _canonical_text(SEMANTIC_SPEC_ID)
        + _uint32(SEMANTIC_SPEC_VERSION)
        + _canonical_text("SEM_FILTER")
        + _canonical_text("text")
        + _canonical_text("tristate")
        + _canonical_text(plan.instruction)
        + _canonical_text(PROMPT_PROGRAM_ID)
        + _uint32(PROMPT_PROGRAM_VERSION)
        + _canonical_text(PROMPT_PROGRAM_DIGEST)
        + _canonical_text(RESULT_PARSER_ID)
        + _uint32(RESULT_PARSER_VERSION)
        + _canonical_text(RESULT_PARSER_DIGEST)
        + _canonical_text("PROPAGATE_NULL")
        + _canonical_text("FAIL_QUERY")
        + _canonical_text("INPUT_ORDER")
        + _canonical_text(plan.model_id)
        + _uint32(0)
        + _uint32(1)
        + _uint32(8)
        + _uint32(1)
        + b"\x00"
        + _canonical_text("\n")
    )
    return hashlib.sha256(canonical).hexdigest()


def physical_algorithm_digest() -> str:
    canonical = (
        b"semloom-physical-algorithm-v2\0"
        + _canonical_text(PHYSICAL_ALGORITHM)
        + _canonical_text(PHYSICAL_ROLE)
    )
    return hashlib.sha256(canonical).hexdigest()


def provider_execution_digest(
    model_id: str,
    *,
    provider_execution_id: str = GOLDEN_EXECUTION_ID,
) -> str:
    SemanticFilterPlan(instruction="validation", model_id=model_id)
    if not isinstance(provider_execution_id, str) or not provider_execution_id:
        raise ValueError("provider_execution_id must be non-empty text")
    canonical = (
        b"semloom-provider-execution-v3\0"
        + _uint32(PROTOCOL_VERSION)
        + _canonical_text(provider_execution_id)
        + _canonical_text(model_id)
    )
    return hashlib.sha256(canonical).hexdigest()


def semantic_payload_digest(
    *,
    semantic_spec_sha256: str,
    input_value: str,
    canonical_messages_utf8: bytes,
) -> str:
    _require_sha256(semantic_spec_sha256, "INVALID_SEMANTIC_SPEC_DIGEST")
    if not isinstance(input_value, str):
        raise TypeError("input_value must be text")
    if not isinstance(canonical_messages_utf8, bytes):
        raise TypeError("canonical_messages_utf8 must be bytes")
    encoded_input = input_value.encode("utf-8")
    if len(encoded_input) > MAX_INPUT_BYTES:
        raise ValueError("input length is outside the wire contract")
    canonical = (
        b"semloom-payload-v3\0"
        + semantic_spec_sha256.encode("ascii")
        + b"\x00"
        + _uint64(len(encoded_input))
        + encoded_input
        + _uint64(len(canonical_messages_utf8))
        + canonical_messages_utf8
    )
    return hashlib.sha256(canonical).hexdigest()


def completion_evidence_digest(
    *,
    semantic_spec_sha256: str,
    physical_algorithm_sha256: str,
    provider_execution_sha256: str,
    semantic_payload_sha256: str,
    sequence: int,
    raw_output: str,
    finish_reason: str,
    response_model_id: str,
    prompt_tokens: int,
    output_tokens: int,
) -> str:
    for value, code in (
        (semantic_spec_sha256, "INVALID_SEMANTIC_SPEC_DIGEST"),
        (physical_algorithm_sha256, "INVALID_PHYSICAL_ALGORITHM_DIGEST"),
        (provider_execution_sha256, "INVALID_PROVIDER_EXECUTION_DIGEST"),
        (semantic_payload_sha256, "INVALID_SEMANTIC_PAYLOAD_DIGEST"),
    ):
        _require_sha256(value, code)
    for value, label in (
        (sequence, "sequence"),
        (prompt_tokens, "prompt_tokens"),
        (output_tokens, "output_tokens"),
    ):
        if type(value) is not int or value < 0 or value >= 2**64:
            raise ValueError(f"{label} must be uint64")
    canonical = (
        b"semloom-completion-v3\0"
        + semantic_spec_sha256.encode("ascii")
        + physical_algorithm_sha256.encode("ascii")
        + provider_execution_sha256.encode("ascii")
        + semantic_payload_sha256.encode("ascii")
        + _uint64(sequence)
        + _canonical_text(raw_output)
        + _canonical_text(finish_reason)
        + _canonical_text(response_model_id)
        + _uint64(prompt_tokens)
        + _uint64(output_tokens)
    )
    return hashlib.sha256(canonical).hexdigest()


def build_open_message(
    plan: SemanticFilterPlan,
    *,
    provider_execution_id: str = GOLDEN_EXECUTION_ID,
) -> dict[str, Any]:
    return {
        "type": "open",
        "protocol_version": PROTOCOL_VERSION,
        "semantic_spec_digest": semantic_spec_digest(plan),
        "physical_algorithm_digest": physical_algorithm_digest(),
        "provider_execution_digest": provider_execution_digest(
            plan.model_id,
            provider_execution_id=provider_execution_id,
        ),
        "provider_execution_id": provider_execution_id,
        "operator_kind": "SEM_FILTER",
        "semantic_spec_id": SEMANTIC_SPEC_ID,
        "semantic_spec_version": SEMANTIC_SPEC_VERSION,
        "physical_algorithm": PHYSICAL_ALGORITHM,
        "physical_role": PHYSICAL_ROLE,
        "prompt_program_digest": PROMPT_PROGRAM_DIGEST,
        "result_parser_digest": RESULT_PARSER_DIGEST,
        "model_id": plan.model_id,
        "generation_constraints": dict(GENERATION_CONSTRAINTS),
        "null_policy": "PROPAGATE_NULL",
        "error_policy": "FAIL_QUERY",
        "order_policy": "INPUT_ORDER",
        "input_type": "text",
        "raw_output_type": "tristate_ascii",
    }


def build_task_message(
    plan: SemanticFilterPlan,
    *,
    sequence: int,
    input_value: str,
    provider_execution_id: str = GOLDEN_EXECUTION_ID,
) -> dict[str, Any]:
    if type(sequence) is not int or sequence < 0 or sequence >= 2**64:
        raise ValueError("sequence must be uint64")
    message_bytes = canonical_messages(plan.instruction, input_value)
    semantic_digest = semantic_spec_digest(plan)
    return {
        "type": "task",
        "protocol_version": PROTOCOL_VERSION,
        "sequence": str(sequence),
        "semantic_spec_digest": semantic_digest,
        "physical_algorithm_digest": physical_algorithm_digest(),
        "provider_execution_digest": provider_execution_digest(
            plan.model_id,
            provider_execution_id=provider_execution_id,
        ),
        "semantic_payload_digest": semantic_payload_digest(
            semantic_spec_sha256=semantic_digest,
            input_value=input_value,
            canonical_messages_utf8=message_bytes,
        ),
        "canonical_messages": json.loads(message_bytes),
    }


def build_error_message(code: str, *, sequence: int | None) -> dict[str, Any]:
    """Build the strict redacted wire-v3 error object."""
    if code not in ERROR_CODES:
        raise ValueError("code is outside the wire-v3 error contract")
    if sequence is not None and (
        type(sequence) is not int or sequence < 0 or sequence >= 2**64
    ):
        raise ValueError("sequence must be null or uint64")
    return {
        "type": "error",
        "protocol_version": PROTOCOL_VERSION,
        "sequence": None if sequence is None else str(sequence),
        "code": code,
    }


def validate_open(
    message: dict[str, Any],
    *,
    provider_execution_id: str = GOLDEN_EXECUTION_ID,
) -> OpenContext:
    if set(message) != _OPEN_FIELDS or message.get("type") != "open":
        raise ProtocolError("INVALID_OPEN")
    if type(message["protocol_version"]) is not int or message["protocol_version"] != 3:
        raise ProtocolError("INVALID_OPEN")
    model_id = message["model_id"]
    try:
        SemanticFilterPlan(instruction="validation", model_id=model_id)
    except (TypeError, ValueError) as error:
        raise ProtocolError("INVALID_OPEN") from error
    expected_values = {
        "provider_execution_id": provider_execution_id,
        "operator_kind": "SEM_FILTER",
        "semantic_spec_id": SEMANTIC_SPEC_ID,
        "semantic_spec_version": SEMANTIC_SPEC_VERSION,
        "physical_algorithm": PHYSICAL_ALGORITHM,
        "physical_role": PHYSICAL_ROLE,
        "prompt_program_digest": PROMPT_PROGRAM_DIGEST,
        "result_parser_digest": RESULT_PARSER_DIGEST,
        "null_policy": "PROPAGATE_NULL",
        "error_policy": "FAIL_QUERY",
        "order_policy": "INPUT_ORDER",
        "input_type": "text",
        "raw_output_type": "tristate_ascii",
    }
    if any(message[key] != value for key, value in expected_values.items()):
        raise ProtocolError("INVALID_OPEN")
    constraints = message["generation_constraints"]
    if (
        not isinstance(constraints, dict)
        or set(constraints) != set(GENERATION_CONSTRAINTS)
        or constraints != GENERATION_CONSTRAINTS
        or type(constraints["stream"]) is not bool
        or any(
            type(constraints[key]) is not int
            for key in ("temperature", "top_p", "max_tokens", "n")
        )
    ):
        raise ProtocolError("INVALID_OPEN")
    semantic_digest = message["semantic_spec_digest"]
    physical_digest = message["physical_algorithm_digest"]
    execution_digest = message["provider_execution_digest"]
    for value in (semantic_digest, physical_digest, execution_digest):
        _require_sha256(value, "INVALID_OPEN")
    if physical_digest != physical_algorithm_digest():
        raise ProtocolError("INVALID_OPEN")
    if execution_digest != provider_execution_digest(
        model_id,
        provider_execution_id=provider_execution_id,
    ):
        raise ProtocolError("INVALID_OPEN")
    return OpenContext(semantic_digest, physical_digest, execution_digest, model_id)


def validate_task(
    message: dict[str, Any],
    *,
    expected_sequence: int,
    open_context: OpenContext,
) -> tuple[int, str]:
    if set(message) != _TASK_FIELDS or message.get("type") != "task":
        raise ProtocolError("INVALID_TASK")
    if type(message["protocol_version"]) is not int or message["protocol_version"] != 3:
        raise ProtocolError("INVALID_TASK")
    sequence = decimal_uint64(message["sequence"])
    if sequence != expected_sequence:
        raise ProtocolError("INVALID_TASK")
    if (
        message["semantic_spec_digest"] != open_context.semantic_spec_digest
        or message["physical_algorithm_digest"] != open_context.physical_algorithm_digest
        or message["provider_execution_digest"] != open_context.provider_execution_digest
    ):
        raise ProtocolError("INVALID_TASK")
    messages = message["canonical_messages"]
    if (
        not isinstance(messages, list)
        or len(messages) != 2
        or any(
            not isinstance(item, dict) or set(item) != _MESSAGE_FIELDS
            for item in messages
        )
        or messages[0].get("role") != "system"
        or messages[1].get("role") != "user"
        or not isinstance(messages[0].get("content"), str)
        or not isinstance(messages[1].get("content"), str)
        or not messages[0]["content"].startswith(
            SYSTEM_DIRECTIVE + INSTRUCTION_SEPARATOR
        )
        or messages[0]["content"] == SYSTEM_DIRECTIVE + INSTRUCTION_SEPARATOR
    ):
        raise ProtocolError("INVALID_TASK")
    input_value = messages[1]["content"]
    if len(input_value.encode("utf-8")) > MAX_INPUT_BYTES:
        raise ProtocolError("INVALID_TASK")
    normalized_messages = [
        {"role": messages[0]["role"], "content": messages[0]["content"]},
        {"role": messages[1]["role"], "content": messages[1]["content"]},
    ]
    message_bytes = json.dumps(
        normalized_messages,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    payload_digest = message["semantic_payload_digest"]
    _require_sha256(payload_digest, "INVALID_TASK")
    expected_payload_digest = semantic_payload_digest(
        semantic_spec_sha256=open_context.semantic_spec_digest,
        input_value=input_value,
        canonical_messages_utf8=message_bytes,
    )
    if payload_digest != expected_payload_digest:
        raise ProtocolError("INVALID_TASK")
    return sequence, payload_digest


def decimal_uint64(value: object) -> int:
    if not isinstance(value, str) or (
        value != "0"
        and (
            not value.isascii()
            or not value.isdigit()
            or value[0] == "0"
        )
    ):
        raise ProtocolError("INVALID_TASK")
    result = int(value)
    if result >= 2**64:
        raise ProtocolError("INVALID_TASK")
    return result


def _require_sha256(value: object, code: str) -> None:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ProtocolError(code)


__all__ = [
    "ERROR_CODES",
    "GOLDEN_EXECUTION_ID",
    "GENERATION_CONSTRAINTS",
    "MAX_FRAME_BYTES",
    "MAX_INFLIGHT_TASKS",
    "MAX_INPUT_BYTES",
    "PHYSICAL_ALGORITHM",
    "PROMPT_PROGRAM_DIGEST",
    "PROTOCOL_VERSION",
    "RESULT_PARSER_DIGEST",
    "SemanticFilterPlan",
    "build_open_message",
    "build_error_message",
    "build_task_message",
    "canonical_messages",
    "completion_evidence_digest",
    "decimal_uint64",
    "physical_algorithm_digest",
    "provider_execution_digest",
    "semantic_payload_digest",
    "semantic_spec_digest",
    "validate_open",
    "validate_task",
]
