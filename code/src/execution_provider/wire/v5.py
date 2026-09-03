"""Version-five generative Map identities; transport and session code are shared."""

import hashlib
import json
import struct
from dataclasses import dataclass

from ..completion import Completion
from ..semantic_map import (
    SemanticMapPlan, PHYSICAL_ALGORITHM, PHYSICAL_ROLE, input_utf8, canonical_messages,
    validate_completion_values, UINT64_MAX, SEMANTIC_SPEC_ID,
    PROMPT_PROGRAM_DIGEST, RESULT_PARSER_DIGEST, MAX_INPUT_BYTES, MAX_OUTPUT_BYTES,
)
from .framing import MAX_FRAME_BYTES, ProtocolError, has_duplicate_fields

PROTOCOL_VERSION = 5
PLAN_SCHEMA_VERSION = 4
GOLDEN_EXECUTION_ID = "semloom.provider.golden.uds.v5"
FIXED_EXECUTION_ID = "semloom.provider.openai-compatible-fixed.uds.v5"
MAX_INFLIGHT_TASKS = 1

_OPEN_VALUES = {
    "type": "open", "protocol_version": PROTOCOL_VERSION, "operator_kind": "SEM_MAP",
    "semantic_spec_id": SEMANTIC_SPEC_ID, "semantic_spec_version": 1,
    "physical_algorithm": PHYSICAL_ALGORITHM, "physical_role": PHYSICAL_ROLE,
    "prompt_program_digest": PROMPT_PROGRAM_DIGEST, "result_parser_digest": RESULT_PARSER_DIGEST,
    "null_policy": "PROPAGATE_NULL", "error_policy": "FAIL_QUERY", "order_policy": "INPUT_ORDER",
    "input_type": "text", "raw_output_type": "text", "max_input_bytes": MAX_INPUT_BYTES,
    "max_output_bytes": MAX_OUTPUT_BYTES,
}
_IDENTITY_FIELDS = {"semantic_spec_digest", "physical_algorithm_digest", "provider_execution_digest"}
_OPEN_FIELDS = set(_OPEN_VALUES) | _IDENTITY_FIELDS | {"provider_execution_id", "model_id", "generation_constraints"}
_TASK_FIELDS = _IDENTITY_FIELDS | {"type", "protocol_version", "sequence", "semantic_payload_digest", "canonical_messages"}
_COMPLETION_FIELDS = _IDENTITY_FIELDS | {"type", "protocol_version", "sequence", "semantic_payload_digest",
    "raw_output", "response_model_id", "prompt_tokens", "output_tokens", "finish_reason", "completion_evidence_digest"}
ERROR_CODES = frozenset({"GATEWAY_INTERNAL", "GOLDEN_FIXTURE_INVALID", "GOLDEN_FIXTURE_MISSING", "INVALID_OPEN",
    "INVALID_TASK", "MODEL_REQUEST_REJECTED", "MODEL_RESPONSE_INVALID", "MODEL_TIMEOUT", "MODEL_UNAVAILABLE",
    "OUTPUT_TOO_LARGE"})


@dataclass(frozen=True)
class OpenContext:
    semantic_spec_digest: str
    physical_algorithm_digest: str
    provider_execution_digest: str
    model_id: str
    max_tokens: int

    @property
    def generation_profile(self) -> None:
        return None


def _text(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("!I", len(encoded)) + encoded


def _uint64(value: int) -> bytes:
    if type(value) is not int or not 0 <= value <= UINT64_MAX:
        raise ValueError("invalid Map uint64")
    return struct.pack("!Q", value)


def _digest_bytes(value: str) -> bytes:
    if type(value) is not str or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("invalid Map digest")
    return value.encode("ascii")


def semantic_spec_digest(plan: SemanticMapPlan) -> str:
    return plan.digest


def physical_algorithm_digest() -> str:
    return hashlib.sha256(b"semloom-physical-algorithm-v2\0"
                          + _text(PHYSICAL_ALGORITHM) + _text(PHYSICAL_ROLE)).hexdigest()


def provider_execution_digest(model_id: str, *, provider_execution_id: str = GOLDEN_EXECUTION_ID) -> str:
    SemanticMapPlan("validation", model_id, 1)
    if provider_execution_id not in (GOLDEN_EXECUTION_ID, FIXED_EXECUTION_ID):
        raise ValueError("unsupported Map execution identity")
    return hashlib.sha256(b"semloom-provider-execution-v5\0" + struct.pack("!I", PROTOCOL_VERSION)
                          + _text(provider_execution_id) + _text(model_id)).hexdigest()


def semantic_payload_digest(*, semantic_spec_sha256: str, input_value: str,
                            canonical_messages_utf8: bytes) -> str:
    digest = _digest_bytes(semantic_spec_sha256)
    encoded = input_utf8(input_value)
    if type(canonical_messages_utf8) is not bytes:
        raise TypeError("Map canonical messages must be bytes")
    if len(canonical_messages_utf8) > MAX_FRAME_BYTES:
        raise ValueError("invalid Map canonical message length")
    return hashlib.sha256(b"semloom-payload-v5\0" + digest + b"\0"
                          + _uint64(len(encoded)) + encoded
                          + _uint64(len(canonical_messages_utf8)) + canonical_messages_utf8).hexdigest()


def completion_evidence_digest(*, semantic_spec_sha256: str, physical_algorithm_sha256: str,
    provider_execution_sha256: str, semantic_payload_sha256: str, sequence: int,
    raw_output: str, finish_reason: str, response_model_id: str, prompt_tokens: int, output_tokens: int) -> str:
    completion = Completion(raw_output, response_model_id, prompt_tokens, output_tokens, finish_reason)
    validate_completion_values(completion)
    identity = b"".join(_digest_bytes(value) for value in (semantic_spec_sha256, physical_algorithm_sha256,
                        provider_execution_sha256, semantic_payload_sha256))
    return hashlib.sha256(b"semloom-completion-v5\0" + identity + _uint64(sequence)
        + _text(raw_output) + _text(finish_reason) + _text(response_model_id)
        + _uint64(prompt_tokens) + _uint64(output_tokens)).hexdigest()


def _identity_fields(plan: SemanticMapPlan, execution_id: str) -> dict[str, str]:
    return {"semantic_spec_digest": plan.digest, "physical_algorithm_digest": physical_algorithm_digest(),
            "provider_execution_digest": provider_execution_digest(plan.model_id, provider_execution_id=execution_id)}


def build_open_message(plan: SemanticMapPlan, *, provider_execution_id: str = GOLDEN_EXECUTION_ID) -> dict:
    return {**_OPEN_VALUES, **_identity_fields(plan, provider_execution_id),
            "provider_execution_id": provider_execution_id, "model_id": plan.model_id,
            "generation_constraints": plan.generation_constraints()}


def build_task_message(plan: SemanticMapPlan, *, sequence: int, input_value: str,
                       provider_execution_id: str = GOLDEN_EXECUTION_ID) -> dict:
    _uint64(sequence)
    messages = canonical_messages(plan.instruction, input_value)
    return {"type": "task", "protocol_version": PROTOCOL_VERSION, "sequence": str(sequence),
            **_identity_fields(plan, provider_execution_id),
            "semantic_payload_digest": semantic_payload_digest(semantic_spec_sha256=plan.digest,
                input_value=input_value, canonical_messages_utf8=messages),
            "canonical_messages": json.loads(messages)}


def _require_fields(message: object, fields: set[str]) -> None:
    if not isinstance(message, dict) or has_duplicate_fields(message) or set(message) != fields:
        raise ValueError("invalid Map message fields")


def _matches(actual: object, expected: object) -> bool:
    return type(actual) is type(expected) and actual == expected


def validate_open(message: dict, *, provider_execution_id: str = GOLDEN_EXECUTION_ID) -> OpenContext:
    """Validate fixed values and independent identities; instruction arrives in task."""
    try:
        _require_fields(message, _OPEN_FIELDS)
        if any(not _matches(message[key], value) for key, value in _OPEN_VALUES.items()):
            raise ValueError("invalid Map open values")
        if not _matches(message["provider_execution_id"], provider_execution_id):
            raise ValueError("invalid Map execution identity")
        constraints = message["generation_constraints"]
        _require_fields(constraints, {"temperature", "top_p", "max_tokens", "n", "stream", "stop"})
        plan = SemanticMapPlan("validation", message["model_id"], constraints["max_tokens"])
        if any(not _matches(constraints[key], value) for key, value in plan.generation_constraints().items()):
            raise ValueError("invalid Map generation values")
        for key in _IDENTITY_FIELDS:
            _digest_bytes(message[key])
        if (message["physical_algorithm_digest"] != physical_algorithm_digest() or
            message["provider_execution_digest"] != provider_execution_digest(plan.model_id,
                provider_execution_id=provider_execution_id)):
            raise ValueError("invalid Map execution identity")
        return OpenContext(message["semantic_spec_digest"], message["physical_algorithm_digest"],
                           message["provider_execution_digest"], plan.model_id, plan.max_tokens)
    except (KeyError, TypeError, ValueError):
        raise ProtocolError("INVALID_OPEN") from None


def decimal_uint64(value: object) -> int:
    if (type(value) is not str or not 1 <= len(value) <= 20 or not value.isascii() or not value.isdigit()
        or (len(value) > 1 and value[0] == "0")):
        raise ProtocolError("INVALID_TASK")
    parsed = int(value)
    if parsed > UINT64_MAX:
        raise ProtocolError("INVALID_TASK")
    return parsed


def validate_task(message: dict, *, expected_sequence: int, open_context: OpenContext) -> tuple[int, str]:
    """Bind actual message contents to open identity on every synchronous task."""
    try:
        _require_fields(message, _TASK_FIELDS)
        if message["type"] != "task" or not _matches(message["protocol_version"], PROTOCOL_VERSION):
            raise ValueError("invalid Map task version")
        sequence = decimal_uint64(message["sequence"])
        if sequence != expected_sequence:
            raise ValueError("invalid Map task sequence")
        for key in _IDENTITY_FIELDS:
            if not _matches(message[key], getattr(open_context, key)):
                raise ValueError("invalid Map task identity")
        messages = message["canonical_messages"]
        if type(messages) is not list or len(messages) != 2:
            raise ValueError("invalid Map messages")
        for item, role in zip(messages, ("system", "user")):
            _require_fields(item, {"role", "content"})
            if not _matches(item["role"], role):
                raise ValueError("invalid Map message role")
        instruction, input_value = messages[0]["content"], messages[1]["content"]
        plan = SemanticMapPlan(instruction, open_context.model_id, open_context.max_tokens)
        if plan.digest != open_context.semantic_spec_digest:
            raise ValueError("invalid Map task semantics")
        message_bytes = canonical_messages(instruction, input_value)
        payload = message["semantic_payload_digest"]
        _digest_bytes(payload)
        expected = semantic_payload_digest(semantic_spec_sha256=plan.digest, input_value=input_value,
                                           canonical_messages_utf8=message_bytes)
        if payload != expected:
            raise ValueError("invalid Map payload identity")
        return sequence, payload
    except (KeyError, TypeError, ValueError):
        raise ProtocolError("INVALID_TASK") from None


def build_error_message(code: str, *, sequence: int | None) -> dict:
    if type(code) is not str or code not in ERROR_CODES:
        raise ValueError("invalid Map error code")
    if sequence is not None:
        _uint64(sequence)
    return {"type": "error", "protocol_version": PROTOCOL_VERSION,
            "sequence": None if sequence is None else str(sequence), "code": code}


def validate_error(message: dict, *, expected_sequence: int | None) -> str:
    try:
        _require_fields(message, {"type", "protocol_version", "sequence", "code"})
        if message["type"] != "error" or not _matches(message["protocol_version"], PROTOCOL_VERSION):
            raise ValueError("invalid Map error version")
        code = message["code"]
        if type(code) is not str or code not in ERROR_CODES:
            raise ValueError("invalid Map error code")
        if expected_sequence is None:
            if message["sequence"] is not None:
                raise ValueError("invalid Map open error sequence")
        elif decimal_uint64(message["sequence"]) != expected_sequence:
            raise ValueError("invalid Map task error sequence")
        return code
    except (KeyError, TypeError, ValueError, ProtocolError):
        raise ProtocolError("INVALID_ERROR") from None


def _completion_evidence(context: OpenContext, payload_digest: str, sequence: int, completion: Completion) -> str:
    return completion_evidence_digest(semantic_spec_sha256=context.semantic_spec_digest,
        physical_algorithm_sha256=context.physical_algorithm_digest,
        provider_execution_sha256=context.provider_execution_digest, semantic_payload_sha256=payload_digest,
        sequence=sequence, raw_output=completion.raw_output, finish_reason=completion.finish_reason,
        response_model_id=completion.response_model_id, prompt_tokens=completion.prompt_tokens,
        output_tokens=completion.output_tokens)


def _check_model_usage(context: OpenContext, completion: Completion) -> None:
    if completion.response_model_id != context.model_id or completion.output_tokens > context.max_tokens:
        raise ValueError("invalid Map completion metadata")


def build_completion_message(context: OpenContext, *, sequence: int, payload_digest: str, completion: Completion) -> dict:
    """Preserve valid non-stop states for PG policy; do not send oversized text."""
    try:
        length = validate_completion_values(completion)
        _check_model_usage(context, completion)
    except (TypeError, ValueError):
        raise ProtocolError("MODEL_RESPONSE_INVALID") from None
    if length > MAX_OUTPUT_BYTES:
        raise ProtocolError("OUTPUT_TOO_LARGE")
    evidence = _completion_evidence(context, payload_digest, sequence, completion)
    return {"type": "completion", "protocol_version": PROTOCOL_VERSION, "sequence": str(sequence),
        "semantic_spec_digest": context.semantic_spec_digest, "physical_algorithm_digest": context.physical_algorithm_digest,
        "provider_execution_digest": context.provider_execution_digest, "semantic_payload_digest": payload_digest,
        "raw_output": completion.raw_output, "response_model_id": completion.response_model_id,
        "prompt_tokens": str(completion.prompt_tokens), "output_tokens": str(completion.output_tokens),
        "finish_reason": completion.finish_reason, "completion_evidence_digest": evidence}


def validate_completion(message: dict, *, expected_sequence: int, payload_digest: str, open_context: OpenContext) -> Completion:
    """Decode evidence-bound values; only the operator applies stop-only semantics."""
    try:
        _require_fields(message, _COMPLETION_FIELDS)
        if message["type"] != "completion" or not _matches(message["protocol_version"], PROTOCOL_VERSION):
            raise ValueError("invalid Map completion version")
        sequence = decimal_uint64(message["sequence"])
        if sequence != expected_sequence or not _matches(message["semantic_payload_digest"], payload_digest):
            raise ValueError("invalid Map completion task")
        for key in _IDENTITY_FIELDS:
            if not _matches(message[key], getattr(open_context, key)):
                raise ValueError("invalid Map completion identity")
        completion = Completion(message["raw_output"], message["response_model_id"],
            decimal_uint64(message["prompt_tokens"]), decimal_uint64(message["output_tokens"]), message["finish_reason"])
        length = validate_completion_values(completion)
        if not _matches(message["completion_evidence_digest"], _completion_evidence(open_context, payload_digest, sequence, completion)):
            raise ValueError("invalid Map completion evidence")
        _check_model_usage(open_context, completion)
    except (KeyError, TypeError, ValueError, ProtocolError):
        raise ProtocolError("INVALID_COMPLETION") from None
    if length > MAX_OUTPUT_BYTES:
        raise ProtocolError("OUTPUT_TOO_LARGE")
    return completion
