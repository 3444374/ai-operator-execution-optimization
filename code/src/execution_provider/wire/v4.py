"""Strict version-4 exact SemFilter wire contract."""
from .semantic import (
    ExactFilterWire,
    ERROR_CODES,
    GENERATION_CONSTRAINTS,
    MAX_FRAME_BYTES,
    MAX_INFLIGHT_TASKS,
    MAX_INPUT_BYTES,
    PHYSICAL_ALGORITHM,
    PHYSICAL_ROLE,
    PROMPT_PROGRAM_DIGEST,
    RESULT_PARSER_DIGEST,
    SEMANTIC_SPEC_ID,
    SEMANTIC_SPEC_VERSION,
    PROMPT_PROGRAM_ID,
    PROMPT_PROGRAM_VERSION,
    RESULT_PARSER_ID,
    RESULT_PARSER_VERSION,
    SYSTEM_DIRECTIVE,
    INSTRUCTION_SEPARATOR,
    SemanticFilterPlan,
    OpenContext,
    canonical_messages,
    decimal_uint64,
    physical_algorithm_digest,
)

PROTOCOL_VERSION = 4
PLAN_SCHEMA_VERSION = 3
GOLDEN_EXECUTION_ID = "semloom.provider.golden.uds.v4"
_CODEC = ExactFilterWire(PROTOCOL_VERSION)

semantic_spec_digest = _CODEC.semantic_spec_digest
provider_execution_digest = _CODEC.provider_execution_digest
semantic_payload_digest = _CODEC.semantic_payload_digest
completion_evidence_digest = _CODEC.completion_evidence_digest
build_open_message = _CODEC.build_open_message
build_task_message = _CODEC.build_task_message
build_error_message = _CODEC.build_error_message
validate_open = _CODEC.validate_open
validate_task = _CODEC.validate_task

__all__ = [
    "ERROR_CODES",
    "GENERATION_CONSTRAINTS",
    "MAX_FRAME_BYTES",
    "MAX_INFLIGHT_TASKS",
    "MAX_INPUT_BYTES",
    "PHYSICAL_ALGORITHM",
    "PHYSICAL_ROLE",
    "PROMPT_PROGRAM_DIGEST",
    "RESULT_PARSER_DIGEST",
    "SEMANTIC_SPEC_ID",
    "SEMANTIC_SPEC_VERSION",
    "PROMPT_PROGRAM_ID",
    "PROMPT_PROGRAM_VERSION",
    "RESULT_PARSER_ID",
    "RESULT_PARSER_VERSION",
    "SYSTEM_DIRECTIVE",
    "INSTRUCTION_SEPARATOR",
    "SemanticFilterPlan",
    "OpenContext",
    "canonical_messages",
    "decimal_uint64",
    "physical_algorithm_digest",
    "semantic_spec_digest",
    "provider_execution_digest",
    "semantic_payload_digest",
    "completion_evidence_digest",
    "build_open_message",
    "build_task_message",
    "build_error_message",
    "validate_open",
    "validate_task",
    "PROTOCOL_VERSION",
    "PLAN_SCHEMA_VERSION",
    "GOLDEN_EXECUTION_ID",
]
