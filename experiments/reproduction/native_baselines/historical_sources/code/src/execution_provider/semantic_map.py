"""Fixed generative Map plan values, messages and completion policy; no transport."""

from dataclasses import dataclass
from enum import IntEnum
import hashlib
import struct

from .message_encoding import encode_messages
from .completion import Completion

MAX_INSTRUCTION_BYTES = 4096
MAX_INPUT_BYTES = 163_840
MAX_OUTPUT_BYTES = 65_536
MAX_MODEL_BYTES = 128
MAX_GENERATION_TOKENS = 4096
MAX_FINISH_REASON_BYTES = 32
UINT64_MAX = 2**64 - 1
SEMANTIC_SPEC_ID = "semloom.semantic.sem_map.generate.v1"
PROMPT_PROGRAM_ID = "semloom.sem_map.chat.v1"
RESULT_PARSER_ID = "semloom.sem_map.utf8_text.v1"
PHYSICAL_ALGORITHM = "MODEL_REFERENCE_SYNC_V1"
PHYSICAL_ROLE = "reference"


def _text(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("!I", len(encoded)) + encoded


PROMPT_PROGRAM_DIGEST = hashlib.sha256(
    b"semloom-prompt-program-v1\0" + _text(PROMPT_PROGRAM_ID) + struct.pack("!I", 1)
    + _text("system") + _text("content") + _text("instruction-verbatim")
    + _text("user") + _text("content") + _text("input-verbatim")
).hexdigest()
RESULT_PARSER_DIGEST = hashlib.sha256(
    b"semloom-result-parser-v1\0" + _text(RESULT_PARSER_ID) + struct.pack("!I", 1)
    + _text("utf8-no-nul-no-trim") + _text("stop-only") + struct.pack("!I", MAX_OUTPUT_BYTES)
).hexdigest()


@dataclass(frozen=True)
class SemanticMapPlan:
    """The three variable values consumed by the fixed Map generation contract."""

    instruction: str
    model_id: str
    max_tokens: int

    def __post_init__(self) -> None:
        _validate_text(self.instruction, MAX_INSTRUCTION_BYTES, allow_empty=False)
        _validate_text(self.model_id, MAX_MODEL_BYTES, allow_empty=False)
        if type(self.max_tokens) is not int or not 1 <= self.max_tokens <= MAX_GENERATION_TOKENS:
            raise ValueError("invalid Map max_tokens")

    def generation_constraints(self) -> dict[str, object]:
        """Return fresh effective values, with absent stop represented by None."""
        return {"temperature": 0, "top_p": 1, "max_tokens": self.max_tokens,
                "n": 1, "stream": False, "stop": None}

    def canonical_bytes(self) -> bytes:
        """Encode effective semantics, including program/parser and text limits."""
        return (
            b"semloom-semantic-spec-v4\0" + struct.pack("!I", 4)
            + _text(SEMANTIC_SPEC_ID) + struct.pack("!I", 1)
            + _text("SEM_MAP") + _text("text") + _text("text") + _text(self.instruction)
            + _text(PROMPT_PROGRAM_ID) + struct.pack("!I", 1) + _text(PROMPT_PROGRAM_DIGEST)
            + _text(RESULT_PARSER_ID) + struct.pack("!I", 1) + _text(RESULT_PARSER_DIGEST)
            + _text("PROPAGATE_NULL") + _text("FAIL_QUERY") + _text("INPUT_ORDER")
            + _text(self.model_id) + struct.pack("!IIII", 0, 1, self.max_tokens, 1)
            + b"\0\0" + struct.pack("!II", MAX_INPUT_BYTES, MAX_OUTPUT_BYTES)
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


def _validate_text(value: str, limit: int | None, *, allow_empty: bool) -> int:
    if not isinstance(value, str):
        raise TypeError("Map message contents must be text")
    if (not allow_empty and not value) or (limit is not None and len(value) > limit) or "\0" in value:
        raise ValueError("invalid Map message text")
    try:
        length = len(value.encode("utf-8"))
    except UnicodeEncodeError:
        raise ValueError("invalid Map message text") from None
    if limit is not None and length > limit:
        raise ValueError("invalid Map message text")
    return length


def canonical_messages(instruction: str, input_value: str) -> bytes:
    """Build verbatim system/user messages without a Filter directive or parser."""
    _validate_text(instruction, MAX_INSTRUCTION_BYTES, allow_empty=False)
    _validate_text(input_value, MAX_INPUT_BYTES, allow_empty=True)
    return encode_messages(instruction, input_value)


def input_utf8(input_value: str) -> bytes:
    """Validate a Map input for identity encoding without building any messages."""
    _validate_text(input_value, MAX_INPUT_BYTES, allow_empty=True)
    return input_value.encode("utf-8")


class MapCompletionStatus(IntEnum):
    VALID = 0
    INVALID = 1
    TOO_LARGE = 2
    INCOMPLETE = 3


def validate_completion_values(completion: Completion) -> int:
    """Validate representation and return byte length, without applying plan policy."""
    if type(completion) is not Completion:
        raise ValueError("invalid Map completion")
    length = _validate_text(completion.raw_output, None, allow_empty=True)
    _validate_text(completion.response_model_id, MAX_MODEL_BYTES, allow_empty=False)
    _validate_text(completion.finish_reason, MAX_FINISH_REASON_BYTES, allow_empty=False)
    for count in (completion.prompt_tokens, completion.output_tokens):
        if type(count) is not int or not 0 <= count <= UINT64_MAX:
            raise ValueError("invalid Map completion")
    return length


def completion_status(plan: SemanticMapPlan, completion: Completion) -> MapCompletionStatus:
    """Classify raw values in representation/model/usage, length, finish order."""
    try:
        length = validate_completion_values(completion)
    except (TypeError, ValueError):
        return MapCompletionStatus.INVALID
    if completion.response_model_id != plan.model_id or completion.output_tokens > plan.max_tokens:
        return MapCompletionStatus.INVALID
    if length > MAX_OUTPUT_BYTES:
        return MapCompletionStatus.TOO_LARGE
    if completion.finish_reason != "stop":
        return MapCompletionStatus.INCOMPLETE
    return MapCompletionStatus.VALID
