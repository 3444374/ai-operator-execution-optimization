"""Pure message compilation for generative SemMap; not a wire execution entry."""

from .message_encoding import encode_messages

MAX_INSTRUCTION_BYTES = 4096
MAX_INPUT_BYTES = 163_840


def _validate_text(value: str, limit: int, *, allow_empty: bool) -> None:
    if not isinstance(value, str):
        raise TypeError("Map message contents must be text")
    if (not allow_empty and not value) or len(value) > limit or "\0" in value:
        raise ValueError("invalid Map message text")
    try:
        length = len(value.encode("utf-8"))
    except UnicodeEncodeError:
        raise ValueError("invalid Map message text") from None
    if length > limit:
        raise ValueError("invalid Map message text")


def canonical_messages(instruction: str, input_value: str) -> bytes:
    """Build verbatim system/user messages without a Filter directive or parser."""
    _validate_text(instruction, MAX_INSTRUCTION_BYTES, allow_empty=False)
    _validate_text(input_value, MAX_INPUT_BYTES, allow_empty=True)
    return encode_messages(instruction, input_value)
