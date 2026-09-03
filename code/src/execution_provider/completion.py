"""Raw immutable completion values; each semantic operator owns validation policy."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Completion:
    """Unmodified model text and metadata, without transport or parser behavior."""

    raw_output: str
    response_model_id: str
    prompt_tokens: int
    output_tokens: int
    finish_reason: str
