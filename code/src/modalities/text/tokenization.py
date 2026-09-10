"""Count one complete chat template, rejecting ambiguous token container shapes."""

from collections.abc import Mapping


def chat_token_count(tokenizer, messages) -> int:
    tokens = tokenizer.apply_chat_template(
        messages, tokenize=True, add_generation_prompt=True, return_dict=False,
    )
    if isinstance(tokens, Mapping):
        tokens = tokens.get("input_ids")
    if not isinstance(tokens, (list, tuple)) or not tokens:
        raise ValueError("tokenizer must return a nonempty token ID sequence")
    if any(type(token) is not int or token < 0 for token in tokens):
        raise ValueError("tokenizer token IDs must be a flat sequence of nonnegative integers")
    return len(tokens)
