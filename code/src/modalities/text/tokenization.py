"""Count one complete chat template, rejecting ambiguous token container shapes."""

from collections.abc import Mapping


def chat_token_count(tokenizer, messages, *, max_tokens=None) -> int:
    if max_tokens is not None and (type(max_tokens) is not int or max_tokens < 1):
        raise ValueError("token count limit must be a positive integer")
    # The extra token detects overflow; this never changes the outgoing messages.
    options = {} if max_tokens is None else dict(truncation=True, max_length=max_tokens + 1)
    tokens = tokenizer.apply_chat_template(
        messages, tokenize=True, add_generation_prompt=True, return_dict=False,
        **options,
    )
    if isinstance(tokens, Mapping):
        tokens = tokens.get("input_ids")
    if not isinstance(tokens, (list, tuple)) or not tokens:
        raise ValueError("tokenizer must return a nonempty token ID sequence")
    if any(type(token) is not int or token < 0 for token in tokens):
        raise ValueError("tokenizer token IDs must be a flat sequence of nonnegative integers")
    return len(tokens)
