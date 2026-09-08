"""Shared strict model response decoding for synchronous and asynchronous transports."""

from ..completion import Completion


def parse_completion(value: object) -> Completion:
    if not isinstance(value, dict):
        raise ValueError("response must be an object")
    model_id = value.get("model")
    choices = value.get("choices")
    usage = value.get("usage")
    if (
        not isinstance(model_id, str)
        or not isinstance(choices, list)
        or len(choices) != 1
        or not isinstance(choices[0], dict)
        or not isinstance(usage, dict)
    ):
        raise ValueError("response is missing completion fields")
    choice = choices[0]
    message = choice.get("message")
    finish_reason = choice.get("finish_reason")
    prompt_tokens = usage.get("prompt_tokens")
    output_tokens = usage.get("completion_tokens")
    if (
        not isinstance(message, dict)
        or not isinstance(message.get("content"), str)
        or not isinstance(finish_reason, str)
        or type(prompt_tokens) is not int
        or type(output_tokens) is not int
        or prompt_tokens < 0
        or output_tokens < 0
        or prompt_tokens >= 2**64
        or output_tokens >= 2**64
    ):
        raise ValueError("response completion fields have invalid types")
    return Completion(
        raw_output=message["content"],
        response_model_id=model_id,
        prompt_tokens=prompt_tokens,
        output_tokens=output_tokens,
        finish_reason=finish_reason,
    )
