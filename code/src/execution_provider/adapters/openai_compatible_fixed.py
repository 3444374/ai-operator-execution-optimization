"""Fixed OpenAI-compatible Chat Completions adapter for exact SemFilter."""

from __future__ import annotations

import json
import os
import re
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping
from urllib import error, parse, request

from .v3_session import CompletionAdapterError, V3Completion, V3CompletionRequest


FIXED_EXECUTION_ID = "semloom.provider.openai-compatible-fixed.uds.v3"
MAX_MODEL_RESPONSE_BYTES = 1_048_576
_CONFIG_FIELDS = {"endpoint_url", "model_id", "timeout_ms", "bearer_token_env"}
_REQUIRED_CONFIG_FIELDS = {"endpoint_url", "model_id", "timeout_ms"}
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True)
class FixedModelConfig:
    """Process-owned endpoint identity and bounded request configuration."""

    endpoint_url: str
    model_id: str
    timeout_ms: int
    bearer_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.endpoint_url, str):
            raise ValueError("endpoint_url must be an absolute HTTP(S) URL")
        parsed = parse.urlsplit(self.endpoint_url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError("endpoint_url must be an absolute HTTP(S) URL")
        if not isinstance(self.model_id, str) or not (
            1 <= len(self.model_id.encode("utf-8")) <= 128
        ):
            raise ValueError("model_id length is outside the plan contract")
        if type(self.timeout_ms) is not int or not (1 <= self.timeout_ms <= 300_000):
            raise ValueError("timeout_ms must be an integer from 1 to 300000")
        if self.bearer_token is not None and (
            not isinstance(self.bearer_token, str) or not self.bearer_token
        ):
            raise ValueError("bearer_token must be non-empty when configured")


def load_fixed_model_config(
    path: Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> FixedModelConfig:
    """Load one strict repository-external fixed endpoint configuration."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("invalid fixed model configuration") from None
    if (
        not isinstance(value, dict)
        or set(value) - _CONFIG_FIELDS
        or not _REQUIRED_CONFIG_FIELDS.issubset(value)
    ):
        raise ValueError("invalid fixed model configuration fields")
    token_environment = value.get("bearer_token_env")
    bearer_token = None
    if token_environment is not None:
        if not isinstance(token_environment, str) or not _ENVIRONMENT_NAME.fullmatch(
            token_environment
        ):
            raise ValueError("invalid bearer token environment name")
        environment = os.environ if environ is None else environ
        bearer_token = environment.get(token_environment)
        if not bearer_token:
            raise ValueError("configured bearer token environment variable is unset")
    return FixedModelConfig(
        endpoint_url=value["endpoint_url"],
        model_id=value["model_id"],
        timeout_ms=value["timeout_ms"],
        bearer_token=bearer_token,
    )


class OpenAICompatibleFixedAdapter:
    """Send each validated task to one fixed non-streaming model endpoint."""

    execution_id = FIXED_EXECUTION_ID

    def __init__(self, config: FixedModelConfig) -> None:
        self._config = config
        self.model_id = config.model_id

    def complete(self, completion_request: V3CompletionRequest) -> V3Completion:
        if completion_request.model_id != self._config.model_id:
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        payload = json.dumps(
            {
                "model": completion_request.model_id,
                "messages": list(completion_request.canonical_messages),
                **completion_request.generation_constraints,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self._config.bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._config.bearer_token}"
        http_request = request.Request(
            self._config.endpoint_url,
            data=payload,
            headers=headers,
            method="POST",
        )
        try:
            with request.urlopen(  # noqa: S310 - URL is validated process configuration
                http_request,
                timeout=self._config.timeout_ms / 1000,
            ) as response:
                response_bytes = response.read(MAX_MODEL_RESPONSE_BYTES + 1)
        except error.HTTPError as failure:
            code = (
                "MODEL_REQUEST_REJECTED"
                if 400 <= failure.code < 500
                else "MODEL_UNAVAILABLE"
            )
            raise CompletionAdapterError(code) from None
        except (TimeoutError, socket.timeout):
            raise CompletionAdapterError("MODEL_TIMEOUT") from None
        except (error.URLError, OSError):
            raise CompletionAdapterError("MODEL_UNAVAILABLE") from None
        if len(response_bytes) > MAX_MODEL_RESPONSE_BYTES:
            raise CompletionAdapterError("MODEL_RESPONSE_INVALID")
        try:
            response_value = json.loads(response_bytes.decode("utf-8"))
            return _parse_completion(response_value)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            raise CompletionAdapterError("MODEL_RESPONSE_INVALID") from None


def _parse_completion(value: object) -> V3Completion:
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
    return V3Completion(
        raw_output=message["content"],
        response_model_id=model_id,
        prompt_tokens=prompt_tokens,
        output_tokens=output_tokens,
        finish_reason=finish_reason,
    )


__all__ = [
    "FIXED_EXECUTION_ID",
    "FixedModelConfig",
    "OpenAICompatibleFixedAdapter",
    "load_fixed_model_config",
]
