"""Validated fixed model configuration shared by synchronous and asynchronous HTTP."""

from __future__ import annotations
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping
from urllib import parse

VLLM_CHOICE_FORMAT = "vllm_structured_outputs"
MAX_MODEL_RESPONSE_BYTES = 1_048_576
_CONFIG_FIELDS = {"endpoint_url", "model_id", "timeout_ms", "bearer_token_env", "choice_format"}
_REQUIRED_CONFIG_FIELDS = {"endpoint_url", "model_id", "timeout_ms"}
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True)
class FixedModelConfig:
    """Process-owned endpoint identity and bounded request configuration."""

    endpoint_url: str
    model_id: str
    timeout_ms: int
    bearer_token: str | None = field(default=None, repr=False)
    choice_format: str | None = None

    def __post_init__(self) -> None:
        if self.choice_format is not None and self.choice_format != VLLM_CHOICE_FORMAT:
            raise ValueError("invalid fixed model choice format")
        if not isinstance(self.endpoint_url, str):
            raise ValueError("endpoint_url must be an absolute HTTP(S) URL")
        parsed = parse.urlsplit(self.endpoint_url)
        try:
            endpoint_port = parsed.port
        except ValueError:
            raise ValueError("endpoint_url must be an absolute HTTP(S) URL") from None
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or endpoint_port == 0
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
        choice_format=value.get("choice_format"),
    )
