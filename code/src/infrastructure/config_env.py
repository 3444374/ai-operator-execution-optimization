"""Strict environment-variable expansion for portable experiment configs."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping


ENV_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_text(
    value: str,
    label: str,
    *,
    environment: Mapping[str, str] | None = None,
) -> str:
    """Expand ``${NAME}`` references and reject every unset variable."""

    resolved = os.environ if environment is None else environment
    missing = sorted(
        name for name in set(ENV_REFERENCE.findall(value)) if name not in resolved
    )
    if missing:
        raise ValueError(
            f"{label} references unset environment variable(s): "
            + ", ".join(missing)
        )
    return ENV_REFERENCE.sub(lambda match: resolved[match.group(1)], value)


def expand_scalar(
    value: object,
    label: str,
    *,
    environment: Mapping[str, str] | None = None,
) -> object:
    """Expand a JSON scalar, preserving a full-reference numeric/bool type."""

    if not isinstance(value, str):
        return value
    expanded = expand_text(value, label, environment=environment)
    if ENV_REFERENCE.fullmatch(value):
        try:
            return json.loads(expanded)
        except json.JSONDecodeError:
            return expanded
    return expanded


def expand_structure(
    value: object,
    label: str,
    *,
    environment: Mapping[str, str] | None = None,
) -> object:
    """Recursively expand every string in a JSON-compatible structure."""

    if isinstance(value, dict):
        return {
            key: expand_structure(
                item,
                f"{label}.{key}",
                environment=environment,
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            expand_structure(
                item,
                f"{label}[{index}]",
                environment=environment,
            )
            for index, item in enumerate(value)
        ]
    return expand_scalar(value, label, environment=environment)
