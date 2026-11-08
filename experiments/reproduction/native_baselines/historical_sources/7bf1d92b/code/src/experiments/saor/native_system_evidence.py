"""Evidence sealing helpers; no experiment dispatch lives here."""

from __future__ import annotations

import json
from pathlib import Path

from src.baselines.common.redact import redact_argument_list, redact_text


def atomic_json(path: Path, payload: object) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def persisted_failure(error: BaseException) -> str:
    """Return a secret-safe exception string for durable evidence."""

    return redact_text(f"{type(error).__name__}: {error}")


def persisted_command(arguments: list[object]) -> list[str]:
    """Return a secret-safe command while retaining reproducible structure."""

    return redact_argument_list([str(item) for item in arguments])
