"""Evidence sealing helpers; no experiment dispatch lives here."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from src.infrastructure.redact import redact_argument_list, redact_text


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


def sha256_file(path: Path) -> str:
    """Return the immutable SHA-256 identity of one artifact."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_payload(payload: object) -> str:
    """Return the canonical JSON SHA-256 identity of a resolved contract."""

    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
