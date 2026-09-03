"""Canonical JSON encoding for caller-validated two-message text tasks."""

import json


def encode_messages(system: str, user: str) -> bytes:
    """Encode verbatim role contents; each operator owns text validation and policy."""
    return json.dumps(
        [{"role": "system", "content": system}, {"role": "user", "content": user}],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
