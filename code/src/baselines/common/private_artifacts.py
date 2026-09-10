"""Write new private data verbatim; keep public summaries a separate operation."""

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path


def content_digest(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def require_outside_git(path: Path) -> None:
    resolved = path.resolve()
    if any((parent / ".git").exists() for parent in (resolved, *resolved.parents)):
        raise ValueError("private artifacts must be outside a Git checkout")


def new_private_directory(path: Path) -> None:
    require_outside_git(path)
    path.mkdir(parents=True, mode=0o700, exist_ok=False)


@contextmanager
def open_private_text(path: Path, *, newline=None):
    """Refuse overwrite/symlinks and flush private bytes without redaction."""
    require_outside_git(path)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline=newline) as stream:
        try:
            yield stream
        finally:
            stream.flush()
            os.fsync(stream.fileno())


def write_private_json(path: Path, value: object) -> None:
    payload = json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    with open_private_text(path) as stream:
        stream.write(payload)


def workload_summary(manifest: dict) -> dict:
    """Export only counts and hashes, never arbitrary workload fields or IDs."""
    if manifest.get("schema") == "squad_v11_pg_map_v1":
        counts = {key: len(manifest["splits"][key]) for key in ("tuning", "evaluation")}
    elif manifest.get("schema") == "semloom.sharegpt.first_human.v1":
        counts = {"selected": len(manifest["rows"])}
    else:
        raise ValueError("unsupported private workload schema")
    payload = {key: value for key, value in manifest.items() if key != "sha256"}
    if manifest.get("sha256") != content_digest(payload):
        raise ValueError("workload identity mismatch")
    return {
        "schema": "semloom.workload.public_summary.v1",
        "private_manifest_sha256": content_digest(payload),
        "row_counts": counts,
    }
