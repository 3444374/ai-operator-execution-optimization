"""Write new private data verbatim; keep public summaries a separate operation."""

from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import uuid


def content_digest(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def file_identity(path):
    """Describe file bytes without loading the complete file into memory."""
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1048576),b''):
            digest.update(block)
    return dict(sha256=digest.hexdigest(),bytes=Path(path).stat().st_size)


def json_metric(value):
    """Keep upstream infinite-error meaning while writing strict JSON."""
    if isinstance(value,float) and not math.isfinite(value):
        return 'Infinity' if value>0 else '-Infinity' if value<0 else 'NaN'
    if isinstance(value,dict):
        return {k:json_metric(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):
        return [json_metric(v) for v in value]
    return value


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


def publish_private_json(path: Path, value: object) -> None:
    """Expose a complete fresh record atomically to a concurrent observer."""
    path = Path(path)
    pending = path.with_name('.pending-' + uuid.uuid4().hex)
    write_private_json(pending, value)
    os.link(pending, path)  # Exclusive publication: never replace old evidence.
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
        pending.unlink()
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
