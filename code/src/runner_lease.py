"""Exclusive ownership for one scenario runner per output directory."""

from __future__ import annotations

import errno
import json
import os
import socket
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


LEASE_NAME = ".runner-lease.json"
_REQUIRED_FIELDS = {
    "hostname",
    "pid",
    "process_start_id",
    "owner_token",
    "config_fingerprint",
    "repository_commit",
    "started_epoch_s",
}


@dataclass(frozen=True)
class RunnerOwner:
    hostname: str
    pid: int
    process_start_id: str
    owner_token: str

    def __post_init__(self) -> None:
        if not self.hostname or self.pid <= 0 or not self.owner_token:
            raise ValueError("runner owner fields must be non-empty and valid")


class RunnerLease:
    def __init__(
        self,
        path: Path,
        owner: RunnerOwner,
        recovered_owner: dict[str, object] | None = None,
    ) -> None:
        self.path = path
        self.owner = owner
        self.recovered_owner = recovered_owner
        self._released = False

    def __enter__(self) -> RunnerLease:
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()

    def release(self) -> None:
        if self._released:
            return
        if not self.path.exists():
            self._released = True
            return
        current = _read_lease(self.path)
        if current["owner_token"] != self.owner.owner_token:
            raise RuntimeError("runner lease ownership changed before release")
        self.path.unlink()
        self._released = True


def acquire_runner_lease(
    output_dir: Path,
    *,
    config_fingerprint: str,
    repository_commit: str,
    recover_stale: bool = False,
    owner: RunnerOwner | None = None,
    process_alive: Callable[[int], bool] | None = None,
) -> RunnerLease:
    if not config_fingerprint or not repository_commit:
        raise ValueError(
            "config_fingerprint and repository_commit must be non-empty"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    lease_path = output_dir / LEASE_NAME
    resolved_owner = owner or _current_owner()
    resolved_process_alive = process_alive or is_process_alive
    record = _lease_record(
        resolved_owner,
        config_fingerprint=config_fingerprint,
        repository_commit=repository_commit,
    )
    try:
        _write_exclusive(lease_path, record)
        return RunnerLease(lease_path, resolved_owner)
    except FileExistsError:
        existing = _read_lease(lease_path)

    if existing["config_fingerprint"] != config_fingerprint:
        raise RuntimeError(
            "runner lease config fingerprint does not match this experiment"
        )
    if _lease_is_active(existing, resolved_owner, resolved_process_alive):
        raise RuntimeError(
            "output directory already has an active runner "
            f"on {existing['hostname']} with pid {existing['pid']}"
        )
    if not recover_stale:
        raise RuntimeError(
            "output directory has a stale runner lease; "
            "inspect it and use explicit stale recovery"
        )

    lease_path.unlink()
    try:
        _write_exclusive(lease_path, record)
    except FileExistsError as exc:
        raise RuntimeError(
            "another runner acquired the output directory during stale recovery"
        ) from exc
    return RunnerLease(
        lease_path,
        resolved_owner,
        recovered_owner=existing,
    )


def is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.EPERM:
            return True
        return False
    return True


def _current_owner() -> RunnerOwner:
    pid = os.getpid()
    return RunnerOwner(
        hostname=socket.gethostname(),
        pid=pid,
        process_start_id=_process_start_id(pid),
        owner_token=uuid.uuid4().hex,
    )


def _process_start_id(pid: int) -> str:
    stat_path = Path("/proc") / str(pid) / "stat"
    try:
        raw = stat_path.read_text(encoding="utf-8")
    except OSError:
        return ""
    closing_parenthesis = raw.rfind(")")
    if closing_parenthesis < 0:
        return ""
    fields_from_state = raw[closing_parenthesis + 1 :].split()
    return fields_from_state[19] if len(fields_from_state) > 19 else ""


def _lease_record(
    owner: RunnerOwner,
    *,
    config_fingerprint: str,
    repository_commit: str,
) -> dict[str, object]:
    return {
        "hostname": owner.hostname,
        "pid": owner.pid,
        "process_start_id": owner.process_start_id,
        "owner_token": owner.owner_token,
        "config_fingerprint": config_fingerprint,
        "repository_commit": repository_commit,
        "started_epoch_s": time.time(),
    }


def _write_exclusive(path: Path, record: dict[str, object]) -> None:
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        0o600,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(record, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise


def _read_lease(path: Path) -> dict[str, object]:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"runner lease is unreadable: {path}") from exc
    if not isinstance(decoded, dict) or not _REQUIRED_FIELDS.issubset(decoded):
        raise RuntimeError(f"runner lease has an invalid schema: {path}")
    if (
        not isinstance(decoded["hostname"], str)
        or not decoded["hostname"]
        or isinstance(decoded["pid"], bool)
        or not isinstance(decoded["pid"], int)
        or decoded["pid"] <= 0
        or not isinstance(decoded["owner_token"], str)
        or not decoded["owner_token"]
        or not isinstance(decoded["config_fingerprint"], str)
        or not decoded["config_fingerprint"]
    ):
        raise RuntimeError(f"runner lease has invalid owner fields: {path}")
    return decoded


def _lease_is_active(
    existing: dict[str, object],
    current: RunnerOwner,
    process_alive: Callable[[int], bool],
) -> bool:
    if existing["hostname"] != current.hostname:
        return True
    pid = int(existing["pid"])
    if not process_alive(pid):
        return False
    recorded_start = str(existing.get("process_start_id", ""))
    observed_start = _process_start_id(pid)
    return not (
        recorded_start
        and observed_start
        and recorded_start != observed_start
    )
