"""Linux /proc collection, validation, and FD classification.

Only reads and classifies: no thresholds and no experiment policy live
here. Every failure surfaces as SnapshotStatus/errors or None fields —
never as zeros or empty sets that could fake a green verdict.
"""
from __future__ import annotations

import os
import re
import time
from pathlib import Path
from typing import Mapping

from .model import (
    FdIdentity,
    FdSnapshotAttempt,
    UnixSocketInfo,
    FdKind,
    PgFileClassificationContext,
    ProcessSnapshot,
    SampleTick,
    SnapshotStatus,
)

_SOCKET_INODE_PATTERN = re.compile(r"^(?:socket|pipe):\[(\d+)\]$")
_FILE_INODE_PATTERN = re.compile(r"^(\d+)$")
_PG_TEMP_DIR = "pgsql_tmp"
_CONSISTENT_RETRIES = 3


def _read_link(pid: int, fd: int) -> str | None:
    try:
        return os.readlink(f"/proc/{pid}/fd/{fd}")
    except OSError:
        return None


def _list_fds(pid: int) -> list[int] | None:
    """Return sorted fd numbers, or None when the directory is unreadable."""
    try:
        entries = os.listdir(f"/proc/{pid}/fd")
    except OSError:
        return None
    numbers = []
    for entry in entries:
        try:
            numbers.append(int(entry))
        except ValueError:
            continue
    return sorted(numbers)


def _parse_inode(target: str) -> int | None:
    match = _SOCKET_INODE_PATTERN.match(target)
    if match:
        return int(match.group(1))
    match = _FILE_INODE_PATTERN.match(target)
    if match:
        return int(match.group(1))
    return None


def unix_socket_table() -> dict[int, UnixSocketInfo] | None:
    """Read AF_UNIX identities. None means unreadable; {} is a valid empty table.

    Malformed rows raise ValueError so parse and read failures stay distinct.
    """
    try:
        text = Path("/proc/net/unix").read_text(encoding="ascii")
    except OSError:
        return None
    lines = text.splitlines()
    if not lines or not lines[0].startswith("Num"):
        raise ValueError("unix_table_header")
    table = {}
    for line in lines[1:]:
        parts = line.split(maxsplit=7)
        if len(parts) < 7:
            raise ValueError("unix_table_row")
        inode = int(parts[6])
        table[inode] = UnixSocketInfo(
            parts[7] if len(parts) == 8 else None,
            int(parts[3], 16), int(parts[5], 16), int(parts[4], 16))
    return table


def process_start_time_ticks(pid: int) -> int | None:
    """Read the scheduler start time used to detect pid reuse."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="ascii", errors="replace")
    except OSError:
        return None
    tail = text[text.rfind(")") + 1:].split()
    try:
        return int(tail[19])
    except (IndexError, ValueError):
        return None


def _filenode_of(basename: str) -> int | None:
    match = re.fullmatch(r"(\d+)(?:_(?:fsm|vm|init))?(?:\.\d+)?", basename)
    return int(match.group(1)) if match else None


def classify_target(
    target: str,
    unix_paths_by_inode: dict[int, str | None],
    provider_socket_path: str | None,
    pg_context: PgFileClassificationContext | None = None,
) -> tuple[FdKind, int | None, str | None]:
    """Return ``(kind, inode, unix_path)`` for one descriptor target."""
    inode = _parse_inode(target)
    if target.startswith("socket:[") and inode is not None:
        info = unix_paths_by_inode.get(inode, "")
        path = info.path if isinstance(info, UnixSocketInfo) else info
        if path is None:
            return FdKind.UNBOUND_UNIX_SOCKET, inode, None
        if path and provider_socket_path is not None and \
                str(Path(path)) == str(Path(provider_socket_path)):
            kind = (FdKind.PROVIDER_UDS_LISTENER
                    if isinstance(info, UnixSocketInfo) and info.flags & 0x10000
                    else FdKind.PROVIDER_UDS_CONNECTED)
            return kind, inode, path
        if path:
            return FdKind.SOCKET_OTHER, inode, path
        # Present in the table without a path column entry: treat as named.
        return FdKind.SOCKET_OTHER, inode, None
    if target.startswith("pipe:"):
        return FdKind.PIPE, inode, None
    if target.startswith("anon_inode:"):
        return FdKind.EVENTFD_OR_ANON_INODE, inode, None
    if target.startswith("/"):
        lowered = target.lower()
        if _PG_TEMP_DIR in Path(target).parts:
            return FdKind.POSTGRES_TEMP_FILE, inode, None
        if pg_context is not None and Path(target).is_relative_to(Path(pg_context.data_directory)):
            basename = os.path.basename(target)
            filenode = _filenode_of(basename)
            if filenode is not None:
                if filenode in pg_context.toast_filenodes:
                    return FdKind.TOAST_RELATION_FILE, inode, None
                if filenode in pg_context.relation_filenodes:
                    return FdKind.RELATION_FILE, inode, None
            # Numeric basename under PGDATA whose filenode the run never
            # learned from the catalog: unknown, never guessed — relation
            # classification requires the exact filenode evidence.
            return FdKind.UNKNOWN, inode, None
        base = os.path.basename(target)
        if lowered.endswith((".sock", ".socket")):
            return FdKind.SOCKET_OTHER, inode, None
        return FdKind.REGULAR_FILE_OTHER, inode, None
    return FdKind.UNKNOWN, inode, None


def _read_rss_threads(pid: int) -> tuple[int | None, int | None]:
    rss_bytes = None
    thread_count = None
    try:
        statm = Path(f"/proc/{pid}/statm").read_text(encoding="ascii").split()
        page_size = getattr(os, "sysconf", lambda _name: 4096)("SC_PAGE_SIZE")
        rss_bytes = int(statm[1]) * page_size
    except (OSError, ValueError, IndexError):
        pass
    try:
        status = Path(f"/proc/{pid}/status").read_text(encoding="ascii", errors="replace")
        thread_count = next(
            int(line.split()[1]) for line in status.splitlines()
            if line.startswith("Threads:"))
    except (OSError, StopIteration, ValueError):
        pass
    return rss_bytes, thread_count


def _consistent_fd_snapshot(
    pid, unix_paths_by_inode, provider_socket_path, pg_context, role_overrides,
):
    """Keep every attempt; only the final successful read defines current state."""
    attempts = []
    fds = None
    errors = []
    for _ in range(_CONSISTENT_RETRIES):
        started = time.monotonic_ns()
        first = _list_fds(pid)
        errors = []
        if first is None:
            errors = ["fd_list_unreadable"]
            attempts.append(FdSnapshotAttempt(started, time.monotonic_ns(), None, tuple(errors)))
            return None, errors, tuple(attempts)
        identities = []
        for fd in first:
            target = _read_link(pid, fd)
            if target is None:
                errors.append(f"fd_readlink_unreadable:{fd}")
                identities.append(FdIdentity(fd, "", FdKind.UNKNOWN))
                continue
            kind, inode, unix_path = classify_target(
                target, unix_paths_by_inode, provider_socket_path, pg_context)
            if role_overrides:
                kind = role_overrides.get(kind, kind)
            # Files and pipes have identity too; socket identity comes from its target.
            if inode is None:
                try:
                    inode = os.stat(f"/proc/{pid}/fd/{fd}").st_ino
                except OSError:
                    errors.append(f"fd_inode_unreadable:{fd}")
            identities.append(FdIdentity(fd, target, kind, inode, unix_path))
        second = _list_fds(pid)
        if second is None:
            errors.append("fd_list_unreadable")
        elif first != second:
            errors.append("fd_set_changed_during_read")
        fds = tuple(identities)
        attempts.append(FdSnapshotAttempt(started, time.monotonic_ns(), fds, tuple(errors)))
        if not errors:
            return fds, [], tuple(attempts)
    return fds, errors, tuple(attempts)


def snapshot_process(
    pid: int,
    *,
    monotonic_ns: int,
    unix_paths_by_inode: dict[int, str | None],
    provider_socket_path: str | None = None,
    pg_context: PgFileClassificationContext | None = None,
    role_overrides: dict[FdKind, FdKind] | None = None,
    expected_start_time_ticks: int | None = None,
) -> ProcessSnapshot:
    """Collect one validated snapshot for ``pid``.

    Unreadable data stays None and the status explains it. A changed
    process start time (pid reuse) makes the snapshot invalid outright.
    """
    observed_start = time.monotonic_ns()
    errors: list[str] = []
    start_time = process_start_time_ticks(pid)
    if start_time is None:
        return ProcessSnapshot(
            pid=pid, process_start_time_ticks=None, monotonic_ns=monotonic_ns,
            status=SnapshotStatus.INVALID, errors=("stat_unreadable",))
    if (expected_start_time_ticks is not None
            and start_time != expected_start_time_ticks):
        return ProcessSnapshot(
            pid=pid, process_start_time_ticks=start_time,
            monotonic_ns=monotonic_ns, status=SnapshotStatus.INVALID,
            errors=("process_replaced",))
    rss_bytes, thread_count = _read_rss_threads(pid)
    if rss_bytes is None:
        errors.append("statm_unreadable")
    if thread_count is None:
        errors.append("status_unreadable")
    fds, fd_errors, attempts = _consistent_fd_snapshot(
        pid, unix_paths_by_inode, provider_socket_path, pg_context,
        role_overrides)
    errors.extend(fd_errors)
    if process_start_time_ticks(pid) != start_time:
        errors.append("process_replaced")
    status = SnapshotStatus.VALID
    if fds is None or "process_replaced" in errors or "fd_list_unreadable" in errors:
        status = SnapshotStatus.INVALID
    elif errors:
        status = SnapshotStatus.PARTIAL
    return ProcessSnapshot(
        pid=pid, process_start_time_ticks=start_time,
        monotonic_ns=monotonic_ns, status=status,
        errors=tuple(errors), rss_bytes=rss_bytes,
        thread_count=thread_count, fds=fds,
        observed_start_ns=observed_start, observed_end_ns=time.monotonic_ns(),
        fd_attempts=attempts)


def sample_tick(
    pids: Mapping[str, int],
    *,
    monotonic_ns: int | None = None,
    provider_socket_path: str | None = None,
    pg_context: PgFileClassificationContext | None = None,
    expected_start_times: Mapping[str, int] | None = None,
) -> SampleTick:
    """Observe every role under one shared /proc/net/unix view."""
    if monotonic_ns is None:
        monotonic_ns = time.monotonic_ns()
    errors = []
    try:
        unix_paths = unix_socket_table()
        unix_valid = unix_paths is not None
        if not unix_valid:
            errors.append("unix_table_unreadable")
    except (ValueError, UnicodeError):
        unix_paths = None
        unix_valid = False
        errors.append("unix_table_parse_error")
    processes: dict[str, ProcessSnapshot] = {}
    for role, pid in pids.items():
        expected = (expected_start_times or {}).get(role)
        processes[role] = snapshot_process(
            pid, monotonic_ns=monotonic_ns,
            unix_paths_by_inode=unix_paths or {},
            provider_socket_path=provider_socket_path,
            pg_context=pg_context,
            expected_start_time_ticks=expected)
    return SampleTick(
        monotonic_ns=monotonic_ns, unix_table_valid=unix_valid,
        processes=processes, ended_ns=time.monotonic_ns(), errors=tuple(errors))
