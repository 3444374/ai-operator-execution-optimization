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

from .model import (
    FdIdentity,
    FdKind,
    PgFileClassificationContext,
    ProcessSnapshot,
    SampleTick,
    SnapshotStatus,
)

_SOCKET_INODE_PATTERN = re.compile(r"^socket:\[(\d+)\]$")
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


def unix_socket_table() -> dict[int, str | None]:
    """Map every AF_UNIX socket inode to its bound path (None when unbound).

    /proc/net/unix lists connected-but-unbound client sockets too, as rows
    whose trailing path column is absent entirely (7 columns vs 8 for a
    bound socket) — verified empirically on the target kernel. Keeping
    those rows (value None) lets the generic collector mark such
    descriptors UNBOUND_UNIX_SOCKET instead of lumping them with unrelated
    named sockets.
    """
    table: dict[int, str | None] = {}
    try:
        text = Path("/proc/net/unix").read_text(encoding="ascii", errors="replace")
    except OSError:
        return table
    for line in text.splitlines()[1:]:
        parts = line.split(maxsplit=7)
        if len(parts) < 7 or not parts[6].isdigit():
            continue
        table[int(parts[6])] = parts[7] if len(parts) > 7 else None
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
    base = basename
    for suffix in ("_fsm", "_vm", "_init"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    if "." in base:
        base = base.split(".", 1)[0]
    if base.isdigit():
        return int(base)
    return None


def classify_target(
    target: str,
    unix_paths_by_inode: dict[int, str | None],
    provider_socket_path: str | None,
    pg_context: PgFileClassificationContext | None = None,
) -> tuple[FdKind, int | None, str | None]:
    """Return ``(kind, inode, unix_path)`` for one descriptor target."""
    inode = _parse_inode(target)
    if target.startswith("socket:[") and inode is not None:
        path = unix_paths_by_inode.get(inode, "")
        if path is None:
            return FdKind.UNBOUND_UNIX_SOCKET, inode, None
        if path and provider_socket_path is not None and \
                str(Path(path)) == str(Path(provider_socket_path)):
            return FdKind.PROVIDER_UDS_CONNECTED, inode, path
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
        if _PG_TEMP_DIR in lowered:
            return FdKind.POSTGRES_TEMP_FILE, inode, None
        if pg_context is not None and target.startswith(pg_context.data_directory):
            basename = os.path.basename(target)
            filenode = _filenode_of(basename)
            if filenode is not None:
                if filenode in pg_context.toast_filenodes:
                    return FdKind.TOAST_RELATION_FILE, inode, None
                if filenode in pg_context.relation_filenodes:
                    return FdKind.RELATION_FILE, inode, None
        base = os.path.basename(target)
        if re.fullmatch(r"\d+(_fsm|_vm|_init)?(\.\d+)?", base):
            return FdKind.RELATION_FILE, inode, None
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
    pid: int,
    unix_paths_by_inode: dict[int, str | None],
    provider_socket_path: str | None,
    pg_context: PgFileClassificationContext | None,
    role_overrides: dict[FdKind, FdKind] | None,
) -> tuple[tuple[FdIdentity, ...] | None, list[str]]:
    """Read the fd set until two consecutive listings agree.

    Returns (identities, errors): identities stays None when a consistent
    read could not be obtained, so the caller marks the snapshot invalid.
    """
    errors: list[str] = []
    for _attempt in range(_CONSISTENT_RETRIES):
        first = _list_fds(pid)
        if first is None:
            errors.append("fd_list_unreadable")
            return None, errors
        targets: dict[int, str | None] = {}
        unreadable = False
        for fd in first:
            target = _read_link(pid, fd)
            if target is None:
                unreadable = True
                break
            targets[fd] = target
        if unreadable:
            errors.append("fd_readlink_race")
            continue
        second = _list_fds(pid)
        if second is None:
            errors.append("fd_list_unreadable")
            return None, errors
        if second != first:
            errors.append("fd_set_changed_during_read")
            continue
        identities = []
        for fd, target in targets.items():
            kind, inode, unix_path = classify_target(
                target, unix_paths_by_inode, provider_socket_path, pg_context)
            if role_overrides is not None:
                kind = role_overrides.get(kind, kind)
            identities.append(FdIdentity(
                fd=fd, target=target, kind=kind, inode=inode,
                unix_path=unix_path))
        return tuple(identities), errors
    return None, errors


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
    fds, fd_errors = _consistent_fd_snapshot(
        pid, unix_paths_by_inode, provider_socket_path, pg_context,
        role_overrides)
    errors.extend(fd_errors)
    status = SnapshotStatus.VALID
    if fds is None:
        status = SnapshotStatus.INVALID
    elif errors:
        status = SnapshotStatus.PARTIAL
    return ProcessSnapshot(
        pid=pid, process_start_time_ticks=start_time,
        monotonic_ns=monotonic_ns, status=status,
        errors=tuple(errors), rss_bytes=rss_bytes,
        thread_count=thread_count, fds=fds)


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
    unix_paths = unix_socket_table()
    unix_valid = bool(unix_paths)
    processes: dict[str, ProcessSnapshot] = {}
    for role, pid in pids.items():
        expected = (expected_start_times or {}).get(role)
        gateway_overrides = {FdKind.PROVIDER_UDS_CONNECTED: FdKind.PROVIDER_UDS_CONNECTED}
        backend_overrides = {FdKind.PROVIDER_UDS_CONNECTED: FdKind.PROVIDER_UDS_CONNECTED}
        overrides = backend_overrides if role == "backend" else gateway_overrides
        processes[role] = snapshot_process(
            pid, monotonic_ns=monotonic_ns,
            unix_paths_by_inode=unix_paths,
            provider_socket_path=provider_socket_path,
            pg_context=pg_context,
            role_overrides=overrides,
            expected_start_time_ticks=expected)
    return SampleTick(
        monotonic_ns=monotonic_ns, unix_table_valid=unix_valid,
        processes=processes)
