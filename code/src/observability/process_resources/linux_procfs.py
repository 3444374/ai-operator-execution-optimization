"""Linux /proc collection and FD classification for process-resource traces.

Only reads and classifies: no thresholds and no experiment policy live here.
Socket classification pairs ``/proc/<pid>/fd`` symlink targets with
``/proc/net/unix`` so that a provider UDS descriptor is identified by its
bound path, never by "it is a socket".
"""
from __future__ import annotations

import os
import re
from pathlib import Path

from .model import FdIdentity, FdKind, ProcessSnapshot

_SOCKET_INODE_PATTERN = re.compile(r"^socket:\[(\d+)\]$")
_FILE_INODE_PATTERN = re.compile(r"^(\d+)$")
_PG_TEMP_DIR = "pgsql_tmp"
_TOAST_SUFFIXES = ("_toast",)


def _read_link(pid: int, fd: int) -> str | None:
    try:
        return os.readlink(f"/proc/{pid}/fd/{fd}")
    except OSError:
        return None


def _list_fds(pid: int) -> list[int]:
    try:
        entries = os.listdir(f"/proc/{pid}/fd")
    except OSError:
        return []
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


def unix_socket_table() -> dict[int, str]:
    """Map AF_UNIX socket inode -> bound path from ``/proc/net/unix``.

    Unbound/anonymous entries have no usable path and are omitted; callers
    treat a missing inode as unclassified rather than guessing.
    """
    table: dict[int, str] = {}
    try:
        text = Path("/proc/net/unix").read_text(encoding="ascii", errors="replace")
    except OSError:
        return table
    for line in text.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 8 or not parts[6].isdigit():
            continue
        table[int(parts[6])] = parts[7]
    return table


def classify_target(
    target: str,
    unix_paths_by_inode: dict[int, str],
    provider_socket_path: str | None,
) -> tuple[FdKind, int | None, str | None]:
    """Return ``(kind, inode, unix_path)`` for one descriptor target."""
    inode = _parse_inode(target)
    if target.startswith("socket:[") and inode is not None:
        path = unix_paths_by_inode.get(inode)
        if path is not None and provider_socket_path is not None:
            resolved = str(Path(path))
            if resolved == str(Path(provider_socket_path)):
                # A listener and its accepted/connection peers share the
                # bound path; process role decides client vs accepted vs
                # listener, which callers supply via ``role_kind``.
                return FdKind.PROVIDER_UDS_ACCEPTED, inode, path
        if path is not None:
            return FdKind.SOCKET_OTHER, inode, path
        return FdKind.SOCKET_OTHER, inode, None
    if target.startswith("pipe:"):
        return FdKind.PIPE, inode, None
    if target.startswith("anon_inode:"):
        return FdKind.EVENTFD_OR_ANON_INODE, inode, None
    if target.startswith("/"):
        lowered = target.lower()
        if _PG_TEMP_DIR in lowered:
            return FdKind.POSTGRES_TEMP_FILE, inode, None
        if lowered.endswith(_TOAST_SUFFIXES):
            return FdKind.TOAST_RELATION_FILE, inode, None
        base = os.path.basename(target)
        # PostgreSQL relation files are numeric basenames such as "16384"
        # or fork suffixes "16384_fsm"/"16384_vm" under the data directory.
        if re.fullmatch(r"\d+(_fsm|_vm|_init)?", base):
            return FdKind.RELATION_FILE, inode, None
        if lowered.endswith((".sock", ".socket")):
            return FdKind.SOCKET_OTHER, inode, None
        return FdKind.REGULAR_FILE_OTHER, inode, None
    return FdKind.UNKNOWN, inode, None


def snapshot_process(
    pid: int,
    *,
    monotonic_ns: int,
    provider_socket_path: str | None,
    unix_paths_by_inode: dict[int, str] | None = None,
    role_overrides: dict[FdKind, FdKind] | None = None,
    rss_bytes: int | None = None,
    thread_count: int | None = None,
) -> ProcessSnapshot:
    """Collect one classified snapshot for ``pid``.

    ``role_overrides`` lets the caller refine shared-path classifications by
    process role: a gateway turns provider-path descriptors into listeners or
    accepted connections, a backend into the client end.
    """
    if unix_paths_by_inode is None:
        unix_paths_by_inode = unix_socket_table()
    if rss_bytes is None or thread_count is None:
        try:
            statm = Path(f"/proc/{pid}/statm").read_text(encoding="ascii").split()
            rss_pages = int(statm[1])
        except (OSError, ValueError, IndexError):
            rss_pages = 0
        if rss_bytes is None:
            page_size = getattr(os, "sysconf", lambda _name: 4096)("SC_PAGE_SIZE")
            rss_bytes = rss_pages * page_size
        if thread_count is None:
            try:
                status = Path(f"/proc/{pid}/status").read_text(encoding="ascii", errors="replace")
                thread_count = next(
                    int(line.split()[1]) for line in status.splitlines()
                    if line.startswith("Threads:")
                )
            except (OSError, StopIteration, ValueError):
                thread_count = 0
    identities: list[FdIdentity] = []
    for fd in _list_fds(pid):
        target = _read_link(pid, fd)
        if target is None:
            identities.append(FdIdentity(fd=fd, target="", kind=FdKind.UNKNOWN))
            continue
        kind, inode, unix_path = classify_target(target, unix_paths_by_inode, provider_socket_path)
        if role_overrides is not None:
            kind = role_overrides.get(kind, kind)
        identities.append(FdIdentity(fd=fd, target=target, kind=kind, inode=inode, unix_path=unix_path))
    return ProcessSnapshot(
        monotonic_ns=monotonic_ns,
        rss_bytes=rss_bytes,
        thread_count=thread_count,
        fds=tuple(identities),
    )
