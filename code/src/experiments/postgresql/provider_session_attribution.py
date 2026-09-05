"""Single-session experiment attribution using peer identity and co-observed FDs.

SO_PEERCRED identifies a process, not its FD. This bounded experiment uses a
unique new unbound socket and the event's accepted inode in the same usable
observation batch. It is not a general Unix socket topology algorithm.
"""
from dataclasses import dataclass, replace
import json
from pathlib import Path

from src.observability.process_resources.model import (
    FdKind, ResourceTrace, SnapshotStatus)


@dataclass(frozen=True)
class SessionWindow:
    session_id: int
    start_ns: int
    end_ns: int
    peer_pid: int | None
    accepted_fd: int | None
    accepted_inode: int | None
    gateway_pid: int | None = None


@dataclass(frozen=True)
class AttributionResult:
    attribution: dict | None
    problems: list[str]


def load_session_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="ascii").splitlines()
            if line.strip()]


def session_windows(events: list[dict]) -> list[SessionWindow]:
    """Reject duplicate, orphan, backwards, and unclosed lifecycle events."""
    opened, seen, windows = {}, set(), []
    tasks, terminals = set(), set()
    last_ns = -1
    for event in events:
        ns = event["monotonic_ns"]
        if not isinstance(ns, int) or ns < last_ns:
            raise ValueError("event_time_reversed")
        last_ns = ns
        kind = event["event"]
        if kind == "session_start":
            sid = event["session_id"]
            if sid in seen:
                raise ValueError("duplicate_session_start")
            seen.add(sid)
            opened[sid] = event
        elif kind == "session_end":
            sid = event["session_id"]
            if sid not in opened:
                raise ValueError("orphan_session_end")
            start = opened.pop(sid)
            if event.get("connection_closed") is not True:
                raise ValueError("session_socket_not_closed")
            if any(key[0] == sid and key not in terminals for key in tasks):
                raise ValueError("unfinished_task")
            windows.append(SessionWindow(
                sid, start["monotonic_ns"], ns, start.get("peer_pid"),
                start.get("accepted_fd"), start.get("accepted_socket_inode"),
                start.get("gateway_pid")))
        elif kind in ("task", "task_complete", "task_error"):
            sid, task = event["session_id"], event["task"]
            if sid not in opened:
                raise ValueError("task_outside_session")
            key = (sid, task)
            if kind == "task":
                if key in tasks:
                    raise ValueError("duplicate_task")
                tasks.add(key)
            else:
                if key not in tasks or key in terminals:
                    raise ValueError("orphan_or_duplicate_task_terminal")
                terminals.add(key)
        else:
            raise ValueError("unknown_session_event")
    if opened:
        raise ValueError("unclosed_session")
    return sorted(windows, key=lambda w: w.start_ns)


def attribute_provider_sessions(*, backend_pid, baseline, trace, windows):
    problems, entries = [], []
    base = baseline.get("backend")
    gateway_base = baseline.get("gateway")
    if base is None or base.fds is None or gateway_base is None:
        return AttributionResult(None, ["backend_baseline_unreadable"])
    base_ids = {fd.identity for fd in base.fds}
    for previous, current in zip(windows, windows[1:]):
        if current.start_ns < previous.end_ns:
            problems.append("concurrent_sessions")
    for window in windows:
        prefix = f"session{window.session_id}_"
        if window.peer_pid != backend_pid or base.pid != backend_pid:
            problems.append(prefix + "peer_mismatch")
        if window.gateway_pid is not None and window.gateway_pid != gateway_base.pid:
            problems.append(prefix + "gateway_mismatch")
        if window.accepted_inode is None:
            problems.append(prefix + "accepted_inode_unrecorded")
        candidates, paired, usable = {}, set(), 0
        for tick in trace.ticks:
            end = tick.ended_ns if tick.ended_ns is not None else tick.monotonic_ns
            if not window.start_ns <= tick.monotonic_ns <= end <= window.end_ns:
                continue
            backend, gateway = (tick.processes.get(r) for r in ("backend", "gateway"))
            if (not tick.unix_table_valid or backend is None or gateway is None
                    or backend.status is not SnapshotStatus.VALID
                    or gateway.status is not SnapshotStatus.VALID
                    or backend.process_identity != base.process_identity
                    or gateway.process_identity != gateway_base.process_identity
                    or backend.fds is None or gateway.fds is None):
                continue
            usable += 1
            found = [fd for fd in backend.fds
                     if fd.kind is FdKind.UNBOUND_UNIX_SOCKET and fd.identity not in base_ids]
            for fd in found:
                candidates[fd.identity] = fd
            accepted = any(fd.inode == window.accepted_inode
                           and (window.accepted_fd is None or fd.fd == window.accepted_fd)
                           and fd.kind is FdKind.PROVIDER_UDS_CONNECTED
                           for fd in gateway.fds)
            if accepted and len(found) == 1:
                paired.add(found[0].identity)
        if usable == 0:
            problems.append(prefix + "no_ticks_in_window")
        if len(candidates) != 1:
            problems.append(prefix + f"candidates_{len(candidates)}")
        if len(paired) != 1 or set(candidates) != paired:
            problems.append(prefix + "accepted_inode_unseen_same_tick")
        entry = {"session_id": window.session_id, "start_ns": window.start_ns,
                 "end_ns": window.end_ns, "candidate_fds": sorted(f.fd for f in candidates.values()),
                 "attributed": None}
        if len(candidates) == 1 and set(candidates) == paired:
            fd = next(iter(candidates.values()))
            entry["attributed"] = {
                "fd": fd.fd, "inode": fd.inode, "target": fd.target,
                "peer_pid": base.pid, "process_start_time_ticks": base.process_start_time_ticks,
                "accepted_inode": window.accepted_inode, "accepted_fd": window.accepted_fd,
                "gateway_pid": gateway_base.pid,
                "gateway_start_time_ticks": gateway_base.process_start_time_ticks}
        entries.append(entry)
    if not windows:
        problems.append("no_session_windows")
    if problems:
        return AttributionResult(None, problems)
    return AttributionResult({"sessions": entries, "problems": []}, [])


def reclassify_clients(trace: ResourceTrace, attribution: dict, windows=None) -> ResourceTrace:
    """Classify only the corresponding session/process/FD/inode observation."""
    entries = attribution["sessions"]
    ticks = []
    for tick in trace.ticks:
        backend = tick.processes.get("backend")
        if backend is None or backend.fds is None:
            ticks.append(tick)
            continue
        def classified(fd):
            for entry in entries:
                identity = entry["attributed"]
                if (identity and entry["start_ns"] <= tick.monotonic_ns <= entry["end_ns"]
                        and backend.process_identity == (identity["peer_pid"], identity["process_start_time_ticks"])
                        and fd.kind is FdKind.UNBOUND_UNIX_SOCKET
                        and fd.identity == (identity["fd"], identity["inode"], identity["target"])):
                    return replace(fd, kind=FdKind.PROVIDER_UDS_CONNECTED)
            return fd
        ticks.append(replace(tick, processes={**tick.processes,
            "backend": replace(backend, fds=tuple(classified(fd) for fd in backend.fds))}))
    return replace(trace, ticks=tuple(ticks), fd_correlation_evidence={
        str(e["session_id"]): e for e in entries})


def residual_provider_fds(trace, attribution):
    """Keep associated identities visible after session_end for cleanup checks."""
    residuals = []
    if not trace.ticks or attribution is None:
        return residuals
    for entry in attribution["sessions"]:
        identity = entry["attributed"]
        if not identity:
            continue
        for role, pid_key, start_key, fd_key, inode_key in (
            ("backend", "peer_pid", "process_start_time_ticks", "fd", "inode"),
            ("gateway", "gateway_pid", "gateway_start_time_ticks", "accepted_fd", "accepted_inode")):
            snapshot = trace.ticks[-1].processes.get(role)
            if snapshot is None or snapshot.fds is None:
                continue
            if snapshot.process_identity != (identity[pid_key], identity[start_key]):
                continue
            for fd in snapshot.fds:
                if fd.fd == identity[fd_key] and fd.inode == identity[inode_key]:
                    residuals.append({"role": role, "session_id": entry["session_id"],
                                      "fd": fd.fd, "inode": fd.inode})
    return residuals
