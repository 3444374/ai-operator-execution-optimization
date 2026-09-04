"""Provider UDS session attribution from gateway events and process ticks.

Experiment-specific reasoning that the generic /proc collector must not
own: under the synchronous single-session SemMap contract, a backend
unbound AF_UNIX socket is the provider client end only when the gateway
session evidence uniquely supports it. All five conditions must hold:

1. every gateway session_start.peer_pid equals the observed backend pid;
2. within each session's active window, the backend gained exactly one
   unbound AF_UNIX socket relative to baseline;
3. during that window the gateway holds the accepted socket whose inode
   the session_start event recorded;
4. the candidate's lifetime overlaps the session window;
5. at most one session is active at any instant (synchronous contract).

Any ambiguity returns ``None`` attributions so the qualification can be
inconclusive instead of guessed.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from src.observability.process_resources.model import (
    FdIdentity,
    FdKind,
    ProcessSnapshot,
    ResourceTrace,
    SnapshotStatus,
)


@dataclass(frozen=True)
class SessionWindow:
    session_id: int
    start_ns: int
    end_ns: int
    peer_pid: int | None
    accepted_fd: int | None
    accepted_inode: int | None


def load_session_events(path: Path) -> list[dict]:
    events = []
    for line in path.read_text(encoding="ascii").splitlines():
        if line.startswith("{"):
            events.append(json.loads(line))
    return events


def session_windows(events: list[dict]) -> list[SessionWindow]:
    """Fold session_start/session_end events into windows."""
    windows: list[SessionWindow] = []
    open_sessions: dict[int, SessionWindow] = {}
    for event in events:
        if event.get("event") == "session_start":
            window = SessionWindow(
                session_id=event["session_id"],
                start_ns=event["monotonic_ns"],
                end_ns=event["monotonic_ns"],
                peer_pid=event.get("peer_pid"),
                accepted_fd=event.get("accepted_fd"),
                accepted_inode=event.get("accepted_socket_inode"))
            open_sessions[window.session_id] = window
        elif event.get("event") == "session_end":
            window = open_sessions.pop(event["session_id"], None)
            if window is not None:
                windows.append(SessionWindow(
                    session_id=window.session_id,
                    start_ns=window.start_ns,
                    end_ns=event["monotonic_ns"],
                    peer_pid=window.peer_pid,
                    accepted_fd=window.accepted_fd,
                    accepted_inode=window.accepted_inode))
    windows.extend(open_sessions.values())
    windows.sort(key=lambda w: w.start_ns)
    return windows


def _active_count(windows: list[SessionWindow], when_ns: int) -> int:
    return sum(1 for w in windows if w.start_ns <= when_ns <= w.end_ns)


def attribute_provider_sessions(
    *,
    backend_pid: int,
    baseline: Mapping[str, ProcessSnapshot],
    trace: ResourceTrace,
    windows: list[SessionWindow],
) -> dict | None:
    """Attribute backend unbound sockets to gateway provider sessions.

    Returns per-session attribution evidence, or None when the evidence
    cannot support a unique attribution (caller must go inconclusive).
    """
    problems: list[str] = []
    base = baseline.get("backend")
    if base is None or base.fds is None:
        return None
    base_unbound = {item.fd: item for item in base.fds
                    if item.kind is FdKind.UNBOUND_UNIX_SOCKET}

    for window in windows:
        if window.peer_pid is None:
            problems.append(f"session{window.session_id}_peer_pid_missing")
        elif window.peer_pid != backend_pid:
            problems.append(f"session{window.session_id}_peer_mismatch")

    attribution: dict = {"sessions": [], "problems": problems}
    for window in windows:
        candidates: dict[int, FdIdentity] = {}
        for tick in trace.ticks:
            if not (window.start_ns <= tick.monotonic_ns <= window.end_ns):
                continue
            backend = tick.processes.get("backend")
            if backend is None or backend.fds is None:
                continue
            if backend.status is not SnapshotStatus.VALID:
                continue
            for item in backend.fds:
                if (item.kind is FdKind.UNBOUND_UNIX_SOCKET
                        and item.fd not in base_unbound):
                    candidates.setdefault(item.fd, item)
        if len(candidates) != 1:
            problems.append(
                f"session{window.session_id}_candidates_{len(candidates)}")
            attribution["sessions"].append({
                "session_id": window.session_id,
                "candidate_fds": sorted(candidates),
                "attributed": None})
            continue
        fd, identity = next(iter(candidates.items()))
        # Gateway must hold the recorded accepted inode during the window.
        accepted_seen = False
        for tick in trace.ticks:
            if not (window.start_ns <= tick.monotonic_ns <= window.end_ns):
                continue
            gateway = tick.processes.get("gateway")
            if gateway is None or gateway.fds is None:
                continue
            for item in gateway.fds:
                if window.accepted_inode is not None and \
                        item.inode == window.accepted_inode:
                    accepted_seen = True
        if not accepted_seen and window.accepted_inode is not None:
            problems.append(f"session{window.session_id}_accepted_inode_unseen")
        attribution["sessions"].append({
            "session_id": window.session_id,
            "candidate_fds": [fd],
            "attributed": {
                "fd": fd,
                "inode": identity.inode,
                "accepted_inode": window.accepted_inode,
                "peer_pid": window.peer_pid,
            }})
    # Synchronous single-session contract. A window with no tick inside it
    # cannot support the concurrency check; that is already an attribution
    # gap flagged as candidates_0 above, not a crash.
    for window in windows:
        in_window = [
            _active_count(windows, tick.monotonic_ns)
            for tick in trace.ticks
            if window.start_ns <= tick.monotonic_ns <= window.end_ns]
        if in_window and max(in_window) > 1:
            problems.append(f"session{window.session_id}_concurrent_{max(in_window)}")
    attribution["problems"] = problems
    if problems:
        return None
    return attribution


def reclassify_clients(
    trace: ResourceTrace, attribution: dict,
) -> ResourceTrace:
    """Rewrite attributed backend sockets as provider clients in a copy.

    The raw trace is never mutated; the rewritten copy feeds the policy
    together with the attribution evidence.
    """
    attributed = {
        entry["attributed"]["fd"]: entry["attributed"]
        for entry in attribution["sessions"] if entry["attributed"]}
    from src.observability.process_resources.model import ProcessSnapshot, SampleTick
    ticks = []
    for tick in trace.ticks:
        backend = tick.processes.get("backend")
        if backend is None or backend.fds is None or not attributed:
            ticks.append(tick)
            continue
        fds = tuple(
            FdIdentity(
                fd=item.fd, target=item.target,
                kind=FdKind.PROVIDER_UDS_CONNECTED,
                inode=item.inode, unix_path=None)
            if item.fd in attributed else item
            for item in backend.fds)
        rewritten = ProcessSnapshot(
            pid=backend.pid,
            process_start_time_ticks=backend.process_start_time_ticks,
            monotonic_ns=backend.monotonic_ns,
            status=backend.status, errors=backend.errors,
            rss_bytes=backend.rss_bytes,
            thread_count=backend.thread_count, fds=fds)
        ticks.append(SampleTick(
            monotonic_ns=tick.monotonic_ns,
            unix_table_valid=tick.unix_table_valid,
            processes={**tick.processes, "backend": rewritten}))
    return ResourceTrace(baseline=trace.baseline, ticks=tuple(ticks),
                         fd_correlation_evidence={
                             fd: {"rule": "session-attribution",
                                  "peer_pid": entry["peer_pid"],
                                  "accepted_inode": entry["accepted_inode"]}
                             for fd, entry in attributed.items()})
