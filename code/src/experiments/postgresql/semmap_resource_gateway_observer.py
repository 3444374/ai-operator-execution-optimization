#!/usr/bin/env python3
"""Gateway observer for SemMap resource qualification.

Wraps the unchanged execution-provider gateway and records session
lifecycle evidence WITHOUT touching model payloads or outputs. For every
accepted session it logs the gateway pid, the accepted socket's inode
(parsed from /proc/self/fd), and the peer identity read via SO_PEERCRED
(pid/uid/gid), plus a monotonic timestamp. Task records carry only the
payload digest and counts.

This observer is experiment tooling: it never enters the PostgreSQL or
gateway production paths and never decides pass/fail.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import struct
import sys
import time


def _peer_credentials(connection: socket.socket) -> tuple[int, int, int] | None:
    """Read SO_PEERCRED (pid, uid, gid) from a connected AF_UNIX socket."""
    try:
        data = connection.getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
        pid, uid, gid = struct.unpack("3i", data)
        return pid, uid, gid
    except OSError:
        return None


def _accepted_inode(connection: socket.socket) -> int | None:
    """Parse the socket inode behind the accepted fd via /proc/self/fd."""
    try:
        target = os.readlink(f"/proc/self/fd/{connection.fileno()}")
    except OSError:
        return None
    if target.startswith("socket:[") and target.endswith("]"):
        digits = target[len("socket:["):-1]
        if digits.isdigit():
            return int(digits)
    return None


class SessionObserver:
    """Record closure and completion separately; neither implies SQL delivery."""
    def __init__(self, record):
        self.record = record
        self.sessions = 0
        self.tasks = 0
        self.current_session = None

    def run_session(self, connection, run, **keywords):
        self.sessions += 1
        self.current_session = self.sessions
        current = self.current_session
        peer = _peer_credentials(connection)
        self.record({"event": "session_start", "session_id": current,
                     "monotonic_ns": time.monotonic_ns(), "gateway_pid": os.getpid(),
                     "accepted_fd": connection.fileno(),
                     "accepted_socket_inode": _accepted_inode(connection),
                     "peer_pid": peer[0] if peer else None,
                     "peer_uid": peer[1] if peer else None,
                     "peer_gid": peer[2] if peer else None})
        reason = "returned"
        try:
            return run(connection, **keywords)
        except BaseException:
            reason = "raised"
            raise
        finally:
            self.record({"event": "session_end", "session_id": current,
                         "monotonic_ns": time.monotonic_ns(), "termination": reason,
                         "connection_closed": connection.fileno() == -1})
            self.current_session = None

    def complete(self, request, complete):
        self.tasks += 1
        task = self.tasks
        self.record({"event": "task", "session_id": self.current_session,
                     "task": task, "payload_digest": request.semantic_payload_digest,
                     "monotonic_ns": time.monotonic_ns()})
        try:
            result = complete(request)
        except Exception as error:
            self.record({"event": "task_error", "session_id": self.current_session,
                         "task": task, "monotonic_ns": time.monotonic_ns(),
                         "error_type": type(error).__name__})
            raise
        self.record({"event": "task_complete", "session_id": self.current_session,
                     "task": task, "monotonic_ns": time.monotonic_ns()})
        return result


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--events", type=Path, required=True)
    args, gateway_args = parser.parse_known_args()
    from src.execution_provider import server
    original_adapter, original_session = server.GoldenCompletionAdapter, server._run_session
    with args.events.open("x", encoding="ascii", buffering=1) as handle:
        observer = SessionObserver(lambda value: handle.write(json.dumps(value) + "\n"))
        class ObservedGoldenAdapter(original_adapter):
            def complete(self, request):
                return observer.complete(request, super().complete)
        def observed_session(connection, **keywords):
            return observer.run_session(connection, original_session, **keywords)
        server.GoldenCompletionAdapter, server._run_session = ObservedGoldenAdapter, observed_session
        old_argv = sys.argv
        sys.argv = [sys.argv[0]] + (gateway_args[1:] if gateway_args[:1] == ["--"] else gateway_args)
        try:
            return server.main()
        finally:
            sys.argv = old_argv
            server.GoldenCompletionAdapter, server._run_session = original_adapter, original_session


if __name__ == "__main__":
    raise SystemExit(main())
