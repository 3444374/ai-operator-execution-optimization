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


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--events", type=Path, required=True)
    args, gateway_args = parser.parse_known_args()
    args.events.touch(exist_ok=False)
    handle = args.events.open("a", encoding="ascii", buffering=1)

    def record(value):
        handle.write(json.dumps(value, separators=(",", ":")) + "\n")

    from src.execution_provider import server

    session_count = 0
    task_count = 0
    original_adapter = server.GoldenCompletionAdapter

    class ObservedGoldenAdapter(original_adapter):
        def complete(self, request):
            nonlocal task_count
            task_count += 1
            record({
                "event": "task",
                "task": task_count,
                "payload_digest": request.semantic_payload_digest,
                "monotonic_ns": time.monotonic_ns(),
            })
            return super().complete(request)

    original_session = server._run_session

    def observed_session(connection, **keywords):
        nonlocal session_count
        session_count += 1
        current = session_count
        peer = _peer_credentials(connection)
        record({
            "event": "session_start",
            "session_id": current,
            "monotonic_ns": time.monotonic_ns(),
            "gateway_pid": os.getpid(),
            "accepted_fd": connection.fileno(),
            "accepted_socket_inode": _accepted_inode(connection),
            "peer_pid": peer[0] if peer else None,
            "peer_uid": peer[1] if peer else None,
            "peer_gid": peer[2] if peer else None,
        })
        try:
            return original_session(connection, **keywords)
        finally:
            record({
                "event": "session_end",
                "session_id": current,
                "monotonic_ns": time.monotonic_ns(),
            })

    server.GoldenCompletionAdapter = ObservedGoldenAdapter
    server._run_session = observed_session
    sys.argv = [sys.argv[0]] + (
        gateway_args[1:] if gateway_args[:1] == ["--"] else gateway_args)
    try:
        raise SystemExit(server.main())
    finally:
        handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
