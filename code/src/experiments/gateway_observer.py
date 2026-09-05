"""Passive session and completion observation shared by Filter and Map checks."""
from __future__ import annotations

import os
import socket
import struct
import time


def _peer_credentials(connection: socket.socket) -> tuple[int, int, int] | None:
    """Read SO_PEERCRED (pid, uid, gid) from a connected AF_UNIX socket."""
    if not hasattr(socket, "SO_PEERCRED"):
        return None
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

class ObservedAdapter:
    """Preserve provider identity and decorate only completion observation."""
    def __init__(self, adapter, observe):
        self.adapter = adapter
        self.observe = observe
        self.model_id = adapter.model_id

    def execution_id_for(self, version):
        return self.adapter.execution_id_for(version)

    def complete(self, request):
        return self.observe(request, self.adapter.complete)
