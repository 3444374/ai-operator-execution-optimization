"""Serve bounded independent connections without assigning idle peers model capacity."""

from dataclasses import dataclass
from enum import Enum
import socket
import threading
import time
from typing import Callable

from .wire.framing import MAX_FRAME_BYTES


@dataclass(frozen=True)
class GatewayLimits:
    """Independent connection and request ceilings; every frame has a time limit."""

    max_connections: int = 8
    max_active_requests: int = 1
    frame_timeout_ms: int = 120000

    def __post_init__(self):
        for value in (self.max_connections, self.max_active_requests, self.frame_timeout_ms):
            if type(value) is not int or value < 1:
                raise ValueError("gateway limits must be positive integers")


def interrupt_connection(connection):
    """Wake blocked I/O; its worker retains ownership until it exits."""
    try:
        connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def accept_connection(listener, *, occupied, maximum, timeout_s):
    try:
        connection, _ = listener.accept()
    except (TimeoutError, BlockingIOError):
        return None
    if occupied() >= maximum:
        connection.close()
        return None
    connection.settimeout(timeout_s)
    return connection


class ConnectionWorkers:
    """Common socket/thread lifetime; session semantics stay in the supplied handler."""

    def __init__(self):
        self.workers = {}

    def start(self, connection, handler):
        def run():
            try:
                handler(connection)
            finally:
                connection.close()

        worker = threading.Thread(target=run, name="semloom-connection")
        self.workers[worker] = connection
        try:
            worker.start()
        except BaseException:
            del self.workers[worker]
            connection.close()
            raise
        return worker

    def reap(self):
        for worker in tuple(self.workers):
            if not worker.is_alive():
                worker.join()
                del self.workers[worker]

    def active_count(self):
        self.reap()
        return len(self.workers)

    def interrupt(self):
        for connection in tuple(self.workers.values()):
            interrupt_connection(connection)

    def drain(self, stop):
        while self.workers:
            if stop.is_set():
                self.interrupt()
            for worker in tuple(self.workers):
                worker.join(timeout=0.05)
            self.reap()


class GatewayRuntime:
    """Own accepted sockets/threads; the caller owns the listener and injected handler.

    No thread-pool queue exists. A full connection set closes new peers immediately.
    Normal finite-session runs drain; external stop wakes all socket readers and
    waits for dispatched handlers. Injected model Adapters must bound their calls.
    """

    def __init__(self, handle_session: Callable[[socket.socket], None], limits: GatewayLimits):
        self._handle_session = handle_session
        self._limits = limits

    def serve(self, listener: socket.socket, stop: threading.Event, *, session_limit: int = 0):
        workers = ConnectionWorkers()
        failed = threading.Event()
        admitted = 0

        def handle(connection):
            try:
                self._handle_session(connection)
            except OSError:
                pass  # Disconnect, deadline and shutdown are connection-local.
            except BaseException:
                failed.set()
                stop.set()

        listener.settimeout(0.25)
        try:
            while not stop.is_set() and (not session_limit or admitted < session_limit):
                workers.reap()
                connection = accept_connection(
                    listener,
                    occupied=workers.active_count,
                    maximum=self._limits.max_connections,
                    timeout_s=self._limits.frame_timeout_ms / 1000,
                )
                if connection is None:
                    continue
                workers.start(connection, handle)
                admitted += 1
        except BaseException:
            stop.set()
            raise
        finally:
            workers.drain(stop)
        if failed.is_set():
            raise RuntimeError("gateway session handler failed")


_EMPTY = object()


@dataclass(frozen=True)
class PendingOffer:
    request: object
    sequence: int


class ConnectionMailbox:
    """One command and one reply; cancellation never competes for the command slot."""

    def __init__(self, session, wake, timeout):
        self.session = session  # Read/mutated only by the owner, except request_cancel().
        self.wake, self.timeout = wake, timeout
        self.condition = threading.Condition()
        self.command = None
        self.reply = _EMPTY
        self.cancelled = threading.Event()
        self.done = threading.Event()
        self.clean = False

    def cancel(self):
        self.cancelled.set()
        if self.session is not None:
            self.session.request_cancel()  # The core's thread-safe flag-only operation.
        self.wake.notify()
        with self.condition:
            self.condition.notify_all()

    def call(self, operation, args, byte_count=0):
        if not 0 <= byte_count <= MAX_FRAME_BYTES:
            raise ValueError("connection command exceeds byte budget")
        with self.condition:
            if self.cancelled.is_set():
                raise ConnectionResetError("connection cancelled")
            if self.command is not None or self.reply is not _EMPTY:
                raise RuntimeError("connection RPC already pending")
            self.command = (operation, args)
            self.wake.notify()
            deadline = time.monotonic() + self.timeout
            while self.reply is _EMPTY and not self.cancelled.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.cancel()
                    raise TimeoutError("connection command deadline expired")
                self.condition.wait(remaining)
            reply, self.reply = self.reply, _EMPTY
            if reply is _EMPTY:
                raise ConnectionResetError("connection cancelled")
            if isinstance(reply, BaseException):
                raise reply
            return reply


class SessionProxy:
    def __init__(self, mailbox):
        self.mailbox = mailbox

    def offer(self, tasks):
        if type(tasks) is not tuple or len(tasks) != 1 or type(tasks[0]) is not PendingOffer:
            raise ValueError("one sealed request per connection RPC is required")
        return self.mailbox.call("offer", tasks, MAX_FRAME_BYTES)

    def advance(self, maximum):
        # Only one result leaves the core per send slot; other READY results stay charged there.
        return self.mailbox.call("advance", (1,))

    def release(self, leases):
        return self.mailbox.call("release", (leases,), 16 * len(leases))


class ConnectionEnd(Enum):
    STREAM_CLEAN_END = "stream_clean_end"
    PEER_FAILURE = "peer_failure"
    SERVICE_STOP = "service_stop"


@dataclass
class ConnectionState:
    connection: socket.socket
    mailbox: ConnectionMailbox
    job: object
    worker: threading.Thread
    closing: bool = False
    interrupted: bool = False
    connection_id: int = -1
    end_reason: ConnectionEnd | None = None


class ConnectionCapacity:
    """Track classified image connections separately from model allocations."""

    def __init__(self, maximum):
        self.maximum = maximum
        self.handshakes = set()
        self.standalone = set()

    def may_accept(self):
        return not self.handshakes

    def accept(self, connection):
        self.handshakes.add(connection)

    def admit_standalone(self, connection):
        if connection in self.standalone:
            return
        if len(self.standalone) >= self.maximum:
            raise ValueError("image connection capacity exhausted")
        self.standalone.add(connection)
        self.handshakes.discard(connection)

    def release(self, connection):
        self.handshakes.discard(connection)
        self.standalone.discard(connection)
