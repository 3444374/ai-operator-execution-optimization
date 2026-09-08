"""Serve bounded independent connections without assigning idle peers model capacity."""

from dataclasses import dataclass
import socket
import threading
from typing import Callable


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
