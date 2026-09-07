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
                raise ValueError('gateway limits must be positive integers')


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
        workers: dict[threading.Thread, socket.socket] = {}
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
            finally:
                connection.close()

        listener.settimeout(.25)
        try:
            while not stop.is_set() and (not session_limit or admitted < session_limit):
                try:
                    connection, _ = listener.accept()
                except TimeoutError:
                    continue
                for worker in tuple(workers):
                    if not worker.is_alive():
                        worker.join()
                        del workers[worker]
                if len(workers) >= self._limits.max_connections:
                    connection.close()
                    continue
                connection.settimeout(self._limits.frame_timeout_ms / 1000)
                worker = threading.Thread(target=handle, args=(connection,), name='semloom-session')
                workers[worker] = connection
                try:
                    worker.start()
                except BaseException:
                    del workers[worker]
                    connection.close()
                    stop.set()
                    raise
                admitted += 1
        except BaseException:
            stop.set()
            raise
        finally:
            closing = False
            while workers:
                if stop.is_set() and not closing:
                    for connection in workers.values():
                        try:
                            connection.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                    closing = True
                for worker in tuple(workers):
                    worker.join(timeout=.05)
                    if not worker.is_alive():
                        del workers[worker]
        if failed.is_set():
            raise RuntimeError('gateway session handler failed')
