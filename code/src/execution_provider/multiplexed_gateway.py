"""Bounded connection mailboxes around one Job-aware execution owner and one v6 protocol."""

from dataclasses import dataclass, replace, asdict
from collections import deque
import select
import socket
import threading
import time

from .adapters.incremental_execution import build_fixed_model_execution
from .adapters.incremental_session import IncrementalMapProtocol
from .adapters.model_config import MAX_MODEL_RESPONSE_BYTES
from .wire.framing import MAX_FRAME_BYTES
from .wire import v6
from .completion import CompletionAdapterError
from .gateway_runtime import ConnectionWorkers, accept_connection, interrupt_connection
from ..scheduling.core.session import WakeSignal
from ..scheduling.core.session_contract import LeaseId, SessionSpec

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
        self.session.request_cancel()  # The core's explicitly thread-safe flag-only operation.
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


class MapConnection(IncrementalMapProtocol):
    def __init__(self, gateway, connection, mailbox, job, limits):
        self.gateway, self.connection, self.mailbox = gateway, connection, mailbox
        self._session = SessionProxy(mailbox)
        self.model_id = gateway.model_id
        self.max_tasks = limits.held_tasks
        self.poll_interval_s = limits.poll_interval_s
        self.job, self.session_id = job, mailbox.session.session_id
        self._transport_error = None

    def _observer(self, event):
        self.gateway.observe(dict(event, job_id=self.job.job_id, engine_session_id=self.session_id))

    def _peer_stopped(self):
        return self.mailbox.cancelled.is_set() or bool(
            select.select([self.connection], [], [], 0)[0]
        )

    def wait_for_progress(self, result):
        self.gateway.progress.wait(result.generation, self.poll_interval_s)

    @staticmethod
    def prepare_task(request, sequence):
        return PendingOffer(request, sequence)

    def run(self, handler):
        def incremental(connection, opened):
            self.mailbox.clean = bool(self.run_incremental(connection, opened))

        try:
            handler(self.connection, incremental_handler=incremental)
        except Exception:
            self.mailbox.clean = False
        finally:
            if not self.mailbox.clean:
                self.mailbox.cancel()
            self.mailbox.done.set()
            self.gateway.engine.wake.notify()


@dataclass
class ConnectionState:
    connection: socket.socket
    mailbox: ConnectionMailbox
    job: object
    worker: threading.Thread
    closing: bool = False
    interrupted: bool = False


class MultiSessionMapGateway:
    """One owner thread; registered Jobs reserve storage before network workers start."""

    def __init__(
        self,
        config,
        *,
        max_jobs,
        max_connections,
        max_tasks,
        max_active_requests,
        frame_timeout_ms,
        input_bytes=None,
        result_bytes=None,
        observer=None,
        execution_factory=None,
        execute=None,
    ):
        if any(
            type(value) is not int or value <= 0
            for value in (
                max_jobs,
                max_connections,
                max_tasks,
                max_active_requests,
                frame_timeout_ms,
            )
        ):
            raise ValueError("gateway capacities and deadlines must be positive integers")
        if max_jobs > max_connections:
            raise ValueError("active Job count must fit connection capacity")
        if any(
            value is not None and (type(value) is not int or value <= 0)
            for value in (input_bytes, result_bytes)
        ):
            raise ValueError("gateway byte budgets must be positive integers")
        total_input = max_tasks * MAX_FRAME_BYTES if input_bytes is None else input_bytes
        total_result = (
            max_tasks * MAX_MODEL_RESPONSE_BYTES if result_bytes is None else result_bytes
        )
        self.model_id = config.model_id
        self.max_connections, self.frame_timeout = max_connections, frame_timeout_ms / 1000
        self._observer, self._observer_lock = observer, threading.Lock()
        self.progress = WakeSignal()
        self.connections = {}
        self.workers = ConnectionWorkers()
        self.waiting = deque()
        self._known_jobs = set()
        self.execution = (execution_factory or build_fixed_model_execution)(
            config,
            max_tasks=max_tasks,
            max_active_requests=max_active_requests,
            input_bytes=total_input,
            result_bytes=total_result,
            observer=self.observe,
            max_jobs=max_jobs,
            registered_jobs=True,
            execute=execute,
        )
        self.engine = self.execution.engine
        if self.engine.jobs.maximum != max_jobs:
            self.execution.close()
            raise ValueError("execution factory Job capacity differs from gateway registration")
        # Reserved independently of task leases, including transient encode/decode copies.
        self.connection_buffer_bytes = (
            2 * MAX_FRAME_BYTES + 2 * MAX_MODEL_RESPONSE_BYTES + MAX_FRAME_BYTES
        )

    def execution_id_for(self, version):
        return v6.EXECUTION_ID if version == 6 else None

    def observe(self, event):
        if self._observer:
            with self._observer_lock:
                self._observer(event)

    def _accept(self, connection, handler, sequence):
        try:
            job, session, budget = self.execution.open_job(
                f"pg-connection-{sequence}",
                SessionSpec("registered", "map", "fixed-chat", work_unit=self.execution.work_unit),
            )
        except ValueError:
            return False
        try:
            session.dispatch_enabled = False  # A v6 poll exposes the accepted organization window.
            connection.settimeout(self.frame_timeout)
            mailbox = ConnectionMailbox(session, self.engine.wake, self.frame_timeout)
            adapter = MapConnection(self, connection, mailbox, job, session.limits)
            self.observe(
                {
                    "event": "job_opened",
                    "job_id": job.job_id,
                    "engine_session_id": session.session_id,
                    "job_budget": asdict(budget),
                    "connection_buffer_bytes": self.connection_buffer_bytes,
                }
            )
            worker = self.workers.start(connection, lambda _: adapter.run(handler))
            self.connections[session.session_id] = ConnectionState(connection, mailbox, job, worker)
            self._known_jobs.add(job.job_id)
        except BaseException:
            connection.close()
            if "session" in locals():
                self.connections.pop(session.session_id, None)
                session.cancel()
                session.close()
            self.engine.close_job(job)
            raise
        return True

    def _command(self, state):
        mailbox, session = state.mailbox, state.mailbox.session
        with mailbox.condition:
            if mailbox.command is None or mailbox.cancelled.is_set():
                return
            operation, args = mailbox.command
            mailbox.command = None
            try:
                if operation == "offer":
                    pending = args[0]
                    task = self.execution.prepare_task(pending.request, pending.sequence)
                    result = session.offer((task,))
                elif operation == "advance":
                    session.dispatch_enabled = True
                    result = replace(session.advance(1), generation=self.progress.generation)
                elif operation == "release":
                    result = session.release(args[0])
                else:
                    raise ValueError("unknown owner command")
            except Exception:
                session._fail("owner command failed")
                # Do not retain traceback frames or arbitrary policy exception payloads in replies.
                result = CompletionAdapterError("MODEL_UNAVAILABLE")
            mailbox.reply = result
            mailbox.condition.notify_all()

    @staticmethod
    def _interrupt(state):
        if state.interrupted:
            return
        # Repeated cancel notifications would self-wake the drain loop and starve I/O exit.
        state.interrupted = True
        state.mailbox.cancel()
        interrupt_connection(state.connection)

    def _controls(self):
        self.workers.reap()
        for session_id, state in tuple(self.connections.items()):
            mailbox, session = state.mailbox, state.mailbox.session
            if mailbox.cancelled.is_set() and not state.closing:
                state.closing = True
                self.engine.close_job(state.job)
                self._interrupt(state)
            if not mailbox.done.is_set():
                continue
            state.worker.join()
            # The sender no longer holds these bytes. Reclaim even unpublished replies.
            leases = tuple(
                LeaseId(session_id, r.lease) for r in session._records() if r.phase == "LEASED"
            )
            if leases:
                session.release(leases)
            if mailbox.clean and not self.engine.capacity.usage(session_id).held_tasks:
                session.seal()
                session.advance(1)
            else:
                session.cancel()
            session.close()
            if state.job in self.engine.jobs.jobs:
                self.engine.close_job(state.job)
            with mailbox.condition:
                mailbox.command = None
                mailbox.reply = _EMPTY
            del self.connections[session_id]
            self.observe(
                {
                    "event": "connection_closed",
                    "job_id": state.job.job_id,
                    "engine_session_id": session_id,
                    "usage": asdict(self.engine.capacity.usage(session_id)),
                }
            )

    def serve(self, listener, stopping, *, handler, session_limit=0):
        listener.setblocking(False)
        served, deadline = 0, None
        try:
            while True:
                generation = self.engine.wake.generation
                if self.engine.error:
                    stopping.set()
                if stopping.is_set():
                    while self.waiting:
                        self.waiting.popleft()[0].close()
                    deadline = deadline or time.monotonic() + self.execution.drain_timeout_s
                    for state in tuple(self.connections.values()):
                        self._interrupt(state)
                admissions_done = bool(session_limit and served >= session_limit)
                if not stopping.is_set() and not admissions_done:
                    connection = accept_connection(
                        listener,
                        occupied=lambda: len(self.connections) + len(self.waiting),
                        maximum=self.max_connections,
                        timeout_s=self.frame_timeout,
                    )
                    if connection is not None:
                        connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, MAX_FRAME_BYTES)
                        connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, MAX_FRAME_BYTES)
                        self.waiting.append((connection, time.monotonic(), served))
                        served += 1
                self._controls()
                if self.waiting:
                    connection, arrived, number = self.waiting[0]
                    if time.monotonic() >= arrived + self.frame_timeout:
                        self.waiting.popleft()[0].close()
                    elif self._accept(connection, handler, number):
                        self.waiting.popleft()
                for state in tuple(self.connections.values()):
                    self._command(state)
                progress = self.engine.advance()
                live = {job.job_id for job in self.engine.jobs.jobs}
                for job_id in self._known_jobs - live:
                    self.observe(
                        {
                            "event": "job_drained",
                            "job_id": job_id,
                            "usage": asdict(self.engine.capacity.usage(job_id=job_id)),
                        }
                    )
                self._known_jobs.intersection_update(live)
                if progress.events:
                    self.progress.notify()
                if (
                    (stopping.is_set() or admissions_done)
                    and not self.connections
                    and not self.waiting
                ):
                    if not self.engine.capacity.usage().held_tasks:
                        break
                    deadline = deadline or time.monotonic() + self.execution.drain_timeout_s
                if deadline and time.monotonic() >= deadline:
                    raise RuntimeError("remote outcome unconfirmed; remaining capacity quarantined")
                self.engine.wake.wait(generation, self.engine.capacity.limits.poll_interval_s)
            if self.engine.error:
                raise RuntimeError("shared execution pool quarantined")
        finally:
            while self.waiting:
                self.waiting.popleft()[0].close()
            for state in tuple(self.connections.values()):
                self._interrupt(state)
            for state in tuple(self.connections.values()):
                state.worker.join(self.frame_timeout)
            self._controls()

    def request_stop(self):
        for state in tuple(self.connections.values()):
            self._interrupt(state)
        self.engine.wake.notify()

    def close(self):
        closed = self.execution.close()
        self.observe(
            {
                "event": "transport_closed",
                "closed": closed,
                "usage": asdict(self.engine.capacity.usage()),
            }
        )
        return closed
