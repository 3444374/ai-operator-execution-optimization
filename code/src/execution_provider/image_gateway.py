"""Own typed image connections, method reservations and the existing Core."""

from collections import deque
from dataclasses import asdict, dataclass, replace
import socket
import threading
import time

from .adapters.image_execution import build_image_execution
from .adapters.image_method import ImageEmbeddingMethod
from .completion import CompletionAdapterError
from .gateway_runtime import (
    _EMPTY, ConnectionCapacity, ConnectionEnd, ConnectionMailbox, ConnectionState,
    ConnectionWorkers, accept_connection, interrupt_connection,
)
from .image_connection import ImageConnection
from .wire import image
from .wire.framing import MAX_FRAME_BYTES, ProtocolError, encode_frame, read_frame
from ..scheduling.core.session import WakeSignal
from ..scheduling.core.session_contract import SessionSpec, State, TaskProfile, OfferResult, AdvanceResult
from ..semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
from ..semantic_methods.continuation import MethodLimits
from ..semantic_methods.driver import MethodDriver, MethodResult, RowIdentity
from ..modalities.image.contracts import build_image_work_descriptor


@dataclass(frozen=True)
class ImageAdvance:
    progress: AdvanceResult
    results: tuple[MethodResult, ...]


class ImageGateway:
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
        self.image_config = config
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
            max_tasks * MAX_FRAME_BYTES if result_bytes is None else result_bytes
        )
        self.model_id = config.model_id
        self.max_connections, self.frame_timeout = max_connections, frame_timeout_ms / 1000
        self._observer, self._observer_lock = observer, threading.Lock()
        self.progress = WakeSignal()
        self.connections = {}
        self.workers = ConnectionWorkers()
        self.waiting = deque()
        self._known_jobs = set()
        self.execution = (execution_factory or build_image_execution)(
            config,
            max_tasks=max_tasks,
            max_active_requests=max_active_requests,
            input_bytes=total_input,
            result_bytes=total_result,
            observer=self.observe,
            max_jobs=max_jobs,
            execute=execute,
        )
        self.engine = self.execution.engine
        self.connection_capacity = ConnectionCapacity(max_connections)
        if self.engine.jobs.maximum != max_jobs:
            self.execution.close()
            raise ValueError("execution factory Job capacity differs from gateway registration")
        # Reserved independently of task leases, including transient encode/decode copies.
        self.connection_buffer_bytes = 5 * MAX_FRAME_BYTES + max_tasks * 128
        self.method_limits = MethodLimits(262144, 1, config.plan.result_bytes, 1)
        self.row_reservation = row_reservation(self.method_limits)
        self.method_pool = MethodBudgetPool(MethodCapacity(max_tasks,
            max_tasks * self.row_reservation))
        self.method_drivers = {}

    def validate_opening_message(self, opened):
        mode, _ = image.validate_open(opened, self.image_config.plan)
        if mode != self.image_config.mode:
            raise ProtocolError("INVALID_OPEN")

    def standalone_spec(self):
        digest = self.image_config.plan.digest
        return SessionSpec("registered", "image", digest, "ai_embed", "pixels",
                           (TaskProfile("image", digest, "ai_embed"),))

    def _register(self, state):
        if state.job is not None:
            raise ValueError("connection already registered")
        job, session, budget = self.execution.open_job("pg-connection", self.standalone_spec())
        state.job, state.mailbox.session = job, session
        session.set_dispatch_enabled(False)
        self._known_jobs.add(job.job_id)
        self.observe({"event": "job_opened", "job_id": job.job_id,
            "engine_session_id": session.session_id, "job_budget": asdict(budget),
            "connection_buffer_bytes": self.connection_buffer_bytes})
        grant = self.method_pool.allocate(MethodCapacity(
            session.limits.held_tasks, session.limits.held_tasks * self.row_reservation))
        plan = self.image_config.plan

        def describe_work(row, ordinal, request):
            return build_image_work_descriptor(row_count=1, encoded_bytes=len(request.payload),
                model_revision=plan.model_revision, processor_revision=plan.processor_revision,
                dtype=plan.dtype, input_size=plan.input_size, embedding_dimension=plan.dimension)

        try:
            self.method_drivers[session.session_id] = MethodDriver(session,
                ImageEmbeddingMethod(plan), self.method_limits, grant, describe_work=describe_work)
        except BaseException:
            grant.close()
            raise
        self._observe_method("image_method_opened", session.session_id)

    def _observe_method(self, event, session_id):
        self.observe({"event": event, "engine_session_id": session_id,
            "method_capacity": {"runs": self.method_pool.capacity.runs, "bytes": self.method_pool.capacity.bytes},
            "method_allocated": self.method_pool.allocated, "method_used": self.method_pool.used})

    def session_command(self, state, operation, args):
        session = state.mailbox.session
        driver = self.method_drivers[session.session_id]
        if operation == "offer":
            pending = args[0]
            # Validate the wire identity before transferring this row's ownership.
            self.execution.prepare_task(pending.request, pending.sequence)
            accepted = driver.offer_row(RowIdentity(pending.sequence, "pg-image"), pending.request.encoded)
            self._observe_method("image_row_accepted" if accepted else "image_row_backpressured", session.session_id)
            return OfferResult(int(accepted), "ACCEPTED" if accepted else "BACKPRESSURE",
                               "" if accepted else "method capacity", self.progress.generation)
        if operation == "advance":
            session.set_dispatch_enabled(True)
            progress = driver.advance(1)
            return ImageAdvance(replace(progress, deliveries=(), generation=self.progress.generation), driver.results(1))
        if operation == "release":
            for row in args[0]:
                driver.release_result(row)
            self._observe_method("image_result_released", session.session_id)
            return None
        raise ValueError("unknown image owner command")

    def before_connection_release(self, state):
        session = state.mailbox.session
        if session is None:
            return
        driver = self.method_drivers.pop(session.session_id, None)
        if driver is None:
            return
        try:
            if (state.mailbox.clean and not state.mailbox.cancelled.is_set()
                    and driver.error is None and session.state in (State.OPEN, State.DRAINING)):
                driver.end_input()
                driver.advance(1)  # Seal after all row results have been released.
                driver.advance(1)  # Observe the now-empty sealed session.
        finally:
            driver.close()
            self._observe_method("image_method_closed", session.session_id)

    def observe(self, event):
        if self._observer:
            with self._observer_lock:
                self._observer(event)

    def _accept(self, connection, sequence):
        connection.settimeout(self.frame_timeout)
        mailbox = ConnectionMailbox(None, self.engine.wake, self.frame_timeout)
        state = ConnectionState(connection, mailbox, None, None, connection_id=sequence)
        self.connections[sequence] = state
        self.connection_capacity.accept(sequence)
        try:
            state.worker = self.workers.start(
                connection, lambda _: self._run_connection(state)
            )
        except BaseException:
            self.connection_capacity.release(sequence)
            del self.connections[sequence]
            connection.close()
            raise
        return True

    def _run_connection(self, state):
        mailbox, connection = state.mailbox, state.connection
        try:
            opened = read_frame(connection)
            if opened is None:
                return
            self.validate_opening_message(opened)
            mailbox.call("standalone", (), 0)
            session = mailbox.session
            adapter = ImageConnection(self, connection, mailbox, state.job, session.limits)
            try:
                mailbox.clean = bool(adapter.run_image(opened))
            except (ProtocolError, ValueError, CompletionAdapterError):
                connection.sendall(encode_frame({"type": "error", "protocol_version": 7,
                                                 "code": "IMAGE_EXECUTION_FAILED"}))
                raise
        except Exception:
            mailbox.clean = False
        finally:
            if state.end_reason is None:
                state.end_reason = (ConnectionEnd.STREAM_CLEAN_END if mailbox.clean
                                    else ConnectionEnd.PEER_FAILURE)
            if not mailbox.clean:
                mailbox.cancel()
            connection.close()
            mailbox.done.set()
            self.engine.wake.notify()

    def _command(self, state):
        mailbox, session = state.mailbox, state.mailbox.session
        with mailbox.condition:
            if mailbox.command is None or mailbox.cancelled.is_set():
                return
            operation, args = mailbox.command
            mailbox.command = None
            try:
                if operation == "standalone":
                    self.connection_capacity.admit_standalone(state.connection_id)
                    if len(self.engine.jobs.jobs) >= self.engine.jobs.maximum:
                        mailbox.command = (operation, args)
                        return
                    result = self._register(state)
                else:
                    result = self.session_command(state, operation, args)
            except Exception:
                if session is not None and session.state not in (State.FINISHED, State.FAILED, State.CANCELLED):
                    session.fail("owner command failed")
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

    def _end_connection(self, state, *, consumer_stopped=False):
        session = state.mailbox.session
        if consumer_stopped and session is not None:
            session.close_consumer(clean=state.end_reason == ConnectionEnd.STREAM_CLEAN_END)
        if state.job in self.engine.jobs.jobs:
            self.engine.close_job(state.job)

    def _controls(self):
        self.workers.reap()
        for connection_id, state in tuple(self.connections.items()):
            mailbox, session = state.mailbox, state.mailbox.session
            job = self.engine.jobs.jobs.get(state.job)
            if job is not None and job.closing and not state.closing:
                self._interrupt(state)
            if mailbox.cancelled.is_set() and not state.closing:
                state.closing = True
                self._end_connection(state)
                self._interrupt(state)
            if not mailbox.done.is_set():
                continue
            state.worker.join()
            self.before_connection_release(state)
            self._end_connection(state, consumer_stopped=True)
            self.connection_capacity.release(connection_id)
            with mailbox.condition:
                mailbox.command = None
                mailbox.reply = _EMPTY
            del self.connections[connection_id]
            if state.job is not None:
                self.observe(
                    {
                        "event": "connection_closed",
                        "job_id": state.job.job_id,
                        "engine_session_id": session.session_id if session else None,
                        "usage": asdict(
                            self.engine.capacity.usage(
                                session.session_id if session else None, job_id=state.job.job_id
                            )
                        ),
                    }
                )

    def serve(self, listener, stopping, *, session_limit=0):
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
                        state.end_reason = ConnectionEnd.SERVICE_STOP
                        self._interrupt(state)
                admissions_done = bool(session_limit and served >= session_limit)
                if (
                    not stopping.is_set()
                    and not admissions_done
                    and self.connection_capacity.may_accept()
                ):
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
                    elif self._accept(connection, number):
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
                if not progress.has_immediate_work:
                    timeout = self.engine.capacity.limits.poll_interval_s
                    if progress.next_deadline is not None:
                        timeout = min(timeout, max(0.0, progress.next_deadline - self.engine.clock()))
                    if deadline is not None:
                        timeout = min(timeout, max(0.0, deadline - time.monotonic()))
                    self.engine.wake.wait(generation, timeout)
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
            state.end_reason = ConnectionEnd.SERVICE_STOP
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
        return (closed and not self.method_drivers and self.method_pool.used == (0, 0)
                and self.method_pool.allocated == (0, 0)
                and self.engine.capacity.usage().held_tasks == 0)
