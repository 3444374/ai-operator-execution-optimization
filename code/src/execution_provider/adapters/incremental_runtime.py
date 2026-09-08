"""Connection and resource lifecycle for incremental Map execution."""

from __future__ import annotations

import select
import socket
import time
from dataclasses import asdict
from typing import Callable

from ...scheduling.core.session_contract import OfferedTask, SessionSpec
from ..completion import CompletionRequest
from .model_config import FixedModelConfig
from .incremental_execution import build_fixed_model_execution


class IncrementalMapRuntime:
    """The socket owner drives the core; its I/O worker never touches PostgreSQL state.

    This adapter deliberately supports one active connection. It drains cancelled work
    before accepting another connection; unknown work quarantines this engine.
    """

    def __init__(
        self,
        config: FixedModelConfig,
        *,
        execute=None,
        observer=None,
        max_tasks=1,
        max_active_requests=None,
        input_bytes=None,
        result_bytes=None,
        execution_factory=None,
    ):
        self.config = config
        self.model_id = config.model_id
        self._connection = None
        self._session = None
        self._connections = 0
        self._observer = observer
        self._stopping: Callable[[], bool] = lambda: False
        self.execution = (execution_factory or build_fixed_model_execution)(
            config,
            execute=execute,
            observer=observer,
            max_tasks=max_tasks,
            max_active_requests=max_active_requests,
            input_bytes=input_bytes,
            result_bytes=result_bytes,
        )
        self.engine = self.execution.engine
        self.max_tasks = self.engine.capacity.limits.held_tasks

    @property
    def _transport_error(self):
        return self.execution.error_code()

    def serve_connection(self, connection, handler, stopping=lambda: False):
        if self._connection is not None or self.engine.error:
            raise RuntimeError("incremental engine unavailable")
        self._connections += 1
        self._connection, self._stopping = connection, stopping
        self._session = self.engine.open(
            SessionSpec(
                f"pg-{self._connections}",
                "map",
                "fixed-chat",
                work_unit=self.execution.work_unit,
            )
        )
        try:
            handler(connection)
        finally:
            engine_session_id = self._session.session_id
            if not self.engine.capacity.usage().held_tasks and not self.engine.error:
                self._session.seal()
                self._session.advance(1)
            else:
                self._session.cancel()
            self._session.close()
            self._session = None
            self._connection = None
            deadline = time.monotonic() + self.execution.drain_timeout_s
            # PG may already have returned a cancellation error. Keep the shared credit
            # until the old remote response arrives, and never deliver it to a new query.
            while self.engine.capacity.usage().held_tasks:
                if self.engine.error or time.monotonic() >= deadline:
                    raise RuntimeError("remote outcome unconfirmed; engine quarantined")
                self.engine.reap(1)
                self.engine.wake.wait(
                    self.engine.wake.generation, self.engine.capacity.limits.poll_interval_s
                )

            if self._observer:
                self._observer(
                    {
                        "event": "drained",
                        "engine_session_id": engine_session_id,
                        "usage": asdict(self.engine.capacity.usage()),
                    }
                )

    def _peer_stopped(self):
        if self._stopping():
            return True
        readable, _, _ = select.select([self._connection], [], [], 0)
        if not readable:
            return False
        # Both protocols await one RPC reply at a time, even with concurrent model work.
        # Readability during that wait is EOF or invalid RPC pipelining.
        return True

    def wait_for_progress(self, result):
        self.engine.wake.wait(result.generation, self.engine.capacity.limits.poll_interval_s)

    def prepare_task(self, request: CompletionRequest, sequence: int) -> OfferedTask:
        return self.execution.prepare_task(request, sequence)

    def request_stop(self):
        if self._connection is not None:
            try:
                self._connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        closed = self.execution.close()
        if self._observer:
            self._observer(
                {
                    "event": "transport_closed",
                    "closed": closed,
                    "usage": asdict(self.engine.capacity.usage()),
                }
            )
        return closed
