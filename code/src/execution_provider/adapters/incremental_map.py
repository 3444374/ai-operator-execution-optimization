"""Window-one PG Map bridge: one service engine, asynchronous HTTP, existing v5 semantics."""

from __future__ import annotations

import asyncio
import json
import select
import socket
import time
from dataclasses import asdict
from typing import Callable

from ...planning.work import StageWork, WorkDescriptor
from ...scheduling.core.models import EndpointSnapshot, TopologySnapshot
from ...scheduling.core.session import SessionEngine
from ...scheduling.core.session_contract import (
    OfferedTask,
    SessionLimits,
    SessionSpec,
    State,
    TaskInfo,
)
from ...scheduling.core.session_policy import SessionPolicies
from ...scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from ...scheduling.organization.session_window import WorkWindowOrganizer
from ...scheduling.runtime.async_backend import BoundedAsyncBackend
from ...scheduling.submission_control.admission import StaticAdmissionController
from ..completion import Completion
from ..wire import v5
from ..wire.framing import MAX_FRAME_BYTES
from .completion_response import parse_completion
from .openai_compatible_fixed import FixedModelConfig, MAX_MODEL_RESPONSE_BYTES
from .semantic_session import CompletionAdapterError, CompletionRequest


class IncrementalMapAdapter:
    """The socket owner drives the core; its I/O worker never touches PostgreSQL state.

    This adapter deliberately supports one active connection. It drains cancelled work
    before accepting another connection; unknown work quarantines this engine.
    """

    def __init__(self, config: FixedModelConfig, *, execute=None, observer=None, max_tasks=1):
        if type(max_tasks) is not int or not 1 <= max_tasks <= 64:
            raise ValueError("invalid incremental task capacity")
        self.max_tasks = max_tasks
        self.config = config
        self.model_id = config.model_id
        self._client = None
        self._connection = None
        self._session = None
        self._sequence = 0
        self._connections = 0
        self._transport_error = None
        self._observer = observer
        self._stopping: Callable[[], bool] = lambda: False
        self._backend = BoundedAsyncBackend(
            execute or self._execute,
            max_tasks=max_tasks,
            notify=lambda: self.engine.wake.notify(),
            finalize=self._finalize,
        )
        timeout = config.timeout_ms / 1000 + 1
        limits = SessionLimits(
            max_tasks,
            max_tasks * MAX_FRAME_BYTES,
            max_tasks * MAX_MODEL_RESPONSE_BYTES,
            max_tasks,
            max_tasks,
            max_tasks,
            MAX_FRAME_BYTES,
            MAX_MODEL_RESPONSE_BYTES,
            1024,
            8,
            timeout,
            0.01,
        )
        topology = TopologySnapshot(
            (
                EndpointSnapshot(
                    "model",
                    config.endpoint_url,
                    "default",
                    "0",
                    True,
                    0,
                    0,
                    0.0,
                    1.0,
                ),
            ),
            time.monotonic(),
        )
        self.engine = SessionEngine(
            limits,
            self._backend,
            SessionPolicies(
                StaticAdmissionController(max_tasks),
                RoundRobinEndpointRouter(),
                topology,
                "default",
                organize=WorkWindowOrganizer(max_tasks, max_tasks),
            ),
            sink=self._observe,
        )

    def execution_id_for(self, version: int) -> str | None:
        return v5.INCREMENTAL_EXECUTION_ID if version == 5 else None

    def _observe(self, event, key):
        if self._observer:
            self._observer(
                {"event": event, "key": asdict(key), "usage": asdict(self.engine.capacity.usage())}
            )

    async def _execute(self, request, endpoint):
        import httpx

        if endpoint != "model":
            raise ValueError("unknown endpoint")
        if self._client is None:
            headers = {"Content-Type": "application/json", "Accept-Encoding": "identity"}
            if self.config.bearer_token:
                headers["Authorization"] = f"Bearer {self.config.bearer_token}"
            self._client = httpx.AsyncClient(
                headers=headers,
                trust_env=False,
                timeout=None,
                follow_redirects=False,
                limits=httpx.Limits(
                    max_connections=self.max_tasks, max_keepalive_connections=self.max_tasks
                ),
            )
        if self._observer:
            self._observer({"event": "http_started", "key": asdict(request.key)})
        try:
            # Total deadline includes DNS, connection setup and bounded response reads.
            async with asyncio.timeout(self.config.timeout_ms / 1000):
                async with self._client.stream(
                    "POST", self.config.endpoint_url, content=request.task.payload
                ) as response:
                    buffer = bytearray()
                    async for chunk in response.aiter_raw(chunk_size=4096):
                        if len(buffer) + len(chunk) > request.task.max_result_bytes:
                            raise ValueError("response exceeds bound")
                        buffer.extend(chunk)
                    if not 200 <= response.status_code < 300:
                        code = (
                            "MODEL_REQUEST_REJECTED"
                            if 400 <= response.status_code < 500
                            else "MODEL_UNAVAILABLE"
                            if response.status_code >= 500
                            else "MODEL_RESPONSE_INVALID"
                        )
                        return json.dumps({"bridge_error": code}).encode()
                    return bytes(buffer)
        except TimeoutError:
            self._transport_error = "MODEL_TIMEOUT"
            raise
        except Exception:
            # Disconnecting HTTP does not prove that the model has stopped computing.
            self._transport_error = "MODEL_UNAVAILABLE"
            raise
        finally:
            if self._observer:
                self._observer({"event": "http_finished", "key": asdict(request.key)})

    async def _finalize(self):
        if self._client is not None:
            await self._client.aclose()

    def serve_connection(self, connection, handler, stopping=lambda: False):
        if self._connection is not None or self.engine.error:
            raise RuntimeError("incremental engine unavailable")
        self._connections += 1
        self._connection, self._stopping = connection, stopping
        self._sequence = 0
        self._session = self.engine.open(
            SessionSpec(
                f"pg-{self._connections}",
                "map",
                "fixed-chat",
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
            deadline = time.monotonic() + self.engine.capacity.limits.wait_timeout_s
            # PG may already have returned a cancellation error. Keep the shared credit
            # until the old remote response arrives, and never deliver it to a new query.
            while self.engine.capacity.usage().held_tasks:
                if self.engine.error or time.monotonic() >= deadline:
                    raise RuntimeError("remote outcome unconfirmed; engine quarantined")
                self.engine.reap(1)
                self.engine.wake.wait(self.engine.wake.generation, 0.01)

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
        # A window-one peer cannot send the next task before receiving this result.
        # Readability here is EOF or an invalid pipelined frame; both stop dispatch.
        return True

    def complete(self, request: CompletionRequest) -> Completion:
        if (
            self._session is None
            or request.protocol_version != 5
            or request.model_id != self.model_id
        ):
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        if request.generation_profile is not None:
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        task = self.prepare_task(request, self._sequence)
        if self._peer_stopped():
            raise ConnectionResetError("provider peer stopped")
        if self._session.offer((task,)).accepted_prefix_count != 1:
            raise CompletionAdapterError("MODEL_REQUEST_REJECTED")
        self._sequence += 1
        while True:
            if self._peer_stopped():
                self._session.cancel()
                raise ConnectionResetError("provider peer stopped")
            result = self._session.advance(1)
            if result.state == State.FAILED:
                raise CompletionAdapterError(
                    self._transport_error or "MODEL_UNAVAILABLE",
                    remote_outcome_unknown=bool(self.engine.capacity.usage().active_requests),
                )
            if result.deliveries:
                (delivery,) = result.deliveries
                try:
                    value = json.loads(delivery.result)
                    if isinstance(value, dict) and "bridge_error" in value:
                        raise CompletionAdapterError(value["bridge_error"])
                    return parse_completion(value)
                except (ValueError, UnicodeError, TypeError):
                    raise CompletionAdapterError("MODEL_RESPONSE_INVALID") from None
                finally:
                    self._session.release((delivery.lease_id,))
            if not result.has_immediate_work:
                self.engine.wake.wait(result.generation, 0.01)

    def prepare_task(self, request: CompletionRequest, sequence: int) -> OfferedTask:
        payload = json.dumps(
            {
                "model": request.model_id,
                "messages": list(request.canonical_messages),
                **request.generation_constraints,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        info = TaskInfo(
            "pg-map",
            sequence,
            "generate",
            WorkDescriptor(
                (StageWork("model", 1, "work_units"),),
                "model",
                "request-count",
            ),
        )
        return OfferedTask(sequence, payload, 1, MAX_MODEL_RESPONSE_BYTES, info=info)

    def request_stop(self):
        if self._connection is not None:
            try:
                self._connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        closed = self._backend.close()
        if self._observer:
            self._observer(
                {
                    "event": "transport_closed",
                    "closed": closed,
                    "usage": asdict(self.engine.capacity.usage()),
                }
            )
        return closed
