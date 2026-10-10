"""Observe Job-labelled HTTP requests while forwarding each request exactly once.

The gateway adds no admission limit, application queue, retry, cache, routing, or
payload rewrite.  It records arrival/completion clocks and endpoint-reported token
usage so otherwise framework-owned execution paths share one passive observation
contract.

The original epoch fields retain their meanings, including
``response_completed_epoch_s`` (response ready, before writing). The additional
monotonic fields start at handler entry after HTTP headers have been parsed and
end only when aiohttp Response.write_eof returns successfully. This is a proxy
HTTP interval; it excludes earlier SDK work, client receipt and client parsing.
write_eof completion means local transport writing/flow control completed, not a
peer acknowledgement. Missing stages remain None after failure.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote


_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}

_MONOTONIC_FIELDS = (
    "request_body_read_completed_monotonic_ns",
    "before_forward_started_monotonic_ns",
    "before_forward_completed_monotonic_ns",
    "upstream_dispatch_started_monotonic_ns",
    "upstream_connection_queued_started_monotonic_ns",
    "upstream_connection_queued_completed_monotonic_ns",
    "upstream_connection_create_started_monotonic_ns",
    "upstream_connection_create_completed_monotonic_ns",
    "upstream_connection_reused_monotonic_ns",
    "upstream_headers_send_started_monotonic_ns",
    "upstream_response_body_read_completed_monotonic_ns",
    "upstream_attempt_finished_monotonic_ns",
    "after_forward_started_monotonic_ns",
    "after_forward_completed_monotonic_ns",
    "response_ready_monotonic_ns",
    "response_write_started_monotonic_ns",
    "response_write_completed_monotonic_ns",
    "request_terminal_monotonic_ns",
)


def _record_failure(row: dict[str, Any], error: BaseException, phase: str) -> None:
    """Retain error identity without persisting exception text or credentials."""

    row["status"] = "failed"
    if not row["error_type"]:
        row["error_type"] = type(error).__name__
        row["error_phase"] = phase
    row["errors"].append({
        "phase": phase,
        "error_type": type(error).__name__,
        "observed_monotonic_ns": time.monotonic_ns(),
    })


@dataclass(frozen=True)
class GatewayRoute:
    """Bind one externally visible Job/endpoint path to one backend URL."""

    job_id: str
    endpoint_id: str
    upstream_url: str


class ObservationGateway:
    """Run one unbounded async pass-through gateway in a background thread."""

    def __init__(
        self,
        *,
        routes: tuple[GatewayRoute, ...],
        trace_path: Path,
        bind_host: str = "127.0.0.1",
        bind_port: int = 0,
        request_timeout_s: float = 600.0,
        before_forward: Callable[[GatewayRoute, bytes], None] | None = None,
        after_forward: Callable[[GatewayRoute, bytes, bytes, int], None] | None = None,
        query_identity: Callable[[], str | None] | None = None,
        fresh_upstream_connections: bool = False,
    ) -> None:
        if not routes:
            raise ValueError("observation gateway requires at least one route")
        keys = [(route.job_id, route.endpoint_id) for route in routes]
        if any(not job or not endpoint for job, endpoint in keys):
            raise ValueError("gateway Job and endpoint identities must be non-empty")
        if len(set(keys)) != len(keys):
            raise ValueError("observation gateway routes must be unique")
        if not math.isfinite(request_timeout_s) or request_timeout_s <= 0:
            raise ValueError("gateway request timeout must be finite and positive")
        if not isinstance(fresh_upstream_connections, bool):
            raise ValueError("fresh upstream connections must be a boolean diagnostic option")
        self._routes = {(route.job_id, route.endpoint_id): route for route in routes}
        self._trace_path = trace_path
        self._bind_host = bind_host
        self._bind_port = bind_port
        self._request_timeout_s = request_timeout_s
        self._before_forward = before_forward
        self._after_forward = after_forward
        self._query_identity = query_identity
        self._fresh_upstream_connections = fresh_upstream_connections
        self._upstream_pool_generation = 0
        self._reset_upstream: Callable[[], Any] | None = None
        self._trace_rows: list[dict[str, object]] = []
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None
        self._port: int | None = None

    def __enter__(self) -> "ObservationGateway":
        self.start()
        return self

    def __exit__(self, *_error: object) -> None:
        self.stop()

    def start(self) -> None:
        """Bind the gateway before a measured Job release occurs."""

        if self._thread is not None:
            raise RuntimeError("observation gateway is already started")
        if self._trace_path.exists():
            raise FileExistsError(
                f"observation gateway trace already exists: {self._trace_path}"
            )
        self._trace_path.parent.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Thread(
            target=self._run,
            name="observation-gateway",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=15.0):
            raise RuntimeError("observation gateway did not become ready")
        if self._startup_error is not None:
            raise RuntimeError("observation gateway startup failed") from self._startup_error

    def stop(self) -> None:
        """Stop the gateway after every client request has completed."""

        if self._thread is None:
            return
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=15.0)
        if self._thread.is_alive():
            raise RuntimeError("observation gateway did not stop cleanly")
        self._thread = None
        self._loop = None
        self._reset_upstream = None

    def endpoint_url(self, job_id: str, endpoint_id: str) -> str:
        """Return the Job-labelled Chat Completions URL for one route."""

        if (job_id, endpoint_id) not in self._routes:
            raise KeyError(f"unknown observation route: {job_id}/{endpoint_id}")
        if self._port is None:
            raise RuntimeError("observation gateway is not running")
        return (
            f"http://{self._bind_host}:{self._port}/observe/"
            f"{quote(job_id, safe='')}/{quote(endpoint_id, safe='')}"
            "/v1/chat/completions"
        )

    def urls_for_job(
        self, job_id: str, endpoint_ids: tuple[str, ...]
    ) -> tuple[str, ...]:
        """Return route URLs in the caller's frozen endpoint order."""

        return tuple(self.endpoint_url(job_id, endpoint) for endpoint in endpoint_ids)

    def snapshot(self, timeout_s: float = 5.0) -> list[dict[str, Any]]:
        """Copy settled observations on the gateway loop without stopping its client."""
        if self._loop is None or not math.isfinite(timeout_s) or timeout_s <= 0:
            raise ValueError("a running gateway and positive snapshot timeout are required")

        async def copy_rows():
            deadline = time.monotonic() + timeout_s
            while any(row["request_terminal_monotonic_ns"] is None for row in self._trace_rows):
                if time.monotonic() >= deadline:
                    raise TimeoutError("gateway requests remain unsettled")
                await asyncio.sleep(.001)
            return json.loads(json.dumps(self._trace_rows))

        return asyncio.run_coroutine_threadsafe(copy_rows(), self._loop).result(timeout_s + 1)

    def reset_upstream_connections(self, timeout_s: float = 5.0) -> None:
        """Replace an idle pool between queries; reject any unsettled request."""

        if (self._loop is None or self._reset_upstream is None
                or not math.isfinite(timeout_s) or timeout_s <= 0):
            raise ValueError("a running gateway and positive reset timeout are required")
        asyncio.run_coroutine_threadsafe(self._reset_upstream(), self._loop).result(timeout_s)

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        runner: Any = None
        trace_stream = None
        trace_rows = self._trace_rows
        try:
            from aiohttp import ClientSession, ClientTimeout, TCPConnector, TraceConfig, web

            trace_stream = self._trace_path.open("x", encoding="utf-8")
            session: Any = None
            sequence = iter(range(2**63))

            class ObservedResponse(web.Response):
                """Observe the framework's existing writer without replacing it."""

                def __init__(self, row: dict[str, Any], **kwargs: Any) -> None:
                    super().__init__(**kwargs)
                    self._observation_row = row

                async def prepare(self, request: Any) -> Any:
                    try:
                        return await super().prepare(request)
                    except BaseException as error:
                        row = self._observation_row
                        _record_failure(row, error, "response_prepare")
                        row["response_write_status"] = "failed"
                        row["request_terminal_monotonic_ns"] = time.monotonic_ns()
                        raise

                async def write_eof(self, data: bytes = b"") -> None:
                    row = self._observation_row
                    if row["response_write_completed_monotonic_ns"] is not None:
                        # aiohttp may call this again after an already sent response.
                        await super().write_eof(data)
                        return
                    row["response_write_started_monotonic_ns"] = time.monotonic_ns()
                    try:
                        await super().write_eof(data)
                    except BaseException as error:
                        _record_failure(row, error, "response_write")
                        row["response_write_status"] = "failed"
                        row["request_terminal_monotonic_ns"] = time.monotonic_ns()
                        raise
                    completed = time.monotonic_ns()
                    row["response_write_completed_monotonic_ns"] = completed
                    row["request_terminal_monotonic_ns"] = completed
                    row["response_write_status"] = "completed"

            async def headers_send_started(_session: Any, context: Any, _params: Any) -> None:
                row = context.trace_request_ctx
                # aiohttp's documented headers-sent signal runs at entry to its
                # header writer. It is not evidence of remote receipt.
                row["upstream_headers_send_count"] += 1
                if row["upstream_headers_send_started_monotonic_ns"] is None:
                    row["upstream_headers_send_started_monotonic_ns"] = time.monotonic_ns()

            def connection_signal(name: str) -> Any:
                async def record(_session: Any, context: Any, _params: Any) -> None:
                    row = context.trace_request_ctx
                    row[name + "_count"] += 1
                    if row[name + "_monotonic_ns"] is None:
                        row[name + "_monotonic_ns"] = time.monotonic_ns()
                return record

            def new_session() -> Any:
                connector = TCPConnector(limit=0, limit_per_host=0,
                    force_close=self._fresh_upstream_connections)
                trace_config = TraceConfig()
                trace_config.on_request_headers_sent.append(headers_send_started)
                for signal, name in (
                    (trace_config.on_connection_queued_start, "upstream_connection_queued_started"),
                    (trace_config.on_connection_queued_end, "upstream_connection_queued_completed"),
                    (trace_config.on_connection_create_start, "upstream_connection_create_started"),
                    (trace_config.on_connection_create_end, "upstream_connection_create_completed"),
                    (trace_config.on_connection_reuseconn, "upstream_connection_reused"),
                ):
                    signal.append(connection_signal(name))
                return ClientSession(connector=connector,
                    timeout=ClientTimeout(total=self._request_timeout_s),
                    trace_configs=[trace_config])

            async def reset_upstream() -> None:
                nonlocal session
                if any(row["request_terminal_monotonic_ns"] is None for row in trace_rows):
                    raise RuntimeError("cannot replace upstream connections during a request")
                previous = session
                # Swap before yielding so a newly arriving request uses the new pool.
                session = new_session()
                self._upstream_pool_generation += 1
                await previous.close()

            async def observe(request: Any) -> Any:
                request_id = next(sequence)
                received_monotonic_ns = time.monotonic_ns()
                received_epoch_s = time.time()
                job_id = str(request.match_info["job_id"])
                endpoint_id = str(request.match_info["endpoint_id"])
                route = self._routes.get((job_id, endpoint_id))
                if route is None:
                    return web.json_response({"error": "unknown route"}, status=404)
                row: dict[str, Any] = {
                    "schema_version": 1,
                    "timing_schema_version": 1,
                    "timing_clock": "time.monotonic_ns",
                    "timing_scope": "proxy_handler_entry_to_response_write_eof_return",
                    "gateway_request_id": request_id,
                    "job_id": job_id,
                    "endpoint_id": endpoint_id,
                    "received_epoch_s": received_epoch_s,
                    "received_monotonic_ns": received_monotonic_ns,
                    "upstream_start_epoch_s": None,
                    "upstream_response_epoch_s": None,
                    "response_completed_epoch_s": None,
                    "dispatch_delay_s": None,
                    "upstream_status": 0,
                    "client_status": 0,
                    "callback_error_type": "",
                    "upstream_response_status": None,
                    "retry_count": 0,
                    "upstream_connection_policy": (
                        "fresh_per_request" if self._fresh_upstream_connections else "pooled"),
                    "upstream_pool_generation": self._upstream_pool_generation,
                    "request_body_sha256": None,
                    "forwarded_body_sha256": None,
                    "forwarded": False,
                    "status": "incomplete",
                    "error_type": "",
                    "error_phase": "",
                    "errors": [],
                    "response_write_status": "not_started",
                    "upstream_headers_send_count": 0,
                    "upstream_connection_queued_started_count": 0,
                    "upstream_connection_queued_completed_count": 0,
                    "upstream_connection_create_started_count": 0,
                    "upstream_connection_create_completed_count": 0,
                    "upstream_connection_reused_count": 0,
                    **dict.fromkeys(_MONOTONIC_FIELDS),
                    **_response_usage(b""),
                }
                if self._query_identity is not None:
                    row["query_id"] = self._query_identity()
                # The evidence buffer also retains failures during body reading.
                # No synchronous disk I/O is added to request handling.
                trace_rows.append(row)
                try:
                    body = await request.read()
                    row["request_body_read_completed_monotonic_ns"] = time.monotonic_ns()
                except BaseException as error:
                    _record_failure(row, error, "request_body_read")
                    row["request_terminal_monotonic_ns"] = time.monotonic_ns()
                    raise
                body_sha256 = hashlib.sha256(body).hexdigest()
                row["request_body_sha256"] = body_sha256
                headers = {
                    name: value
                    for name, value in request.headers.items()
                    if name.lower() not in _HOP_BY_HOP_HEADERS
                }
                upstream_start_epoch_s = time.time()
                row["upstream_start_epoch_s"] = upstream_start_epoch_s
                row["dispatch_delay_s"] = max(0.0, upstream_start_epoch_s - received_epoch_s)
                upstream_status = 0
                response_body = b""
                response_headers: dict[str, str] = {}
                phase = "before_forward"
                try:
                    row["before_forward_started_monotonic_ns"] = time.monotonic_ns()
                    if self._before_forward is not None:
                        self._before_forward(route, body)
                    row["before_forward_completed_monotonic_ns"] = time.monotonic_ns()
                    row["forwarded"] = True
                    row["forwarded_body_sha256"] = body_sha256
                    phase = "upstream_request"
                    row["upstream_dispatch_started_monotonic_ns"] = time.monotonic_ns()
                    async with session.post(
                        route.upstream_url,
                        data=body,
                        headers=headers,
                        trace_request_ctx=row,
                    ) as response:
                        upstream_status = int(response.status)
                        row["upstream_response_status"] = upstream_status
                        phase = "upstream_response_read"
                        response_body = await response.read()
                        row["upstream_response_body_read_completed_monotonic_ns"] = time.monotonic_ns()
                        response_headers = {
                            name: value
                            for name, value in response.headers.items()
                            if name.lower() not in _HOP_BY_HOP_HEADERS
                        }
                        phase = "upstream_response_release"
                except asyncio.CancelledError as error:
                    _record_failure(row, error, phase)
                    row["request_terminal_monotonic_ns"] = time.monotonic_ns()
                    raise
                except Exception as error:  # third-party transport boundary
                    _record_failure(row, error, phase)
                    upstream_status = 502
                    response_body = json.dumps(
                        {"error": "observation gateway upstream request failed"}
                    ).encode("utf-8")
                    response_headers = {"Content-Type": "application/json"}
                finally:
                    if row["upstream_dispatch_started_monotonic_ns"] is not None:
                        row["upstream_attempt_finished_monotonic_ns"] = time.monotonic_ns()
                        if row["request_terminal_monotonic_ns"] is not None:
                            row["request_terminal_monotonic_ns"] = time.monotonic_ns()
                upstream_response_epoch_s = time.time()
                row["upstream_response_epoch_s"] = upstream_response_epoch_s
                client_status = upstream_status
                client_body = response_body
                client_headers = response_headers
                try:
                    row["after_forward_started_monotonic_ns"] = time.monotonic_ns()
                    if self._after_forward is not None:
                        self._after_forward(route, body, response_body, upstream_status)
                    row["after_forward_completed_monotonic_ns"] = time.monotonic_ns()
                except asyncio.CancelledError as error:
                    _record_failure(row, error, "after_forward")
                    row["request_terminal_monotonic_ns"] = time.monotonic_ns()
                    raise
                except Exception as error:
                    row["callback_error_type"] = type(error).__name__
                    _record_failure(row, error, "after_forward")
                    client_status = 502
                    client_body = b'{"error":"observation callback failed"}'
                    client_headers = {"Content-Type": "application/json"}
                usage = _response_usage(response_body)
                row.update({
                    "response_ready_monotonic_ns": time.monotonic_ns(),
                    "response_completed_epoch_s": time.time(),
                    "upstream_status": upstream_status,
                    "client_status": client_status,
                    **usage,
                    "status": (
                        "completed"
                        if 200 <= upstream_status < 300 and not row["error_type"]
                        else "failed"
                    ),
                })
                return ObservedResponse(
                    row,
                    status=client_status,
                    body=client_body,
                    headers=client_headers,
                )

            async def health(_request: Any) -> Any:
                return web.json_response(
                    {
                        "status": "ok",
                        "policy": "pass_through_no_queue_no_retry",
                        "upstream_connection_policy": (
                            "fresh_per_request" if self._fresh_upstream_connections else "pooled"),
                        "upstream_pool_generation": self._upstream_pool_generation,
                        "route_count": len(self._routes),
                    }
                )

            async def initialize() -> Any:
                nonlocal session
                session = new_session()
                self._reset_upstream = reset_upstream
                app = web.Application(client_max_size=64 * 1024 * 1024)

                async def close_session(_app: Any) -> None:
                    await session.close()

                app.on_cleanup.append(close_session)
                app.router.add_get("/health", health)
                app.router.add_post(
                    "/observe/{job_id}/{endpoint_id}/v1/chat/completions",
                    observe,
                )
                app_runner = web.AppRunner(app, access_log=None)
                await app_runner.setup()
                site = web.TCPSite(
                    app_runner, self._bind_host, self._bind_port
                )
                await site.start()
                sockets = site._server.sockets  # aiohttp has no public bound-port API
                self._port = int(sockets[0].getsockname()[1])
                return app_runner

            runner = loop.run_until_complete(initialize())
            self._ready.set()
            loop.run_forever()
        except BaseException as error:
            self._startup_error = error
            self._ready.set()
        finally:
            self._reset_upstream = None
            if runner is not None:
                loop.run_until_complete(runner.cleanup())
            if trace_stream is not None:
                for row in sorted(
                    trace_rows,
                    key=lambda item: int(item["gateway_request_id"]),
                ):
                    trace_stream.write(
                        json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                    )
                trace_stream.flush()
                trace_stream.close()
            loop.close()


def _response_usage(body: bytes) -> dict[str, int | None]:
    """Read endpoint usage without altering or reserializing the response."""

    try:
        decoded = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        decoded = {}
    usage = decoded.get("usage", {}) if isinstance(decoded, dict) else {}
    if not isinstance(usage, dict):
        usage = {}

    def integer(name: str) -> int | None:
        value = usage.get(name)
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else None

    return {
        "actual_prompt_tokens": integer("prompt_tokens"),
        "actual_output_tokens": integer("completion_tokens"),
        "actual_total_tokens": integer("total_tokens"),
    }
