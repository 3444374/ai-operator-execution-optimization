"""Observe Job-labelled HTTP requests while forwarding each request exactly once.

The gateway adds no admission limit, application queue, retry, cache, routing, or
payload rewrite.  It records arrival/completion clocks and endpoint-reported token
usage so otherwise framework-owned execution paths share one passive observation
contract.
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
from typing import Any
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
        self._routes = {(route.job_id, route.endpoint_id): route for route in routes}
        self._trace_path = trace_path
        self._bind_host = bind_host
        self._bind_port = bind_port
        self._request_timeout_s = request_timeout_s
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

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        runner: Any = None
        trace_stream = None
        trace_rows: list[dict[str, object]] = []
        try:
            from aiohttp import ClientSession, ClientTimeout, TCPConnector, web

            trace_stream = self._trace_path.open("x", encoding="utf-8")
            session: Any = None
            sequence = iter(range(2**63))

            async def observe(request: Any) -> Any:
                request_id = next(sequence)
                received_epoch_s = time.time()
                job_id = str(request.match_info["job_id"])
                endpoint_id = str(request.match_info["endpoint_id"])
                route = self._routes.get((job_id, endpoint_id))
                if route is None:
                    return web.json_response({"error": "unknown route"}, status=404)
                body = await request.read()
                body_sha256 = hashlib.sha256(body).hexdigest()
                headers = {
                    name: value
                    for name, value in request.headers.items()
                    if name.lower() not in _HOP_BY_HOP_HEADERS
                }
                upstream_start_epoch_s = time.time()
                upstream_status = 0
                response_body = b""
                response_headers: dict[str, str] = {}
                error_type = ""
                try:
                    async with session.post(
                        route.upstream_url,
                        data=body,
                        headers=headers,
                    ) as response:
                        upstream_status = int(response.status)
                        response_body = await response.read()
                        response_headers = {
                            name: value
                            for name, value in response.headers.items()
                            if name.lower() not in _HOP_BY_HOP_HEADERS
                        }
                except Exception as error:  # third-party transport boundary
                    error_type = type(error).__name__
                    upstream_status = 502
                    response_body = json.dumps(
                        {"error": "observation gateway upstream request failed"}
                    ).encode("utf-8")
                    response_headers = {"Content-Type": "application/json"}
                upstream_response_epoch_s = time.time()
                usage = _response_usage(response_body)
                row = {
                    "schema_version": 1,
                    "gateway_request_id": request_id,
                    "job_id": job_id,
                    "endpoint_id": endpoint_id,
                    "received_epoch_s": received_epoch_s,
                    "upstream_start_epoch_s": upstream_start_epoch_s,
                    "upstream_response_epoch_s": upstream_response_epoch_s,
                    "response_completed_epoch_s": time.time(),
                    "dispatch_delay_s": max(
                        0.0, upstream_start_epoch_s - received_epoch_s
                    ),
                    "upstream_status": upstream_status,
                    "retry_count": 0,
                    "request_body_sha256": body_sha256,
                    "forwarded_body_sha256": hashlib.sha256(body).hexdigest(),
                    **usage,
                    "status": (
                        "completed"
                        if 200 <= upstream_status < 300 and not error_type
                        else "failed"
                    ),
                    "error_type": error_type,
                }
                # Keep the observation hot path free of synchronous disk I/O.
                # This is an evidence buffer, not a request/admission queue.
                trace_rows.append(row)
                return web.Response(
                    status=upstream_status,
                    body=response_body,
                    headers=response_headers,
                )

            async def health(_request: Any) -> Any:
                return web.json_response(
                    {
                        "status": "ok",
                        "policy": "pass_through_no_queue_no_retry",
                        "route_count": len(self._routes),
                    }
                )

            async def initialize() -> Any:
                nonlocal session
                connector = TCPConnector(limit=0, limit_per_host=0)
                session = ClientSession(
                    connector=connector,
                    timeout=ClientTimeout(total=self._request_timeout_s),
                )
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
