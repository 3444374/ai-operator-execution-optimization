"""Bounded fixed-model HTTP transport; it owns no queueing or scheduling policy."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from http.cookiejar import CookieJar
import json
from dataclasses import asdict
from pathlib import Path


class _RequestCookieJar(CookieJar):
    """Use CookieJar's normal redirect rules within one execute task only."""

    def __init__(self):
        super().__init__()
        self._active = ContextVar('fixed_model_request_cookies', default=None)

    @contextmanager
    def request_scope(self):
        token = self._active.set(CookieJar())
        try:
            yield
        finally:
            self._active.reset(token)

    def _current(self):
        jar = self._active.get()
        # Client inspection/preparation outside an execution sees no stored cookies.
        return CookieJar() if jar is None else jar

    def __iter__(self):
        return iter(self._current())

    def extract_cookies(self, response, request):
        return self._current().extract_cookies(response, request)

    def add_cookie_header(self, request):
        return self._current().add_cookie_header(request)

    def set_cookie(self, cookie):
        return self._current().set_cookie(cookie)

    def clear(self, *args):
        return self._current().clear(*args)

    def clear_session_cookies(self):
        return self._current().clear_session_cookies()


def exception_details(error):
    """Bounded diagnostic identity; never include messages, URLs or payloads."""
    kind = lambda value: (type(value).__module__ + '.' + type(value).__name__)[:160]
    causes, seen, cause = [], {id(error)}, error.__cause__ or error.__context__
    while cause is not None and id(cause) not in seen and len(causes) < 4:
        causes.append(kind(cause))
        seen.add(id(cause))
        cause = cause.__cause__ or cause.__context__
    reason = {'exception_type': kind(error), 'cause_types': causes}
    trace = error.__traceback__
    if trace is not None:
        while trace.tb_next is not None:
            trace = trace.tb_next
        reason['origin'] = {'file': Path(trace.tb_frame.f_code.co_filename).name[:128],
                            'function': trace.tb_frame.f_code.co_name[:128],
                            'line': trace.tb_lineno}
    return reason


class AsyncFixedModelTransport:
    def __init__(self, config, max_active_requests, observer=None):
        self.config = config
        self.max_active_requests = max_active_requests
        self._observer = observer
        self._client = None
        self._request_cookies = _RequestCookieJar()

    async def execute(self, request, endpoint):
        try:
            with self._request_cookies.request_scope():
                return await self._execute(request, endpoint)
        except BaseException as error:
            if self._observer:
                try:
                    self._observer({'event': 'http_error', 'key': asdict(request.key),
                                    'reason': exception_details(error)})
                except Exception:
                    pass  # A failed diagnostic must not replace the execution error.
            raise

    def prepare(self):
        """Create the reusable client without connecting or sending a request."""
        import httpx

        if self._client is None:
            headers = {"Content-Type": "application/json", "Accept-Encoding": "identity"}
            if self.config.bearer_token:
                headers["Authorization"] = f"Bearer {self.config.bearer_token}"
            self._client = httpx.AsyncClient(
                headers=headers,
                cookies=self._request_cookies,
                trust_env=False,
                timeout=None,
                follow_redirects=False,
                limits=httpx.Limits(
                    max_connections=self.max_active_requests,
                    max_keepalive_connections=self.max_active_requests,
                ),
            )
        # Also covers an explicitly supplied client (for example a MockTransport)
        # without replacing its transport, connection pool or redirect settings.
        if self._client.cookies.jar is not self._request_cookies:
            self._client.cookies = self._request_cookies

    async def _execute(self, request, endpoint):
        if endpoint != "model":
            raise ValueError("unknown endpoint")
        self.prepare()
        if self._observer:
            self._observer({"event": "http_started", "key": asdict(request.key)})
        try:
            # Total deadline includes DNS, connection setup and bounded response reads.
            async with asyncio.timeout(self.config.timeout_ms / 1000):
                async with self._client.stream(
                    "POST", self.config.endpoint_url, content=request.task.payload
                ) as response:
                    buffer = bytearray(self.response_prefix(response))
                    if len(buffer) > request.task.max_result_bytes:
                        raise ValueError("response exceeds bound")
                    async for chunk in response.aiter_raw(chunk_size=4096):
                        if len(buffer) + len(chunk) > request.task.max_result_bytes:
                            raise ValueError("response exceeds bound")
                        buffer.extend(chunk)
                    return self.response_result(response, bytes(buffer))
        finally:
            if self._observer:
                self._observer({"event": "http_finished", "key": asdict(request.key)})

    def response_prefix(self, response):
        return b""

    def response_result(self, response, buffer):
        if not 200 <= response.status_code < 300:
            code = (
                "MODEL_REQUEST_REJECTED"
                if 400 <= response.status_code < 500
                else "MODEL_UNAVAILABLE"
                if response.status_code >= 500
                else "MODEL_RESPONSE_INVALID"
            )
            return json.dumps({"bridge_error": code}).encode()
        return buffer

    async def close(self):
        if self._client is not None:
            await self._client.aclose()
