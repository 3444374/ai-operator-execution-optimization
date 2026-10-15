"""Complete HTTP calls on pinned Daft and Ray Data graphs.

Vendor graphs own request concurrency and buffering. A single consumer thread lets
the synchronous database caller stop a blocked graph; it is never a request pool.
HTTP errors and transport failures are values for the original operator parser.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import json
import math
import os
from queue import Empty, Full, Queue
import sys
import threading
import time
from urllib.parse import urlsplit

from src.baselines.common.redact import redact_text
from src.execution_provider.adapters.full_response import (
    FullModelResponse, decode_full_response, encode_full_response,
)
from src.experiments.execution_timing import is_timing_error, timing_span, timing_thread_target
from .prepared_map import NativeGraphOptions


_PROCESSOR_URL = "http://localhost/native-complete-call"
_DAFT_CONTROLS = {}
_HTTP_CALL_COOKIES = ContextVar('native_http_call_cookies', default=None)


def _observed_graph_cleanup(backend, actions):
    """Attempt every known cleanup, preserving execution and real cleanup errors."""
    if not actions:
        return
    primary = sys.exception()
    try:
        with timing_span('native_call.graph_cleanup', backend=backend,
                         scope='adapter iterator and graph resource cleanup calls'):
            failures = []
            for stage, action in actions:
                try:
                    action()
                except BaseException as error:
                    failures.append((stage, error))
            if failures:
                first = next((error for _, error in failures if not is_timing_error(error)), failures[0][1])
                for stage, error in failures:
                    if error is not first:
                        first.add_note('Native ' + stage + ' also failed: ' + type(error).__name__)
                raise first
    except BaseException as cleanup:
        if primary is None or isinstance(primary, GeneratorExit):
            raise
        if is_timing_error(primary) and not is_timing_error(cleanup):
            cleanup.add_note('Detailed timing also failed: ' + type(primary).__name__)
            raise
        primary.add_note('Native graph cleanup also failed: ' + type(cleanup).__name__)
        if cleanup is not primary:
            for note in tuple(getattr(cleanup, '__notes__', ())):
                primary.add_note(note)


def _processor_timing_fields():
    ray = sys.modules.get('ray')
    try:
        if ray is not None and ray.is_initialized():
            actor = ray.get_runtime_context().get_actor_id()
            if actor is not None:
                return {'actor_id': str(actor)}
    except Exception:
        pass
    return {}


def _daft_control(key):
    control = _DAFT_CONTROLS.get(key)
    if control is None:
        raise RuntimeError("native Daft graph control is no longer live")
    return control


@dataclass(frozen=True)
class NativeCall:
    call_id: str
    row_id: int
    payload: bytes = field(repr=False)
    endpoint: str = field(repr=False)
    headers: tuple[tuple[str, str], ...] = field(repr=False)
    timeout_s: float
    connect_timeout_s: float | None = None
    max_response_bytes: int = 8 * 1024 * 1024
    skip_auto_headers: tuple[str, ...] = ()
    follow_redirects: bool = False

    def __post_init__(self):
        if type(self.call_id) is not str or not self.call_id or len(self.call_id.encode()) > 4096:
            raise ValueError("native call requires a finite occurrence identity")
        if type(self.row_id) is not int or not 0 <= self.row_id < 2**63:
            raise ValueError("native call row identity must fit a signed 64-bit integer")
        if type(self.payload) is not bytes or not 0 < len(self.payload) <= 8 * 1024 * 1024:
            raise ValueError("native call requires complete finite payload bytes")
        url = urlsplit(self.endpoint)
        if (url.scheme not in ("http", "https") or not url.hostname or url.username
                or url.password or url.fragment):
            raise ValueError("native call requires an explicit HTTP endpoint")
        if (type(self.headers) is not tuple or len(self.headers) > 128
                or any(type(item) is not tuple or len(item) != 2
                       or any(type(value) is not str or "\r" in value or "\n" in value for value in item)
                       for item in self.headers)
                or sum(len(value.encode()) for item in self.headers for value in item) > 8192):
            raise ValueError("native call headers must be finite name/value pairs")
        for value in (self.timeout_s, self.connect_timeout_s):
            if value is not None and (type(value) not in (int, float) or not math.isfinite(value)
                                      or not 0 < value <= 120):
                raise ValueError("native request timeout must be at most 120 seconds")
        if self.timeout_s is None:
            raise ValueError("native request needs a finite timeout")
        if type(self.max_response_bytes) is not int or not 1 <= self.max_response_bytes <= 8 * 1024 * 1024:
            raise ValueError("native response byte allowance must be at most 8 MiB")
        if (type(self.skip_auto_headers) is not tuple or len(self.skip_auto_headers) > 16
                or any(type(name) is not str or not name or "\r" in name or "\n" in name
                       for name in self.skip_auto_headers) or type(self.follow_redirects) is not bool):
            raise ValueError("native call requires explicit header and redirect settings")


@dataclass(frozen=True)
class NativeCallOutcome:
    call_id: str
    row_id: int
    response: FullModelResponse | None
    transport_error: str = ""
    execution_error: str = ""


def outcome_kind(outcome):
    if any(type(value) is not str or len(value.encode()) > 4096
           for value in (outcome.transport_error, outcome.execution_error)):
        raise ValueError("native outcome diagnostics exceed their allowance")
    kinds = [("response", outcome.response is not None),
             ("transport_error", bool(outcome.transport_error)),
             ("execution_error", bool(outcome.execution_error))]
    if sum(active for _, active in kinds) != 1:
        raise ValueError("native outcome requires one HTTP, transport or execution result")
    return next(name for name, active in kinds if active)


def raise_execution_error(outcome):
    if outcome.execution_error:
        name, _, message = outcome.execution_error.partition(":")
        kind = {"ValueError": ValueError, "TypeError": TypeError,
                "OverflowError": OverflowError, "AssertionError": AssertionError}.get(name, RuntimeError)
        error = kind(message.lstrip())
        error.native_execution_error = outcome.execution_error
        error.full_response = outcome.response
        raise error


def native_error_text(error):
    diagnostic = f"{type(error).__name__}: {error}"
    notes = getattr(error, '__notes__', ())
    if notes:
        diagnostic += '\n' + '\n'.join(notes)
    return redact_text(diagnostic).encode()[:4096].decode(errors="ignore")


def _call_wire(call):
    return json.dumps(dict(call_id=call.call_id, row_id=call.row_id,
        payload=base64.b64encode(call.payload).decode(), endpoint=call.endpoint,
        headers=call.headers, timeout_s=call.timeout_s, connect_timeout_s=call.connect_timeout_s,
        max_response_bytes=call.max_response_bytes, skip_auto_headers=call.skip_auto_headers,
        follow_redirects=call.follow_redirects), separators=(",", ":"))


def _call_from_wire(wire):
    value = json.loads(wire)
    value["payload"] = base64.b64decode(value["payload"], validate=True)
    value["headers"] = tuple(tuple(item) for item in value["headers"])
    value["skip_auto_headers"] = tuple(value["skip_auto_headers"])
    return NativeCall(**value)


def _outcome_wire(outcome):
    value = dict(call_id=outcome.call_id, row_id=outcome.row_id,
        response=(base64.b64encode(encode_full_response(outcome.response)).decode()
                  if outcome.response is not None else None),
        transport_error=outcome.transport_error)
    if outcome.execution_error:
        value["execution_error"] = outcome.execution_error
    return json.dumps(value, separators=(",", ":"))


def _outcome_from_wire(wire):
    value = json.loads(wire)
    if value["response"] is not None:
        value["response"] = decode_full_response(base64.b64decode(value["response"], validate=True))
    return NativeCallOutcome(**value)


@dataclass
class _HttpClient:
    loop: object
    session: object

    @property
    def closed(self):
        return self.session.closed

    async def close(self):
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("native HTTP client belongs to a different event loop")
        await self.session.close()


def _new_http_client():
    import aiohttp
    class CallCookieJar(aiohttp.DummyCookieJar):
        # Redirects share this call's default jar; concurrent calls never share it.
        def update_cookies(self, cookies, response_url=None):
            jar = _HTTP_CALL_COOKIES.get()
            if jar is not None:
                if response_url is None:
                    jar.update_cookies(cookies)
                else:
                    jar.update_cookies(cookies, response_url)

        def update_cookies_from_headers(self, headers, response_url):
            jar = _HTTP_CALL_COOKIES.get()
            if jar is not None:
                jar.update_cookies_from_headers(headers, response_url)

        def filter_cookies(self, request_url):
            jar = _HTTP_CALL_COOKIES.get()
            return jar.filter_cookies(request_url) if jar is not None else super().filter_cookies(request_url)

    # The vendor graph owns concurrency; this pool adds no separate admission.
    return _HttpClient(asyncio.get_running_loop(), aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None), auto_decompress=False,
        cookie_jar=CallCookieJar(), connector=aiohttp.TCPConnector(limit=0)))


@asynccontextmanager
async def _http_session(client):
    import aiohttp
    cookies = _HTTP_CALL_COOKIES.set(aiohttp.CookieJar())
    owned, primary = None, None
    try:
        if client is None:
            client = owned = _new_http_client()
        elif asyncio.get_running_loop() is not client.loop:
            raise RuntimeError("native HTTP client belongs to a different event loop")
        yield client.session
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            if owned is not None:
                try:
                    await owned.close()
                except BaseException as cleanup:
                    if primary is None:
                        raise
                    primary.add_note('Native HTTP pool close also failed: ' + type(cleanup).__name__)
        finally:
            _HTTP_CALL_COOKIES.reset(cookies)


async def _close_http_client(client):
    await client.close()


async def _request(call, control=None, *, client=None):
    """One request kernel; no status interpretation, retries, routing or scheduling."""
    import aiohttp
    if control is not None and control.stopped.is_set():
        return NativeCallOutcome(call.call_id, call.row_id, None, "NativeCallCancelled")
    if client is None and control is not None:
        client = control.client()
        if client is None:
            return NativeCallOutcome(call.call_id, call.row_id, None, "NativeCallCancelled")
    try:
        timeout = aiohttp.ClientTimeout(total=call.timeout_s,
            connect=call.connect_timeout_s or call.timeout_s)
        async with _http_session(client) as session:
            registration = control.register_session(session) if control is not None else None
            try:
                if control is not None and control.stopped.is_set():
                    return NativeCallOutcome(call.call_id, call.row_id, None, "NativeCallCancelled")
                async with session.post(call.endpoint, data=call.payload, headers=call.headers,
                                        allow_redirects=call.follow_redirects, max_redirects=20,
                                        timeout=timeout, skip_auto_headers=call.skip_auto_headers) as response:
                    buffer = bytearray()
                    async for chunk in response.content.iter_chunked(4096):
                        if len(buffer) + len(chunk) > call.max_response_bytes:
                            raise ValueError("native complete response exceeds its byte allowance")
                        buffer.extend(chunk)
                    full = FullModelResponse(response.status,
                        tuple((name.decode("ascii"), value.decode("latin-1"))
                              for name, value in response.raw_headers), bytes(buffer),
                        f"HTTP/{response.version.major}.{response.version.minor}")
                    if len(encode_full_response(full)) > call.max_response_bytes:
                        raise ValueError("native response with HTTP metadata exceeds its byte allowance")
                    return NativeCallOutcome(call.call_id, call.row_id, full)
            finally:
                if control is not None:
                    control.unregister_session(registration)
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as error:
        # No synthetic HTTP response is created for uncertain transport completion.
        return NativeCallOutcome(call.call_id, call.row_id, None,
                                 native_error_text(error))


class _ProcessorResponse:
    """Ray's HTTP stage sees a successful carrier; the real status stays in the value."""
    status = 200
    reason = "Complete call carrier"

    def __init__(self, outcome):
        self.outcome = outcome

    async def __aenter__(self):
        return self

    async def __aexit__(self, *error):
        pass

    async def json(self):
        return {"native_outcome": _outcome_wire(self.outcome)}


class _ProcessorSession:
    def __init__(self):
        self._client = None

    async def __aenter__(self):
        if not os.environ.get('SEMLOOM_DETAILED_TIMING_DIR'):
            self._client = _new_http_client()
            return self
        self._timing_fields = _processor_timing_fields()
        try:
            with timing_span('native_call.http_session_init', backend='ray-data',
                             scope='local HTTP client and session creation; actor readiness unavailable',
                             **self._timing_fields):
                self._client = _new_http_client()
        except BaseException as error:
            if self._client is not None:
                try:
                    await _close_http_client(self._client)
                except BaseException as cleanup:
                    if is_timing_error(error) and not is_timing_error(cleanup):
                        cleanup.add_note('Detailed timing also failed: ' + type(error).__name__)
                        raise
                    error.add_note('Native HTTP pool close also failed: ' + type(cleanup).__name__)
            raise
        return self

    async def __aexit__(self, *error):
        if self._client is not None:
            try:
                with timing_span('native_call.http_session_close', backend='ray-data',
                                 **getattr(self, '_timing_fields', {})):
                    await _close_http_client(self._client)
            except BaseException as cleanup:
                if error[1] is None:
                    raise
                if is_timing_error(error[1]) and not is_timing_error(cleanup):
                    cleanup.add_note('Detailed timing also failed: ' + type(error[1]).__name__)
                    raise
                error[1].add_note("Native HTTP pool close also failed: " + type(cleanup).__name__)

    async def post(self, url, *, data, headers):
        if url != _PROCESSOR_URL:
            raise ValueError("native Ray carrier destination differs")
        call = _call_from_wire(json.loads(data)["native_call"])
        if self._client is None:
            raise RuntimeError("native Ray HTTP session was not entered")
        with timing_span('native_call.http_call', backend='ray-data', call_id=call.call_id,
                         row_id=call.row_id, scope='request kernel await through complete response consumption',
                         **getattr(self, '_timing_fields', {})):
            return _ProcessorResponse(await _request(call, client=self._client))


def _ray_preprocess(row):
    return {"payload": {"native_call": row["native_call"]}}


def _ray_postprocess(row):
    wire = row["http_response"]["native_outcome"]
    outcome = _outcome_from_wire(wire)
    if outcome.call_id != row["call_id"] or outcome.row_id != row["row_id"]:
        raise ValueError("native Ray response changed its occurrence identity")
    return {"native_outcome": wire}


class _GraphControl:
    def __init__(self):
        self.stopped = threading.Event()
        self._lock = threading.Lock()
        self._sessions = {}
        self._clients = {}
        self._client_closes = {}
        self.result = None
        self._stopped_executor = None
        self._actors = []
        self._killed_actors = set()
        self.ray_async_batches_effective = None

    def register_actor(self, actor):
        with self._lock:
            self._actors.append(actor)
        if self.stopped.is_set():
            self.stop()

    def register_session(self, session):
        with self._lock:
            self._sessions[id(session)] = (asyncio.get_running_loop(), session)
        return id(session)

    def unregister_session(self, key):
        with self._lock:
            self._sessions.pop(key, None)

    def client(self):
        loop = asyncio.get_running_loop()
        with self._lock:
            if self.stopped.is_set():
                return None
            client = self._clients.get(loop)
            if client is None:
                client = self._clients[loop] = _new_http_client()
        return client

    def wait_clients_closed(self, timeout):
        deadline = time.monotonic() + timeout
        with self._lock:
            closing = tuple(self._client_closes.values())
        for future in closing:
            future.result(timeout=max(0, deadline - time.monotonic()))
        with self._lock:
            if any(not connector.closed for connector in self._clients.values()):
                raise RuntimeError("native HTTP pool closure is unconfirmed")

    def stop(self):
        self.stopped.set()
        first_error = None
        with self._lock:
            sessions = tuple(self._sessions.values())
            pooled_sessions = {id(client.session) for client in self._clients.values()}
        for loop, session in sessions:
            if id(session) not in pooled_sessions and not loop.is_closed():
                loop.call_soon_threadsafe(lambda current=session: asyncio.create_task(current.close()))
        with self._lock:
            for loop, client in self._clients.items():
                if client.closed or loop in self._client_closes:
                    continue
                if loop.is_closed() or not loop.is_running():
                    first_error = first_error or RuntimeError("native HTTP pool owner loop stopped before close")
                    continue
                self._client_closes[loop] = asyncio.run_coroutine_threadsafe(
                    _close_http_client(client), loop)
        executor = getattr(self.result, "_current_executor", None)
        if executor is not None and executor is not self._stopped_executor:
            self._stopped_executor = executor
            executor.shutdown(force=True)
        with self._lock:
            actors = tuple(actor for actor in self._actors if id(actor) not in self._killed_actors)
            self._killed_actors.update(id(actor) for actor in actors)
        if actors:
            import ray
            for actor in actors:
                ray.kill(actor, no_restart=True)
        if first_error is not None:
            raise first_error


def _ray_core_rows(calls, options, control):
    from ..controls.ray_core_calls import rows
    yield from rows(calls, options, control)


def _daft_rows(calls, options, control):
    if control.stopped.is_set():
        return
    options.validate_backend('daft')
    import daft
    from daft.runners import get_or_create_runner
    if daft.__version__ != "0.7.21" or get_or_create_runner().name != "native":
        raise RuntimeError("native complete calls require Daft 0.7.21 with its native runner")
    iterator = control_key = None
    try:
        with timing_span('native_call.graph_input', backend='daft'):
            inputs = {"native_call": [_call_wire(call) for call in calls]}
        with timing_span('native_call.graph_build', backend='daft', scope='adapter vendor graph API calls'):
            frame = daft.from_pydict(inputs)
            # Daft validates serialization even for use_process=False. Keep only the
            # primitive key in its function closure; actual stop/session state is local.
            control_key = id(control)
            _DAFT_CONTROLS[control_key] = control

            @daft.func.batch(return_dtype=daft.DataType.string(), use_process=False,
                            batch_size=1, max_concurrency=options.concurrency - 1,
                            max_retries=0, on_error="raise")
            async def complete(wires):
                if len(wires) != 1:
                    raise ValueError("native Daft requires one complete call per batch")
                outcome = await _request(_call_from_wire(wires.to_pylist()[0]), _daft_control(control_key))
                return daft.Series.from_pylist([_outcome_wire(outcome)])

            result = frame.with_column("native_outcome", complete(daft.col("native_call"))).select("native_outcome")
        if control.stopped.is_set():
            return
        with timing_span('native_call.iterator_open', backend='daft',
                         scope='vendor iterator API call; actor readiness unavailable'):
            iterator = result.iter_rows(results_buffer_size=1)
        with timing_span('native_call.consume', backend='daft',
                         scope='native iterator through caller yields; includes consumer yield and backpressure wait'):
            for row in iterator:
                if control.stopped.is_set():
                    return
                yield _outcome_from_wire(row["native_outcome"])
    finally:
        actions = []
        if iterator is not None:
            actions.append(('iterator_close', iterator.close))
        if control_key is not None:
            actions.append(('control_release', lambda: _DAFT_CONTROLS.pop(control_key, None)))
        _observed_graph_cleanup('daft', actions)


def _ray_rows(calls, options, control):
    if control.stopped.is_set():
        return
    options.validate_backend('ray-data')
    import ray
    from ray.data.llm import HttpRequestProcessorConfig, build_processor
    from ray.data._internal.planner.plan_udf_map_op import DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY
    if ray.__version__ != "2.56.1" or not ray.is_initialized():
        raise RuntimeError("native complete calls require caller-owned Ray 2.56.1")
    control.ray_async_batches_effective = DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY
    if DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY != options.ray_async_batches_per_actor:
        raise RuntimeError("native Ray asynchronous batch limit differs from its import-time setting")
    iterator = result = None
    try:
        with timing_span('native_call.graph_input', backend='ray-data'):
            inputs = [dict(call_id=call.call_id, row_id=call.row_id, native_call=_call_wire(call)) for call in calls]
        with timing_span('native_call.graph_build', backend='ray-data', scope='adapter vendor graph API calls'):
            dataset = ray.data.from_items(inputs, override_num_blocks=min(len(inputs), options.source_blocks))
            processor = build_processor(HttpRequestProcessorConfig(url=_PROCESSOR_URL,
                batch_size=options.ray_batch_rows, concurrency=options.ray_actor_pool,
                max_retries=0, session_factory=_ProcessorSession), preprocess=_ray_preprocess,
                postprocess=_ray_postprocess, preprocess_map_kwargs={"max_retries": 0},
                postprocess_map_kwargs={"max_retries": 0})
            for stage in processor.stages.values():
                stage.map_batches_kwargs.update(max_restarts=0, max_task_retries=0, max_concurrency=1)
            result = control.result = processor(dataset)
        if control.stopped.is_set():
            return
        with timing_span('native_call.iterator_open', backend='ray-data',
                         scope='vendor iterator API call; actor readiness unavailable'):
            iterator = iter(result.iter_rows())
        with timing_span('native_call.consume', backend='ray-data',
                         scope='native iterator through caller yields; includes consumer yield and backpressure wait'):
            for row in iterator:
                if control.stopped.is_set():
                    return
                yield _outcome_from_wire(row["native_outcome"])
    finally:
        actions = []
        close = getattr(iterator, 'close', None)
        if close is not None:
            actions.append(('iterator_close', close))
        executor = getattr(result, '_current_executor', None)
        if executor is not None:
            actions.append(('executor_shutdown', lambda: executor.shutdown(force=True)))
        _observed_graph_cleanup('ray-data', actions)


class NativeCallExecutor:
    """Finite complete calls on vendor graphs, author IMLane or a Core reference."""
    def __init__(self, backend, options=None, *, max_batch_rows=4096,
                 max_batch_bytes=32 * 1024 * 1024, imlane_config=None):
        if backend not in ("daft", "ray-data", "ray", "imlane"):
            raise ValueError("complete-call backend must be daft, ray-data, ray or imlane")
        if any(type(value) is not int or value <= 0 for value in (max_batch_rows, max_batch_bytes)):
            raise ValueError("native complete-call batch limits must be positive integers")
        if backend != "imlane" and imlane_config is not None:
            raise ValueError("IMLane configuration requires the IMLane backend")
        self._imlane = None
        if backend == "imlane":
            from .imlane_calls import IMLaneCallConfig, IMLaneCallExecutor
            if type(imlane_config) is not IMLaneCallConfig:
                raise TypeError('explicit IMLaneCallConfig required')
            max_batch_rows = min(max_batch_rows, imlane_config.max_batch_rows)
            self._imlane = IMLaneCallExecutor(imlane_config, options,
                max_batch_rows=max_batch_rows,
                max_batch_bytes=min(max_batch_bytes, imlane_config.max_input_payload_bytes))
            options = self._imlane.options
            max_batch_rows, max_batch_bytes = self._imlane.max_batch_rows, self._imlane.max_batch_bytes
        self.backend, self.options = backend, options or NativeGraphOptions()
        self.options.validate_backend(backend)
        self.max_batch_rows, self.max_batch_bytes = max_batch_rows, max_batch_bytes
        self.last_close_report = None
        self.last_cleanup_errors = ()
        self._unsettled = False
        self._batch_lock = threading.Lock()

    def __call__(self, calls, cancelled=None):
        return self.iter_batch(calls, cancelled=cancelled)

    def close(self):
        if self._imlane is not None:
            self._imlane.close()

    def iter_batch(self, calls, *, cancelled=None, timeout_s=120):
        if not self._batch_lock.acquire(blocking=False):
            raise RuntimeError('native executor already owns an active batch')
        try:
            yield from self._iter_batch(calls, cancelled=cancelled, timeout_s=timeout_s)
        finally:
            self._batch_lock.release()

    def _iter_batch(self, calls, *, cancelled=None, timeout_s=120):
        if self._unsettled:
            raise RuntimeError('native graph has unconfirmed work; caller-owned runtime cleanup required before replacement')
        self.last_close_report = None
        self.last_cleanup_errors = ()
        if self._imlane is not None:
            self.last_close_report = None
            self._imlane.last_close_report = None
            iterator = self._imlane.iter_batch(calls, cancelled=cancelled, timeout_s=timeout_s)
            try:
                for outcome in iterator:
                    if outcome.transport_error:
                        self._unsettled = True
                    yield outcome
            finally:
                try:
                    iterator.close()
                finally:
                    report = self._imlane.last_close_report
                    if report is not None:
                        self._unsettled = self._unsettled or (
                            report.get('external_coordinator_requires_cleanup') is True)
                        self.last_close_report = dict(report, unconfirmed_work=self._unsettled,
                                                      reuse_allowed=not self._unsettled)
            return
        if (timeout_s is None and self.backend not in ("daft", "ray-data")
                or timeout_s is not None and (type(timeout_s) not in (int, float)
                or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120)):
            raise ValueError("native complete-call batch requires a finite deadline or supervised vendor completion")
        prepared, expected, payload_bytes = [], {}, 0
        for call in calls:
            if type(call) is not NativeCall or call.call_id in expected:
                raise ValueError("native complete-call batch repeats or changes occurrence identity")
            payload_bytes += len(call.payload)
            if len(prepared) == self.max_batch_rows or payload_bytes > self.max_batch_bytes:
                raise ValueError("native complete-call input batch exceeds its declared allowance")
            expected[call.call_id] = call.row_id
            prepared.append(call)
        calls = tuple(prepared)
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        self.last_close_report = None
        if not calls:
            return
        control = _GraphControl()
        queue = Queue(maxsize=1)
        finished = threading.Event()
        failures = []

        def consume():
            iterator = None
            try:
                iterator = {"daft": _daft_rows, "ray-data": _ray_rows, "ray": _ray_core_rows}[self.backend](
                    calls, self.options, control)
                for outcome in iterator:
                    while not control.stopped.is_set():
                        try:
                            queue.put(outcome, timeout=0.01)
                            break
                        except Full:
                            pass
                    if control.stopped.is_set():
                        break
            except BaseException as error:
                failures.append(("graph_consume", error))
            finally:
                try:
                    if iterator is not None:
                        iterator.close()
                except BaseException as error:
                    failures.append(("graph_close", error))
                finished.set()

        thread = threading.Thread(target=timing_thread_target(consume), name="native-call-consumer", daemon=True)
        primary = None
        received = set()
        total_bytes = 0
        transport_uncertain = False
        clean = False
        try:
            if cancelled and cancelled():
                raise InterruptedError("native complete-call batch cancelled before graph start")
            thread.start()
            while True:
                if cancelled and cancelled():
                    raise InterruptedError("native complete-call batch cancelled")
                if deadline is not None and time.monotonic() >= deadline:
                    raise TimeoutError("native complete-call batch deadline exceeded")
                try:
                    outcome = queue.get(timeout=0.01 if deadline is None else min(0.01, max(0.001, deadline - time.monotonic())))
                except Empty:
                    if not finished.is_set():
                        continue
                    if failures:
                        raise failures[0][1]
                    if len(received) != len(calls):
                        raise ValueError("native graph response batch is incomplete")
                    clean = True
                    return
                if outcome.call_id not in expected or outcome.call_id in received or expected[outcome.call_id] != outcome.row_id:
                    raise ValueError("native graph response changed its occurrence identity")
                outcome_kind(outcome)
                raise_execution_error(outcome)
                total_bytes += (len(encode_full_response(outcome.response)) if outcome.response is not None
                                else len(outcome.transport_error.encode()))
                if total_bytes > self.max_batch_bytes:
                    raise ValueError("native complete-call results exceed their batch byte allowance")
                received.add(outcome.call_id)
                transport_uncertain = transport_uncertain or bool(outcome.transport_error)
                yield outcome
        except BaseException as error:
            primary = error
            raise
        finally:
            cleanup = None
            try:
                control.stop()
            except BaseException as error:
                cleanup = error
            if thread.ident is not None:
                stop_deadline = time.monotonic() + min(120, max(call.timeout_s for call in calls) + 5)
                while thread.is_alive() and time.monotonic() < stop_deadline:
                    thread.join(0.05)
                    try:
                        # Ray creates its executor lazily during the first next().
                        control.stop()
                    except BaseException as error:
                        if cleanup is None:
                            cleanup = error
            stopped = not thread.is_alive()
            try:
                control.wait_clients_closed(1)
            except BaseException as error:
                if cleanup is None:
                    cleanup = error
                clean = False
            worker_errors = tuple(failures)
            clean = clean and stopped and cleanup is None
            # Normal EOF can run graph cleanup and report its failure as graph_consume.
            self._unsettled = self._unsettled or (thread.ident is not None and (
                not stopped or len(received) != len(calls) or cleanup is not None
                or transport_uncertain
                or bool(worker_errors)))
            clean = clean and not self._unsettled
            self.last_cleanup_errors = tuple((stage, error) for stage, error in worker_errors
                                             if stage == "graph_close")
            if cleanup is not None:
                self.last_cleanup_errors += (("control_stop", cleanup),)
            self.last_close_report = dict(graph_consumer_stopped=stopped,
                clean=clean, received=len(received), prepared=len(calls),
                unconfirmed_work=self._unsettled, reuse_allowed=not self._unsettled,
                transport_completion_uncertain=transport_uncertain,
                remote_http_stopped="unavailable", owned_ray_actors=len(control._actors),
                ray_actor_shutdown_requested=len(control._killed_actors),
                http_pool_scope="native_processor_batch" if self.backend == "ray-data" else "graph_event_loop",
                http_pool_count="unavailable" if self.backend == "ray-data" else len(control._clients),
                http_pools_closed="unavailable" if self.backend == "ray-data" else
                    all(connector.closed for connector in control._clients.values()),
                owned_ray_actor_ids=[actor._actor_id.hex() for actor in control._actors],
                declared_options=dict(vars(self.options)),
                batch_storage=dict(input_rows=len(calls), input_payload_bytes=payload_bytes,
                    max_input_rows=self.max_batch_rows, max_input_payload_bytes=self.max_batch_bytes,
                    max_consumed_response_bytes=self.max_batch_bytes,
                    response_allowance_sum_bytes=sum(call.max_response_bytes for call in calls),
                    resident_memory_limit='not established by encoded-byte limits'),
                native_graph_parameters=(
                    dict(batch_size=1, max_concurrency=self.options.concurrency - 1)
                    if self.backend == "daft" else
                    dict(actor_pool_range=list(self.options.ray_actor_pool),
                         http_batch_rows=self.options.ray_batch_rows,
                         async_batches_setting=self.options.ray_async_batches_per_actor,
                         async_batches_effective=control.ray_async_batches_effective,
                         request_row_envelope=self.options.ray_request_row_envelope,
                         active_http_upper_bound=(self.options.ray_active_http_upper_bound
                             if control.ray_async_batches_effective == self.options.ray_async_batches_per_actor else None),
                         stage_max_concurrency=1, source_blocks=min(len(calls), self.options.source_blocks))
                    if self.backend == "ray-data" else dict(actor_count=self.options.concurrency)),
                worker_errors_complete=stopped,
                worker_errors=[dict(stage=stage, type=type(error).__module__ + "." + type(error).__name__)
                               for stage, error in worker_errors])
            if not stopped and cleanup is None:
                cleanup = RuntimeError("native graph consumer did not stop; remote completion is uncertain")
                self.last_cleanup_errors += (("consumer_stop", cleanup),)
            additional = tuple((stage, error) for stage, error in worker_errors if error is not primary)
            if cleanup is not None:
                additional += (("graph_cleanup", cleanup),)
            self.last_close_report["cleanup_errors"] = [
                dict(stage=stage, type=type(error).__module__ + "." + type(error).__name__)
                for stage, error in self.last_cleanup_errors]
            if additional:
                if (primary is None or isinstance(primary, GeneratorExit) or
                        is_timing_error(primary) and any(not is_timing_error(error) for _, error in additional)):
                    first = next((error for _, error in additional if not is_timing_error(error)), additional[0][1])
                    for stage, error in additional:
                        if error is not first:
                            first.add_note("Native " + stage + " also failed: " + type(error).__name__)
                            for note in tuple(getattr(error, '__notes__', ())):
                                first.add_note(note)
                    if primary is not None and is_timing_error(primary):
                        first.add_note('Detailed timing also failed: ' + type(primary).__name__)
                    raise first
                for stage, error in additional:
                    primary.add_note("Native " + stage + " also failed: " + type(error).__name__)
                    if error is not primary:
                        for note in tuple(getattr(error, '__notes__', ())):
                            primary.add_note(note)
