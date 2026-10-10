"""Query-scoped Sema request service; the author process still owns SQL and supply."""

from __future__ import annotations

import asyncio
import base64
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, replace
import hashlib
import json
import math
from pathlib import Path
import threading
import time
from urllib.parse import quote

from ...baselines.common.private_artifacts import open_private_text, write_private_json
from ...baselines.common.redact import redact_text
from ...baselines.text.products.sema import prepare_projection


_HOP_HEADERS = {
    'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization',
    'te', 'trailer', 'transfer-encoding', 'upgrade', 'host', 'content-length',
}


@dataclass(frozen=True)
class SemaServiceLimits:
    max_requests: int
    request_bytes: int
    response_bytes: int
    timeout_s: float

    def __post_init__(self):
        for value in (self.max_requests, self.request_bytes, self.response_bytes):
            if type(value) is not int or value < 1:
                raise ValueError('Sema service byte and request limits must be positive integers')
        if (isinstance(self.timeout_s, bool) or not isinstance(self.timeout_s, (int, float))
                or not math.isfinite(self.timeout_s) or self.timeout_s <= 0):
            raise ValueError('Sema service timeout must be finite and positive')


class SemaServiceError(RuntimeError):
    """Keep the first complete HTTP failure independently of CLI exit status."""

    def __init__(self, sequence, status, body, headers, phase='http_response'):
        super().__init__('Sema request service failed at ' + phase + '; status=' + str(status))
        self.sequence, self.status, self.body = sequence, status, body
        self.headers, self.phase = headers, phase
        self.observed_ns = time.monotonic_ns()


@dataclass(frozen=True)
class _Reply:
    status: int
    body: bytes
    headers: tuple[tuple[str, str], ...]


class SemaRequestService:
    """Transparent forwarding with query ownership and a first-error stop policy.

    The I/O thread runs aiohttp; it is not a model worker pool. Native requests
    remain independent, including equal payloads and author joint prompts.
    """

    mode = 'transparent'

    def __init__(self, *, query_id, upstream_url, limits, trace_path, before_post=None, upstream_concurrency=0):
        if not isinstance(query_id, str) or not query_id or len(query_id.encode()) > 128:
            raise ValueError('Sema query identity must contain 1..128 UTF-8 bytes')
        if type(limits) is not SemaServiceLimits:
            raise ValueError('Sema service requires explicit limits')
        if type(upstream_concurrency) is not int or not 0 <= upstream_concurrency <= 256:
            raise ValueError('Sema upstream connector capacity must be from 0 to 256')
        self.upstream_concurrency = upstream_concurrency
        self.query_id, self.upstream_url, self.limits = query_id, upstream_url, limits
        self.trace_path, self.before_post = Path(trace_path), before_post
        self.first_error = None
        self.cleanup_errors = []
        self._cleanup_failures = []
        self._cleanup_error = None
        self._cleanup_lock = threading.Lock()
        self._cleanup_started = threading.Event()
        self._cancel_native = None
        self._halted = threading.Event()
        self._ready = threading.Event()
        self._thread = self._loop = None
        self._runner = None
        self._http_handlers = set()
        self._listeners_remaining = None
        self._http_connections_remaining = None
        self._port = None
        self._startup_error = None
        self._rows = []
        self._sequence = self._forwarded = self._rejected = 0

    def bind_native_cancel(self, callback):
        if self._cancel_native is not None:
            raise RuntimeError('Sema request service already has a native process owner')
        self._cancel_native = callback
        if self._halted.is_set():
            self._cancel_owner()

    def unbind_native_cancel(self, callback):
        if self._cancel_native != callback:
            raise RuntimeError('Sema native process owner differs')
        self._cancel_native = None

    def _cancel_owner(self):
        if self._cancel_native is not None:
            try:
                self._cancel_native()
            except BaseException as error:
                self._record_cleanup_error('native_cancel', error)

    def _record_cleanup_error(self, stage, error):
        label = type(error).__name__
        with self._cleanup_lock:
            # Preserve the existing labels for these two previously recorded actions.
            self.cleanup_errors.append(label if stage in ('native_cancel', 'executor_close')
                                       else stage + ':' + label)
            self._cleanup_failures.append({'stage': stage, 'type': label,
                                           'observed_ns': time.monotonic_ns()})
            if self._cleanup_error is None:
                self._cleanup_error = error

    def cancel(self):
        """Stop further sends before signaling the existing native process owner."""
        self._halted.set()
        self._cancel_owner()

    def end_input(self):
        """The native SQL completion marker proves that no more calls can follow."""
        self._halted.set()

    def _fail(self, sequence, reply, phase):
        if self.first_error is None:
            self.first_error = SemaServiceError(
                sequence, reply.status, reply.body, reply.headers, phase)
        self.cancel()

    @property
    def endpoint_url(self):
        if self._port is None:
            raise RuntimeError('Sema request service is not running')
        return ('http://127.0.0.1:' + str(self._port) + '/sema/'
                + quote(self.query_id, safe='') + '/v1/chat/completions')

    @property
    def summary(self):
        with self._cleanup_lock:
            cleanup_errors = list(self.cleanup_errors)
            cleanup_failures = list(self._cleanup_failures)
        return {
            'query_id': self.query_id, 'mode': self.mode,
            'upstream_concurrency': self.upstream_concurrency,
            'upstream_capacity_owner': 'aiohttp TCPConnector; zero retains unlimited forwarding',
            'native_task_ready': {'status': 'unavailable', 'reason': 'author request pool precedes llm_url'},
            'native_row_association': {'status': 'unavailable', 'reason': 'HTTP payload has no native row identity'},
            'received_requests': self._sequence, 'forwarded_posts': self._forwarded,
            'rejected_requests': self._rejected,
            'first_error': None if self.first_error is None else {
                'sequence': self.first_error.sequence, 'status': self.first_error.status,
                'phase': self.first_error.phase,
                'observed_ns': self.first_error.observed_ns,
                'body_sha256': hashlib.sha256(self.first_error.body).hexdigest(),
            },
            'cleanup_errors': cleanup_errors,
            'cleanup_failures': cleanup_failures,
            'first_cleanup_error': None if not cleanup_failures else dict(cleanup_failures[0]),
            'service_thread_alive': None if self._thread is None else self._thread.is_alive(),
            'io_loop_closed': None if self._loop is None else self._loop.is_closed(),
            'listeners_remaining': self._listeners_remaining,
            'listener_shutdown_confirmed': (None if self._listeners_remaining is None
                                             else self._listeners_remaining == 0),
            'http_connections_remaining': self._http_connections_remaining,
            'http_handlers_remaining': len(self._http_handlers),
        }

    def __enter__(self):
        if self._thread is not None or self._halted.is_set():
            raise RuntimeError('Sema request service cannot be restarted')
        # Use the existing private artifact writer for exclusive creation and permissions.
        self._trace_context = open_private_text(self.trace_path)
        self._trace_stream = self._trace_context.__enter__()
        self._start_runtime()
        if not self._ready.wait(45) or self._startup_error is not None:
            primary = self._startup_error
            if primary is None:
                primary = RuntimeError('Sema request service startup did not settle')
            self.__exit__(type(primary), primary, primary.__traceback__)
            raise RuntimeError('Sema request service startup failed') from self._startup_error
        return self

    def __exit__(self, *_error):
        query_error = _error[1] if _error and _error[0] is not None else None
        known_http_error = self.first_error
        primary = query_error if query_error is not None else known_http_error
        if primary is None:
            primary = self._startup_error
        self._halted.set()
        try:
            self.cancel()
        except BaseException as error:
            self._record_cleanup_error('cancel', error)
        self._stop_runtime()
        # Errors first observed while waiting for this exit were not already
        # reported to the caller. Select them once rather than re-read the flag
        # when deciding whether a cleanup failure should propagate.
        if primary is None:
            primary = self.first_error
            if primary is None:
                primary = self._startup_error
        try:
            if getattr(self, '_trace_stream', None) is not None:
                for row in tuple(dict(row) for row in self._rows):
                    self._trace_stream.write(redact_text(json.dumps(row, sort_keys=True)) + '\n')
        except BaseException as error:
            self._record_cleanup_error('trace_write', error)
        finally:
            try:
                if getattr(self, '_trace_context', None) is not None:
                    self._trace_context.__exit__(None, None, None)
            except BaseException as error:
                self._record_cleanup_error('trace_close', error)
        if self.first_error is not None:
            error = self.first_error
            try:
                write_private_json(self.trace_path.with_suffix('.first-error.json'), {
                    'query_id': self.query_id, 'request_sequence': error.sequence,
                    'status': error.status, 'headers': error.headers, 'phase': error.phase,
                    'observed_ns': error.observed_ns,
                    'body_base64': base64.b64encode(error.body).decode('ascii'),
                    'cleanup_errors': list(self.cleanup_errors),
                    'service_thread_alive': self.summary['service_thread_alive'],
                    'io_loop_closed': self.summary['io_loop_closed'],
                })
            except BaseException as error:
                self._record_cleanup_error('first_error_write', error)
        if self.cleanup_errors:
            try:
                write_private_json(self.trace_path.with_suffix('.cleanup.json'), {
                    **self.summary, 'primary_error_type': type(primary if primary is not None
                                                             else self._cleanup_error).__name__,
                })
            except BaseException as error:
                self._record_cleanup_error('cleanup_write', error)
        if self._runtime_stopped() and self._listeners_remaining == 0:
            self._port = None
        failure = primary if primary is not None else self._cleanup_error
        if failure is not None and self.cleanup_errors:
            failure.add_note('Sema cleanup errors: ' + ','.join(self.cleanup_errors))
        if query_error is None and known_http_error is None and failure is not None:
            raise failure

    def _start_runtime(self):
        self._thread = threading.Thread(target=self._run, name='sema-request-service', daemon=True)
        self._thread.start()

    def _runtime_stopped(self):
        return self._thread is None or not self._thread.is_alive()

    def _stop_runtime(self):
        if (self._loop is not None and self._thread is not None and self._thread.is_alive()
                and not self._loop.is_closed() and not self._cleanup_started.is_set()):
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except BaseException as error:
                self._record_cleanup_error('loop_stop', error)
        if self._thread is not None:
            try:
                self._thread.join(self.limits.timeout_s + 5)
            except BaseException as error:
                self._record_cleanup_error('thread_wait', error)
            if self._thread.is_alive():
                self._record_cleanup_error('thread_wait',
                    TimeoutError('Sema request service cleanup did not settle'))

    async def _forward(self, body, headers, row, client):
        if self._halted.is_set():
            raise RuntimeError('Sema query has stopped')
        if self.before_post is not None:
            self.before_post(body)
        if self._halted.is_set():
            raise RuntimeError('Sema query has stopped')
        row['forward_started_ns'] = time.monotonic_ns()
        self._forwarded += 1
        trace = {'trace_request_ctx': row} if self.upstream_concurrency else {}
        async with client.post(self.upstream_url, data=body, headers=headers,
                               allow_redirects=False, **trace) as response:
            value = bytearray()
            async for chunk in response.content.iter_chunked(4096):
                if len(value) + len(chunk) > self.limits.response_bytes:
                    raise ValueError('Sema response exceeds the service limit')
                value.extend(chunk)
            row['model_returned_ns'] = time.monotonic_ns()
            return _Reply(response.status, bytes(value), tuple(
                (k, v) for k, v in response.headers.items() if k.lower() not in _HOP_HEADERS))

    async def _initialize_executor(self):
        pass

    def _connection_traces(self):
        if not self.upstream_concurrency:
            return []
        from aiohttp import TraceConfig
        trace = TraceConfig()

        async def queued_start(_session, context, _params):
            context.trace_request_ctx['connector_wait_started_ns'] = time.monotonic_ns()

        async def check_stopped(_session, _context, _params):
            if self._halted.is_set():
                raise RuntimeError('Sema query stopped before upstream connection acquisition')

        async def queued_end(_session, context, _params):
            context.trace_request_ctx['connector_wait_ended_ns'] = time.monotonic_ns()
            await check_stopped(_session, context, _params)

        trace.on_connection_queued_start.append(queued_start)
        trace.on_connection_queued_end.append(queued_end)
        trace.on_connection_create_start.append(check_stopped)
        return [trace]

    async def _close_executor(self):
        pass

    def _release_response(self, row):
        pass

    async def _close_http_runner(self, runner):
        try:
            await runner.cleanup()
        except BaseException as error:
            self._record_cleanup_error('runner_cleanup', error)
            # cleanup() may stop at the first failed site. Use public lifecycle
            # operations to stop the remaining listeners and settle response owners.
            for site in tuple(runner.sites):
                try:
                    await site.stop()
                except BaseException as error:
                    self._record_cleanup_error('site_stop', error)
            server = runner.server
            if server is not None:
                try:
                    await runner.shutdown()
                except BaseException as error:
                    self._record_cleanup_error('runner_shutdown', error)
                try:
                    await server.shutdown(self.limits.timeout_s)
                except BaseException as error:
                    self._record_cleanup_error('http_connections_close', error)
                try:
                    await runner.cleanup()
                except BaseException as error:
                    self._record_cleanup_error('runner_cleanup_retry', error)
        # Server.shutdown can schedule handler cancellation just before returning.
        # Observe the handlers' finally blocks before touching their Core leases.
        await asyncio.sleep(0)
        if self._http_handlers:
            tasks = tuple(self._http_handlers)
            for task in tasks:
                task.cancel()
            _done, pending = await asyncio.wait(tasks, timeout=self.limits.timeout_s)
            if pending:
                self._record_cleanup_error('http_handlers_close',
                    TimeoutError('Sema HTTP response owners did not settle'))
        self._listeners_remaining = len(runner.sites)
        self._http_connections_remaining = 0 if runner.server is None else len(runner.server.connections)

    async def _initialize_http(self):
        from aiohttp import ClientSession, ClientTimeout, TCPConnector, web
        runner = client = None

        async def handle_request(request):
            if request.match_info['query_id'] != self.query_id:
                return web.Response(status=404, body=b'{"error":"unknown Sema query"}')
            sequence = self._sequence
            self._sequence += 1
            row = {'query_id': self.query_id, 'request_sequence': sequence, 'mode': self.mode,
                   'native_task_ready_ns': None, 'native_task_ready_status': 'unavailable',
                   'proxy_arrived_ns': time.monotonic_ns(), 'body_read_ns': None,
                   'forward_started_ns': None, 'model_returned_ns': None,
                   'response_written_ns': None, 'request_body_sha256': None,
                   'response_body_sha256': None, 'http_status': None, 'error_type': None}
            # Successful/model attempts and the first rejection fit the declared run size.
            if len(self._rows) <= self.limits.max_requests:
                self._rows.append(row)
            if self._halted.is_set() or sequence >= self.limits.max_requests:
                self._rejected += 1
                if sequence >= self.limits.max_requests and not self._halted.is_set():
                    self._fail(sequence, _Reply(429, b'{"error":"Sema run request limit reached"}', ()),
                               'request_limit')
                return web.Response(status=410, body=b'{"error":"Sema query is not accepting requests"}')
            try:
                body = await request.read()
                row['body_read_ns'] = time.monotonic_ns()
                row['request_body_sha256'] = hashlib.sha256(body).hexdigest()
                headers = {k: v for k, v in request.headers.items() if k.lower() not in _HOP_HEADERS}
                reply = await self._forward(body, headers, row, client)
                if not 200 <= reply.status < 300:
                    self._fail(sequence, reply, 'http_response')
            except asyncio.CancelledError:
                row['error_type'] = 'CancelledError'
                self.cancel()
                self._release_response(row)
                raise
            except Exception as error:
                row['error_type'] = type(error).__name__
                reply = _Reply(502, b'{"error":"Sema request transport failed"}',
                               (('Content-Type', 'application/json'),))
                self._fail(sequence, reply, 'transport')
            row['http_status'] = reply.status
            row['response_body_sha256'] = hashlib.sha256(reply.body).hexdigest()
            response = web.Response(status=reply.status, body=reply.body, headers=reply.headers)
            try:
                await response.prepare(request)
                await response.write_eof()
                row['response_written_ns'] = time.monotonic_ns()
            except Exception as error:
                row['error_type'] = type(error).__name__
                self._fail(sequence, reply, 'response_write')
                raise
            finally:
                self._release_response(row)
            return response

        async def handle(request):
            task = asyncio.current_task()
            self._http_handlers.add(task)
            try:
                return await handle_request(request)
            finally:
                self._http_handlers.discard(task)

        async def initialize():
            nonlocal client, runner
            client = ClientSession(connector=TCPConnector(limit=self.upstream_concurrency,
                                                         limit_per_host=self.upstream_concurrency),
                                   timeout=ClientTimeout(total=self.limits.timeout_s),
                                   auto_decompress=False, trust_env=False,
                                   trace_configs=self._connection_traces())
            self._client = client
            await self._initialize_executor()
            app = web.Application(client_max_size=self.limits.request_bytes)
            app.router.add_post('/sema/{query_id}/v1/chat/completions', handle)
            runner = web.AppRunner(app, access_log=None, shutdown_timeout=self.limits.timeout_s)
            self._runner = runner
            await runner.setup()
            site = web.TCPSite(runner, '127.0.0.1', 0)
            await site.start()
            self._port = site._server.sockets[0].getsockname()[1]

        await initialize()

    async def _shutdown_http(self):
        self._cleanup_started.set()
        runner, client = self._runner, getattr(self, '_client', None)
        if runner is not None:
            try:
                await self._close_http_runner(runner)
            except BaseException as error:
                self._record_cleanup_error('runner_cleanup', error)
        else:
            self._listeners_remaining = self._http_connections_remaining = 0
        if not self._http_handlers:
            try:
                await self._close_executor()
            except BaseException as error:
                self._record_cleanup_error('executor_close', error)
        else:
            self._record_cleanup_error('executor_close',
                RuntimeError('Sema HTTP response owners still hold execution results'))
        if client is not None:
            try:
                await client.close()
            except BaseException as error:
                self._record_cleanup_error('client_close', error)

    def _run(self):
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(self._initialize_http())
            self._ready.set()
            loop.run_forever()
        except BaseException as error:
            self._startup_error = error
        finally:
            self._cleanup_started.set()
            self._ready.set()
            loop.run_until_complete(self._shutdown_http())
            if not self._http_handlers and self._http_connections_remaining == 0:
                try:
                    loop.close()
                except BaseException as error:
                    self._record_cleanup_error('loop_close', error)
            else:
                self._record_cleanup_error('loop_close',
                    RuntimeError('Sema HTTP ownership remains unresolved; loop retained'))


class SemaProjection:
    """Single-query lifecycle around the existing prepared author process."""

    def __init__(self, native, service, preparation_started_ns):
        self.native, self.service = native, service
        self.preparation_started_ns = preparation_started_ns
        self.ready_ns = time.monotonic_ns()
        self.submitted_ns = self.finished_ns = None
        self.status = 'ready'

    @property
    def pid(self):
        return self.native.pid

    @property
    def returncode(self):
        return self.native.returncode

    @property
    def summary(self):
        return {'status': self.status, 'preparation_started_ns': self.preparation_started_ns,
                'native_ready_ns': self.ready_ns, 'native_sql_submitted_ns': self.submitted_ns,
                'native_query_finished_ns': self.finished_ns,
                'native_query_elapsed_ns': (None if self.finished_ns is None else
                                            self.finished_ns - self.submitted_ns),
                'full_query_elapsed_ns': (None if self.finished_ns is None else
                                          self.finished_ns - self.preparation_started_ns),
                'request_service': None if self.service is None else self.service.summary}

    def execute(self):
        if self.status != 'ready':
            raise RuntimeError('Sema projection has already been submitted or stopped')
        self.status = 'running'
        self.submitted_ns = time.monotonic_ns()
        completed = False
        try:
            yield from self.native.execute()
            if self.service is not None and self.service.first_error is not None:
                raise self.service.first_error
            completed = True
            self.status = 'completed'
            if self.service is not None:
                self.service.end_input()
        except Exception:
            if self.status != 'cancelled':
                self.status = 'failed'
            if self.service is not None and self.service.first_error is not None:
                raise self.service.first_error
            raise
        finally:
            self.finished_ns = time.monotonic_ns()
            if not completed:
                self.cancel()

    def cancel(self):
        if self.status not in ('completed', 'failed'):
            self.status = 'cancelled'
        if self.service is not None:
            self.service.cancel()
        else:
            self.native.cancel()


@contextmanager
def prepare_sema_projection(values, plan, model, *, binary, root, num_threads, service=None, native=None):
    """Reuse the pinned CLI lifecycle for direct or explicitly routed requests."""
    preparation_started_ns = time.monotonic_ns()
    routed = model if service is None else replace(model, endpoint_url=service.endpoint_url)
    borrowed = native is not None
    if borrowed:
        native.replace_source(values, root, routed.endpoint_url)
    context = nullcontext(native) if borrowed else prepare_projection(
        values, plan, routed, binary=binary, root=root, num_threads=num_threads)
    with context as native:
        if service is not None:
            service.bind_native_cancel(native.cancel)
        query = SemaProjection(native, service, preparation_started_ns)
        try:
            yield query
        finally:
            if query.status != 'completed':
                query.cancel()
            elif borrowed and service is not None:
                service.unbind_native_cancel(native.cancel)
