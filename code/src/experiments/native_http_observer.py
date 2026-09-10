"""Observe native aiohttp worker POSTs without adding a scheduler or retries.

Each worker session gets a private bounded event writer. Shared budget spending
is durable before network I/O; its measured cost remains inside query time.
"""
from dataclasses import dataclass
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import time
import uuid

from .buffered_events import BufferedEvents
from src.baselines.common.private_artifacts import content_digest


@contextmanager
def observe_native_httpx(budget, record, endpoint, *, timeout_s=None):
    """Account native SDK worker sends in an isolated process, including retries."""
    import httpx
    sync_send, async_send = httpx.Client.send, httpx.AsyncClient.send
    deadline = time.monotonic()+timeout_s if timeout_s is not None else None
    if getattr(sync_send, '_native_query_observer', False):
        raise RuntimeError('nested native HTTP observers are unsupported')

    def before(request):
        if request.method.upper() != 'POST':
            return None
        if str(request.url) != endpoint:
            raise ValueError('native SDK attempted an undeclared POST destination')
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError('native query deadline reached before POST')
        payload = request.content
        started = time.monotonic_ns()
        attempt = budget.reserve(hashlib.sha256(payload).hexdigest())
        if deadline is not None:
            remaining=deadline-time.monotonic()
            if remaining <= 0:
                raise TimeoutError('native query deadline reached during budget reservation')
            current=request.extensions.get('timeout',{})
            request.extensions['timeout']={name:min(remaining,current.get(name) or remaining)
                for name in ('connect','read','write','pool')}
        values = json.loads(payload)
        record(dict(event='request',attempt=attempt,monotonic_ns=time.monotonic_ns(),
            budget_reserve_ns=time.monotonic_ns()-started,request_values_sha256=content_digest(values),
            request_bytes_sha256=hashlib.sha256(payload).hexdigest(),body=values))
        record(dict(event='http_started',attempt=attempt,monotonic_ns=time.monotonic_ns()))
        return attempt

    def finished(attempt, response, error):
        if attempt is None:
            return
        if response is not None and error is None:
            try:
                value = response.json()
            except ValueError:
                value = None
            record(dict(event='http_response',attempt=attempt,status=response.status_code,
                        response=value,monotonic_ns=time.monotonic_ns()))
        record(dict(event='http_finished',attempt=attempt,monotonic_ns=time.monotonic_ns(),
                    error_type=type(error).__name__ if error is not None else None))

    def finish_safely(attempt, response, error):
        try:
            finished(attempt,response,error)
        except BaseException as failure:
            if error is None:
                raise
            error.add_note('Native HTTP observation also failed: '+type(failure).__name__)

    def sync(client, request, *args, **kwargs):
        if kwargs.get('stream'):
            raise ValueError('native SDK observation requires a non-streaming HTTP response')
        attempt = before(request)
        response = error = None
        try:
            kwargs['follow_redirects'] = False
            response = sync_send(client,request,*args,**kwargs)
            response.read()
            return response
        except BaseException as failure:
            error = failure
            raise
        finally:
            finish_safely(attempt,response,error)

    async def asynchronous(client, request, *args, **kwargs):
        if kwargs.get('stream'):
            raise ValueError('native SDK observation requires a non-streaming HTTP response')
        attempt = before(request)
        response = error = None
        try:
            kwargs['follow_redirects'] = False
            response = await async_send(client,request,*args,**kwargs)
            await response.aread()
            return response
        except BaseException as failure:
            error = failure
            raise
        finally:
            finish_safely(attempt,response,error)

    sync._native_query_observer = True
    httpx.Client.send, httpx.AsyncClient.send = sync, asynchronous
    try:
        yield
    finally:
        httpx.Client.send, httpx.AsyncClient.send = sync_send, async_send


@dataclass(frozen=True)
class NativeSessionFactory:
    budget: object
    events_directory: str
    endpoint: str
    timeout_s: float

    def __call__(self):
        return ObservedSession(self)


class ObservedSession:
    def __init__(self, config):
        self.config = config
        self.session = self.events = None

    async def __aenter__(self):
        import aiohttp
        self.events = BufferedEvents(Path(self.config.events_directory) / (uuid.uuid4().hex + '.jsonl'))
        try:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.timeout_s))
        except BaseException:
            self.events.close()
            raise
        return self

    async def __aexit__(self, *_):
        try:
            await self.session.close()
        finally:
            self.events.close()

    async def post(self, url, *, data, headers):
        if url != self.config.endpoint or not isinstance(data, str):
            raise ValueError('native request does not match the declared endpoint/body')
        payload = data.encode('utf-8')
        before = time.monotonic_ns()
        attempt = self.config.budget.reserve(hashlib.sha256(payload).hexdigest())
        reserved = time.monotonic_ns()
        self.events.record(dict(event='request', attempt=attempt, monotonic_ns=reserved,
            budget_reserve_ns=reserved-before, request_bytes_sha256=hashlib.sha256(payload).hexdigest(),
            request_values_sha256=content_digest(json.loads(data)), body=json.loads(data)))
        self.events.record(dict(event='http_started', attempt=attempt, monotonic_ns=time.monotonic_ns()))
        try:
            response = await self.session.post(url, data=data, headers=headers, allow_redirects=False)
            return ObservedResponse(response, self.events, attempt)
        except BaseException as error:
            self.events.record(dict(event='http_finished', attempt=attempt, monotonic_ns=time.monotonic_ns(),
                                    error_type=type(error).__name__))
            raise


class ObservedResponse:
    def __init__(self, response, events, attempt):
        self.response, self.events, self.attempt = response, events, attempt

    @property
    def status(self):
        return self.response.status

    @property
    def reason(self):
        return self.response.reason

    async def __aenter__(self):
        await self.response.__aenter__()
        return self

    async def __aexit__(self, *error):
        try:
            return await self.response.__aexit__(*error)
        finally:
            self.events.record(dict(event='http_finished', attempt=self.attempt,
                                    monotonic_ns=time.monotonic_ns(), status=self.status))

    async def json(self):
        value = await self.response.json()
        self.events.record(dict(event='http_response', attempt=self.attempt,
                                monotonic_ns=time.monotonic_ns(), response=value))
        return value
