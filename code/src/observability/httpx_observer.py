"""Observe complete HTTPX POSTs using a caller-supplied request allowance."""
from contextlib import contextmanager
import hashlib
import json
import time

from src.baselines.common.private_artifacts import content_digest


@contextmanager
def _observe_native_httpx_client(budget, record, endpoint, *, timeout_s=None):
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
