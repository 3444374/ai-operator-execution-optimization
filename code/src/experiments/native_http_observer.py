"""Observe native aiohttp worker POSTs without adding a scheduler or retries.

Each worker session gets a private bounded event writer. Ordinary accounting
commits before sends; mapped descriptors register inside a durably prepaid unit.
Preparation, send-time accounting and cleanup are measured separately.
"""
from dataclasses import dataclass
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import time
import uuid

from .buffered_events import BufferedEvents
from .request_identity import RAY_IDENTITY_FIELD
from .request_budget_client import open_request_budget
from src.baselines.common.private_artifacts import content_digest
from src.observability.httpx_observer import _observe_native_httpx_client


@contextmanager
def observe_native_httpx(budget, record, endpoint, *, timeout_s=None):
    """Account native SDK worker sends in an isolated process, including retries."""
    with open_request_budget(budget) as client:
        with _observe_native_httpx_client(client, record, endpoint, timeout_s=timeout_s):
            yield


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
        self.budget = self._budget_scope = None

    async def __aenter__(self):
        import aiohttp
        self.events = BufferedEvents(Path(self.config.events_directory) / (uuid.uuid4().hex + '.jsonl'))
        try:
            scope = open_request_budget(self.config.budget)
            self.budget = scope.__enter__()
            self._budget_scope = scope
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.timeout_s))
        except BaseException as error:
            try:
                self.events.close()
            except BaseException as failure:
                error.add_note('Native event cleanup also failed: ' + type(failure).__name__)
            if self._budget_scope is not None:
                self._budget_scope.__exit__(type(error), error, error.__traceback__)
            raise
        return self

    async def __aexit__(self, *error):
        failure = error[1]
        try:
            await self.session.close()
        except BaseException as closing:
            failure = closing if failure is None else failure
            if failure is not closing:
                failure.add_note('Native session cleanup also failed: ' + type(closing).__name__)
        try:
            self.events.close()
        except BaseException as closing:
            failure = closing if failure is None else failure
            if failure is not closing:
                failure.add_note('Native event cleanup also failed: ' + type(closing).__name__)
        self._budget_scope.__exit__(type(failure) if failure else None, failure,
                                   failure.__traceback__ if failure else None)
        if failure is not None and failure is not error[1]:
            raise failure

    async def post(self, url, *, data, headers):
        if url != self.config.endpoint or not isinstance(data, str):
            raise ValueError('native request does not match the declared endpoint/body')
        values = json.loads(data)
        identity = values.pop(RAY_IDENTITY_FIELD, None)
        if not isinstance(identity, dict) or set(identity) != {'row_id', 'source_position'}:
            raise ValueError('native Ray request lacks its input occurrence identity')
        data = json.dumps(values, ensure_ascii=False, separators=(',', ':'))
        payload = data.encode('utf-8')
        before = time.monotonic_ns()
        attempt = self.budget.reserve(hashlib.sha256(payload).hexdigest())
        reserved = time.monotonic_ns()
        self.events.record(dict(event='request', attempt=attempt, identity=identity, monotonic_ns=reserved,
            budget_reserve_ns=reserved-before, request_bytes_sha256=hashlib.sha256(payload).hexdigest(),
            request_values_sha256=content_digest(json.loads(data)), body=json.loads(data)))
        self.events.record(dict(event='http_started', attempt=attempt, monotonic_ns=time.monotonic_ns()))
        try:
            response = await self.session.post(url, data=data, headers=headers, allow_redirects=False)
            return ObservedResponse(response, self.events, attempt, identity)
        except BaseException as error:
            self.events.record(dict(event='http_finished', attempt=attempt, monotonic_ns=time.monotonic_ns(),
                                    error_type=type(error).__name__))
            raise


class ObservedResponse:
    def __init__(self, response, events, attempt, identity):
        self.response, self.events, self.attempt = response, events, attempt
        self.identity = identity

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
        if not isinstance(value, dict) or RAY_IDENTITY_FIELD in value:
            raise ValueError('native response conflicts with observation metadata')
        return dict(value, **{RAY_IDENTITY_FIELD: self.identity})
