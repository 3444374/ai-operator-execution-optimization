"""Observe native HTTPX requests using caller-owned accounting and events."""

from contextlib import contextmanager

from src.observability.httpx_observer import _observe_native_httpx_client
from .request_budget_client import open_request_budget


@contextmanager
def observe_native_httpx(budget, record, endpoint, *, timeout_s=None):
    """Account native SDK worker sends in an isolated process, including retries."""
    with open_request_budget(budget) as client:
        with _observe_native_httpx_client(client, record, endpoint, timeout_s=timeout_s):
            yield
