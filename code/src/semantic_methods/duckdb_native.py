"""DuckDB's prepared vector calls executed by a vendor-owned native graph."""
from __future__ import annotations

import math
from typing import Callable

from src.baselines.common.redact import redact_text
from .duckdb_ai import DuckDBCall, DuckDBResponse, DuckDBSemLoomBridge, MAX_RESPONSE_BYTES


class DuckDBNativeExecutor:
    """Convert complete calls and responses without changing DuckDB's parser.

    The supplied executor uses Daft Native, Ray Core, Ray Data or author IMLane.
    Closing this iterator closes that graph; no SemLoom execution is created.
    """

    def __init__(self, executor, *, timeout_s: float = 120):
        if executor.backend not in ('daft', 'ray', 'ray-data', 'imlane'):
            raise ValueError('DuckDB native execution requires daft, ray, ray-data or imlane')
        if type(timeout_s) not in (int, float) or not math.isfinite(timeout_s) or not 0 < timeout_s <= 120:
            raise ValueError('DuckDB native batch timeout must be positive and at most 120 seconds')
        self.executor = executor
        self.backend = executor.backend
        self.timeout_s = timeout_s
        self.last_cleanup_error: str | None = None
        self.last_delivered_count = 0

    def __call__(self, calls: tuple[DuckDBCall, ...], cancelled: Callable[[], bool]):
        from src.baselines.text.frameworks.native_calls import NativeCall

        self.last_cleanup_error = None
        self.last_delivered_count = 0
        if not calls:
            return
        if cancelled():
            raise InterruptedError('DuckDB query cancelled')
        prepared = []
        for call in calls:
            headers = []
            for header in call.headers:
                name, separator, value = header.partition(':')
                if not separator or not name:
                    raise ValueError('DuckDB prepared request has an invalid header')
                headers.append((name, value.lstrip(' \t')))
            prepared.append(NativeCall(
                call_id=call.call_id, row_id=call.row, payload=call.payload,
                endpoint=call.endpoint, headers=tuple(headers), timeout_s=call.timeout_seconds,
                connect_timeout_s=call.connect_timeout_seconds, max_response_bytes=MAX_RESPONSE_BYTES,
                skip_auto_headers=('Accept-Encoding',),
            ))
        positions = {call.call_id: call.row for call in calls}
        if len(positions) != len(calls):
            raise ValueError('DuckDB batch repeats a call identity')
        received: set[str] = set()
        pending: dict[str, DuckDBResponse] = {}
        next_call = 0
        iterator = None
        primary_error = None
        try:
            iterator = self.executor.iter_batch(tuple(prepared), cancelled=cancelled, timeout_s=self.timeout_s)
            for outcome in iterator:
                if cancelled():
                    raise InterruptedError('DuckDB query cancelled')
                if (outcome.call_id not in positions or outcome.call_id in received
                        or outcome.row_id != positions[outcome.call_id]):
                    raise ValueError('DuckDB native response changes or repeats call identity')
                received.add(outcome.call_id)
                if outcome.transport_error:
                    if outcome.response is not None:
                        raise ValueError('DuckDB native response contains both transport error and HTTP response')
                    response = DuckDBResponse(outcome.call_id, b'', 0, -1, outcome.transport_error)
                elif outcome.response is None:
                    raise ValueError('DuckDB native response lacks HTTP response or transport error')
                else:
                    # The C++ FinishCompletion owns JSON, usage, truncation and provider error interpretation.
                    response = DuckDBResponse(outcome.call_id, outcome.response.body,
                                              outcome.response.status_code, -1)
                pending[outcome.call_id] = response
                # Native workers parse on completion, but AiCompletionFunction
                # selects the first failing input job after joining its workers.
                # Restoring this order here keeps that final exception selection.
                while next_call < len(calls) and calls[next_call].call_id in pending:
                    if cancelled():
                        raise InterruptedError('DuckDB query cancelled')
                    response = pending.pop(calls[next_call].call_id)
                    next_call += 1
                    self.last_delivered_count += 1
                    yield response
            if len(received) != len(calls) or pending or next_call != len(calls):
                raise ValueError('DuckDB native response is incomplete')
        except GeneratorExit:
            raise
        except BaseException as error:
            primary_error = error
            raise
        finally:
            if iterator is not None:
                try:
                    iterator.close()
                except BaseException as error:
                    self.last_cleanup_error = redact_text(f'{type(error).__name__}: {error}')[:4096]
                    if primary_error is None:
                        raise


class DuckDBNativeBridge(DuckDBSemLoomBridge):
    """Register the named native batch executor through the existing complete-batch ABI."""

    def __init__(self, extension, executor: DuckDBNativeExecutor, *, collect_timings=False):
        super().__init__(extension, executor, executor=executor.backend, collect_timings=collect_timings)
