"""DuckDB's prepared vector calls executed by a vendor-owned native graph."""
from __future__ import annotations

import math
from dataclasses import replace
from typing import Callable

from src.baselines.common.redact import redact_text
from src.baselines.text.frameworks.native_calls import NativeCallOutcome, outcome_kind, raise_execution_error
from .duckdb_ai import (
    DuckDBCall, DuckDBResponse, DuckDBSemLoomBridge,
    MAX_BATCH_ROWS, MAX_BATCH_BYTES, MAX_RESPONSE_BYTES,
)


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
        max_rows = min(MAX_BATCH_ROWS, getattr(self.executor, 'max_batch_rows', MAX_BATCH_ROWS))
        native_bytes = getattr(self.executor, 'max_batch_bytes', MAX_BATCH_BYTES)
        result_limit = min(MAX_BATCH_BYTES, native_bytes)
        if len(calls) > max_rows:
            raise ValueError('DuckDB native input exceeds the selected batch row allowance')
        positions = {call.call_id: call.row for call in calls}
        if len(positions) != len(calls):
            raise ValueError('DuckDB batch repeats a call identity')
        prepared = []
        payload_bytes = input_bytes = 0
        for call in calls:
            payload_bytes += len(call.payload)
            input_bytes += len(call.payload) + sum(len(header.encode()) for header in call.headers)
            if input_bytes > MAX_BATCH_BYTES or payload_bytes > native_bytes:
                raise ValueError('DuckDB native input exceeds the selected batch byte allowance')
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
        received: set[str] = set()
        pending: dict[str, DuckDBResponse] = {}
        result_bytes = 0
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
                normalized = NativeCallOutcome(outcome.call_id, outcome.row_id, outcome.response,
                    outcome.transport_error, getattr(outcome, 'execution_error', ''))
                kind = outcome_kind(normalized)
                if kind == 'execution_error':
                    raise_execution_error(replace(normalized, execution_error=redact_text(normalized.execution_error)))
                if kind == 'transport_error':
                    response = DuckDBResponse(outcome.call_id, b'', 0, -1, normalized.transport_error)
                else:
                    raw = normalized.response
                    if type(raw.body) is not bytes:
                        raise TypeError('DuckDB native response body must be bytes')
                    if type(raw.status_code) is not int or not 100 <= raw.status_code <= 599:
                        raise ValueError('DuckDB native response has invalid HTTP status')
                    # The C++ FinishCompletion owns JSON, usage, truncation and provider error interpretation.
                    response = DuckDBResponse(outcome.call_id, raw.body, raw.status_code, -1)
                result_bytes += len(response.body) + len(response.transport_error.encode())
                # Charge before retaining an out-of-order suffix, and do not reset
                # on delivery: both pending storage and cumulative results are bounded.
                if len(response.body) > MAX_RESPONSE_BYTES or result_bytes > result_limit:
                    raise ValueError('DuckDB native response exceeds its batch byte allowance')
                received.add(outcome.call_id)
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
                cleanup_error = None
                try:
                    iterator.close()
                except BaseException as error:
                    cleanup_error = error
                reported = list(getattr(self.executor, 'last_cleanup_errors', ()))
                if cleanup_error is not None and all(error is not cleanup_error for _, error in reported):
                    reported.append(('close', cleanup_error))
                self.last_cleanup_error = ('\n'.join(
                    redact_text(f'{stage}: {type(error).__name__}: {error}') for stage, error in reported
                )[:4096] or None)
                if cleanup_error is not None:
                    if primary_error is None:
                        raise cleanup_error
                    primary_error.add_note('DuckDB native graph close also failed: ' + type(cleanup_error).__name__)


class DuckDBNativeBridge(DuckDBSemLoomBridge):
    """Register the named native batch executor through the existing complete-batch ABI."""

    def __init__(self, extension, executor: DuckDBNativeExecutor, *, collect_timings=False):
        super().__init__(extension, executor, executor=executor.backend, collect_timings=collect_timings)
