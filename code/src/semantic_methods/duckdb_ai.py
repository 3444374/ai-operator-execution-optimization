"""Whole-vector duckdb-ai method adapter; execution is owned by the supplied batch entry."""
from __future__ import annotations

import ctypes as ct
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

from src.baselines.common.redact import redact_text

MAX_BATCH_BYTES = 32 * 1024 * 1024
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class DuckDBCall:
    row: int
    query_id: str
    call_id: str
    model: str
    endpoint: str
    payload: bytes
    headers: tuple[str, ...]
    estimated_tokens: int
    timeout_seconds: int
    connect_timeout_seconds: int
    ready_ns: int


@dataclass(frozen=True)
class DuckDBResponse:
    call_id: str
    body: bytes
    http_status: int
    elapsed_ms: int
    transport_error: str = ''


class _Call(ct.Structure):
    _fields_ = [('row', ct.c_uint64), ('query_id', ct.c_char_p), ('call_id', ct.c_char_p),
                ('model', ct.c_char_p), ('endpoint', ct.c_char_p), ('payload', ct.c_void_p),
                ('payload_size', ct.c_uint64), ('headers', ct.POINTER(ct.c_char_p)),
                ('header_count', ct.c_uint64), ('estimated_tokens', ct.c_int64),
                ('timeout_seconds', ct.c_int64), ('connect_timeout_seconds', ct.c_int64),
                ('ready_ns', ct.c_int64)]


class _Response(ct.Structure):
    _fields_ = [('row', ct.c_uint64), ('body', ct.c_void_p), ('body_size', ct.c_uint64),
                ('http_status', ct.c_int64), ('elapsed_ms', ct.c_int64), ('error', ct.c_void_p),
                ('error_size', ct.c_uint64)]


_Cancelled = ct.CFUNCTYPE(ct.c_int, ct.c_void_p)
_Consume = ct.CFUNCTYPE(ct.c_int, ct.c_uint64, ct.POINTER(_Response), ct.c_void_p)
_Batch = ct.CFUNCTYPE(ct.c_int, ct.c_uint64, ct.POINTER(_Call), ct.c_uint64,
                     ct.POINTER(_Response), _Cancelled, _Consume, ct.c_void_p)
_Release = ct.CFUNCTYPE(None, ct.c_uint64)
# C++ borrows callbacks. Keep their Python owners alive until explicit unregister.
_registered: dict[str, 'DuckDBSemLoomBridge'] = {}
_registry_lock = threading.Lock()


class DuckDBSemLoomBridge:
    """Register a trusted synchronous finite-batch entry after LOAD of the patched binary.

    One bridge owns each loaded extension in a process. The entry must handle cancellation,
    preserve complete responses, and return one response per call_id. It must not invoke
    DuckDB SQL or install another per-row request executor. No payload is logged here.
    """
    def __init__(self, extension: str | Path,
                 execute: Callable[[tuple[DuckDBCall, ...], Callable[[], bool]], Iterable[DuckDBResponse]]):
        self.path = str(Path(extension).resolve())
        self.execute = execute
        self.last_error: str | None = None
        self.last_cleanup_error: str | None = None
        self.batch_sizes: list[int] = []
        self._buffers: dict[int, list] = {}
        self._lock = threading.Lock()
        self._closed = False
        self.library = ct.CDLL(self.path)
        self.library.duckdb_ai_semloom_abi_v1.restype = ct.c_int
        if self.library.duckdb_ai_semloom_abi_v1() != 1:
            raise RuntimeError('unsupported DuckDB SemLoom batch ABI')
        self.library.duckdb_ai_semloom_register_v1.argtypes = [_Batch, _Release]
        self.library.duckdb_ai_semloom_register_v1.restype = ct.c_int
        self.library.duckdb_ai_semloom_unregister_v1.argtypes = [_Batch]
        self.library.duckdb_ai_semloom_unregister_v1.restype = ct.c_int
        self._callback = _Batch(self._dispatch)
        self._release_callback = _Release(self._release)
        with _registry_lock:
            if self.path in _registered or not self.library.duckdb_ai_semloom_register_v1(
                    self._callback, self._release_callback):
                raise RuntimeError('DuckDB SemLoom callback is already owned or active')
            _registered[self.path] = self

    @property
    def retained_batches(self) -> int:
        with self._lock:
            return len(self._buffers)

    def _release(self, batch_id: int) -> None:
        with self._lock:
            self._buffers.pop(batch_id, None)

    def _dispatch(self, batch_id, raw_calls, count, raw_responses, is_cancelled, consume, context) -> int:
        cancelled = lambda: bool(is_cancelled(context))
        buffers: list = []
        iterator = None
        status = 0
        primary_error = cleanup_error = None
        with self._lock:
            self.last_error = self.last_cleanup_error = None
            self._buffers[batch_id] = buffers
            self.batch_sizes.append(count)
            # Bounded diagnostic history, independent of query size.
            del self.batch_sizes[:-64]
        try:
            if cancelled():
                status = 1
            else:
                calls = tuple(DuckDBCall(
                    row=item.row, query_id=item.query_id.decode(), call_id=item.call_id.decode(),
                    model=item.model.decode(), endpoint=item.endpoint.decode(),
                    payload=ct.string_at(item.payload, item.payload_size),
                    headers=tuple(item.headers[j].decode() for j in range(item.header_count)),
                    estimated_tokens=item.estimated_tokens, timeout_seconds=item.timeout_seconds,
                    connect_timeout_seconds=item.connect_timeout_seconds, ready_ns=item.ready_ns,
                ) for item in (raw_calls[i] for i in range(count)))
                if len({call.call_id for call in calls}) != count:
                    raise ValueError('DuckDB batch repeats a call identity')
                positions = {call.call_id: i for i, call in enumerate(calls)}
                received: set[str] = set()
                result_bytes = 0
                iterator = iter(self.execute(calls, cancelled))
                for response in iterator:
                    if response.call_id not in positions or response.call_id in received:
                        raise ValueError('DuckDB batch response repeats or changes call identity')
                    if not isinstance(response.body, bytes):
                        raise TypeError('DuckDB batch response body must be bytes')
                    error = response.transport_error.encode()
                    result_bytes += len(response.body) + len(error)
                    if (len(response.body) > MAX_RESPONSE_BYTES or len(error) > MAX_RESPONSE_BYTES
                            or result_bytes > MAX_BATCH_BYTES):
                        raise ValueError('DuckDB batch response exceeds its byte limit')
                    if not 0 <= response.http_status <= 599 or response.elapsed_ms < -1:
                        raise ValueError('DuckDB batch response has invalid status or elapsed time')
                    received.add(response.call_id)
                    body_buffer = ct.create_string_buffer(response.body)
                    error_buffer = ct.create_string_buffer(error)
                    buffers.extend([body_buffer, error_buffer])
                    index = positions[response.call_id]
                    raw_responses[index] = _Response(calls[index].row, ct.addressof(body_buffer), len(response.body),
                                                    response.http_status, response.elapsed_ms,
                                                    ct.addressof(error_buffer), len(error))
                    parsed = consume(index, ct.byref(raw_responses[index]), context)
                    if parsed == 1:
                        status = 3
                        break
                    if parsed != 0:
                        raise ValueError('DuckDB native response consumer rejected metadata')
                if status == 0:
                    if cancelled():
                        status = 1
                    elif len(received) != count:
                        raise ValueError('DuckDB batch response is incomplete')
        except BaseException as error:
            primary_error = redact_text(f'{type(error).__name__}: {error}')[:4096]
            status = 1 if cancelled() else 2
        finally:
            if iterator is not None:
                try:
                    close = getattr(iterator, 'close', None)
                    if close is not None:
                        close()
                except BaseException as error:
                    cleanup_error = redact_text(f'{type(error).__name__}: {error}')[:4096]
        if status == 0 and cleanup_error is not None:
            primary_error = cleanup_error
            status = 2
        with self._lock:
            self.last_error = primary_error
            self.last_cleanup_error = cleanup_error
        return status

    def enable(self, connection) -> None:
        if self._closed:
            raise RuntimeError('DuckDB SemLoom bridge is closed')
        connection.execute("SET duckdb_ai_executor = 'semloom'")

    def close(self) -> None:
        with _registry_lock:
            if self._closed:
                return
            if not self.library.duckdb_ai_semloom_unregister_v1(self._callback):
                raise RuntimeError('DuckDB SemLoom bridge still has an active batch')
            _registered.pop(self.path, None)
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class DuckDBNativeTaskExecutor:
    """Thin finite-batch consumer of the common session API, with no request pool.

    The supplied execution owns its Engine, organization and Daft/Ray transport.
    The service owner creates and closes execution. This adapter releases each delivery
    after copying its full response into the native vector's bounded result storage.
    """
    def __init__(self, execution, config, *, describe_work=None):
        self.execution, self.config = execution, config
        self.describe_work = describe_work
        self.last_close_report = None
        self.last_cleanup_error = None

    def __call__(self, calls: tuple[DuckDBCall, ...], cancelled: Callable[[], bool]):
        from src.execution_provider.adapters.native_tasks import NativeTaskSession, prepare_native_task
        from src.execution_provider.adapters.full_response import decode_full_response
        from src.scheduling.core.session_contract import State

        if not calls:
            return
        # The fixed transport uses these identities and bearer header, never provider guesses.
        for call in calls:
            expected_auth = 'Authorization: Bearer ' + self.config.bearer_token if self.config.bearer_token else None
            auth = next((h for h in call.headers if h.lower().startswith('authorization:')), None)
            if (call.model != self.config.model_id or call.endpoint != self.config.endpoint_url
                    or call.timeout_seconds * 1000 != self.config.timeout_ms or auth != expected_auth
                    or call.connect_timeout_seconds != call.timeout_seconds):
                raise ValueError('DuckDB prepared call differs from the common fixed transport')
        flow = NativeTaskSession(self.execution, 'duckdb:' + calls[0].call_id, 'duckdb-ai-map')
        offset = 0
        pending = ()
        sealed = False
        finished = False
        try:
            while True:
                if cancelled():
                    flow.request_cancel()
                    raise InterruptedError('DuckDB query cancelled')
                if offset < len(calls):
                    stop = min(len(calls), offset + flow.limits.offer_tasks)
                    # Keep rejected tasks and their work description; only fill new suffix positions.
                    first_new = offset + len(pending)
                    pending += tuple(prepare_native_task(
                        call.payload, i, row_sequence=call.row, call_id=call.call_id,
                        work=self.describe_work(call) if self.describe_work else None,
                        max_result_bytes=flow.limits.item_result_bytes,
                    ) for i, call in enumerate(calls[first_new:stop], first_new))
                    result = flow.offer(pending)
                    if result.status not in ('ACCEPTED', 'BACKPRESSURE'):
                        raise RuntimeError('DuckDB task offer rejected: ' + result.reason)
                    offset += result.accepted_prefix_count
                    pending = pending[result.accepted_prefix_count:]
                if offset == len(calls) and not sealed:
                    flow.end_input()
                    sealed = True
                progress = flow.advance(flow.limits.offer_tasks)
                for delivery in progress.deliveries:
                    try:
                        raw = decode_full_response(delivery.result)
                        call = calls[delivery.key.sequence]
                        if delivery.info.call_id != call.call_id or delivery.info.row_sequence != call.row:
                            raise ValueError('DuckDB Core delivery changed row association')
                        response = DuckDBResponse(call.call_id, raw.body, raw.status_code, -1)
                    finally:
                        flow.release((delivery.lease_id,))
                    # -1 preserves unavailable HTTP-only elapsed time. It is never queue time or zero.
                    yield response
                if progress.state == State.FINISHED:
                    finished = True
                    return
                if progress.state in (State.FAILED, State.CANCELLED):
                    raise RuntimeError('DuckDB common execution stopped: ' + str(progress.error))
                flow.wait(progress, min(0.01, flow.limits.poll_interval_s))
        finally:
            primary = sys.exc_info()[1]
            try:
                self.last_close_report = flow.close(clean=finished)
            except BaseException as error:
                if primary is None:
                    raise
                self.last_cleanup_error = redact_text(f'{type(error).__name__}: {error}')[:4096]
