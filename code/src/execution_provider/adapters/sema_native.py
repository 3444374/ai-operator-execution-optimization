"""Already-arrived Sema HTTP calls executed by finite native vendor graphs."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, replace
import threading
import time
import traceback

from ...baselines.text.frameworks.native_calls import NativeCall, NativeCallExecutor, NativeCallOutcome
from ...baselines.common.redact import redact_text
from .full_response import FullModelResponse, encode_full_response
from .sema_service import SemaRequestService, _HOP_HEADERS, _Reply


_SNAPSHOT_BYTES = 32 * 1024 * 1024
_TRANSPORT_REPLY = _Reply(502, b'{"error":"Sema request transport failed"}',
                          (('Content-Type', 'application/json'),))


@dataclass
class _Received:
    call: NativeCall
    row: dict
    future: asyncio.Future
    deadline: float


class SemaNativeService(SemaRequestService):
    """A bounded source/sink; native graphs own all work within each snapshot.

    Request sequence identifies an HTTP occurrence, never a Sema source row.
    Snapshots contain only requests already received; a sequential sender does
    not need to supply any future request before receiving its current response.
    """

    _uses_upstream_client = False

    def __init__(self, *, query_id, upstream_url, limits, trace_path, backend, options=None,
                 held_request_capacity=128, snapshot_rows=128, executor=None):
        if backend not in ('daft', 'ray-data', 'imlane'):
            raise ValueError('Sema native backend must be daft, ray-data or imlane')
        if any(type(value) is not int or not 1 <= value <= 128
               for value in (held_request_capacity, snapshot_rows)):
            raise ValueError('Sema native source sizes must be from 1 to 128')
        super().__init__(query_id=query_id, upstream_url=upstream_url, limits=limits,
                         trace_path=trace_path, upstream_concurrency=held_request_capacity,
                         held_request_capacity=held_request_capacity)
        if limits.timeout_s > 120 or max(limits.request_bytes, limits.response_bytes) > 8 * 1024 * 1024:
            raise ValueError('Sema native complete calls require at most 120 seconds and 8 MiB')
        if executor is None:
            if backend == 'imlane':
                raise ValueError('Sema IMLane requires an explicit native complete-call executor')
            executor = NativeCallExecutor(backend, options, max_batch_rows=snapshot_rows,
                                          max_batch_bytes=_SNAPSHOT_BYTES)
        if (not callable(getattr(executor, 'iter_batch', None))
                or getattr(executor, 'backend', backend) != backend):
            raise ValueError('Sema executor must provide the selected native complete-call API')
        self.mode = 'native-' + backend + '-request-service'
        self.backend, self.executor, self.snapshot_rows = backend, executor, snapshot_rows
        self._pending = deque()
        self._active = {}
        self._received = set()
        self._complete_response_ids = set()
        self._wake = self._driver = self._bridge_thread = self._driver_error = None
        self._snapshot_count = self._prepared_calls = self._completed_http = self._snapshot_response_bytes = 0
        self._close_reports = []
        self._uncertain = set()
        self._graph_stop = threading.Event()
        self._normal_input_ended = self._normal_exit_cancel = False
        self._snapshot_body_stopped = True
        self._snapshot_errors = ()

    @property
    def summary(self):
        return {**super().summary, 'native_backend': self.backend,
                'upstream_capacity_owner': 'native vendor graph within an arrived-request snapshot',
                'upstream_concurrency': getattr(getattr(self.executor, 'options', None), 'concurrency', None),
                'forwarded_posts': None, 'forwarded_posts_status': 'unavailable; kernel not observed',
                'snapshot_rows': self.snapshot_rows, 'snapshot_bytes': _SNAPSHOT_BYTES,
                'pre_admission_body_capacity_scope': 'shared service intake before body reading; retained through local owners',
                'pre_admission_body_max_bytes': self.limits.intake_bytes,
                'snapshot_count': self._snapshot_count, 'prepared_calls': self._prepared_calls,
                'completed_http_responses': self._completed_http,
                'native_close_reports': list(self._close_reports),
                'last_close_report': self._close_reports[-1] if self._close_reports else None,
                'uncertain_requests': len(self._uncertain),
                'uncertain_request_sequences': sorted(self._uncertain),
                'native_bridge_thread_alive': self._bridge_thread is not None and self._bridge_thread.is_alive(),
                'native_driver_stopped': self._driver is None or self._driver.done(),
                'native_graph_abort_requested': self._graph_stop.is_set(),
                'native_timing': 'driver snapshot/start/receive only; model HTTP times unavailable'}

    async def _initialize_executor(self):
        await super()._initialize_executor()
        self._wake = asyncio.Event()
        self._driver = asyncio.create_task(self._drive(), name='sema-native-snapshot-source')

    def cancel(self):
        normal_exit = self._normal_exit_cancel and self.first_error is None
        self._normal_exit_cancel = False
        if not normal_exit:
            self._graph_stop.set()
        super().cancel()
        if self._loop is not None and not self._loop.is_closed() and self._wake is not None:
            self._loop.call_soon_threadsafe(self._stop_intake)

    def end_input(self):
        self._normal_input_ended = True
        super().end_input()

    def __exit__(self, *error):
        self._normal_exit_cancel = (self._normal_input_ended and self.first_error is None
            and (not error or error[0] is None) and not self._pending
            and set(self._active) <= self._complete_response_ids and not self._graph_stop.is_set())
        try:
            return super().__exit__(*error)
        finally:
            self._normal_exit_cancel = False

    def _stop_intake(self):
        while self._pending:
            entry = self._pending.popleft()
            self._entry_error(entry, RuntimeError('Sema query has stopped'))
            self._release_body(entry.call.row_id, native=True)
        if self._wake is not None:
            self._wake.set()

    async def _forward(self, body, headers, row, client):
        loop = asyncio.get_running_loop()
        deadline = self._request_deadline(row)
        row['native_intake_started_ns'] = time.monotonic_ns()
        entry = None
        try:
            async with asyncio.timeout_at(deadline):
                if self._halted.is_set():
                    raise RuntimeError('Sema query has stopped')
                await self._held_slots.acquire()
                self._held_sequences.add(row['request_sequence'])
                self._peak_held_requests = max(self._peak_held_requests, len(self._held_sequences))
                row['service_accepted_ns'] = time.monotonic_ns()
                if self._halted.is_set():
                    raise RuntimeError('Sema query has stopped')
                sequence = row['request_sequence']
                pairs = headers.items() if hasattr(headers, 'items') else headers
                call = NativeCall(self.query_id + ':http:' + str(sequence), sequence, body,
                                  self.upstream_url, tuple((str(k), str(v)) for k, v in pairs),
                                  min(120, self.limits.timeout_s),
                                  max_response_bytes=self.limits.response_bytes)
                entry = _Received(call, row, loop.create_future(), deadline)
                self._retain_native_body(row)
                self._pending.append(entry)
                row['native_enqueued_ns'] = time.monotonic_ns()
                self._wake.set()
                return await asyncio.shield(entry.future)
        except BaseException:
            self.cancel()
            if entry is not None:
                entry.future.cancel()
                if entry.call.call_id in self._active and entry.call.call_id not in self._received:
                    self._uncertain.add(entry.call.row_id)
                else:
                    try:
                        self._pending.remove(entry)
                    except ValueError:
                        pass
                    else:
                        self._release_body(entry.call.row_id, native=True)
            self._release_response(row)
            raise
        finally:
            body = call = entry = None

    def _entry_error(self, entry, error):
        entry.row['native_error_type'] = type(error).__name__
        if not entry.future.done():
            entry.future.set_exception(error)

    def _take_snapshot(self):
        snapshot, size = [], 0
        now = asyncio.get_running_loop().time()
        while self._pending and len(snapshot) < self.snapshot_rows:
            entry = self._pending[0]
            if entry.future.done() or entry.deadline <= now:
                self._pending.popleft()
                self._entry_error(entry, TimeoutError('Sema native intake deadline exceeded'))
                self._release_body(entry.call.row_id, native=True)
                continue
            if size + len(entry.call.payload) > _SNAPSHOT_BYTES:
                break
            snapshot.append(self._pending.popleft())
            size += len(entry.call.payload)
        return tuple(snapshot)

    async def _drive(self):
        while True:
            await self._wake.wait()
            self._wake.clear()
            if self._halted.is_set():
                self._stop_intake()
                return
            snapshot = self._take_snapshot()
            if not snapshot:
                continue
            self._snapshot_count += 1
            self._prepared_calls += len(snapshot)
            self._active = {entry.call.call_id: entry for entry in snapshot}
            self._received = set()
            self._complete_response_ids = set()
            self._snapshot_response_bytes = 0
            self._snapshot_body_stopped = False
            captured_ns = time.monotonic_ns()
            captured_bytes = sum(len(entry.call.payload) for entry in snapshot)
            for entry in snapshot:
                entry.row.update(native_snapshot=self._snapshot_count, native_snapshot_ns=captured_ns,
                                 native_snapshot_rows=len(snapshot),
                                 native_snapshot_bytes=captured_bytes)
            del entry
            done = asyncio.get_running_loop().create_future()
            timeout = min(entry.deadline for entry in snapshot) - asyncio.get_running_loop().time()
            self._bridge_thread = threading.Thread(target=self._run_snapshot,
                args=(snapshot, max(0.000001, timeout), done), name='sema-native-graph-bridge', daemon=True)
            self._bridge_thread.start()
            await done
            # The callback may run before the bridge returns and drops its calls.
            # Keep each original body charged through both local owners.
            while self._bridge_thread.is_alive():
                await asyncio.sleep(0.001)
            # Saved failure tracebacks must not keep a finished graph's payload
            # locals after their input ownership is returned. Keep stack locations.
            errors, seen = list(self._snapshot_errors) + [self._driver_error, self._cleanup_error], set()
            while errors:
                error = errors.pop()
                if error is None or id(error) in seen:
                    continue
                seen.add(id(error))
                traceback.clear_frames(error.__traceback__)
                errors.extend((error.__cause__, error.__context__))
            self._snapshot_errors = ()
            sequences = tuple(entry.call.row_id for entry in snapshot)
            snapshot = ()
            self._active = {}
            self._received = set()
            if self._snapshot_body_stopped:
                for sequence in sequences:
                    self._release_body(sequence, native=True)
            if self._pending:
                self._wake.set()

    def _run_snapshot(self, snapshot, timeout, done):
        iterator, error, cleanup_error = None, None, None
        try:
            started_ns = time.monotonic_ns()
            self._loop.call_soon_threadsafe(self._graph_started, started_ns)
            calls = tuple(replace(entry.call, timeout_s=min(120,
                                max(0.000001, entry.deadline - time.monotonic()))) for entry in snapshot)
            iterator = iter(self.executor.iter_batch(calls,
                            cancelled=self._graph_stop.is_set, timeout_s=min(120, timeout)))
            for outcome in iterator:
                self._loop.call_soon_threadsafe(self._accept_outcome, outcome, time.monotonic_ns())
        except BaseException as failure:
            error = failure
        finally:
            try:
                if iterator is not None and callable(getattr(iterator, 'close', None)):
                    iterator.close()
            except BaseException as failure:
                cleanup_error = failure
                if error is None:
                    error = failure
                else:
                    error.add_note('Sema native iterator cleanup: ' + type(failure).__name__)
            report = None
            try:
                report = getattr(self.executor, 'last_close_report', None)
                report = None if report is None else dict(report)
            except BaseException as failure:
                cleanup_error = failure
                error = error or failure
                report = None
            self._loop.call_soon_threadsafe(self._snapshot_finished, error,
                                            report, done, cleanup_error)

    def _graph_started(self, stamp):
        for entry in self._active.values():
            entry.row['native_graph_started_ns'] = stamp

    def _accept_outcome(self, outcome, stamp):
        try:
            if type(outcome) is not NativeCallOutcome:
                raise ValueError('Sema native graph returned a different outcome type')
            entry = self._active.get(outcome.call_id)
            if entry is None or outcome.call_id in self._received or outcome.row_id != entry.call.row_id:
                raise ValueError('Sema native graph changed or repeated HTTP occurrence identity')
            if (outcome.response is None) == (not outcome.transport_error):
                raise ValueError('Sema native outcome requires a complete response or transport error')
            self._received.add(outcome.call_id)
            entry.row['native_bridge_outcome_ns'] = stamp
            entry.row['native_outcome_received_ns'] = time.monotonic_ns()
            if outcome.response is None:
                self._uncertain.add(entry.call.row_id)
                entry.row['native_transport_error'] = redact_text(str(outcome.transport_error))[:4096]
                error = RuntimeError('Sema native transport outcome is uncertain')
                self._fail(entry.call.row_id, _TRANSPORT_REPLY, 'transport', cause=error)
                self._entry_error(entry, error)
                return
            response = outcome.response
            if type(response) is not FullModelResponse:
                raise ValueError('Sema native outcome must contain a complete HTTP response')
            result_bytes = len(encode_full_response(response))
            if result_bytes > self.limits.response_bytes:
                raise ValueError('Sema native complete response exceeds its declared allowance')
            self._snapshot_response_bytes += result_bytes
            if self._snapshot_response_bytes > _SNAPSHOT_BYTES:
                raise ValueError('Sema native snapshot responses exceed the declared byte allowance')
            self._uncertain.discard(entry.call.row_id)
            self._complete_response_ids.add(outcome.call_id)
            self._completed_http += 1
            reply = _Reply(response.status_code, response.body,
                           tuple((k, v) for k, v in response.headers if k.lower() not in _HOP_HEADERS))
            if not 200 <= reply.status < 300:
                self._fail(entry.call.row_id, reply, 'http_response')
            if not entry.future.done():
                entry.future.set_result(reply)
        except Exception as error:
            sequence = next(iter(self._active.values())).call.row_id
            self._fail(sequence, _TRANSPORT_REPLY, 'native_outcome', cause=error)
            for call_id, entry in self._active.items():
                if call_id not in self._complete_response_ids:
                    self._uncertain.add(entry.call.row_id)
                self._entry_error(entry, error)

    def _snapshot_finished(self, error, report, done, cleanup_error):
        self._snapshot_body_stopped = not (
            (report is not None and report.get('graph_consumer_stopped') is False)
            or (cleanup_error is not None and (report is None
                or report.get('graph_consumer_stopped') is not True)))
        try:
            self._close_reports.append(report or {'status': 'unavailable'})
            if report is not None and report.get('graph_consumer_stopped') is False:
                cleanup_error = cleanup_error or RuntimeError('Sema native graph consumer did not stop')
                error = error or cleanup_error
            if report is not None and report.get('clean') is False and not self._graph_stop.is_set():
                cleanup_error = cleanup_error or RuntimeError('Sema native snapshot cleanup was not clean')
                error = error or cleanup_error
            if error is None and len(self._received) != len(self._active):
                error = ValueError('Sema native response snapshot is incomplete')
            if cleanup_error is not None:
                self._record_cleanup_error('native_graph_close', cleanup_error)
            if error is not None:
                expected_stop = isinstance(error, (InterruptedError, TimeoutError)) and self._graph_stop.is_set()
                if not expected_stop or cleanup_error is not None:
                    self._driver_error = self._driver_error or error
                    sequence = next(iter(self._active.values())).call.row_id
                    self._fail(sequence, _TRANSPORT_REPLY, 'native_graph', cause=error)
                for call_id, entry in self._active.items():
                    if call_id not in self._complete_response_ids:
                        self._uncertain.add(entry.call.row_id)
                    self._entry_error(entry, error)
        except Exception as failure:
            self._driver_error = self._driver_error or failure
            self._record_cleanup_error('native_callback', failure)
            self.cancel()
            for entry in self._active.values():
                self._entry_error(entry, failure)
        finally:
            self._snapshot_errors = (error, cleanup_error)
            if not done.done():
                done.set_result(None)

    async def _close_executor(self):
        self._stop_intake()
        if self._driver is not None:
            await asyncio.wait_for(asyncio.shield(self._driver), self.limits.timeout_s + 2)
        if self._bridge_thread is not None:
            self._bridge_thread.join(0)
            if self._bridge_thread.is_alive():
                await asyncio.sleep(0.001)
            if self._bridge_thread.is_alive():
                raise RuntimeError('Sema native bridge remains active; completion is uncertain')
        await super()._close_executor()
        if self._driver_error is not None:
            raise self._driver_error

    async def _shutdown_http(self):
        await super()._shutdown_http()
        if ((self._bridge_thread is not None and self._bridge_thread.is_alive())
                or (self._driver is not None and not self._driver.done())):
            self._http_connections_remaining = None

    def _runtime_stopped(self):
        return (super()._runtime_stopped()
                and (self._bridge_thread is None or not self._bridge_thread.is_alive())
                and (self._driver is None or self._driver.done()))
