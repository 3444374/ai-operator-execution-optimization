"""Already-arrived Sema HTTP calls executed by finite native vendor graphs."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, replace
import threading
import time
import traceback

from ...baselines.text.frameworks.native_calls import (
    NativeCall, NativeCallExecutor, NativeCallOutcome, outcome_kind, raise_execution_error,
)
from ...baselines.common.redact import redact_text
from .full_response import FullModelResponse, encode_full_response
from .sema_service import SemaRequestService, _HOP_HEADERS, _Reply


_TRANSPORT_REPLY = _Reply(502, b'{"error":"Sema request transport failed"}',
                          (('Content-Type', 'application/json'),))


@dataclass(frozen=True)
class SemaNativeLimits:
    """Source and sink bounds for one arrived-request snapshot.

    Pending response bytes remain charged until their HTTP write ends. The
    cumulative bound counts accepted complete encoded responses across one native graph,
    including responses already written; it resets only for the next snapshot.
    Vendor-internal buffering is governed by that graph's own configuration.
    """

    snapshot_rows: int = 128
    snapshot_payload_bytes: int = 32 * 1024 * 1024
    pending_response_bytes: int = 32 * 1024 * 1024
    cumulative_response_bytes: int = 32 * 1024 * 1024

    def __post_init__(self):
        if type(self.snapshot_rows) is not int or not 1 <= self.snapshot_rows <= 128:
            raise ValueError('Sema native snapshot rows must be from 1 to 128')
        for value in (self.snapshot_payload_bytes, self.pending_response_bytes,
                      self.cumulative_response_bytes):
            if type(value) is not int or value < 1:
                raise ValueError('Sema native byte capacities must be positive integers')


@dataclass
class _Received:
    call: NativeCall
    row: dict
    future: asyncio.Future | None
    deadline: float


class SemaNativeService(SemaRequestService):
    """A bounded source/sink; native graphs own all work within each snapshot.

    Request sequence identifies an HTTP occurrence, never a Sema source row.
    Snapshots contain only requests already received; a sequential sender does
    not need to supply any future request before receiving its current response.
    """

    _uses_upstream_client = False

    def __init__(self, *, query_id, upstream_url, limits, trace_path, backend, options=None,
                 held_request_capacity=128, snapshot_rows=None, native_limits=None, executor=None,
                 listen_port=0):
        if backend not in ('daft', 'ray-data', 'imlane'):
            raise ValueError('Sema native backend must be daft, ray-data or imlane')
        if type(held_request_capacity) is not int or not 1 <= held_request_capacity <= 128:
            raise ValueError('Sema native held request capacity must be from 1 to 128')
        if snapshot_rows is not None and (type(snapshot_rows) is not int or not 1 <= snapshot_rows <= 128):
            raise ValueError('Sema native snapshot rows must be from 1 to 128')
        explicit_snapshot = native_limits is not None or snapshot_rows is not None
        if native_limits is None:
            native_limits = SemaNativeLimits(snapshot_rows=128 if snapshot_rows is None else snapshot_rows)
        elif type(native_limits) is not SemaNativeLimits:
            raise ValueError('Sema native service requires typed SemaNativeLimits')
        if snapshot_rows is not None and snapshot_rows != native_limits.snapshot_rows:
            raise ValueError('Sema native snapshot row settings differ')
        snapshot_rows = native_limits.snapshot_rows
        if snapshot_rows > held_request_capacity:
            # Legacy callers supplied only the held capacity. Keep their effective
            # snapshot bound, while rejecting conflicting explicit declarations.
            if not explicit_snapshot:
                native_limits = replace(native_limits, snapshot_rows=held_request_capacity)
                snapshot_rows = held_request_capacity
            else:
                raise ValueError('Sema native snapshot rows exceed held request capacity')
        super().__init__(query_id=query_id, upstream_url=upstream_url, limits=limits,
                         trace_path=trace_path, upstream_concurrency=held_request_capacity,
                         held_request_capacity=held_request_capacity, listen_port=listen_port)
        if limits.timeout_s > 120 or max(limits.request_bytes, limits.response_bytes) > 8 * 1024 * 1024:
            raise ValueError('Sema native complete calls require at most 120 seconds and 8 MiB')
        if executor is None:
            if backend == 'imlane':
                raise ValueError('Sema IMLane requires an explicit native complete-call executor')
            executor = NativeCallExecutor(backend, options, max_batch_rows=snapshot_rows,
                max_batch_bytes=max(native_limits.snapshot_payload_bytes,
                                    native_limits.cumulative_response_bytes))
        if (not callable(getattr(executor, 'iter_batch', None))
                or getattr(executor, 'backend', backend) != backend):
            raise ValueError('Sema executor must provide the selected native complete-call API')
        self.mode = 'native-' + backend + '-request-service'
        self.backend, self.executor, self.snapshot_rows = backend, executor, snapshot_rows
        self.native_limits = native_limits
        self._response_reservations = {}
        self._native_pending_response_bytes = self._native_peak_pending_response_bytes = 0
        self._peak_snapshot_response_bytes = 0
        self._pending = deque()
        self._active = {}
        self._received = set()
        self._complete_response_ids = set()
        self._wake = self._driver = self._bridge_thread = self._driver_error = None
        self._snapshot_count = self._prepared_calls = self._completed_http = self._snapshot_response_bytes = 0
        self._close_reports = []
        self._uncertain = set()
        self._not_submitted = set()
        self._snapshot_outcomes_observed = 0
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
                'snapshot_rows': self.snapshot_rows,
                'snapshot_bytes': self.native_limits.snapshot_payload_bytes,
                'native_limits': dict(vars(self.native_limits)),
                'cumulative_response_scope': 'per_snapshot',
                'snapshot_response_bytes': self._snapshot_response_bytes,
                'snapshot_response_bytes_scope': 'accepted complete encoded responses; rejected sizes remain in request trace',
                'peak_snapshot_response_bytes': self._peak_snapshot_response_bytes,
                'pending_response_bytes': self._native_pending_response_bytes,
                'peak_pending_response_bytes': self._native_peak_pending_response_bytes,
                'pending_response_capacity_scope': 'complete replies accepted by this sink through HTTP write completion; vendor buffers separate',
                'pre_admission_body_capacity_scope': 'shared service intake before body reading; retained through local owners',
                'pre_admission_body_max_bytes': self.limits.intake_bytes,
                'snapshot_count': self._snapshot_count, 'prepared_calls': self._prepared_calls,
                'completed_http_responses': self._completed_http,
                'native_close_reports': list(self._close_reports),
                'last_close_report': self._close_reports[-1] if self._close_reports else None,
                'uncertain_requests': len(self._uncertain),
                'uncertain_request_sequences': sorted(self._uncertain),
                'not_submitted_requests': len(self._not_submitted),
                'not_submitted_request_sequences': sorted(self._not_submitted),
                'native_bridge_thread_alive': self._bridge_thread is not None and self._bridge_thread.is_alive(),
                'native_driver_stopped': self._driver is None or self._driver.done(),
                'native_graph_abort_requested': self._graph_stop.is_set(),
                'native_executor_reusable': (not getattr(self.executor, '_sema_native_unsettled', False)
                                             and getattr(self.executor, '_sema_native_service_owner', None) is None),
                'native_timing': 'driver snapshot/start/receive only; model HTTP times unavailable'}

    async def _initialize_executor(self):
        if getattr(self.executor, '_sema_native_unsettled', False):
            raise RuntimeError('Sema native executor has unsettled work; explicit owner cleanup required')
        owner = getattr(self.executor, '_sema_native_service_owner', None)
        if owner is not None and owner is not self:
            raise RuntimeError('Sema native executor is already owned by an active request service')
        self.executor._sema_native_service_owner = self
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
                if len(body) > self.native_limits.snapshot_payload_bytes:
                    raise ValueError('Sema native call exceeds snapshot payload byte capacity')
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
                if (entry.call.call_id in self._active and entry.call.call_id not in self._received
                        and entry.call.row_id not in self._not_submitted):
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
        if entry.future is not None and not entry.future.done():
            entry.future.set_exception(error)

    def _release_response(self, row):
        reservation = self._response_reservations.pop(row['request_sequence'], None)
        if reservation is not None:
            size, entry = reservation
            self._native_pending_response_bytes -= size
            # A completed future otherwise retains the reply until the whole graph
            # exits, even after its HTTP handler has released the response.
            entry.future = None
            row['native_response_released_ns'] = time.monotonic_ns()
        super()._release_response(row)

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
            if size + len(entry.call.payload) > self.native_limits.snapshot_payload_bytes:
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
            self._snapshot_outcomes_observed = 0
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
                accepted = threading.Event()
                self._loop.call_soon_threadsafe(self._accept_outcome, outcome, time.monotonic_ns(), accepted)
                # Bound the cross-thread result handoff independently of vendor
                # scheduling. A graph cannot queue arbitrary response callbacks.
                while not accepted.wait(0.01):
                    if self._loop.is_closed():
                        raise RuntimeError('Sema response owner loop closed during native delivery')
                outcome = None
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

    def _accept_outcome(self, outcome, stamp, accepted=None):
        self._snapshot_outcomes_observed += 1
        try:
            if type(outcome) is not NativeCallOutcome:
                raise ValueError('Sema native graph returned a different outcome type')
            entry = self._active.get(outcome.call_id)
            if entry is None or outcome.call_id in self._received or outcome.row_id != entry.call.row_id:
                raise ValueError('Sema native graph changed or repeated HTTP occurrence identity')
            outcome_kind(outcome)
            raise_execution_error(outcome)
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
            # A valid full response proves remote completion even when its size is
            # rejected by the configured sink. Do not turn it into unknown work.
            self._uncertain.discard(entry.call.row_id)
            self._complete_response_ids.add(outcome.call_id)
            self._completed_http += 1
            entry.row['native_complete_response_bytes'] = result_bytes
            if result_bytes > self.limits.response_bytes:
                raise ValueError('Sema native complete response exceeds its declared allowance')
            cumulative = self._snapshot_response_bytes + result_bytes
            if cumulative > self.native_limits.cumulative_response_bytes:
                raise ValueError('Sema native snapshot responses exceed the declared byte allowance')
            pending = self._native_pending_response_bytes + result_bytes
            if pending > self.native_limits.pending_response_bytes:
                raise ValueError('Sema native pending responses exceed the declared byte allowance')
            self._snapshot_response_bytes = cumulative
            self._peak_snapshot_response_bytes = max(self._peak_snapshot_response_bytes, cumulative)
            self._native_pending_response_bytes = pending
            self._native_peak_pending_response_bytes = max(self._native_peak_pending_response_bytes, pending)
            self._response_reservations[entry.call.row_id] = result_bytes, entry
            reply = _Reply(response.status_code, response.body,
                           tuple((k, v) for k, v in response.headers if k.lower() not in _HOP_HEADERS))
            if not 200 <= reply.status < 300:
                self._fail(entry.call.row_id, reply, 'http_response')
            if entry.future is not None and not entry.future.done():
                entry.future.set_result(reply)
        except Exception as error:
            sequence = next(iter(self._active.values())).call.row_id
            self._fail(sequence, _TRANSPORT_REPLY, 'native_outcome', cause=error)
            for call_id, entry in self._active.items():
                if call_id not in self._complete_response_ids:
                    self._uncertain.add(entry.call.row_id)
                self._entry_error(entry, error)
        finally:
            if accepted is not None:
                accepted.set()

    def _snapshot_not_submitted(self, report):
        """Accept only the existing IMLane process/submission proof.

        The generic Daft/Ray graph contract has no pre-start proof yet. A failed
        query or a stopped local consumer alone cannot establish an unsent call.
        """
        if (self.backend != 'imlane' or report is None or not self._snapshot_body_stopped
                or report.get('bridge_started') is not False
                or report.get('author_submission_possible') is not False
                or self._snapshot_outcomes_observed or self._received or self._complete_response_ids):
            return False
        for field in ('graph_started', 'author_dispatch_started', 'external_coordinator_requires_cleanup'):
            if field in report and report[field] is not False:
                return False
        if 'received' in report and (type(report['received']) is not int or report['received'] != 0):
            return False
        if 'prepared' in report and (type(report['prepared']) is not int
                                     or report['prepared'] != len(self._active)):
            return False
        return True

    def _snapshot_finished(self, error, report, done, cleanup_error):
        self._snapshot_body_stopped = (report is not None
            and report.get('graph_consumer_stopped') is True)
        try:
            not_submitted = self._snapshot_not_submitted(report)
            if not_submitted:
                for entry in self._active.values():
                    self._uncertain.discard(entry.call.row_id)
                    self._not_submitted.add(entry.call.row_id)
                    entry.row['native_submission_state'] = 'not_submitted'
            all_responses = set(self._active) <= self._complete_response_ids
            complete = len(self._received) == len(self._active)
            if report is not None:
                report['reported_clean'] = report.get('clean')
                report['complete_response_observation'] = all_responses
                report['service_snapshot_complete'] = complete
                report['service_submission_not_started'] = not_submitted
                report['service_query_failed'] = (error is not None or cleanup_error is not None
                                                   or self.first_error is not None or not complete
                                                   or not all_responses or report.get('clean') is not True)
                report['clean'] = (report.get('clean') is True and self._snapshot_body_stopped
                                   and complete and all_responses and error is None and cleanup_error is None)
            if ((not self._snapshot_body_stopped or not complete or not all_responses
                    or error is not None or cleanup_error is not None
                    or report is None or report.get('clean') is not True)
                    and (not not_submitted or cleanup_error is not None or report.get('cleanup_errors'))):
                self.executor._sema_native_unsettled = True
            self._close_reports.append(report or {'status': 'unavailable'})
            if not self._snapshot_body_stopped:
                cleanup_error = cleanup_error or RuntimeError('Sema native graph consumer stop is unconfirmed')
                error = error or cleanup_error
            if (report is not None and report.get('clean') is False and not self._graph_stop.is_set()
                    and not not_submitted):
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
                    if call_id not in self._complete_response_ids and not not_submitted:
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
        try:
            self._stop_intake()
            if self._driver is not None:
                await asyncio.wait_for(asyncio.shield(self._driver), self.limits.timeout_s + 2)
            if self._bridge_thread is not None:
                self._bridge_thread.join(0)
                if self._bridge_thread.is_alive():
                    await asyncio.sleep(0.001)
                if self._bridge_thread.is_alive():
                    self.executor._sema_native_unsettled = True
                    raise RuntimeError('Sema native bridge remains active; completion is uncertain')
            await super()._close_executor()
            if self._driver_error is not None:
                raise self._driver_error
        finally:
            if (getattr(self.executor, '_sema_native_service_owner', None) is self
                    and (self._bridge_thread is None or not self._bridge_thread.is_alive())
                    and (self._driver is None or self._driver.done())
                    and not self._response_reservations):
                self.executor._sema_native_service_owner = None

    async def _shutdown_http(self):
        await super()._shutdown_http()
        if ((self._bridge_thread is not None and self._bridge_thread.is_alive())
                or (self._driver is not None and not self._driver.done())):
            self._http_connections_remaining = None

    def _runtime_stopped(self):
        return (super()._runtime_stopped()
                and (self._bridge_thread is None or not self._bridge_thread.is_alive())
                and (self._driver is None or self._driver.done()))
