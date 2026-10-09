"""Sema HTTP calls offered to the public task/session API and existing Daft/Ray transport."""

import asyncio
from dataclasses import replace
import json
import time

from ...scheduling.core.session_contract import State
from .full_response import MAX_RESPONSE_HEADER_BYTES, decode_full_response
from .model_config import FixedModelConfig, MAX_MODEL_RESPONSE_BYTES
from .native_tasks import NativeTaskSession, prepare_native_task
from .ray_map_transport import RayMapConfig, ray_map_factory
from .sema_service import SemaRequestService, _HOP_HEADERS, _Reply


class SemaSemLoomService(SemaRequestService):
    """Use one query flow; HTTP callers retain unaccepted bodies under the run size limit.

    There is no second model pool or scheduler here. The public session owns
    acceptance, organization, dispatch, result leases and cancellation. The
    author's request pool and SQL parser still surround this service.
    """

    mode = 'sema-method-semloom-request-service'

    def __init__(self, *, query_id, model_config, physical, limits, trace_path,
                 max_held_tasks, max_active_requests, before_post=None):
        if type(model_config) is not FixedModelConfig or type(physical) is not RayMapConfig:
            raise ValueError('Sema SemLoom service requires FixedModelConfig and RayMapConfig')
        if physical.payload_backend != 'daft':
            raise ValueError('Sema SemLoom service requires the Daft payload backend')
        if any(type(v) is not int or not 1 <= v <= 256 for v in (max_held_tasks, max_active_requests)):
            raise ValueError('Sema SemLoom task and active capacities must be from 1 to 256')
        if max_active_requests > max_held_tasks or physical.batch_rows > max_active_requests:
            raise ValueError('Sema SemLoom capacities cannot fit the configured batch')
        if limits.response_bytes + MAX_RESPONSE_HEADER_BYTES + 12 > MAX_MODEL_RESPONSE_BYTES:
            raise ValueError('Sema response limit must leave space for complete response metadata')
        if limits.timeout_s < model_config.timeout_ms / 1000:
            raise ValueError('Sema service deadline must cover its model deadline')
        super().__init__(query_id=query_id, upstream_url=model_config.endpoint_url,
                         limits=limits, trace_path=trace_path, before_post=before_post)
        self.model_config, self.physical = model_config, replace(physical, response_mode='full')
        self.max_held_tasks, self.max_active_requests = max_held_tasks, max_active_requests
        self._execution = self._session = self._pump_task = None
        self._pending = {}
        self._next_task = 0
        self._ended = False
        self._progress_changed = None
        self._core_events = []

    @property
    def summary(self):
        return {**super().summary, 'execution_api': 'NativeTaskSession',
                'payload_backend': self.physical.payload_backend,
                'response_mode': self.physical.response_mode,
                'max_held_tasks': self.max_held_tasks, 'max_active_requests': self.max_active_requests,
                'post_count_scope': 'one pre-send reservation per core task; remote receipt is separate',
                'forward_clock_scope': 'Ray RPC entry, before remote HTTP worker execution',
                'model_return_clock_scope': 'remote worker completion; unavailable without a shared clock',
                'core_events': list(self._core_events)}

    def _before_request(self, task):
        # This existing Ray hook is the sole experiment accounting point.
        if self._halted.is_set():
            raise RuntimeError('Sema query has stopped before model send')
        if self.before_post is not None:
            self.before_post(task.task.payload)
        if self._halted.is_set():
            raise RuntimeError('Sema query has stopped before model send')
        self._forwarded += 1

    def _observe(self, event):
        when = time.monotonic_ns()
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._record_event, event, when)

    def _record_event(self, event, when):
        # Bounded by the finite query and transport events; keep safe event metadata.
        if len(self._core_events) < self.limits.max_requests * 32 + 32:
            self._core_events.append({**event, 'observed_ns': when})
        if event.get('event') == 'ray_http_completed':
            entry = self._pending.get(event['key']['sequence'])
            if entry is not None:
                row = entry['row']
                row['forward_started_ns'] = event['rpc_started_ns']
                row['model_returned_ns'] = event['worker_ended_ns'] if event['shared_clock'] else None
                row['model_returned_observed_ns'] = event['received_ns']
                row['model_clock_shared'] = event['shared_clock']

    def _build_execution(self):
        # Compose the existing factory's pre-send hook with the common response mode.
        # No shared module needs a Sema-specific callback or scheduling branch.
        return ray_map_factory(self.physical, before_request=self._before_request)(
            self.model_config, max_tasks=self.max_held_tasks,
            max_active_requests=self.max_active_requests, observer=self._observe)

    async def _initialize_executor(self):
        self._progress_changed = asyncio.Event()
        self._execution = self._build_execution()
        self._session = NativeTaskSession(self._execution, self.query_id, 'sema-request-service')
        self._pump_task = asyncio.create_task(self._pump())

    def cancel(self):
        super().cancel()
        if self._session is not None and not self._ended:
            self._session.request_cancel()
        if self._progress_changed is not None and self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._progress_changed.set)

    def end_input(self):
        self._ended = True
        super().end_input()
        if self._session is not None:
            self._loop.call_soon_threadsafe(self._session.end_input)

    async def _pump(self):
        while True:
            progress = self._session.advance(self.max_held_tasks)
            for delivery in progress.deliveries:
                entry = self._pending.get(delivery.key.sequence)
                if entry is None or entry['future'].done():
                    self._session.release((delivery.lease_id,))
                    self._progress_changed.set()
                    continue
                entry['lease'] = delivery.lease_id
                try:
                    response = decode_full_response(delivery.result)
                except Exception as error:
                    self._fail(entry['row']['request_sequence'],
                               _Reply(502, b'{"error":"Sema execution failed before a complete response"}', ()),
                               'execution')
                    entry['future'].set_exception(error)
                    continue
                if not 200 <= response.status_code < 300:
                    self._fail(entry['row']['request_sequence'],
                               _Reply(response.status_code, response.body, response.headers), 'http_response')
                entry['future'].set_result(delivery.result)
            if progress.state in (State.FAILED, State.CANCELLED):
                for entry in self._pending.values():
                    if not entry['future'].done():
                        entry['future'].set_exception(RuntimeError('Sema execution stopped'))
                self._progress_changed.set()
            await asyncio.sleep(0 if progress.has_immediate_work else self._session.limits.poll_interval_s)

    async def _forward(self, body, headers, row, _client):
        value = json.loads(body)
        if (not isinstance(value, dict) or value.get('model') != self.model_config.model_id
                or value.get('stream', False) is not False or not isinstance(value.get('messages'), list)):
            raise ValueError('Sema service accepts complete non-streaming calls to its single model')
        deadline = time.monotonic() + self.limits.timeout_s
        task = prepare_native_task(body, self._next_task, row_sequence=row['request_sequence'],
                                   call_id='sema-http-' + str(row['request_sequence']),
                                   max_result_bytes=self.limits.response_bytes + MAX_RESPONSE_HEADER_BYTES + 12)
        while True:
            if self._halted.is_set():
                raise RuntimeError('Sema query has stopped before task acceptance')
            sequence = self._next_task
            if task.sequence != sequence:
                task = replace(task, sequence=sequence)
            offered = self._session.offer((task,))
            if offered.status == 'REJECTED':
                raise ValueError('Sema task rejected by the public session')
            if offered.accepted_prefix_count:
                self._next_task += 1
                row['core_task_sequence'] = sequence
                row['core_accepted_ns'] = time.monotonic_ns()
                entry = {'row': row, 'future': asyncio.get_running_loop().create_future(), 'lease': None}
                self._pending[sequence] = entry
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Sema task acceptance deadline exceeded')
            self._progress_changed.clear()
            await asyncio.wait_for(self._progress_changed.wait(), remaining)
        try:
            raw = await asyncio.wait_for(asyncio.shield(entry['future']), max(0, deadline - time.monotonic()))
            response = decode_full_response(raw)
            if len(response.body) > self.limits.response_bytes:
                raise ValueError('Sema response exceeds its service body limit')
            return _Reply(response.status_code, response.body,
                          tuple((k, v) for k, v in response.headers if k.lower() not in _HOP_HEADERS))
        except BaseException:
            self._release_response(row)
            raise

    def _release_response(self, row):
        sequence = row.get('core_task_sequence')
        entry = self._pending.pop(sequence, None)
        if entry is not None:
            if entry['lease'] is not None:
                self._session.release((entry['lease'],))
                self._progress_changed.set()
            elif not entry['future'].done():
                entry['future'].cancel()

    async def _close_executor(self):
        if self._pump_task is not None:
            self._pump_task.cancel()
            await asyncio.gather(self._pump_task, return_exceptions=True)
        if self._session is None:
            if self._execution is not None:
                self._execution.close()
            return
        for entry in self._pending.values():
            if entry['lease'] is not None:
                self._session.release((entry['lease'],))
            if not entry['future'].done():
                entry['future'].cancel()
        self._pending.clear()
        progress = self._session.advance(self.max_held_tasks)
        for delivery in progress.deliveries:
            self._session.release((delivery.lease_id,))
        self._session.close(clean=self._ended and self.first_error is None)
        deadline = time.monotonic() + self._execution.drain_timeout_s
        while self._execution.engine.capacity.records and time.monotonic() < deadline:
            self._execution.engine.advance()
            await asyncio.sleep(self._session.limits.poll_interval_s)
        if (self._execution.engine.capacity.records or self._execution.engine.jobs.jobs
                or not self._execution.close()):
            raise RuntimeError('Sema SemLoom execution cleanup has unresolved work')
