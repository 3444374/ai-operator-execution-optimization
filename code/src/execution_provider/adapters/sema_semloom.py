"""Sema HTTP calls offered to the public task/session API and existing Ray transport."""

import asyncio
from dataclasses import asdict, replace
import json
import threading
import time

from ...scheduling.core.session_contract import State, Usage
from .full_response import MAX_RESPONSE_HEADER_BYTES, decode_full_response
from .model_config import FixedModelConfig, MAX_MODEL_RESPONSE_BYTES
from .native_tasks import NativeTaskSession, prepare_native_task
from .ray_map_transport import RayMapConfig, ray_map_factory
from .sema_service import SemaRequestService, _HOP_HEADERS, _Reply


class SemaSemLoomExecutor:
    """Optional sequential-query owner; Core and workers stay on one control thread."""

    def __init__(self, *, model_config, physical, max_held_tasks, max_active_requests, observer=None):
        if type(model_config) is not FixedModelConfig or type(physical) is not RayMapConfig:
            raise ValueError('Sema executor requires its fixed model and RayMapConfig')
        if (any(type(v) is not int or not 1 <= v <= 256 for v in (max_held_tasks, max_active_requests))
                or max_active_requests > max_held_tasks or physical.batch_rows > max_active_requests):
            raise ValueError('Sema executor capacities cannot fit the configured batch')
        self.model_config, self.physical = model_config, replace(physical, response_mode='full')
        self.max_held_tasks, self.max_active_requests = max_held_tasks, max_active_requests
        self.observer = observer
        self.execution = self._service = self._loop = self._thread = None
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._startup_error = None
        self._object_bytes = None
        self._execution_closed = False
        self.poisoned = False

    def _before_request(self, task):
        service = self._service
        if service is None:
            raise RuntimeError('Sema executor has no current query before model send')
        service._before_request(task)

    def _observe(self, event):
        if 'object_bytes' in event:
            self._object_bytes = event['object_bytes']
        service = self._service
        if service is not None:
            if (service._session is not None and event.get('key') is not None
                    and event['key']['session_id'] != service._session.session.session_id):
                return
            service._observe(event)
        if self.observer is not None:
            self.observer(event)

    def _build_execution(self):
        return ray_map_factory(self.physical, before_request=self._before_request)(
            self.model_config, max_tasks=self.max_held_tasks,
            max_active_requests=self.max_active_requests, observer=self._observe)

    def _run(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self.execution = self._build_execution()
            self._ready.set()
            if not self._stop_requested.is_set():
                self._loop.run_forever()
        except BaseException as error:
            self._startup_error = error
        finally:
            if self._stop_requested.is_set() and self.execution is not None:
                try:
                    if not self.execution.close(self.execution.drain_timeout_s):
                        raise RuntimeError('Sema executor startup cleanup did not settle')
                    self._execution_closed = True
                except BaseException as error:
                    self._startup_error = self._startup_error or error
            self._loop.close()
            self._ready.set()

    def __enter__(self):
        if self._thread is not None:
            raise RuntimeError('Sema executor owner cannot be restarted')
        self._thread = threading.Thread(target=self._run, name='sema-executor-owner', daemon=True)
        self._thread.start()
        if not self._ready.wait(45) or self._startup_error is not None:
            self.poisoned = True
            self._stop_requested.set()
            if self._loop is not None and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(5)
            raise RuntimeError('Sema executor owner startup did not settle') from self._startup_error
        return self

    def _call(self, operation, timeout):
        if self._thread is None or not self._thread.is_alive() or self._loop.is_closed():
            operation.close()
            raise RuntimeError('Sema executor owner is not running')
        future = asyncio.run_coroutine_threadsafe(operation, self._loop)
        try:
            return future.result(timeout)
        except BaseException:
            self.poisoned = True
            # Cancellation is not proof that a remote request or startup settled.
            future.cancel()
            raise

    async def _start_service(self, service):
        if self.poisoned or self._service is not None:
            raise RuntimeError('Sema executor stopped or already has a query consumer')
        self._service = service
        await service._initialize_http()

    async def _stop_service(self, service):
        if self._service is not service:
            return
        await service._shutdown_http()
        if (service.first_error is not None or service.cleanup_errors or not service._ended
                or self.execution.engine.capacity.usage() != Usage()
                or self.execution.engine.jobs.jobs or self._object_bytes not in (None, 0)):
            self.poisoned = True
        if (not service._http_handlers and service._listeners_remaining == 0
                and service._http_connections_remaining == 0):
            self._service = None

    def _snapshot(self):
        return dict(execution_id=None if self.execution is None else str(id(self.execution)),
            core_usage=None if self.execution is None else asdict(self.execution.engine.capacity.usage()),
            core_jobs=None if self.execution is None else len(self.execution.engine.jobs.jobs),
            object_bytes=self._object_bytes, poisoned=self.poisoned,
            control_thread_alive=self._thread is not None and self._thread.is_alive())

    def snapshot(self):
        async def read():
            return self._snapshot()
        if self._thread is None or not self._thread.is_alive():
            return self._snapshot()
        return self._call(read(), 5)

    async def _close(self):
        if (self._service is not None or self.execution.engine.capacity.usage() != Usage()
                or self.execution.engine.jobs.jobs or self._object_bytes not in (None, 0)):
            raise RuntimeError('Sema executor owner retains a query or unresolved work')
        if not self.execution.close(self.execution.drain_timeout_s):
            raise RuntimeError('Sema executor owner cleanup did not settle')
        self._execution_closed = True

    def __exit__(self, error_type, error, traceback):
        try:
            if not self._execution_closed:
                self._call(self._close(), self.execution.drain_timeout_s + 5)
            if not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(5)
            if self._thread.is_alive():
                raise TimeoutError('Sema executor owner control thread did not stop')
        except BaseException as cleanup:
            self.poisoned = True
            if error is not None:
                error.add_note('Sema executor owner cleanup also failed: ' + type(cleanup).__name__)
            else:
                raise


class SemaSemLoomService(SemaRequestService):
    """Use one query flow; HTTP callers retain unaccepted bodies under the run size limit.

    There is no second model pool or scheduler here. The public session owns
    acceptance, organization, dispatch, result leases and cancellation. The
    author's request pool and SQL parser still surround this service.
    """

    mode = 'sema-method-semloom-request-service'
    _uses_upstream_client = False

    def __init__(self, *, query_id, model_config, physical, limits, trace_path,
                 max_held_tasks, max_active_requests, before_post=None, executor_owner=None):
        if type(model_config) is not FixedModelConfig or type(physical) is not RayMapConfig:
            raise ValueError('Sema SemLoom service requires FixedModelConfig and RayMapConfig')
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
        if executor_owner is not None and (
                not isinstance(executor_owner, SemaSemLoomExecutor)
                or executor_owner.model_config != model_config or executor_owner.physical != self.physical
                or (executor_owner.max_held_tasks, executor_owner.max_active_requests)
                    != (max_held_tasks, max_active_requests)):
            raise ValueError('Sema service differs from its reusable executor configuration')
        self.executor_owner = executor_owner
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
                'executor_scope': 'query' if self.executor_owner is None else 'persistent group diagnostic',
                'io_thread_scope': 'query' if self.executor_owner is None else 'persistent executor owner',
                'execution_id': None if self._execution is None else str(id(self._execution)),
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
        if (self._session is not None and event.get('key') is not None
                and event['key']['session_id'] != self._session.session.session_id):
            return
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
        self._execution = (self._build_execution() if self.executor_owner is None
                           else self.executor_owner.execution)
        self._session = NativeTaskSession(self._execution, self.query_id, 'sema-request-service')
        self._pump_task = asyncio.create_task(self._pump())

    def _start_runtime(self):
        if self.executor_owner is None:
            return super()._start_runtime()
        owner = self.executor_owner
        self._loop, self._thread = owner._loop, owner._thread
        try:
            owner._call(owner._start_service(self), 45)
        except BaseException as error:
            self._startup_error = error
        self._ready.set()

    def _stop_runtime(self):
        if self.executor_owner is None:
            return super()._stop_runtime()
        try:
            self.executor_owner._call(self.executor_owner._stop_service(self), self.limits.timeout_s + 5)
        except BaseException as error:
            self._record_cleanup_error('executor_close', error)

    def _runtime_stopped(self):
        if self.executor_owner is None:
            return super()._runtime_stopped()
        return self._cleanup_started.is_set() and not self._http_handlers

    def __exit__(self, *error):
        try:
            return super().__exit__(*error)
        finally:
            if self.executor_owner is not None and (self.first_error is not None or self.cleanup_errors):
                self.executor_owner.poisoned = True

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
                entry['future'].set_result(response)
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
            response = await asyncio.wait_for(asyncio.shield(entry['future']),
                                              max(0, deadline - time.monotonic()))
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
            if self._execution is not None and self.executor_owner is None:
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
        if (self._execution.engine.capacity.usage() != Usage() or self._execution.engine.jobs.jobs
                or (self.executor_owner is None and not self._execution.close())):
            raise RuntimeError('Sema SemLoom execution cleanup has unresolved work')
