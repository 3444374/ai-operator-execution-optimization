"""Driver ownership and Ray RPC handoff for the async records model actor.

Core owns admission and query state. The existing driver allocates its actor,
tracks data RPC completion and retains uncertain remote work through cleanup.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import asyncio
import json
import threading
import time

from src.serving.backends.vllm_async_engine import AsyncEngineLimits

from .lotus_records_actor_owner import _runtime_env
from .records_capacity import CoreRecordsCapacity, resolve_records_async_limits
from .records_model_adapter import _ActorOwner, _encode, _measure
from .records_async_model import AsyncCoreModelActor


@dataclass(frozen=True)
class DataRpcProgress:
    """Read-only handoff opportunity; the existing RPC gate retains every permit."""

    generation: int
    workers: int
    pending: int
    inflight: int
    batch_submits_pending: int
    unconfirmed: int


class AsyncRecordsActorOwner(_ActorOwner):
    """Opt-in actor allocation hooks; the existing Core owns all query state."""

    model_batch_mode = "independent_final_results"
    native_engine_abort_supported = True
    nonblocking_core_handoff = True
    # Protocol roles, not model request capacity: shutdown/drain and one
    # independent control (snapshot, abort or ACK) must be able to coexist.
    control_rpc_reserve = 2

    def __init__(self, method, engine, generation, resources, *, limits=None, _prepared_only=False, capacity=None):
        super().__init__(method, engine, generation, resources, _prepared_only=_prepared_only)
        if resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1:
            raise ValueError("Async Core requires prepare_batch_rows=model_batch_rows=1")
        maximum = resources.max_active_batches
        if capacity is not None and type(capacity) is not CoreRecordsCapacity:
            raise TypeError('capacity must be CoreRecordsCapacity')
        if capacity is not None and capacity.budget is not None and limits is None:
            raise ValueError('explicit resource budgets require explicit finite IPC limits')
        if capacity is not None and capacity.budget is not None and maximum != capacity.budget.active_requests:
            raise ValueError('actor resources must match the declared hard request budget')
        self.limits = (resolve_records_async_limits(resources,
            CoreRecordsCapacity() if capacity is None else capacity) if limits is None else limits)
        independent = capacity is not None and capacity.budget is not None
        if independent:
            resolve_records_async_limits(resources, capacity, self.limits)
        elif type(self.limits) is not AsyncEngineLimits or self.limits.max_inflight != maximum:
            raise ValueError("Async actor input capacity must match the Core request limit")
        if not independent and self.limits.result_slots < maximum:
            raise ValueError("Async actor needs one reserved result per Core request")
        self.rpc_workers = resources.cpu_workers if resources.model_rpc_workers is None else resources.model_rpc_workers
        self._rpc_loop = self._rpc_wake = None
        self._rpc_pending = self._rpc_inflight = 0
        self._rpc_unconfirmed = {}
        self._rpc_state_lock = threading.Condition()
        self._rpc_observer = None
        self._rpc_generation = self._rpc_batch_submits_pending = 0
        self._rpc_subscription = self._rpc_unsubscribe = None
        self._rpc_observer_error = None

    def _bind_rpc_progress(self, notify):
        if not callable(notify):
            raise TypeError('RPC progress observer must be callable')
        with self._rpc_state_lock:
            if self._rpc_pending:
                raise RuntimeError('RPC progress cannot rebind while calls are pending')
            token = object()
            def current():
                with self._rpc_state_lock:
                    if self._rpc_subscription is not token:
                        return
                notify()
            self._rpc_observer = current
            self._rpc_subscription, self._rpc_unsubscribe = token, None
            self._rpc_generation += 1
            return token

    def _unbind_rpc_progress(self, token):
        with self._rpc_state_lock:
            if self._rpc_subscription is not token:
                return False
            if self._rpc_pending:
                self._rpc_unsubscribe = token
                return False
            self._rpc_observer = self._rpc_subscription = self._rpc_unsubscribe = None
            return True

    def _publish_rpc_progress(self, observer):
        if observer is not None:
            try:
                observer()
            except Exception as error:
                self._rpc_observer_error = type(error).__name__

    def _rpc_changed_locked(self):
        if self._rpc_observer is not None:
            self._rpc_generation += 1
        return self._rpc_observer

    def _data_rpc_progress(self):
        with self._rpc_state_lock:
            return DataRpcProgress(self._rpc_generation, self.rpc_workers,
                self._rpc_pending, self._rpc_inflight, self._rpc_batch_submits_pending,
                len(self._rpc_unconfirmed))

    @classmethod
    def for_prepared(cls, engine, generation, resources, *, limits=None, capacity=None):
        """Own one model for caller-prepared calls, without a Filter formatter.

        The caller owns method/source validation and supplies generation options
        through submit_prepared_core. No preparation actor is allocated here.
        """
        return cls(None, engine, generation, resources, limits=limits, _prepared_only=True, capacity=capacity)

    def _model_actor_type(self):
        return AsyncCoreModelActor

    def _model_close_timeout_s(self):
        # Child API shutdown owns ten seconds, followed by bounded task/IPC
        # teardown. Match the private process owner's complete close window.
        return 15.0

    def execution_metadata(self):
        metadata = dict(model_batch_mode=self.model_batch_mode,
                        native_engine_abort_supported=self.native_engine_abort_supported,
                        async_limits=asdict(self.limits), model_rpc_workers=self.rpc_workers,
                        model_rpc_workers_source=('resources.cpu_workers' if self.resources.model_rpc_workers is None
                                                  else 'resources.model_rpc_workers'),
                        model_rpc_control_reserve=self.control_rpc_reserve,
                        model_rpc_handoff='nonblocking_submit_poll_ack')
        if type(self.model_ready) is dict and 'model_context' in self.model_ready:
            from src.serving.backends.model_context import checked_model_context
            metadata['model_context'] = checked_model_context(self.engine, self.model_ready)
        if self._prepared_only:
            metadata.update(preparation_scope="caller_prepared_messages",
                            prepare_actor_count=len(self.prepare_actors))
        return metadata

    def _model_actor_arguments(self):
        return (asdict(self.engine), asdict(self.generation), self.resources.model_batch_rows,
                asdict(self.limits))

    def _model_actor_options(self):
        return dict(num_cpus=1, num_gpus=1, max_restarts=0, max_task_retries=0,
            # One shutdown/drain waiter plus another control call can progress
            # while data RPCs use only their independently configured permits.
            max_concurrency=self.rpc_workers + self.control_rpc_reserve,
            runtime_env=_runtime_env(self.engine.driver_python))

    @staticmethod
    def _notify_rpc(loop, wake):
        try:
            loop.call_soon_threadsafe(wake.set)
        except RuntimeError:
            if not loop.is_closed():
                raise

    async def _data_rpc(self, method, *args, timings=None):
        loop = asyncio.get_running_loop()
        with self._rpc_state_lock:
            if self._rpc_loop is not loop:
                if self._rpc_pending:
                    raise RuntimeError('model RPCs still belong to the previous transport loop')
                self._rpc_loop = loop
                self._rpc_wake = asyncio.Event()
            self._rpc_pending += 1
            tracked_batch = self._rpc_observer is not None and method == 'try_submit_prepared_batch'
            self._rpc_batch_submits_pending += int(tracked_batch)
            wake = self._rpc_wake
        acquired = issued = finished = False
        ref = None

        def release():
            nonlocal finished
            with self._rpc_state_lock:
                if finished:
                    return
                finished = True
                self._rpc_pending -= 1
                self._rpc_batch_submits_pending -= int(tracked_batch)
                if acquired:
                    self._rpc_inflight -= 1
                self._rpc_unconfirmed.pop(id(ref), None)
                if (not self._rpc_pending and self._rpc_unsubscribe is not None
                        and self._rpc_unsubscribe is self._rpc_subscription):
                    self._rpc_observer = self._rpc_subscription = self._rpc_unsubscribe = None
                observer = self._rpc_changed_locked()
                self._rpc_state_lock.notify_all()
            self._notify_rpc(loop, wake)
            self._publish_rpc_progress(observer)

        def unconfirmed():
            with self._rpc_state_lock:
                if finished:
                    return
                # Only observer failures retain opaque RPC refs, at most the
                # data worker budget; model requests still use held/tickets.
                self._rpc_unconfirmed[id(ref)] = (ref, release)
                observer = self._rpc_changed_locked()
                self._rpc_state_lock.notify_all()
            self._notify_rpc(loop, wake)
            self._publish_rpc_progress(observer)

        def terminal(future):
            if future.cancelled():
                # Cancelling a local concurrent Future is not a Ray terminal.
                unconfirmed()
            else:
                release()

        try:
            while True:
                with self._rpc_state_lock:
                    if self._rpc_unconfirmed:
                        raise RuntimeError('a data RPC has no confirmed terminal observer')
                    if self._rpc_inflight < self.rpc_workers:
                        self._rpc_inflight += 1
                        acquired = True
                        break
                    wake.clear()
                await wake.wait()
            with _measure(timings, 'model_submit'):
                # Keep the existing poll hook visible to optional observers.
                ref = (self.poll_prepared_batch(*args) if method == 'poll_prepared_batch'
                       else getattr(self.model_actor, method).remote(*args))
            issued = True
            try:
                future = ref.future()
                future.add_done_callback(terminal)
                pending = asyncio.wrap_future(future)
            except BaseException:
                unconfirmed()
                raise
            # Task cancellation, including repeated cancellation and loop
            # shutdown, cannot cancel this Future or return its RPC permit.
            return await asyncio.shield(pending)
        finally:
            if not issued:
                release()

    def _rpc_cleanup_status(self, report, *, deadline):
        with self._rpc_state_lock:
            unconfirmed = tuple(self._rpc_unconfirmed.values())
        for ref, release in unconfirmed:
            try:
                # Ray object readiness, unlike a cancelled local Future, proves
                # the issued RPC has returned or raised. This issues no RPC.
                ready, _ = self.ray.wait([ref], num_returns=1,
                    timeout=max(0, deadline-time.monotonic()), fetch_local=False)
            except Exception:
                continue
            if ready:
                release()
        with self._rpc_state_lock:
            while self._rpc_pending and not self._rpc_unconfirmed and time.monotonic() < deadline:
                self._rpc_state_lock.wait(max(0, deadline-time.monotonic()))
            pending, unknown = self._rpc_pending, len(self._rpc_unconfirmed)
        report.update(data_rpc_pending=pending, data_rpc_unconfirmed=unknown)
        if pending:
            report['status'] = 'unknown'
            if 'model_retained' in report:
                report['model_retained'] = False
            report['cleanup_errors'] = list(report.get('cleanup_errors', ())) + ['data_rpc_terminal_unconfirmed']
        return report

    async def _core_result(self, method, arguments, key, maximum, *, timings=None):
        pair = (key.session_id, key.sequence)
        while True:
            response = await self._data_rpc(method, *arguments, timings=timings)
            if response['status'] != 'backpressure':
                break
            await asyncio.sleep(.01)
        if response['status'] == 'rejected':
            return _encode(response['result'], maximum)
        if response['status'] != 'accepted':
            raise RuntimeError('invalid model handoff acknowledgement')
        while True:
            response = json.loads(await self.wait_prepared_batch(pair))
            for receipt in response['results']:
                if tuple(receipt['request_key']) != pair:
                    raise ValueError('model result belongs to another Core request')
                return _encode(receipt['result'], maximum)
            if response['closed'] and not response['more_results']:
                raise RuntimeError('model owner closed without its retained Core receipt')
            await asyncio.sleep(.01)

    def submit_core(self, prepared, deadline, maximum, key, *, timings=None):
        if self._prepared_only:
            raise ValueError("prepared-only owner requires submit_prepared_core")
        return self._core_result('try_submit_core',
            (prepared, deadline, maximum, (key.session_id, key.sequence)), key, maximum, timings=timings)

    def submit_prepared_core(self, prepared, deadline, maximum, key, generation, token_cost=None):
        return self._core_result('try_submit_prepared_core',
            (prepared, deadline, maximum, (key.session_id, key.sequence), generation, token_cost), key, maximum)

    async def submit_prepared_batch(self, calls, batch_key, deadline):
        payload = _encode(dict(calls=calls, batch_key=list(batch_key), deadline=deadline), self.limits.input_bytes)
        self.peak_prepared_batch_rpc_bytes = max(getattr(self, 'peak_prepared_batch_rpc_bytes', 0), len(payload))
        while True:
            response = await self._data_rpc('try_submit_prepared_batch', payload)
            if response['status'] != 'backpressure':
                return response
            await asyncio.sleep(.01)

    def poll_prepared_batch(self, batch_key):
        return self.model_actor.poll_prepared_batch.remote(batch_key)

    async def wait_prepared_batch(self, batch_key):
        return await self._data_rpc('poll_prepared_batch', batch_key)

    def cancel_core(self, key):
        if self.model_actor is not None:
            return self.model_actor.abort_core.remote((key.session_id, key.sequence))

    def acknowledge_core(self, key):
        if self.model_actor is not None:
            return self.model_actor.acknowledge_core.remote((key.session_id, key.sequence))

    def drain_query(self, deadline):
        if self.model_actor is None:
            return dict(status='retained', model_ready=None, model_allocated=False,
                        model_retained=True, cleanup_errors=[])
        return self._rpc_cleanup_status(
            self.wait_ready(self.model_actor.drain_query.remote(deadline), deadline), deadline=deadline)

    def close(self):
        deadline = (self.close_deadline if self.close_deadline is not None else
                    time.monotonic() + self._model_close_timeout_s() + 1.)
        report = super().close()
        report["unconsumed_call_results"] = (report.get("engine_close_report") or {}).get(
            "unconsumed_call_results", [])
        report["unconsumed_core_results"] = (report.get("engine_close_report") or {}).get(
            "unconsumed_core_results", [])
        report["producer_quiescence"] = (report.get("engine_close_report") or {}).get("producer_quiescence")
        return self._rpc_cleanup_status(report, deadline=deadline)
