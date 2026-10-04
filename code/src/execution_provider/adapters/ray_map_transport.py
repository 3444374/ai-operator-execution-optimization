"""Finite payload batches and Ray HTTP workers below the shared engine."""

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass, replace
from functools import partial
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import time
from typing import NamedTuple
import uuid

from ...data.materializers.payloads import PayloadBatchLimits, iter_payload_batches
from ...scheduling.runtime.stage_broker import StageBrokerLimits
from .async_fixed_model import AsyncFixedModelTransport, exception_details


@dataclass(frozen=True)
class RayMapConfig:
    address: str
    workers: int
    batch_rows: int
    window_bytes: int
    object_bytes: int
    worker_pool: str | None = None
    payload_backend: str = 'daft'
    preparation: StageBrokerLimits | None = None

    def __post_init__(self):
        if self.payload_backend not in ('daft', 'arrow'):
            raise ValueError("unknown Ray Map payload backend")
        if self.preparation is not None and (
                type(self.preparation) is not StageBrokerLimits
                or self.preparation.prepare_inflight != 1
                or self.preparation.ready_bytes > self.object_bytes):
            raise ValueError("Map preparation needs one prepare worker within the object byte capacity")
        if type(self.address) is not str or not self.address or self.address in ("auto", "local"):
            raise ValueError("Ray Map requires an explicit existing cluster address")
        if any(type(v) is not int or v < 1 for v in (
                self.workers, self.batch_rows, self.window_bytes, self.object_bytes)):
            raise ValueError("Ray Map capacities must be positive integers")
        if self.object_bytes < self.window_bytes + 8:
            raise ValueError("Ray object capacity must fit one input window")
        if self.worker_pool is not None and (type(self.worker_pool) is not str
                or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,47}", self.worker_pool)):
            raise ValueError("Ray worker pool requires a short explicit service identity")

    @classmethod
    def load(cls, path):
        path = Path(path)
        if path.stat().st_size > 4096:
            raise ValueError("Ray Map configuration is too large")
        value = json.loads(path.read_text())
        if value.get('preparation') is not None:
            value['preparation'] = StageBrokerLimits(**value['preparation'])
        return cls(**value)


@dataclass(frozen=True)
class _RemoteFailure:
    key: object
    stage: str
    reason: dict


def _clock_domain():
    """Identify a shared Linux monotonic clock without exposing host identity."""
    try:
        value = (Path('/proc/sys/kernel/random/boot_id').read_bytes()
                 + os.readlink('/proc/self/ns/time').encode()
                 + time.get_clock_info('monotonic').implementation.encode())
    except OSError:
        return None
    return hashlib.sha256(value).hexdigest()


class _RemoteResult(NamedTuple):
    key: object
    result: bytes
    started_ns: int
    ended_ns: int
    clock_domain: str | None


def _model_identity(config):
    value = json.dumps(asdict(config), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(value).hexdigest()


class _HttpActor:
    def __init__(self, config, concurrency, managed=False):
        self.transport = AsyncFixedModelTransport(config, concurrency)
        self.active = 0
        self.managed, self.owner = managed, None
        self.identity, self.capacity = _model_identity(config), concurrency
        self.clock_domain = _clock_domain()

    async def ready(self):
        return True

    async def claim(self, owner, identity, capacity):
        if type(owner) is not str or not re.fullmatch(r"[0-9a-f]{32}", owner):
            raise ValueError("Ray worker requires an explicit query identity")
        if not self.managed or identity != self.identity or capacity != self.capacity:
            raise ValueError("Ray worker model or request capacity differs")
        if self.owner is not None or self.active:
            raise RuntimeError("Ray worker already has a query owner")
        self.owner = owner
        return True

    async def release(self, owner):
        if not self.managed or self.owner is None or self.owner != owner or self.active:
            raise RuntimeError("Ray worker query ownership has not settled")
        self.owner = None
        return True

    async def execute(self, table, index, template, owner=None):
        key = template.key
        start = time.monotonic_ns()
        stage = "payload"
        counted = False
        try:
            if (self.managed and (owner is None or self.owner != owner)) or (not self.managed and owner is not None):
                raise ValueError("Ray request differs from its query owner")
            self.active += 1
            counted = True
            if (table["session_id"][index].as_py(), table["sequence"][index].as_py()) != (key.session_id, key.sequence):
                raise ValueError("Ray payload row identity differs")
            task = replace(template, task=replace(template.task, payload=table["payload"][index].as_py()))
            stage = "http"
            result = await self.transport.execute(task, "model")
            return _RemoteResult(key, result, start, time.monotonic_ns(), self.clock_domain)
        except Exception as error:
            # Return only safe metadata across Ray; this is not a model receipt.
            return _RemoteFailure(key, stage, exception_details(error))
        finally:
            if counted:
                self.active -= 1

    async def close(self):
        if self.active or self.owner is not None:
            raise RuntimeError("Ray HTTP actor still has active requests or a query owner")
        await self.transport.close()
        return True


def _worker_class(ray, capacity):
    return ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0,
                      max_concurrency=capacity)(_HttpActor)


@contextmanager
def owned_map_worker_pool(ray, config, capacity, physical):
    """Caller-owned named workers; queries claim them exclusively and release them."""
    if (physical.worker_pool is None or not ray.is_initialized()
            or ray.get_runtime_context().gcs_address != physical.address
            or type(capacity) is not int or capacity < max(physical.workers, physical.batch_rows)):
        raise ValueError("worker service requires its declared live Ray cluster and capacities")
    actors = []
    try:
        actor = _worker_class(ray, capacity)
        for index in range(physical.workers):
            actors.append(actor.options(name=f"{physical.worker_pool}-{index}",
                                        namespace="semloom-map").remote(config, capacity, True))
        ray.get([a.ready.remote() for a in actors], timeout=30)
        yield
    finally:
        failure = None
        for actor in actors:
            try:
                ray.get(actor.close.remote(), timeout=config.timeout_ms / 1000 + 1)
            except Exception as error:
                failure = error
            finally:
                try:
                    ray.kill(actor, no_restart=True)
                except Exception as error:
                    failure = error
        if failure is not None:
            raise RuntimeError("worker service cleanup could not be confirmed") from failure


@dataclass
class _Row:
    task: object
    future: object
    sent: bool = False
    cancelled: bool = False


@dataclass
class _Block:
    reference: object
    bytes: int
    members: set


class RayMapTransport:
    """Runs only on the async I/O loop after startup; the core keeps admission.

    Cancellation suppresses unsent rows, but never cancels remote HTTP awaits.
    Each row completes independently. Block references remain charged until all
    their rows have confirmed outcomes; uncertain calls retain their blocks.
    """

    def __init__(self, config, capacity, observer=None, *, physical, before_request=None, ray_api=None):
        if physical.batch_rows > capacity:
            raise ValueError("batch rows exceed the active request capacity")
        if physical.workers > capacity:
            raise ValueError("Ray worker count exceeds the active request capacity")
        self.started_ns = time.monotonic_ns()
        self.clock_domain = _clock_domain()
        self.config, self.physical = config, physical
        self.capacity, self.observer, self.before_request = capacity, observer, before_request
        self.rows, self.pending, self.blocks = {}, deque(), {}
        self._used_bytes = self.ordinal = self.actor_index = 0
        self.preparation = None
        self.observation_failed = False
        self.changed = asyncio.Event()
        self.flusher, self.running, self.unknown = None, set(), set()
        with self._startup_stage("library_import"):
            if ray_api is None:
                import ray as ray_api
        self.ray = ray_api
        self.owns_connection = not self.ray.is_initialized()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="semloom-payload")
        self.actors = []
        self.leased = []
        self.worker_owner = uuid.uuid4().hex if physical.worker_pool else None
        self.first_payload = self.first_submit = True
        try:
            with self._startup_stage("driver_connect"):
                if self.owns_connection:
                    self.ray.init(address=physical.address)
                elif self.ray.get_runtime_context().gcs_address != physical.address:
                    raise ValueError("initialized Ray connection differs from the declared GCS address")
            if physical.worker_pool:
                with self._startup_stage("actor_bind"):
                    for index in range(physical.workers):
                        actor = self.ray.get_actor(f"{physical.worker_pool}-{index}", namespace="semloom-map")
                        if self.ray.get(actor.claim.remote(self.worker_owner, _model_identity(config), capacity), timeout=30) is not True:
                            raise ValueError("Ray worker query claim was not confirmed")
                        self.leased.append(actor)
                        self.actors.append(actor)
            else:
                with self._startup_stage("actor_create"):
                    actor = _worker_class(self.ray, capacity)
                    for _ in range(physical.workers):
                        self.actors.append(actor.remote(config, capacity))
                with self._startup_stage("actor_ready"):
                    self.ray.get([a.ready.remote() for a in self.actors], timeout=30)
        except BaseException:
            self._dispose()
            raise

    @contextmanager
    def _startup_stage(self, stage):
        started = time.monotonic_ns()
        status = "failed"
        try:
            yield
            status = "completed"
        finally:
            self._observe("ray_startup", stage=stage, status=status,
                          elapsed_seconds=(time.monotonic_ns()-started)/1e9)

    def _observe(self, event, **fields):
        if self.observer:
            try:
                self.observer(dict(event=event, object_bytes=self.used_bytes,
                                   object_limit_bytes=self.physical.object_bytes, **fields))
            except Exception:
                self.observation_failed = True

    def cancel_pending(self, key):
        if self.preparation is not None:
            self.preparation.cancel(key)
        row = self.rows.get(key)
        if row is not None and not row.sent:
            row.cancelled = True
            self.changed.set()

    @property
    def used_bytes(self):
        return (self.preparation.snapshot()['object_bytes']
                if self.preparation is not None else self._used_bytes)

    @used_bytes.setter
    def used_bytes(self, value):
        if self.preparation is not None:
            raise RuntimeError('prepared object accounting belongs to its broker')
        self._used_bytes = value

    def prepare_inputs(self, limits, notify):
        """Opt-in single-Job composition; model admission stays with the Core."""
        from .map_preparation import MapInputPreparation
        stage = self.physical.preparation
        if stage is None or self.preparation is not None:
            raise ValueError('Map input preparation requires one explicit setup')
        if (stage.encoded_bytes < limits.item_input_bytes + 24
                or stage.ready_bytes < 2 * (limits.item_input_bytes + 32)
                or stage.ready_work < limits.active_work
                or stage.model_inflight < limits.held_tasks):
            raise ValueError('Map preparation capacities cannot hold one legal input and all borrowed blocks')
        self.preparation = MapInputPreparation(self.ray, self.pool, self.physical,
            max_tasks=limits.held_tasks, notify=notify, observe=self._observe,
            model_signature=_model_identity(self.config))
        return self.preparation

    async def execute(self, task, endpoint):
        if endpoint != "model" or task.key in self.rows or len(self.rows) >= self.capacity:
            raise ValueError("Ray Map admission differs from the core")
        if len(task.task.payload) + 24 > self.physical.window_bytes:
            return b'{"bridge_error":"MODEL_REQUEST_REJECTED"}'
        row = _Row(task, asyncio.get_running_loop().create_future())
        self.rows[task.key] = row
        if self.preparation is not None:
            try:
                prepared = self.preparation.claim(task)
                if prepared is None:
                    return b''
                if prepared is False:
                    return b'{"bridge_error":"MODEL_UNAVAILABLE"}'
                block_id, index = prepared
                await self._run_row(row, block_id, index)
                return await row.future
            finally:
                self.rows.pop(task.key, None)
        self.pending.append(row)
        if self.flusher is None or self.flusher.done():
            self.flusher = asyncio.create_task(self._flush())
        try:
            return await row.future
        finally:
            self.rows.pop(task.key, None)

    async def _work(self, function, *args, stage=None, **fields):
        loop = asyncio.get_running_loop()
        if self.observer is None or stage is None:
            return await loop.run_in_executor(self.pool, function, *args)
        submitted = time.monotonic_ns()
        times = []
        def run():
            times.append(time.monotonic_ns())
            try:
                return function(*args)
            finally:
                times.append(time.monotonic_ns())
        status = 'failed'
        try:
            value = await loop.run_in_executor(self.pool, run)
            status = 'completed'
            return value
        finally:
            resumed = time.monotonic_ns()
            self._observe('ray_work', stage=stage, status=status,
                          submitted_ns=submitted, resumed_ns=resumed,
                          queue_ns=times[0]-submitted if times else None,
                          work_ns=times[1]-times[0] if len(times)==2 else None,
                          resume_ns=resumed-times[1] if len(times)==2 else None,
                          elapsed_ns=resumed-submitted, **fields)

    async def _flush(self):
        while self.pending:
            selected, size = [], 0
            while self.pending and len(selected) < self.capacity:
                row = self.pending[0]
                amount = len(row.task.task.payload) + 24
                if selected and (size + amount > self.physical.window_bytes
                                 or row.task.spec.job_id != selected[0].task.spec.job_id):
                    break
                selected.append(self.pending.popleft())
                size += amount
            by_key = {r.task.key: r for r in selected}
            first_key = dict(session_id=selected[0].task.key.session_id,
                             sequence=selected[0].task.key.sequence)
            data = tuple((r.task.key.session_id, r.task.key.sequence, r.task.task.payload) for r in selected)
            stream = iter_payload_batches(data, PayloadBatchLimits(self.capacity, self.physical.window_bytes),
                                          batch_rows=self.physical.batch_rows, backend=self.physical.payload_backend)
            try:
                while True:
                    measure = self._startup_stage("first_payload") if self.first_payload else nullcontext()
                    self.first_payload = False
                    with measure:
                        table = await self._work(next, stream, None, stage='payload_next', key=first_key)
                    if table is None:
                        break
                    # Count backing Arrow buffers, including any shared slice storage.
                    amount = table.get_total_buffer_size()
                    if amount > self.physical.object_bytes:
                        raise ValueError("one Ray object exceeds its byte capacity")
                    while self.used_bytes + amount > self.physical.object_bytes:
                        if all(row.cancelled for row in by_key.values()):
                            for row in by_key.values():
                                row.future.set_result(b"")
                            by_key.clear()
                            break
                        self.changed.clear()
                        await self.changed.wait()
                    if not by_key:
                        break
                    keys = [(table["session_id"][i].as_py(), table["sequence"][i].as_py())
                            for i in range(table.num_rows)]
                    from ...scheduling.core.session_contract import TaskKey
                    keys = [TaskKey(*key) for key in keys]
                    block_id = self.ordinal
                    self.ordinal += 1
                    self._used_bytes += amount
                    self.blocks[block_id] = _Block(None, amount, set(keys))
                    self._observe("ray_block_reserved", block_id=block_id, rows=len(keys), bytes=amount)
                    try:
                        reference = await self._work(self.ray.put, table, stage='object_put',
                                                     block_id=block_id, key=first_key)
                    except BaseException:
                        for key in keys:
                            self._release(block_id, key)
                        raise
                    self.blocks[block_id].reference = reference
                    self._observe("ray_block_put", block_id=block_id, rows=len(keys), bytes=amount)
                    for index, key in enumerate(keys):
                        row = by_key.pop(key)
                        operation = asyncio.create_task(self._run_row(row, block_id, index))
                        self.running.add(operation)
                        operation.add_done_callback(self.running.discard)
                    del table, reference
            except Exception:
                # No remote method was issued for these remaining members.
                for row in by_key.values():
                    if not row.future.done():
                        row.future.set_result(b'{"bridge_error":"MODEL_UNAVAILABLE"}')
            finally:
                await self._work(stream.close, stage='payload_close', key=first_key)

    def _release(self, block_id, key, *, failed=False):
        if self.preparation is not None:
            self.preparation.confirm(key, failed=failed)
            return
        block = self.blocks[block_id]
        block.members.remove(key)
        if not block.members:
            self._used_bytes -= block.bytes
            del self.blocks[block_id]
            self.changed.set()
            self._observe("ray_block_released", block_id=block_id)

    async def _run_row(self, row, block_id, index):
        key = row.task.key
        if row.cancelled or self.observation_failed or row.future.cancelled():
            self._release(block_id, key, failed=True)
            if not row.future.done():
                row.future.set_result(b"" if row.cancelled else b'{"bridge_error":"MODEL_UNAVAILABLE"}')
            return
        try:
            if self.before_request:
                started = time.monotonic_ns()
                status = 'failed'
                try:
                    guarded = self.before_request(row.task)
                    if inspect.isawaitable(guarded):
                        await guarded
                    status = 'completed'
                finally:
                    self._observe('ray_request_guard', key=dict(session_id=key.session_id, sequence=key.sequence),
                                  status=status, elapsed_ns=time.monotonic_ns()-started)
        except asyncio.CancelledError:
            self._release(block_id, key, failed=True)
            if not row.future.done():
                row.future.set_result(b'')
            raise
        except Exception:
            self._release(block_id, key, failed=True)
            if not row.future.done():
                row.future.set_result(b'{"bridge_error":"MODEL_UNAVAILABLE"}')
            return
        # Accounting may have yielded while the database cancelled this row.
        if row.cancelled or self.observation_failed or row.future.cancelled():
            self._release(block_id, key, failed=True)
            if not row.future.done():
                row.future.set_result(b'' if row.cancelled else b'{"bridge_error":"MODEL_UNAVAILABLE"}')
            return
        actor = self.actors[self.actor_index % len(self.actors)]
        self.actor_index += 1
        template = replace(row.task, task=replace(row.task.task, payload=b""))
        row.sent = True
        stage, remote_reason = "ray_submit", None
        try:
            if self.first_submit:
                self.first_submit = False
                self._observe("ray_first_submit", key=dict(session_id=key.session_id, sequence=key.sequence),
                              elapsed_seconds=(time.monotonic_ns()-self.started_ns)/1e9)
            reference = (self.preparation.reference(block_id) if self.preparation is not None
                         else self.blocks[block_id].reference)
            arguments = (reference, index, template)
            rpc_started = time.monotonic_ns()
            call = actor.execute.remote(*arguments, self.worker_owner) if self.worker_owner else actor.execute.remote(*arguments)
            rpc_returned = time.monotonic_ns()
            stage = "ray_await"
            reply = await call
            received = time.monotonic_ns()
            stage = "result_validation"
            if isinstance(reply, _RemoteFailure):
                if reply.key != key or reply.stage not in ("payload", "http"):
                    raise ValueError("Ray failure differs from its row or execution stage")
                stage, remote_reason = reply.stage, reply.reason
                raise RuntimeError("remote actor execution failed")
            if isinstance(reply, _RemoteResult):
                actual_key, result, started, ended, domain = reply
            else:
                actual_key, result, started, ended = reply
                domain = None
            if actual_key != key or type(result) is not bytes or len(result) > row.task.task.max_result_bytes:
                raise ValueError("Ray result differs from its row or result capacity")
            if type(started) is not int or type(ended) is not int or not 0 <= started <= ended:
                raise ValueError('Ray worker duration is invalid')
            shared_clock = domain is not None and domain == self.clock_domain
            if shared_clock and not rpc_started <= started <= ended <= received:
                raise ValueError('Ray shared-clock observations are not causal')
            self._observe("ray_http_completed", key=dict(session_id=key.session_id, sequence=key.sequence),
                          worker_elapsed_ns=ended-started, submit_elapsed_ns=rpc_returned-rpc_started,
                          await_elapsed_ns=received-rpc_returned, rpc_elapsed_ns=received-rpc_started,
                          rpc_started_ns=rpc_started, rpc_returned_ns=rpc_returned, received_ns=received,
                          worker_started_ns=started, worker_ended_ns=ended,
                          shared_clock=shared_clock,
                          before_worker_ns=started-rpc_started if shared_clock else None,
                          after_worker_ns=received-ended if shared_clock else None)
        except Exception as error:
            reason = remote_reason if remote_reason is not None else exception_details(error)
            self._observe("ray_execution_error", key=dict(session_id=key.session_id, sequence=key.sequence),
                          stage=stage, reason=reason, remote_outcome="unconfirmed")
            self.unknown.add(key)
            if not row.future.done():
                row.future.set_exception(RuntimeError("unconfirmed Ray model execution: " + reason["exception_type"]))
            return
        self._release(block_id, key)
        if not row.future.done():
            row.future.set_result(result if not self.observation_failed else b'{"bridge_error":"MODEL_UNAVAILABLE"}')

    def _dispose(self):
        failure = None
        try:
            actors = self.leased if self.worker_owner else self.actors
            for actor in actors:
                try:
                    if self.worker_owner:
                        if not self.unknown:
                            if self.ray.get(actor.release.remote(self.worker_owner), timeout=30) is not True:
                                raise RuntimeError("Ray worker release was not confirmed")
                    else:
                        self.ray.kill(actor, no_restart=True)
                except Exception as error:
                    failure = error
        finally:
            self.pool.shutdown(wait=True)
            if self.owns_connection:
                self.ray.shutdown()
        if failure is not None:
            raise RuntimeError("owned Ray actor cleanup could not be confirmed") from failure

    def abort_startup(self):
        """No model work exists before the async backend starts accepting tasks."""
        self._dispose()

    async def close(self):
        try:
            if self.flusher:
                await self.flusher
            if self.running:
                await asyncio.gather(*self.running)
            if self.unknown:
                raise RuntimeError("Ray model executions remain unconfirmed")
            if self.preparation is not None:
                self.preparation.close()
            if not self.worker_owner:
                for actor in self.actors:
                    await actor.close.remote()
            if self.blocks or self.used_bytes:
                raise RuntimeError("Ray payload references remain held")
            self._observe("ray_transport_closed", confirmed=True)
            if self.observation_failed:
                raise RuntimeError("Ray transport observation failed")
        finally:
            self._dispose()


def ray_map_factory(physical, before_request=None):
    from .incremental_execution import build_fixed_model_execution
    from ..wire.framing import MAX_FRAME_BYTES
    if physical.window_bytes < MAX_FRAME_BYTES + 24:
        raise ValueError("Ray payload window must fit one legal protocol request and its keys")
    return partial(build_fixed_model_execution,
                   preparation_factory=(lambda transport, limits, notify:
                       transport.prepare_inputs(limits, notify)) if physical.preparation is not None else None,
                   transport_factory=partial(RayMapTransport, physical=physical, before_request=before_request))
