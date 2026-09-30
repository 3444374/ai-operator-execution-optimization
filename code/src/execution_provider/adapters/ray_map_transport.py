"""Optional Daft payload batches and Ray HTTP workers below the shared engine."""

import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from functools import partial
import json
from pathlib import Path
import time

from ...data.materializers.payloads import PayloadBatchLimits, iter_payload_batches
from .async_fixed_model import AsyncFixedModelTransport, exception_details


@dataclass(frozen=True)
class RayMapConfig:
    address: str
    workers: int
    batch_rows: int
    window_bytes: int
    object_bytes: int

    def __post_init__(self):
        if type(self.address) is not str or not self.address or self.address in ("auto", "local"):
            raise ValueError("Ray Map requires an explicit existing cluster address")
        if any(type(v) is not int or v < 1 for v in (
                self.workers, self.batch_rows, self.window_bytes, self.object_bytes)):
            raise ValueError("Ray Map capacities must be positive integers")
        if self.object_bytes < self.window_bytes + 8:
            raise ValueError("Ray object capacity must fit one input window")

    @classmethod
    def load(cls, path):
        path = Path(path)
        if path.stat().st_size > 4096:
            raise ValueError("Ray Map configuration is too large")
        return cls(**json.loads(path.read_text()))


@dataclass(frozen=True)
class _RemoteFailure:
    key: object
    stage: str
    reason: dict


class _HttpActor:
    def __init__(self, config, concurrency):
        self.transport = AsyncFixedModelTransport(config, concurrency)
        self.active = 0

    async def ready(self):
        return True

    async def execute(self, table, index, template):
        key = template.key
        self.active += 1
        start = time.monotonic_ns()
        stage = "payload"
        try:
            if (table["session_id"][index].as_py(), table["sequence"][index].as_py()) != (key.session_id, key.sequence):
                raise ValueError("Ray payload row identity differs")
            task = replace(template, task=replace(template.task, payload=table["payload"][index].as_py()))
            stage = "http"
            result = await self.transport.execute(task, "model")
            return key, result, start, time.monotonic_ns()
        except Exception as error:
            # Return only safe metadata across Ray; this is not a model receipt.
            return _RemoteFailure(key, stage, exception_details(error))
        finally:
            self.active -= 1

    async def close(self):
        if self.active:
            raise RuntimeError("Ray HTTP actor still has active requests")
        await self.transport.close()
        return True


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
        if ray_api is None:
            import ray as ray_api
        self.ray, self.config, self.physical = ray_api, config, physical
        self.capacity, self.observer, self.before_request = capacity, observer, before_request
        self.rows, self.pending, self.blocks = {}, deque(), {}
        self.used_bytes = self.ordinal = self.actor_index = 0
        self.observation_failed = False
        self.changed = asyncio.Event()
        self.flusher, self.running, self.unknown = None, set(), set()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="semloom-payload")
        self.owns_connection = not self.ray.is_initialized()
        self.actors = []
        try:
            if self.owns_connection:
                self.ray.init(address=physical.address)
            elif self.ray.get_runtime_context().gcs_address != physical.address:
                raise ValueError("initialized Ray connection differs from the declared GCS address")
            actor = self.ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0,
                                    max_concurrency=capacity)(_HttpActor)
            for _ in range(physical.workers):
                self.actors.append(actor.remote(config, capacity))
            self.ray.get([a.ready.remote() for a in self.actors], timeout=30)
        except BaseException:
            self._dispose()
            raise

    def _observe(self, event, **fields):
        if self.observer:
            try:
                self.observer(dict(event=event, object_bytes=self.used_bytes,
                                   object_limit_bytes=self.physical.object_bytes, **fields))
            except Exception:
                self.observation_failed = True

    def cancel_pending(self, key):
        row = self.rows.get(key)
        if row is not None and not row.sent:
            row.cancelled = True
            self.changed.set()

    async def execute(self, task, endpoint):
        if endpoint != "model" or task.key in self.rows or len(self.rows) >= self.capacity:
            raise ValueError("Ray Map admission differs from the core")
        if len(task.task.payload) + 24 > self.physical.window_bytes:
            return b'{"bridge_error":"MODEL_REQUEST_REJECTED"}'
        row = _Row(task, asyncio.get_running_loop().create_future())
        self.rows[task.key] = row
        self.pending.append(row)
        if self.flusher is None or self.flusher.done():
            self.flusher = asyncio.create_task(self._flush())
        try:
            return await row.future
        finally:
            self.rows.pop(task.key, None)

    async def _work(self, function, *args):
        return await asyncio.get_running_loop().run_in_executor(self.pool, function, *args)

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
            data = tuple((r.task.key.session_id, r.task.key.sequence, r.task.task.payload) for r in selected)
            stream = iter_payload_batches(data, PayloadBatchLimits(self.capacity, self.physical.window_bytes),
                                          batch_rows=self.physical.batch_rows)
            try:
                while True:
                    table = await self._work(next, stream, None)
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
                    self.used_bytes += amount
                    self.blocks[block_id] = _Block(None, amount, set(keys))
                    self._observe("ray_block_reserved", block_id=block_id, rows=len(keys), bytes=amount)
                    try:
                        reference = await self._work(self.ray.put, table)
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
                await self._work(stream.close)

    def _release(self, block_id, key):
        block = self.blocks[block_id]
        block.members.remove(key)
        if not block.members:
            self.used_bytes -= block.bytes
            del self.blocks[block_id]
            self.changed.set()
            self._observe("ray_block_released", block_id=block_id)

    async def _run_row(self, row, block_id, index):
        key = row.task.key
        if row.cancelled or self.observation_failed:
            self._release(block_id, key)
            row.future.set_result(b"" if row.cancelled else b'{"bridge_error":"MODEL_UNAVAILABLE"}')
            return
        try:
            if self.before_request:
                self.before_request(row.task)
        except Exception:
            self._release(block_id, key)
            row.future.set_result(b'{"bridge_error":"MODEL_UNAVAILABLE"}')
            return
        actor = self.actors[self.actor_index % len(self.actors)]
        self.actor_index += 1
        template = replace(row.task, task=replace(row.task.task, payload=b""))
        row.sent = True
        stage, remote_reason = "ray_submit", None
        try:
            call = actor.execute.remote(self.blocks[block_id].reference, index, template)
            stage = "ray_await"
            reply = await call
            stage = "result_validation"
            if isinstance(reply, _RemoteFailure):
                if reply.key != key or reply.stage not in ("payload", "http"):
                    raise ValueError("Ray failure differs from its row or execution stage")
                stage, remote_reason = reply.stage, reply.reason
                raise RuntimeError("remote actor execution failed")
            actual_key, result, started, ended = reply
            if actual_key != key or type(result) is not bytes or len(result) > row.task.task.max_result_bytes:
                raise ValueError("Ray result differs from its row or result capacity")
            self._observe("ray_http_completed", key=dict(session_id=key.session_id, sequence=key.sequence),
                          worker_elapsed_ns=ended-started)
        except Exception as error:
            reason = remote_reason if remote_reason is not None else exception_details(error)
            self._observe("ray_execution_error", key=dict(session_id=key.session_id, sequence=key.sequence),
                          stage=stage, reason=reason, remote_outcome="unconfirmed")
            self.unknown.add(key)
            row.future.set_exception(RuntimeError("unconfirmed Ray model execution: " + reason["exception_type"]))
            return
        self._release(block_id, key)
        row.future.set_result(result if not self.observation_failed else b'{"bridge_error":"MODEL_UNAVAILABLE"}')

    def _dispose(self):
        failure = None
        try:
            for actor in self.actors:
                try:
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
                   transport_factory=partial(RayMapTransport, physical=physical, before_request=before_request))
