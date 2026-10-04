"""Optional finite Map preparation before the Core grants model admission."""

from dataclasses import asdict, dataclass, replace
import hashlib
import struct
import threading
import time

from ...data.materializers.payloads import PayloadBatchLimits, iter_payload_batches
from ...planning.blocks import StageBlockDescriptor
from ...planning.work import StageWork, WorkDescriptor
from ...scheduling.runtime.stage_broker import BoundedStageBroker
from .async_fixed_model import exception_details


@dataclass
class _PreparedRow:
    task: object
    block_id: str
    index: int
    state: str = 'queued'
    cancelled: bool = False


@dataclass
class _PreparedBlock:
    descriptor: StageBlockDescriptor
    tasks: tuple
    members: set
    reference: object = None
    amount: int = 0
    model_lease: object = None
    failed: bool = False


class MapInputPreparation:
    """Borrow accepted rows, prepare one sealed block at a time, retain references.

    The Core alone grants per-row request/work admission. A broker model lease
    describes a shared block from its first admitted row to its last known
    outcome; it is not a count of active HTTP requests or GPU work.
    """

    def __init__(self, ray, pool, physical, *, max_tasks, notify, observe, model_signature):
        self.ray, self.pool, self.physical = ray, pool, physical
        self.maximum, self.notify, self.observe = max_tasks, notify, observe
        self.model_signature = model_signature
        self.broker = BoundedStageBroker(physical.preparation)
        self.rows, self.blocks = {}, {}
        # Object transitions and their observations share one order. The
        # transport observer reads snapshot() while this lock is held.
        self.lock = threading.RLock()
        self.ordinal = 0
        self.running = False
        self.worker = None

    @staticmethod
    def _work_identity(task):
        work = task.task.info.work if task.task.info is not None else None
        return (work.calibration_signature, work.primary.unit) if work else ('request-count', 'work_units')

    def try_prepare(self, tasks):
        if not self.lock.acquire(blocking=False):
            return 0
        try:
            if (type(tasks) is not tuple or len({t.key for t in tasks}) != len(tasks)
                    or any(t.key in self.rows for t in tasks)):
                raise ValueError('invalid or duplicate prepared Map row')
            available = self.maximum - len(self.rows)
            selected, size, work = [], 0, 0
            held = self.broker.snapshot().encoded_held_bytes
            for task in tasks[:min(available, self.physical.batch_rows)]:
                if task.key in self.rows:
                    raise ValueError('duplicate prepared Map row')
                if selected and (task.spec.job_id != selected[0].spec.job_id
                        or self._work_identity(task) != self._work_identity(selected[0])):
                    break
                amount = len(task.task.payload) + 24
                if (size + amount > self.physical.window_bytes
                        or held + size + amount > self.broker.limits.encoded_bytes
                        or 2 * (size + amount + 8) > self.broker.limits.ready_bytes
                        or work + task.task.estimated_work > self.broker.limits.ready_work):
                    break
                selected.append(task)
                size += amount
                work += task.task.estimated_work
            if not selected:
                return 0
            signature, unit = self._work_identity(selected[0])
            digest = hashlib.sha256()
            for task in selected:
                digest.update(struct.pack('!QQQ', task.key.session_id, task.key.sequence,
                                          len(task.task.payload)))
                digest.update(task.task.payload)
            block_id = f'map-prepared-{self.ordinal}'
            descriptor = StageBlockDescriptor(
                block_id, selected[0].spec.job_id, self.ordinal,
                tuple(f'{t.key.session_id}:{t.key.sequence}' for t in selected),
                'encoded', (len(selected),), 'variable_binary', 'uint8_bytes',
                size, size, 2 * (size + 8), digest.hexdigest(),
                f'map-payload-v1-{self.physical.payload_backend}', self.model_signature,
                WorkDescriptor((StageWork('prepare', len(selected), 'rows'),
                                StageWork('model', work, unit)), 'model', signature),
                time.monotonic())
            self.broker.enqueue_encoded(descriptor)
            self.ordinal += 1
            self.blocks[block_id] = _PreparedBlock(
                descriptor, tuple(selected), {t.key for t in selected})
            for index, task in enumerate(selected):
                self.rows[task.key] = _PreparedRow(task, block_id, index)
            self._schedule()
            return len(selected)
        finally:
            self.lock.release()

    def _schedule(self):
        # Called with the lock held. At most one queued/running drain exists.
        if not self.running and self.broker.snapshot().encoded_queued:
            self.running = True
            self.worker = self.pool.submit(self._drain)

    def _prepare(self, tasks):
        data = tuple((t.key.session_id, t.key.sequence, t.task.payload) for t in tasks)
        stream = iter_payload_batches(data,
            PayloadBatchLimits(len(data), self.physical.window_bytes),
            batch_rows=min(len(data), self.physical.batch_rows),
            backend=self.physical.payload_backend)
        try:
            table = next(stream)
            amount = table.get_total_buffer_size()
            # One sealed block fits one output batch. Count backing buffers,
            # including a slice that retains another buffer's storage.
            if (table.num_rows != len(data) or amount > sum(len(t[2]) + 24 for t in data) + 8
                    or next(stream, None) is not None):
                raise ValueError('prepared Map block differs from its reserved representation')
            return table, amount
        finally:
            stream.close()

    def _drain(self):
        while True:
            with self.lock:
                lease = self.broker.lease_prepare(now_s=time.monotonic())
                if lease is None:
                    self.running = False
                    return
                block = self.blocks[lease.descriptor.block_id]
                for task in block.tasks:
                    self.rows[task.key].state = 'preparing'
                del task
                tasks = block.tasks
            started = time.monotonic_ns()
            reference, amount, failed = None, 0, False
            payload_ns, put_ns, reason = None, None, None
            stage = 'payload'
            table = None
            try:
                table, amount = self._prepare(tasks)
                payload_ns = time.monotonic_ns() - started
                with self.lock:
                    cancelled = all(self.rows[t.key].cancelled for t in tasks)
                    if not cancelled:
                        block.amount = amount
                        self.observe('ray_block_reserved', block_id=block.descriptor.block_id,
                                     rows=len(tasks), bytes=amount)
                if not cancelled:
                    stage = 'object_put'
                    put_started = time.monotonic_ns()
                    reference = self.ray.put(table)
                    put_ns = time.monotonic_ns() - put_started
                del table
            except Exception as error:
                failed = True
                reason = exception_details(error)
            finally:
                # Drop a materialized table even when put fails, before returning
                # its reservation or starting another prepare operation.
                table = None
            put_done = reference is not None and not failed
            keys = tuple(t.key for t in tasks)
            del tasks
            with self.lock:
                block.tasks = ()
                if failed or reference is None:
                    self.broker.fail_prepare(lease.lease_id, requeue=False)
                    self.broker.release_terminal(block.descriptor.block_id)
                    del self.blocks[block.descriptor.block_id]
                    if block.amount:
                        self.observe('ray_block_released', block_id=block.descriptor.block_id)
                    for key in keys:
                        row = self.rows[key]
                        row.state = 'done' if row.cancelled else 'failed'
                        if row.cancelled:
                            row.task = None
                else:
                    block.reference, block.amount = reference, amount
                    prepared = replace(block.descriptor, representation='ray_arrow_payload',
                                       logical_bytes=amount, physical_bytes=amount)
                    self.broker.complete_prepare(lease.lease_id, prepared, now_s=time.monotonic())
                    for key in keys:
                        self.rows[key].state = 'ready'
                self.observe('ray_preparation_completed', block_id=block.descriptor.block_id,
                             rows=len(keys), bytes=amount, failed=failed,
                             stage=stage, reason=reason, payload_ns=payload_ns, put_ns=put_ns,
                             elapsed_ns=time.monotonic_ns() - started, preparation=self.snapshot())
                if put_done:
                    self.observe('ray_block_put', block_id=block.descriptor.block_id,
                                 rows=len(keys), bytes=amount)
                    for key in keys:
                        if self.rows[key].cancelled:
                            self._settle(key, failed=True)
            reference = None
            self.notify()

    def is_ready(self, key):
        if not self.lock.acquire(blocking=False):
            return False
        try:
            return key in self.rows and self.rows[key].state in ('ready', 'failed', 'done')
        finally:
            self.lock.release()

    def claim(self, task):
        with self.lock:
            row = self.rows[task.key]
            if row.cancelled:
                return None
            if row.state == 'done':
                raise ValueError('prepared Map row already settled')
            if row.task.key != task.key or row.task.spec != task.spec or row.task.task != task.task:
                raise ValueError('prepared Map input identity differs from admitted execution')
            if row.state == 'failed':
                row.state, row.task = 'done', None
                return False
            if row.state != 'ready':
                raise ValueError('Map input is not ready for model admission')
            block = self.blocks[row.block_id]
            if block.model_lease is None:
                block.model_lease = self.broker.lease_model(
                    now_s=time.monotonic(), block_id=row.block_id)
                if block.model_lease is None:
                    raise RuntimeError('prepared block has no model lease capacity')
            row.state, row.task = 'modeling', None
            return row.block_id, row.index

    def reference(self, block_id):
        with self.lock:
            return self.blocks[block_id].reference

    def cancel(self, key):
        with self.lock:
            if key in self.rows:
                self.rows[key].cancelled = True

    def _settle(self, key, *, failed):
        row = self.rows[key]
        block = self.blocks[row.block_id]
        row.state, row.task = 'done', None
        block.failed |= failed
        block.members.remove(key)
        if block.members:
            return False
        if block.model_lease is None:
            self.broker.cancel_queued(row.block_id)
        elif block.failed:
            self.broker.fail_model(block.model_lease.lease_id, requeue=False)
        else:
            self.broker.complete_model(block.model_lease.lease_id,
                                       output_row_ids=block.descriptor.row_ids)
        self.broker.release_terminal(row.block_id)
        block.reference = None
        del self.blocks[row.block_id]
        self.observe('ray_block_released', block_id=row.block_id)
        return True

    def confirm(self, key, *, failed=False):
        with self.lock:
            self._settle(key, failed=failed)
            self._schedule()
        self.notify()

    def release(self, key):
        if not self.lock.acquire(blocking=False):
            return False
        try:
            row = self.rows.get(key)
            if row is None:
                return True
            if row.state in ('done', 'failed'):
                del self.rows[key]
                return True
            row.cancelled = True
            if row.state == 'modeling':
                return False  # No local cancellation confirms a remote outcome.
            block = self.blocks[row.block_id]
            if row.state == 'ready':
                self._settle(key, failed=True)
                del self.rows[key]
                self._schedule()
                return True
            if (self.broker.state_of(row.block_id) == 'encoded'
                    and all(self.rows[t.key].cancelled for t in block.tasks)):
                self.broker.cancel_queued(row.block_id)
                self.broker.release_terminal(row.block_id)
                for task in block.tasks:
                    self.rows[task.key].state, self.rows[task.key].task = 'done', None
                del self.blocks[row.block_id]
                del self.rows[key]
                return True
            return False
        finally:
            self.lock.release()

    def snapshot(self):
        with self.lock:
            return dict(held_tasks=len(self.rows), task_limit=self.maximum,
                        object_bytes=sum(b.amount for b in self.blocks.values()),
                        stages=asdict(self.broker.snapshot()))

    def close(self):
        with self.lock:
            if self.rows or self.blocks or not self.broker.is_drained():
                raise RuntimeError('prepared Map inputs remain owned')
