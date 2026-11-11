"""Bounded cross-request P/D pipeline over the existing native async owners."""
from collections import Counter, deque
from dataclasses import replace
from pathlib import Path
import threading
import time

from .async_engine_process import AsyncEngineProcess
from .async_engine_wire import FrameCapacityExceeded
from .prefill_decode_process import NixlPlacement
from .stage_process_cleanup import StageEngineCleanup
from .vllm_protocol import AsyncEngineLimits, GenerationSpec, _integer, _number, _prepared, result_record
from .vllm_sync_process import _publish


class PipelinedPrefillDecodeProcess:
    """Native continuous batching with one logical window across both stages.

    Each P terminal makes that request immediately eligible for D. Atomic IPC
    batches do not introduce a group completion barrier. The prefill window
    limits how much of the logical capacity P can occupy at once; capacity stays
    charged through handoff and D completion. Results return in input order.
    """

    def __init__(self, engine, generation, control_dir, deadline, startup_timeout_s,
                 *, prefill: NixlPlacement, decode: NixlPlacement,
                 limits: AsyncEngineLimits, batch_size: int, prefill_window: int,
                 kv_buffer_device='cuda', close_timeout_s=30.0):
        _integer(batch_size, 'batch_size', minimum=1)
        _integer(prefill_window, 'prefill_window', minimum=1)
        _number(close_timeout_s, 'close_timeout_s', low=0, inclusive_low=False)
        if (type(limits) is not AsyncEngineLimits
                or max(batch_size, prefill_window) > limits.max_inflight):
            raise ValueError('batch and prefill windows must fit the logical request capacity')
        if prefill.side_channel_port == decode.side_channel_port:
            raise ValueError('native stages require distinct side-channel ports')
        if engine.kv_transfer_config is not None or kv_buffer_device not in ('cuda', 'cpu'):
            raise ValueError('the pipeline supplies NIXL configuration with a cuda or cpu buffer')
        self.limits, self.batch_size, self.prefill_window = limits, batch_size, prefill_window
        self.generation, self.deadline = generation, deadline
        self.close_timeout_s = close_timeout_s
        self.control = Path(control_dir)
        self.control.mkdir(parents=True, exist_ok=True)
        self._wakeup, self._cancelled = threading.Event(), threading.Event()
        self._run_lock, self._close_lock = threading.Lock(), threading.Lock()
        self._closed = False
        self.stages, self._process_records = {}, {}
        self.last_run_report = self.last_close_report = None
        configured = replace(engine, kv_transfer_config=dict(kv_connector='NixlConnector',
            kv_role='kv_both', kv_buffer_device=kv_buffer_device, kv_load_failure_policy='fail'))
        for name, placement in [('prefill', prefill), ('decode', decode)]:
            try:
                phase_generation = replace(generation, max_tokens=1) if name == 'prefill' else generation
                owner = AsyncEngineProcess(configured, phase_generation, limits=limits,
                    startup_timeout_s=startup_timeout_s, startup_deadline=deadline,
                    worker_env=placement.environment(),
                    worker_log=self.control / (name + '.log'), progress_signal=self._wakeup)
                self.stages[name] = owner
                self._register(name, owner)
            except BaseException as error:
                failed = getattr(error, 'async_engine_process', None)
                if failed is not None:
                    self.stages[name] = failed
                    self._register(name, failed)
                error.pipeline_close_report = self.close(timeout=close_timeout_s)
                raise

    def _register(self, name, owner):
        record = dict(model_pid=None if owner.process is None else owner.process.pid,
            process_created=owner.process_created,
            status='starting' if owner.ready_report is None else 'ready',
            process_exit_confirmed=owner.process is None, cleanup_errors=[])
        self._process_records[name] = record
        _publish(self.control / ('engine-' + name + '.json'), record)

    def ready(self):
        return dict(mode='native_nixl_async_pipeline', logical_capacity=self.limits.max_inflight,
                    batch_size=self.batch_size, prefill_window=self.prefill_window,
                    stages={name: owner.ready_report for name, owner in self.stages.items()})

    def cancel(self):
        self._cancelled.set()
        self._wakeup.set()

    def run_prepared(self, rows, *, generation=None):
        effective = self.generation if generation is None else generation
        if not isinstance(effective, GenerationSpec):
            raise TypeError('generation must be GenerationSpec')
        identities, _ = _prepared(rows)
        if len({item['query_id'] for item in identities}) > 1:
            raise ValueError('one pipeline call must belong to one query')
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError('the native pipeline already owns a call')
        active, waiting, outputs = {}, deque(), [None] * len(rows)
        pending = {name: {} for name in self.stages}
        batches = {name: Counter() for name in self.stages}
        stage_slots = min(self.limits.max_inflight, self.limits.result_slots,
                          self.limits.result_bytes // self.limits.item_result_bytes)
        next_index = completed = peak_active = 0
        peak_pending = dict(prefill=0, decode=0)
        overlapping_owners = False
        failure = None
        interrupted = None
        unassociated = []
        started = time.monotonic()

        def compose(index, error=None):
            state = active[index]
            receipt = state['receipts'].get('decode', [None])[0]
            if receipt is None:
                uncertain = (index in pending['prefill'].values() or index in pending['decode'].values()
                    or any(item['status'] == 'unknown' for values in state['receipts'].values() for item in values))
                receipt = result_record(identities[index], engine_uid='native_nixl_async_pipeline',
                    start=state['started'], end=time.monotonic(),
                    status='unknown' if uncertain else 'failed', submitted=False,
                    error_type=error or 'DecodeNotCompleted')
            result = dict(receipt, stage_receipts=dict(state['receipts']),
                stage_elapsed_s=time.monotonic()-state['started'], execution_mode='native_nixl_async_pipeline')
            if error and result['status'] == 'succeeded':
                result.update(status='failed', text=None, error_type=error)
            return result

        def collect(name):
            nonlocal completed, failure
            events = self.stages[name].poll(self.limits.result_slots)
            # Preserve every returned member before acting on any failure.
            associated = []
            for event in events:
                index = pending[name].pop(event.request_id, None)
                if index is None:
                    unassociated.append(dict(stage=name, request_id=event.request_id, result=event.result))
                    failure = failure or 'UnexpectedStageReceipt'
                    continue
                receipt = event.result
                if receipt is None:
                    now = time.monotonic()
                    receipt = result_record(identities[index], engine_uid=self.stages[name].ready_report['engine_uid'],
                        start=now, end=now, status='unknown', submitted=False,
                        error_type=event.error_type or 'MissingStageReceipt')
                active[index]['receipts'][name] = [receipt]
                associated.append((index, receipt, event.engine_terminal_confirmed))
            for index, receipt, confirmed in associated:
                if receipt['status'] != 'succeeded' or not confirmed:
                    failure = failure or 'StageRequestFailed'
                    continue
                if name == 'prefill':
                    transfer = receipt.get('kv_transfer_params')
                    if (not isinstance(transfer, dict) or transfer.get('do_remote_prefill') is not True
                            or not transfer.get('remote_engine_id') or not transfer.get('remote_request_id')
                            or not transfer.get('remote_block_ids')):
                        failure = failure or 'MissingNativeKVDescriptor'
                    else:
                        waiting.append(index)
                else:
                    producer = active[index]['receipts']['prefill'][0]
                    error = ('PromptTokenMismatch' if receipt['prompt_token_ids_json'] !=
                             producer['prompt_token_ids_json'] else None)
                    outputs[index] = compose(index, error)
                    del active[index]
                    completed += 1
                    failure = failure or error
            return bool(events)

        def submit(name, indexes):
            nonlocal failure
            while True:
                descriptors = ([dict(do_remote_decode=True, do_remote_prefill=False) for _ in indexes]
                    if name == 'prefill' else [active[i]['receipts']['prefill'][0]['kv_transfer_params'] for i in indexes])
                options = dict(generation=replace(effective, max_tokens=1) if name == 'prefill' else effective,
                               deadline_monotonic=self.deadline)
                try:
                    if len(indexes) == 1:
                        outcome = self.stages[name].submit(rows[indexes[0]],
                            kv_transfer_params=descriptors[0], **options)
                    else:
                        outcome = self.stages[name].submit_batch([rows[i] for i in indexes],
                            kv_transfer_params=descriptors, **options)
                except FrameCapacityExceeded:
                    if len(indexes) == 1:
                        raise
                    # No member was accepted. Repack complete requests within
                    # the byte allowance; this never retries model execution.
                    indexes = indexes[:len(indexes)//2]
                    continue
                break
            if outcome.status == 'queued':
                keys = (outcome.request_id,) if len(indexes) == 1 else outcome.request_ids
                pending[name].update(zip(keys, indexes, strict=True))
                batches[name][len(indexes)] += 1
                peak_pending[name] = max(peak_pending[name], len(pending[name]))
                return len(indexes)
            if outcome.status != 'backpressure':
                failure = 'StageSubmissionRejected:' + str(outcome.reason)
            return 0

        try:
            while completed < len(rows):
                self._wakeup.clear()
                progress = collect('decode') | collect('prefill')
                if failure:
                    break
                if self._closed or self._cancelled.is_set() or (
                        self.deadline is not None and time.monotonic() >= self.deadline):
                    failure = 'PipelineStopped'
                    break
                count = min(len(waiting), self.batch_size, stage_slots-len(pending['decode']))
                if count:
                    indexes = list(waiting)[:count]
                    accepted = submit('decode', indexes)
                    if accepted:
                        for _ in range(accepted):
                            waiting.popleft()
                        progress = True
                count = min(len(rows)-next_index, self.batch_size,
                    self.limits.max_inflight-len(active),
                    min(self.prefill_window, stage_slots)-len(pending['prefill']))
                if count and not failure:
                    indexes = list(range(next_index, next_index+count))
                    queued_at = time.monotonic()
                    accepted = submit('prefill', indexes)
                    if accepted:
                        for index in indexes[:accepted]:
                            active[index] = dict(started=queued_at, receipts={})
                        next_index += accepted
                        peak_active = max(peak_active, len(active))
                        progress = True
                overlapping_owners |= bool(pending['prefill'] and pending['decode'])
                if not progress and not failure:
                    remaining = None if self.deadline is None else max(0, self.deadline-time.monotonic())
                    self._wakeup.wait(remaining)
        except BaseException as error:
            failure = type(error).__name__
            if not isinstance(error, Exception):
                interrupted = error
        finally:
            if failure:
                self.cancel()
                for name, requests in pending.items():
                    for request_id in requests:
                        self.stages[name].abort(request_id)
                self.close(timeout=self.close_timeout_s)
                collect('decode')
                collect('prefill')
            for index in active:
                outputs[index] = compose(index, failure)
            for index in range(next_index, len(rows)):
                now = time.monotonic()
                outputs[index] = result_record(identities[index], engine_uid='native_nixl_async_pipeline',
                    start=now, end=now, status='failed', submitted=False,
                    error_type=failure or 'NotSubmitted')
            self.last_run_report = dict(status='failed' if failure else 'passed', error_type=failure,
                elapsed_s=time.monotonic()-started, logical_capacity=self.limits.max_inflight,
                peak_logical_inflight=peak_active, peak_pending=peak_pending,
                simultaneous_stage_ownership=overlapping_owners,
                ownership_scope='native API in-flight requests; not proof of simultaneous GPU kernels',
                batch_frames={name: dict(histogram) for name, histogram in batches.items()},
                submitted_logical_requests=next_index, completed_logical_requests=completed,
                unassociated_receipts=unassociated)
            self._run_lock.release()
        if interrupted is not None:
            raise interrupted
        return outputs

    def close(self, timeout=30.0):
        _number(timeout, 'close timeout', low=0, inclusive_low=False)
        self.cancel()
        self._closed = True
        deadline = time.monotonic()+timeout
        if not self._close_lock.acquire(timeout=timeout):
            return dict(status='unknown', error_type='CloseInProgress', gpu_exit_confirmed=None)
        try:
            if self.last_close_report is not None and self.last_close_report['status'] == 'closed':
                return self.last_close_report
            reports = {}
            errors = []
            names = list(reversed(self.stages))
            for index, name in enumerate(names):
                remaining = deadline-time.monotonic()
                try:
                    report = (self.stages[name].close(timeout=remaining/(len(names)-index+1))
                              if remaining > 0 else dict(status='unknown', process_exit_confirmed=False))
                except Exception as error:
                    report = dict(status='unknown', process_exit_confirmed=False, error_type=type(error).__name__)
                reports[name] = report
                try:
                    registered = dict(self._process_records[name], **report)
                    registered['direct_child_exit_confirmed'] = report.get('process_exit_confirmed')
                    if report.get('status') != 'closed':
                        # A dead IPC parent does not prove its native children exited.
                        # Reuse registered process-group cleanup on every unknown close.
                        registered['process_exit_confirmed'] = False
                    self._process_records[name] = registered
                    _publish(self.control / ('engine-' + name + '.json'), registered)
                except Exception as error:
                    errors.append(name + '_report:' + type(error).__name__)
            # Each stage already spent its API allowance; no producer can update
            # these registration files while another grace wait elapses.
            cleanup = StageEngineCleanup(self.control).stop(
                grace_s=0, started=bool(self.stages), deadline=deadline)
            observed = {(row.get('model_pid'), row.get('process_created')): row for row in cleanup['reports']}
            for name, registered in self._process_records.items():
                recovered = observed.get((registered['model_pid'], registered['process_created']))
                if recovered is not None:
                    self._process_records[name] = recovered
                    try:
                        _publish(self.control / ('engine-' + name + '.json'), recovered)
                    except Exception as error:
                        errors.append(name + '_cleanup_report:' + type(error).__name__)
            self.last_close_report = dict(
                status='closed' if not errors and cleanup['process_exit_confirmed'] and all(
                    value['status'] == 'closed' for value in reports.values()) else 'unknown',
                stages=reports, registered_process_cleanup=cleanup, cleanup_errors=errors, gpu_exit_confirmed=None)
            return self.last_close_report
        finally:
            self._close_lock.release()
