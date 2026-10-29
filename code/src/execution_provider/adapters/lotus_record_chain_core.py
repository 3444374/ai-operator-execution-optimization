"""Finite LOTUS chains carried by the existing production incremental Core.

By default Core schedules one fused batch at a time. Its work descriptor is a conservative
stage-call upper bound, not a count of GPU requests currently executing. The
shared worker owns methods, original parsers, and the private model process.
Logical transport bytes exclude worker Python copies, audit files and the
consumer's retained relation/call ledger; they are not RSS or GPU-memory limits.
An explicit relation-barrier profile drains a complete Extract prefix before
continuing guarded batch-local segments on that same query's model owner.
The explicit record_local_sections mode uses the shared per-row MethodDriver,
one prepared Async owner, and the same ChainRun across complete Extracts.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future, TimeoutError as FutureTimeout
from dataclasses import asdict, dataclass, replace
from copy import deepcopy
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import socket
import tempfile
import threading
import time
import uuid

from src.infrastructure.private_artifacts import require_outside_git
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.models import EndpointSnapshot, TopologySnapshot
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import (
    OfferedTask, SessionLimits, SessionSpec, SessionTimeouts, State, TaskInfo, TaskProfile,
)
from src.scheduling.core.session_policy import SessionPolicies
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.runtime.async_backend import BoundedAsyncBackend, ConfirmedTaskFailure
from src.scheduling.submission_control.admission import StaticAdmissionController
from src.semantic_methods.lotus.record_chain import LotusRecordChain, RecordOccurrence, ChainRun, stage_identity
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS

from .lotus_chain_actor_backend import LotusChainActorBackend
from .lotus_records_core import CoreRecordsCapacity, close_prepared_core


@dataclass(frozen=True)
class CoreChainCapacity:
    """Per-fused-batch logical bytes and bounded owner progress actions."""

    item_input_bytes: int = 2 * 1024 * 1024
    item_result_bytes: int = 8 * 1024 * 1024
    metadata_bytes: int = 2048
    step_actions: int = 16
    poll_interval_s: float = 0.01

    def __post_init__(self):
        for name in ("item_input_bytes", "item_result_bytes", "metadata_bytes", "step_actions"):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                raise ValueError(name + " must be a positive integer")
        if (type(self.poll_interval_s) not in (int, float)
                or not math.isfinite(self.poll_interval_s) or self.poll_interval_s <= 0):
            raise ValueError("poll_interval_s must be finite and positive")

    def limits(self, resources, stage_count):
        return SessionLimits(
            held_tasks=1, input_bytes=self.item_input_bytes,
            result_bytes=self.item_result_bytes, active_requests=1,
            active_work=resources.model_batch_rows * stage_count, offer_tasks=1,
            item_input_bytes=self.item_input_bytes, item_result_bytes=self.item_result_bytes,
            metadata_bytes=self.metadata_bytes, step_actions=self.step_actions,
            wait_timeout_s=resources.query_timeout_s, poll_interval_s=self.poll_interval_s,
            timeouts=SessionTimeouts(),
        )


def _encode(value, maximum):
    output = bytearray()
    for piece in json.JSONEncoder(ensure_ascii=False, allow_nan=False,
                                  separators=(",", ":")).iterencode(value):
        raw = piece.encode("utf-8")
        if len(output) + len(raw) > maximum:
            raise ValueError("Core chain transport bytes exceed the item reservation")
        output.extend(raw)
    return bytes(output)


def _check(deadline, cancelled=None):
    if cancelled is not None and cancelled():
        raise InterruptedError("Core LOTUS chain cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("Core LOTUS chain deadline expired")


class _ChainTransport:
    """One bounded async slot; all blocking fused-worker work stays off owner ticks."""

    def __init__(self, owner):
        self.owner = owner
        self.worker = None
        self._worker_phase = None
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self._worker_lock = threading.Lock()
        self._close_future = self._close_thread = None
        self._outcomes, self._inputs = {}, {}
        self.backend = BoundedAsyncBackend(
            self._execute, max_tasks=1, notify=lambda: owner.core.wake.notify(),
            isolate_failures=True, finalize=self._finalize,
        )

    async def _finalize(self):
        await asyncio.get_running_loop().shutdown_default_executor()

    def _run(self, task):
        owner = self.owner
        with self._worker_lock:
            if self._stopping.is_set():
                raise InterruptedError("Core chain transport is closing")
            if self.worker is None:
                from src.execution_provider.method_workers.lotus_chain_batch import FusedChainBatch

                worker_type = owner._batch_type or FusedChainBatch
                options = owner._worker_phase_options() if owner.relation_barriers else {}
                if owner.prepared_batching is not None:
                    options['prepared_batching'] = asdict(owner.prepared_batching)
                self.worker = worker_type(
                    owner.chain.describe(), asdict(owner.engine),
                    list(owner.results.bindings.values()), str(owner.control),
                    owner.deadline, owner.resources.startup_timeout_s,
                    socket.gethostname(), owner.control_token,
                    release_completed_audit=owner.release_completed_audit, **options,
                )
                self._worker_phase = owner.phase_stage_ids
            elif self._worker_phase != owner.phase_stage_ids:
                # The control thread admits this phase only after the previous
                # session/transport are drained. Configure the existing worker
                # here, on its execution thread, without another model owner.
                self.worker.begin_relation_phase(
                    owner.chain.describe(), list(owner.results.bindings.values()),
                    initial_parents=owner._phase_parents, columns=owner._phase_columns,
                    allow_nulls=owner._phase_allow_nulls, source_exhausted=True,
                )
                self._worker_phase = owner.phase_stage_ids
            if self._stopping.is_set():
                raise InterruptedError("Core chain transport closed during worker construction")
        options = ({'source_flush_reason': task.task.metadata.decode('ascii')}
                   if owner._streaming_tokens else {})
        value = self.worker.run_rows(json.loads(task.task.payload), **options)
        # Worker materialization and its audit are separate from the bounded
        # Core response. Overflow is an uncertain task, never a fake terminal;
        # close recovers the independent call audit without releasing its credit.
        return _encode(value, task.task.max_result_bytes)

    async def _execute(self, task, endpoint):
        from src.semantic_methods.lotus.record_results import (
            ChainBatchResults, ChainExecutionError,
        )

        with self._lock:
            self._inputs[task.key] = task.task.payload
        payload = await asyncio.to_thread(self._run, task)
        value = json.loads(payload)
        with self._lock:
            self._outcomes[task.key] = payload
        records = [RecordOccurrence.from_dict(json.loads(row["record_json"]))
                   for row in json.loads(task.task.payload)]
        options = self.owner._accountant_options(records) if self.owner.relation_barriers else {}
        audit = ChainBatchResults(records, self.owner.chain, **options)
        try:
            audit.accept(value)
        except ChainExecutionError as error:
            if (any(call["status"] == "unknown" for call in audit.calls)
                    or value.get("cleanup_errors")):
                raise RuntimeError("fused chain borrowing remains unconfirmed") from error
            with self._lock:
                self._inputs.pop(task.key, None)
            raise ConfirmedTaskFailure(task.key, {"exception_type": error.remote_error_type}) from error
        if value.get("cleanup_errors"):
            raise RuntimeError("fused chain worker cleanup remains unconfirmed")
        with self._lock:
            self._inputs.pop(task.key, None)
        return payload

    def acknowledge(self, key):
        with self._lock:
            self._outcomes.pop(key, None)

    def take_receipts(self):
        with self._lock:
            values = tuple(self._outcomes.values())
            self._outcomes.clear()
            return values

    def retained_inputs(self):
        with self._lock:
            return len(self._inputs)

    def phase_drained(self):
        with self._lock:
            return not self._inputs and not self._outcomes and not self._stopping.is_set()

    def close_worker(self, timeout):
        until = time.monotonic() + timeout
        self._stopping.set()
        if not self._worker_lock.acquire(timeout=timeout):
            return dict(status="unknown", process_exit_confirmed=None,
                        engine_api_shutdown_confirmed=False,
                        cleanup_errors=["fused_worker_construction_unconfirmed"])
        try:
            worker = self.worker
        finally:
            self._worker_lock.release()
        if worker is None:
            return dict(status="closed", process_exit_confirmed=True,
                        engine_api_shutdown_confirmed=True, cleanup_errors=[])
        remaining = until - time.monotonic()
        if remaining <= 0:
            return dict(status="unknown", process_exit_confirmed=None,
                        engine_api_shutdown_confirmed=False,
                        cleanup_errors=["fused_worker_cleanup_deadline_expired"])
        # The private engine can be waiting for its in-flight IPC lock. Keep
        # that same owner's cleanup off the control thread and retain its one
        # future on timeout. This never starts another kernel or model instance.
        retry = self._close_future is None
        if (self._close_future is not None and self._close_future.done()
                and not self._close_thread.is_alive()):
            try:
                retry = self._close_future.result().get("status") != "closed"
            except BaseException:
                retry = True
        if retry:
            self._close_future = Future()
            future = self._close_future

            def finish():
                try:
                    future.set_result(worker.close(timeout=remaining))
                except BaseException as error:
                    future.set_exception(error)

            self._close_thread = threading.Thread(target=finish, daemon=True,
                                                   name="core-chain-private-close")
            self._close_thread.start()
        try:
            report = dict(self._close_future.result(timeout=max(0, until - time.monotonic())))
            self._close_thread.join(timeout=max(0, until - time.monotonic()))
            report["cleanup_thread_exit_confirmed"] = not self._close_thread.is_alive()
            if not report["cleanup_thread_exit_confirmed"]:
                report["status"] = "unknown"
            return report
        except FutureTimeout:
            if self._close_future.done():
                # A worker-raised TimeoutError is a completed cleanup error,
                # not evidence that this cleanup thread is still running.
                raise
            return dict(status="unknown", process_exit_confirmed=None,
                        engine_api_shutdown_confirmed=False,
                        cleanup_thread_exit_confirmed=False,
                        cleanup_errors=["private_engine_close_still_running"])


class _CoreChainRows:
    def __init__(self, owner, records, spec, cancelled):
        self.owner, self.source = owner, iter(records)
        self.cancelled = cancelled
        limits = (owner._fused_limits(len(owner.chain.stages))
                  if owner.relation_barriers else None)
        self.session = owner.core.open(spec, limits)
        self.offered, self.sequence = set(), 0
        self.pending = None
        self.source_eof = self.source_closed = self.finished = self.completed = False
        self.last_close_report = None
        self.peak_core_usage = asdict(owner.core.capacity.usage())
        self._phase_records = []
        self._source_close_error = None
        self._token_batches = None

    @property
    def production_core_connected(self):
        return isinstance(self.owner.core, SessionEngine)

    @property
    def calls(self):
        return self.owner.calls

    @property
    def columns(self):
        return self.owner.results.columns

    @property
    def batch_stage_stats(self):
        if not self.owner.relation_barriers:
            return self.owner.results.batch_stage_stats
        return [snapshot for result in self.owner._accountants.values()
                for snapshot in result.batch_stage_stats]

    @property
    def raw_result_batches(self):
        if not self.owner.relation_barriers:
            return self.owner.results.raw_result_batches
        return {key: value for result in self.owner._accountants.values()
                for key, value in result.raw_result_batches.items()}

    @property
    def unassociated_results(self):
        if not self.owner.relation_barriers:
            return self.owner.results.unassociated_results
        return [value for result in self.owner._accountants.values()
                for value in result.unassociated_results]

    def __iter__(self):
        return self

    def _observe(self):
        for key, value in asdict(self.owner.core.capacity.usage()).items():
            self.peak_core_usage[key] = max(value, self.peak_core_usage[key])

    def _pack_source(self):
        """Prepare bounded reads lazily; only the batcher chooses model groups."""
        from transformers import AutoTokenizer
        from src.semantic_methods.lotus.prepared_batching import PreparedRequestBatcher
        from src.semantic_methods.lotus.record_results import _source_binding, record_row

        owner, seen = self.owner, set()
        tokenizer = AutoTokenizer.from_pretrained(
            owner.engine.tokenizer_path or owner.engine.model_path,
            local_files_only=True, trust_remote_code=False)

        def read_chunk():
            for _ in range(owner.resources.model_batch_rows):
                _check(owner.deadline, self.cancelled)
                try:
                    record = next(self.source)
                except StopIteration:
                    self.source_eof = True
                    return
                if type(record) is not RecordOccurrence:
                    raise TypeError('Core chain source must yield RecordOccurrence')
                if (owner.results.bindings.get(record.row_id) != _source_binding(record)
                        or record.row_id in seen):
                    raise ValueError('Core chain source changed or duplicated an occurrence')
                seen.add(record.row_id)
                record = RecordOccurrence.from_dict(record.describe())
                row = owner.chain.methods[0].prepare_rows([record], query_id=owner.chain.query_id)[0]
                yield dict(row, source_row=record_row(record))

        def chunks():
            while not self.source_eof:
                yield read_chunk()

        def size(row):
            # Charge both the pending wire value and its prepared message.
            return (len(_encode([row['source_row']], owner.capacity.item_input_bytes))
                    + len(row['messages_json'].encode('utf-8')))

        yield from PreparedRequestBatcher(owner.prepared_batching, tokenizer).iter_batches(
            chunks(), owner.chain.stages[0].generation,
            max_bytes=owner.capacity.item_input_bytes, size=size)

    def _fill(self):
        from src.semantic_methods.lotus.record_results import _source_binding, record_row

        owner = self.owner
        if self.source_eof or owner.core.capacity.usage().held_tasks:
            return
        if self.pending is None:
            rows, batch_ids, metadata = [], set(), b''
            if owner._streaming_tokens:
                if self._token_batches is None:
                    self._token_batches = self._pack_source()
                try:
                    group = next(self._token_batches)
                    rows = [row['source_row'] for row in group.rows]
                    metadata = group.reason.encode('ascii')
                except StopIteration:
                    pass
            else:
                for _ in range(owner.resources.model_batch_rows):
                    _check(owner.deadline, self.cancelled)
                    try:
                        record = next(self.source)
                    except StopIteration:
                        self.source_eof = True
                        break
                    if type(record) is not RecordOccurrence:
                        raise TypeError("Core chain source must yield RecordOccurrence")
                    if (owner.results.bindings.get(record.row_id) != _source_binding(record)
                            or record.row_id in self.offered or record.row_id in batch_ids):
                        raise ValueError("Core chain source changed or duplicated an occurrence")
                    batch_ids.add(record.row_id)
                    rows.append(record_row(record))
            if not rows:
                self.session.seal()
                return
            payload = _encode(rows, owner.capacity.item_input_bytes)
            upper = len(rows) * len(owner.chain.stages)
            work = WorkDescriptor((StageWork("fused_chain", upper, "stage_call_upper_bound"),),
                                  "fused_chain", "finite-chain-upper-bound-v1")
            self.pending = OfferedTask(
                self.sequence, payload, upper, owner.capacity.item_result_bytes, metadata=metadata,
                info=TaskInfo(hashlib.sha256(payload).hexdigest(), self.sequence,
                              "fused_chain", work),
            )
        _check(owner.deadline, self.cancelled)
        offered = self.session.offer((self.pending,))
        if offered.status == "REJECTED":
            raise ValueError("Core rejected fused chain batch: " + offered.reason)
        if offered.accepted_prefix_count:
            for row in json.loads(self.pending.payload):
                self.offered.add(json.loads(row["record_json"])["row_id"])
            self.sequence += 1
            self.pending = None
            if self.source_eof:
                self.session.seal()
            self._observe()

    def __next__(self):
        self.owner._control()
        if self.finished:
            raise StopIteration
        try:
            while True:
                _check(self.owner.deadline, self.cancelled)
                if self.owner.results.pending:
                    row = self.owner.results.pop()
                    if self.owner._has_next_phase:
                        self._phase_records.append(row)
                        continue
                    return row
                self._fill()
                _check(self.owner.deadline, self.cancelled)
                progress = self.session.advance(1)
                self._observe()
                if progress.deliveries:
                    delivery = progress.deliveries[0]
                    try:
                        self.owner.results.accept(json.loads(delivery.result))
                        self.owner.transport.acknowledge(delivery.key)
                    finally:
                        self.session.release((delivery.lease_id,))
                if progress.state in (State.FAILED, State.CANCELLED):
                    self.owner._collect_receipts(accept=True)
                    raise RuntimeError("Core chain execution failed: " + str(progress.error))
                if progress.state == State.FINISHED:
                    if self.owner.relation_barriers:
                        self.owner.results.verify_success(source_exhausted=self.source_eof)
                    else:
                        self.owner.results.verify_coverage()
                    self.owner.results.finalize_columns()
                    if self.owner._has_next_phase:
                        self.owner._advance_relation_phase(self)
                        continue
                    report = self.owner.close()
                    if report["status"] != "closed":
                        raise RuntimeError("Core chain cleanup remains unconfirmed")
                    self.completed = True
                    report["query_completed"] = True
                    raise StopIteration
                if not progress.has_immediate_work:
                    wait = min(self.owner.capacity.poll_interval_s,
                               max(0, self.owner.deadline - time.monotonic()))
                    self.owner.core.wake.wait(progress.generation, wait)
        except StopIteration:
            raise
        except BaseException as error:
            try:
                report = self.owner.close()
                if report["status"] != "closed":
                    error.add_note("Core chain cleanup remains unconfirmed")
            except BaseException as cleanup:
                error.add_note("Core chain cleanup also failed: " + type(cleanup).__name__)
            error.chain_calls = list(self.calls)
            error.chain_stage_stats = list(self.batch_stage_stats)
            error.chain_raw_results = [row for batch in self.raw_result_batches.values() for row in batch]
            raise

    def _close_query(self):
        self.finished, self.pending = True, None
        errors = self._close_source()
        self.session.close()
        self.owner.core.reap(self.owner.capacity.step_actions)
        return errors

    def _close_source(self):
        errors = []
        self._source_close_error = None
        if not self.source_closed:
            try:
                if self._token_batches is not None:
                    self._token_batches.close()
                close = getattr(self.source, "close", None)
                if close is not None:
                    close()
                self.source_closed = True
            except BaseException as error:
                self._source_close_error = error
                errors.append("source_iterator_close: " + type(error).__name__)
        return errors

    def close(self, *, timeout=None):
        return self.owner.close(timeout=timeout)


class _SectionTransport(LotusChainActorBackend):
    """Register the existing transport's exact prepared calls before its RPC."""

    def __init__(self, owner, maximum):
        self.owner = owner
        super().__init__(owner._actors, maximum, lambda: owner.core.wake.notify(),
                         deadline_monotonic=owner.deadline, on_result=owner._record_local_receipt,
                         token_budget=owner.token_budget)

    def _submit_task(self, task, timings, prepare_method):
        self.owner._register_local_task(task, self._request(task))
        return super()._submit_task(task, timings, prepare_method)


class _RecordLocalRows:
    """Coordinate existing row programs and complete Extracts on one model owner."""

    def __init__(self, owner, records, run, cancelled):
        self.owner, self.source, self.run = owner, iter(records), run
        self.cancelled = cancelled
        self.bindings = {record.row_id: record for record in run.current}
        self.offered = set()
        self.source_eof = self.source_closed = self.finished = self.completed = False
        self.driver = self.session = self.section = self.pending = None
        self.pool = None
        self._output = None
        self._output_exhausted = self._method_complete = self._root_failed = False
        self._source_errors = []
        self.error = self.last_close_report = None
        self.peak_core_usage = asdict(owner.core.capacity.usage())

    @property
    def calls(self):
        return self.owner.calls

    @property
    def columns(self):
        return self.run.columns

    @property
    def batch_stage_stats(self):
        return [deepcopy(self.run.stats)]

    @property
    def raw_result_batches(self):
        return {}

    @property
    def unassociated_results(self):
        return []

    @property
    def production_core_connected(self):
        return self.owner.production_core_connected

    def __iter__(self):
        return self

    def _close_source(self):
        if self.source_closed:
            return
        try:
            close = getattr(self.source, "close", None)
            if close is not None:
                close()
            self.source_closed = True
        except BaseException as error:
            self._source_errors.append("source_iterator_close: " + type(error).__name__)
            raise

    def _pull_source(self):
        from src.semantic_methods.lotus.record_results import _source_binding

        _check(self.owner.deadline, self.cancelled)
        try:
            record = next(self.source)
        except StopIteration:
            self.source_eof = True
            if self.offered != set(self.bindings):
                raise ValueError("record-local source omitted expected occurrences")
            self._close_source()
            _check(self.owner.deadline, self.cancelled)
            raise
        if type(record) is not RecordOccurrence:
            raise ValueError("record-local source changed or duplicated an occurrence")
        # Own this read snapshot once; backpressure may retain the row upstream
        # of MethodDriver for several ticks while the producer reuses its data.
        record = RecordOccurrence.from_dict(record.describe())
        if (record.row_id not in self.bindings
                or record.row_id in self.offered
                or _source_binding(record) != _source_binding(self.bindings[record.row_id])):
            raise ValueError("record-local source changed or duplicated an occurrence")
        self.offered.add(record.row_id)
        return record

    def _begin_phase(self):
        from src.semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
        from src.semantic_methods.driver import MethodDriver
        from src.semantic_methods.lotus.record_continuation import PreparedCallProgram
        from .lotus_record_chain_fifo import LotusChainFIFO

        if self.run.position == len(self.run.chain.stages):
            result = self.run.result()
            self._method_complete, self._output = True, iter(result["records"])
            return
        self._using_source = not self.source_eof
        self._phase_eof = False
        self._extract_results = []
        if self.run.method.stage.kind == "extract":
            # Full-relation preparation never treats the expected declaration
            # alone as proof that the actual source has completed.
            while not self.source_eof:
                try:
                    self._pull_source()
                except StopIteration:
                    break
            self._using_source = False
            stage = self.run.method.stage
            prepared = self.run.prepare()
            self._stage_ids = (stage.stage_id,)
            self.section = None
            if not prepared:
                self.run.complete_relation(source_exhausted=True)
                self._history(None, 0)
                return
            self._prepared = {row["row_id"]: row for row in prepared}
            program = PreparedCallProgram(stage, max_result_bytes=self.owner.capacity.item_result_bytes)
            stages = (stage,)
            self._phase_source = iter(self.run.current)
        else:
            self.section = self.run.delegate_record_local()
            from src.semantic_methods.lotus.record_results import _source_binding
            self._phase_bindings = {row.row_id: _source_binding(row) for row in self.section.records}
            stages = self.section.chain.stages
            self._stage_ids = tuple(stage.stage_id for stage in stages)
            self._phase_source = self.source if self._using_source else iter(self.section.records)
        self._phase_count = 0
        self.session = self.owner.core.open(SessionSpec(self.run.chain.query_id,
            "chain-section-" + str(self.run.position), "lotus-record-prepared-v1",
            work_unit="model_calls" if self.owner.token_budget is None else "tokens",
            task_profiles=tuple(TaskProfile(stage.stage_id, "prepared-chain") for stage in stages)))
        self.owner.transport.bind_query(self.session.session_id, None)
        limits = self.owner.method_limits
        capacity = MethodCapacity(self.owner.resources.max_active_batches,
                                  self.owner.resources.max_active_batches * row_reservation(limits))
        self.pool = MethodBudgetPool(capacity)
        budget = self.pool.allocate(capacity)
        self.driver = (LotusChainFIFO(self.session, self.section.chain, limits, budget,
                                     token_budget=self.owner.token_budget,
                                     **self.owner._ready_options) if self.section is not None
                       else MethodDriver(self.session, program, limits, budget))

    def _fill(self):
        from src.semantic_methods.driver import RowIdentity

        if self._phase_eof:
            return
        if self.pending is None:
            try:
                self.pending = self._pull_source() if self._using_source else next(self._phase_source)
            except StopIteration:
                self._phase_eof = True
                _check(self.owner.deadline, self.cancelled)
                self.driver.end_input()
                return
        row = self.pending
        if self.section is not None:
            accepted = self.driver.offer_record(row,
                parent_call_ids=self.section.initial_parents[(row.row_id, row.source_position)],
                columns=self.section.columns, allow_nulls=self.section.allow_nulls)
        else:
            accepted = self.driver.offer_row(RowIdentity(row.source_position, row.row_id),
                self.driver.method.input(self._prepared[row.row_id],
                    parent_call_ids=self.run.parents[(row.row_id, row.source_position)]))
        if accepted:
            self._phase_count += 1
            self.pending = None

    def _history(self, session_id, inputs):
        self.owner.phase_history.append(dict(phase_stage_ids=list(self._stage_ids),
            source_exhausted=self.source_eof, source_closed=self.source_closed,
            session_id=session_id, input_occurrences=inputs,
            output_occurrences=len(self.run.current), verified_success=True,
            core_usage=asdict(self.owner.core.capacity.usage()),
            transition="record_local_adoption" if self.section is not None else "complete_extract_relation"))

    def _finish_phase(self):
        if (not self.source_eof or not self.source_closed or self.pending is not None
                or any(asdict(self.owner.core.capacity.usage()).values())
                or self.owner.transport.retained_refs() or self.pool.used != (0, 0)):
            raise RuntimeError("record-local phase still owns source, result or transport work")
        retired = self.session.close()
        if retired.status != "CLOSED" or any(asdict(retired.usage).values()):
            raise RuntimeError("record-local session did not retire completely")
        if self.section is not None:
            self.run.adopt_record_local(self.section, source_exhausted=True)
        else:
            self.run.complete(self._extract_results)
            self.run.complete_relation(source_exhausted=True)
        self._history(self.session.session_id, self._phase_count)
        self.driver.close()
        self.driver, self.section = None, None

    def _tick(self):
        _check(self.owner.deadline, self.cancelled)
        if self.driver is None:
            self._begin_phase()
            return
        self._fill()
        _check(self.owner.deadline, self.cancelled)
        bound = min(self.owner.capacity.step_actions, self.session.limits.held_tasks)
        progress = self.driver.advance(bound)
        for result in self.driver.results(bound):
            try:
                value = json.loads(result.value)
                if self.section is not None:
                    self.section.accept(dict(value, batch_id=str(result.row.sequence),
                        inputs=[self._phase_bindings[result.row.call_id]]))
                else:
                    self._extract_results.append(value)
            finally:
                self.driver.release_result(result.row)
        for key, value in asdict(self.owner.core.capacity.usage()).items():
            self.peak_core_usage[key] = max(self.peak_core_usage[key], value)
        if self.driver.finished:
            self._finish_phase()
        elif not progress.has_immediate_work:
            self.owner.core.wake.wait(progress.generation,
                min(self.owner.capacity.poll_interval_s, max(0, self.owner.deadline - time.monotonic())))

    def __next__(self):
        self.owner._control()
        if self.finished:
            raise StopIteration
        try:
            while True:
                _check(self.owner.deadline, self.cancelled)
                if self._output is not None:
                    try:
                        return next(self._output)
                    except StopIteration:
                        self._output_exhausted = True
                        report = self.owner.close()
                        if report["status"] != "closed":
                            raise RuntimeError("record-local query cleanup remains unconfirmed")
                        self.completed = report["query_completed"] = True
                        raise
                self._tick()
        except StopIteration:
            raise
        except BaseException as error:
            self.error = error
            try:
                report = self.owner.close()
                if report["status"] != "closed":
                    error.add_note("record-local query cleanup remains unconfirmed")
            except BaseException as cleanup:
                error.add_note("record-local close also failed: " + type(cleanup).__name__)
            error.chain_calls = self.calls
            error.chain_stage_stats = deepcopy(self.run.stats)
            raise

    def _close_query(self):
        self.finished, self.pending = True, None
        if self.driver is not None:
            self.driver.close()
        if not self._output_exhausted and self.error is None:
            self.error = InterruptedError("record-local consumer closed before result EOF")
        try:
            self._close_source()
        except BaseException:
            pass  # The exact failure stays in _source_errors and the close report.
        return list(self._source_errors)

    def close(self, *, timeout=None):
        return self.owner.close(timeout=timeout)


class SemLoomRecordChainCore:
    """Single-control-thread owner for one finite query; fused is the default.

    This profile requires one CPU worker and one active fused batch. Core owns
    admission and delivery leases; the shared kernel owns the original method
    loop. A fresh owner/query ID is required for independent repeat work.
    record_local_sections instead reuses a prepared async model across legal
    F/M intervals and complete Extracts; it never starts the fused worker.
    """

    _batch_type = None

    def __init__(self, engine: EngineSpec, resources: RecordsResources, *,
                 capacity: CoreChainCapacity | CoreRecordsCapacity | None = None, control_dir=None,
                 execution="fused", actor_owner=None, method_limits=None,
                 release_completed_audit=False, token_budget=None, ready_order="fifo", successor_first=None,
                 ready_clock=None, on_ready_event=None, prepared_batching=None):
        if type(release_completed_audit) is not bool:
            raise TypeError("release_completed_audit must be a boolean")
        if release_completed_audit and execution != "fused":
            raise ValueError("release_completed_audit requires the fused worker")
        self.release_completed_audit = release_completed_audit
        self.engine = EngineSpec.from_dict(asdict(engine))
        self.resources = RecordsResources(**asdict(resources))
        if execution not in ("fused", "record_local_sections"):
            raise ValueError("chain execution requires fused or record_local_sections")
        self.execution = execution
        self.prepared_batching = None
        if prepared_batching is not None:
            from src.semantic_methods.lotus.prepared_batching import PreparedBatching
            if execution != 'fused':
                raise ValueError('prepared_batching requires the synchronous fused path')
            if type(prepared_batching) is not PreparedBatching:
                raise TypeError('prepared_batching must be PreparedBatching or None')
            if (prepared_batching.policy == 'fixed_rows'
                    and prepared_batching.max_rows > self.resources.model_batch_rows):
                raise ValueError('prepared batching max_rows cannot exceed model_batch_rows')
            if self.engine.chat_template is not None:
                raise ValueError('prepared_batching currently requires the tokenizer-owned chat template')
            self.prepared_batching = PreparedBatching(**asdict(prepared_batching))
        self._streaming_tokens = (self.prepared_batching is not None
                                  and self.prepared_batching.policy == 'token_budget')
        self.token_budget = None
        if token_budget is not None:
            from .lotus_chain_token_budget import ChainTokenBudget
            if execution != "record_local_sections":
                raise ValueError("token_budget requires record_local_sections")
            if type(token_budget) is not ChainTokenBudget:
                raise TypeError("token_budget must be ChainTokenBudget or None")
            if token_budget.engine != self.engine:
                raise ValueError("token_budget must belong to the query model")
            self.token_budget = ChainTokenBudget(token_budget.tokenizer, self.engine,
                active_tokens=token_budget.active_tokens,
                tokenizer_signature=token_budget.tokenizer_signature,
                template_signature=token_budget.template_signature,
                expected_output=dict(token_budget.expected_output))
        from src.semantic_methods.driver import SuccessorFirst
        if ready_order not in ("round_robin", "fifo", "successor_first"):
            raise ValueError("unknown continuation ready order")
        if ((ready_order == "successor_first") != (successor_first is not None)
                or successor_first is not None and type(successor_first) is not SuccessorFirst):
            raise ValueError("successor_first order requires its explicit settings")
        if ready_clock is not None and not callable(ready_clock):
            raise TypeError("ready clock must be callable")
        if on_ready_event is not None and not callable(on_ready_event):
            raise TypeError("ready observer must be callable")
        self._ready_options = {}
        if ready_order != "fifo" or any(value is not None for value in
                                      (successor_first, ready_clock, on_ready_event)):
            if execution != "record_local_sections":
                raise ValueError("ready selection options require record_local_sections")
            self._ready_options = dict(ready_order=ready_order, successor_first=successor_first,
                                       clock=ready_clock, on_ready_event=on_ready_event)
        if execution == "fused":
            if resources.cpu_workers != 1 or resources.max_active_batches != 1:
                raise ValueError("Core fused chain requires cpu_workers=max_active_batches=1")
            if actor_owner is not None or method_limits is not None:
                raise ValueError("prepared owner and method limits require record_local_sections")
            self.capacity = CoreChainCapacity() if capacity is None else capacity
            if type(self.capacity) is not CoreChainCapacity:
                raise TypeError("Core chain capacity must be CoreChainCapacity")
        else:
            from src.semantic_methods.continuation import MethodLimits
            from .lotus_records_async_actor import AsyncRecordsActorOwner
            from .lotus_records_core import resolve_core_profile
            if resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1:
                raise ValueError("record-local sections require prepare_batch_rows=model_batch_rows=1")
            resolve_core_profile(self.engine, self.resources, dict(mode="async"))
            self.capacity = CoreRecordsCapacity() if capacity is None else capacity
            if type(self.capacity) is not CoreRecordsCapacity:
                raise TypeError("record-local sections capacity must be CoreRecordsCapacity")
            if method_limits is not None and type(method_limits) is not MethodLimits:
                raise TypeError("record-local method limits must be MethodLimits")
            if actor_owner is not None and (not isinstance(actor_owner, AsyncRecordsActorOwner)
                    or not actor_owner._prepared_only or actor_owner.engine != self.engine
                    or actor_owner.resources != self.resources):
                raise ValueError("record-local sections need the matching prepared-only async owner")
        self._actors, self.method_limits = actor_owner, method_limits
        self._receipt_lock = threading.Lock()
        self._contexts, self._raw_receipts = {}, {}
        self._root_run = None
        self._thread = threading.get_ident()
        self._closing = False
        self.control_dir = control_dir
        self.core = self.transport = self.chain = self.results = self._active = None
        self.control = self.deadline = self.control_token = None
        self.last_close_report = None
        self.query_chain, self._segments = None, ()
        self.relation_barriers = False
        self._phase_index, self._accountants = 0, {}
        self._phase_parents = self._phase_columns = None
        self._phase_allow_nulls = False
        self._source_expected, self._current_relation = (), ()
        self._expected_count = 0
        self.phase_history = []

    @property
    def production_core_connected(self):
        return isinstance(self.core, SessionEngine)

    def _control(self):
        if threading.get_ident() != self._thread:
            raise RuntimeError("Core chain owner requires one control thread")

    @property
    def phase_stage_ids(self):
        return tuple(stage.stage_id for stage in self.chain.stages)

    @property
    def calls(self):
        if self.execution == "record_local_sections":
            return self._local_calls()
        if not self.relation_barriers and self.results is not None:
            return self.results.calls
        return [call for result in self._accountants.values() for call in result.calls]

    @property
    def request_batches(self):
        """Logical groups declared for the synchronous native batch API."""
        return [deepcopy(batch) for result in self._accountants.values()
                for batch in result.request_batches]

    @property
    def _has_next_phase(self):
        return self._phase_index + 1 < len(self._segments)

    def _accountant_options(self, records):
        parents = (None if self._phase_parents is None else {
            (record.row_id, record.source_position): self._phase_parents[
                (record.row_id, record.source_position)] for record in records
        })
        return dict(initial_parents=parents, columns=self._phase_columns)

    def _worker_phase_options(self):
        return dict(initial_parents=self._phase_parents, columns=self._phase_columns,
                    allow_nulls=self._phase_allow_nulls, close_on_source_complete=False)

    def _phase_spec(self):
        return SessionSpec(self.query_chain.query_id,
                           "lotus-record-chain-phase-" + str(self._phase_index),
                           "lotus-record-chain-fused-v1", work_unit="stage_call_upper_bound")

    def _fused_limits(self, stage_count):
        resources = (replace(self.resources, model_batch_rows=self.prepared_batching.max_rows)
                     if self._streaming_tokens else self.resources)
        return self.capacity.limits(resources, stage_count)

    def _advance_relation_phase(self, stream):
        """Cross one complete Extract relation on the single Core control thread."""
        from src.semantic_methods.lotus.record_results import ChainBatchResults
        from src.semantic_methods.lotus.record_relation import complete_extract_relation

        self._control()
        _check(self.deadline, stream.cancelled)
        usage = asdict(self.core.capacity.usage())
        if (not stream.source_eof or stream.pending is not None
                or self.results.pending or any(usage.values())
                or not self.transport.phase_drained()):
            raise RuntimeError("Core relation phase still owns source, delivery or transport work")
        self.results.verify_success(source_exhausted=True)
        errors = stream._close_source()
        if errors:
            raise stream._source_close_error
        retired = stream.session.close()
        if retired.status != "CLOSED" or any(asdict(retired.usage).values()):
            raise RuntimeError("Core relation session did not retire completely")
        completed = complete_extract_relation(
            stream._phase_records, columns=self.results.columns,
            extract_stage=self.chain.stages[-1], query_id=self.query_chain.query_id,
            calls=self.calls, source_exhausted=True,
        )
        _check(self.deadline, stream.cancelled)
        self.phase_history.append(dict(
            phase_stage_ids=list(self.phase_stage_ids), source_exhausted=True,
            session_id=stream.session.session_id, received_occurrences=len(self.results.received),
            output_occurrences=len(completed.records), core_usage=usage,
            barrier="complete_extract_relation", verified_success=True,
        ))
        self._phase_index += 1
        self.chain = self._segments[self._phase_index]
        self._current_relation = completed.records
        self._phase_parents = completed.parent_call_ids
        self._phase_columns = completed.columns
        self._phase_allow_nulls = True
        self.results = ChainBatchResults(completed.records, self.chain,
                                         initial_parents=self._phase_parents,
                                         columns=self._phase_columns)
        self._accountants[self.phase_stage_ids] = self.results
        stream.source = iter(completed.records)
        stream.source_eof = stream.source_closed = False
        stream.offered, stream.sequence, stream.pending = set(), 0, None
        stream._phase_records = []
        stream.session = self.core.open(
            self._phase_spec(), self._fused_limits(len(self.chain.stages)))

    def evaluate(self, records, chain, *, expected, query_id, cancelled=None, columns=None):
        """Return a single-pass relation iterator; publish its frame after completed EOF.

        ``expected`` binds each source occurrence and its values without prompts.
        Without token packing, ``model_batch_rows`` bounds one fused input batch.
        Single-stage token packing uses it only for reads; B and independent
        row/byte guards choose the native submission batch. No preparation pool
        or per-stage Core dispatch is introduced.
        Explicit ``relation_barriers`` chains retain complete intermediate
        Extract relations on the driver. ``columns`` supplies an ordered schema
        even for an empty source. This retention is outside Core's wire limits.
        Calls/usage remain inspectable on failure. Unknown work retains the Core
        task and source bytes, even when a later private-engine cleanup succeeds.

        With record_local_sections, expected and actual source positions must
        increase; protocol occurrence IDs remain unique, while payloads and
        user index labels may repeat. Source EOF/close, complete row finals,
        and drained Core work precede every semantic relation handoff. This
        mode requires caller-owned Ray and one compatible model deployment.
        """
        self._control()
        if self.execution == "record_local_sections":
            return self._evaluate_record_local(records, chain, expected, query_id, cancelled, columns)
        if self._closing or self._active is not None:
            raise RuntimeError("Core chain owner already used or closing")
        chain = LotusRecordChain.from_dict(chain.describe())
        if chain.query_id != query_id:
            raise ValueError("chain and query_id must identify the same query")
        if len({stage.method.model_name for stage in chain.stages}) != 1:
            raise ValueError("Core fused chain requires one model deployment")
        if self.prepared_batching is not None and (
                any(stage.kind not in ('filter', 'map') for stage in chain.stages)
                or len({stage.model_role for stage in chain.stages}) != 1):
            raise ValueError('prepared_batching currently requires Filter/Map with one model role')
        if self._streaming_tokens and len(chain.stages) != 1:
            raise ValueError('cross-read token batching currently requires a single Filter or Map stage')
        if any(stage.generation.max_tokens > self.engine.max_model_len for stage in chain.stages):
            raise ValueError("chain max_tokens exceeds max_model_len")
        expected = tuple(RecordOccurrence.from_dict(record.describe()) for record in expected)
        for name in ("row_id", "source_position"):
            if len({getattr(record, name) for record in expected}) != len(expected):
                raise ValueError("Core chain requires globally unique " + name)
        if any(record.index_updates for record in expected):
            raise ValueError("assemble final Extract index updates before another query")
        if columns is not None and (type(columns) not in (tuple, list)
                                    or any(type(name) is not str for name in columns)
                                    or len(set(columns)) != len(columns)
                                    or any(not set(record.values).issubset(columns) for record in expected)):
            raise ValueError("Core relation columns must cover source values in a unique order")
        self.relation_barriers = chain.relation_barriers
        if columns is not None and not self.relation_barriers:
            raise ValueError("explicit relation columns require relation_barriers=True")
        self.query_chain, self._segments = chain, chain.relation_segments()
        self._source_expected = expected if self.relation_barriers else ()
        self._expected_count = len(expected)
        self.engine.validate_driver_environment()
        spec = (self._phase_spec() if self.relation_barriers else
                SessionSpec(query_id, "lotus-record-chain", "lotus-record-chain-fused-v1",
                            work_unit="stage_call_upper_bound"))
        self.deadline = time.monotonic() + self.resources.query_timeout_s
        _check(self.deadline, cancelled)
        from src.semantic_methods.lotus.record_results import ChainBatchResults

        self.chain = self._segments[0]
        self._phase_columns = None if columns is None else tuple(columns)
        self.results = ChainBatchResults(expected, self.chain, **(
            dict(columns=self._phase_columns) if self.relation_barriers else {}))
        self._accountants[self.phase_stage_ids] = self.results
        self.control = Path(tempfile.mkdtemp(prefix="core-lotus-chain-", dir=self.control_dir))
        try:
            require_outside_git(self.control)
            os.chmod(self.control, 0o700)
            self.control_token = uuid.uuid4().hex
            descriptor = os.open(self.control / "owner", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as output:
                output.write(self.control_token)
            limits = self._fused_limits(len(chain.stages))
            endpoint = EndpointSnapshot("local-chain", "local://chain", "local", "0",
                                        True, 0, 0, None, time.monotonic())
            policies = SessionPolicies(StaticAdmissionController(1), RoundRobinEndpointRouter(),
                                       TopologySnapshot((endpoint,), time.monotonic()), "local")
            self.transport = _ChainTransport(self)
            self.core = SessionEngine(limits, self.transport.backend, policies)
            self._active = _CoreChainRows(self, records, spec, cancelled)
        except BaseException as error:
            try:
                self.close()
            except BaseException as cleanup:
                error.add_note("Core chain startup cleanup: " + type(cleanup).__name__)
            raise
        return self._active

    def _collect_receipts(self, *, accept=False):
        for payload in self.transport.take_receipts():
            value = json.loads(payload)
            accountant = self._accountant_for(value)
            if accept:
                accountant.accept(value)
            else:
                accountant.recover(value)

    def _accountant_for(self, envelope):
        stages = envelope.get("phase_stage_ids")
        if stages is None and not self.relation_barriers:
            return self.results
        if type(stages) is not list or any(type(stage) is not str for stage in stages):
            raise ValueError("Core relation audit lacks its phase identity")
        result = self._accountants.get(tuple(stages))
        if result is None:
            raise ValueError("Core relation audit belongs to an undeclared phase")
        return result

    def close(self, *, timeout=None):
        self._control()
        if timeout is not None and (type(timeout) not in (int, float)
                                    or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("cleanup timeout must be finite and positive")
        if self.execution == "record_local_sections":
            return self._close_record_local(timeout=timeout)
        self._closing = True
        if self.last_close_report is not None and self.last_close_report["status"] == "closed":
            return self.last_close_report
        until = (time.monotonic() + timeout if timeout is not None else self.deadline)
        errors, worker_report = [], None
        if self.control is not None and self.control.exists():
            (self.control / "stop").touch(mode=0o600)
        if self._active is not None:
            errors.extend(self._active._close_query())
        if self.transport is not None:
            try:
                remaining = 0 if until is None else until - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Core chain cleanup deadline expired")
                worker_report = self.transport.close_worker(min(15, remaining))
                errors.extend(worker_report.get("cleanup_errors", []))
            except BaseException as error:
                errors.append("private_engine_close: " + type(error).__name__)
            self.core.reap(self.capacity.step_actions)
            try:
                self._collect_receipts()
            except BaseException as error:
                errors.append("late_receipt: " + type(error).__name__)
        # These are bounded diagnostic files owned by the shared method worker,
        # outside the Core wire reservation. They preserve usage after a failed
        # result encode, cancellation, or unpublished Core delivery.
        if self.control is not None and self.control.exists() and self.results is not None:
            from src.serving.backends.vllm_sync_process import MAX_MESSAGE_BYTES

            maximum = MAX_MESSAGE_BYTES * (len(self.query_chain.stages) + 2) + self.capacity.item_input_bytes
            for path in [*self.control.glob("chain-audit-*.json"),
                         *self.control.glob("chain-failure-*.json")]:
                try:
                    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
                        raise ValueError("invalid or oversized chain audit")
                    envelope = json.loads(path.read_text())
                    self._accountant_for(envelope).recover(envelope)
                except BaseException as error:
                    errors.append("call_audit: " + type(error).__name__)
        transport_closed = self.transport is None
        if self.transport is not None:
            remaining = 0 if until is None else until - time.monotonic()
            if remaining > 0:
                transport_closed = self.transport.backend.close(timeout=min(0.05, remaining))
        usage = None if self.core is None else asdict(self.core.capacity.usage())
        worker_closed = self.transport is None or (worker_report is not None
                                                  and worker_report.get("status") == "closed"
                                                  and worker_report.get("process_exit_confirmed") is True)
        closed = (worker_closed and transport_closed and not errors
                  and (usage is None or not usage["held_tasks"]))
        self.last_close_report = dict(
            implementation="semloom_record_chain_core", production_core_connected=self.production_core_connected,
            status="closed" if closed else "unknown", query_completed=False,
            cleanup_scope="core_transport_and_private_engine_api_observed_os_processes",
            physical_gpu_exit_checked=False, gpu_exit_confirmed=None,
            model_owner=worker_report, transport_closed=transport_closed,
            retained_remote_inputs=0 if self.transport is None else self.transport.retained_inputs(),
            core_usage=usage, cleanup_errors=errors,
            physical_transport="bounded_async_host_fused_worker_private_pipe",
            scheduler_owner="SemLoom SessionEngine",
            method_owner="FusedChainBatch/LotusRecordChain.run_batch",
            ray_used_by_adapter=False, daft_used_by_adapter=False,
            source_materialization="finite expected occurrence/value declaration; incremental source iterator",
            source_pull_batch_rows=self.resources.model_batch_rows,
            source_validation="canonical per-occurrence JSON SHA256",
            result_ownership="decoded batch relation and call ledger belong to consumer after lease release",
            transport_byte_scope="logical wire bytes; excludes worker copies, diagnostic audit and consumer ledger",
            model_batch_mode="finite_synchronous_fused_chain",
            cross_query_model_reuse=False, overlapped_cross_batch_supply=False,
            work_unit="stage_call_upper_bound", core_task="one_fused_input_batch",
            application_call_upper_bound=(0 if self.results is None
                                          else self._expected_count * len(self.query_chain.stages)),
            cpu_workers_applied=True, prepare_batch_rows_applied=False,
            declared_model_evaluations=len(self.calls),
            expected_occurrences=0 if self.results is None else self._expected_count,
            received_occurrences=(0 if not self._accountants else
                                  len(next(iter(self._accountants.values())).received)),
            prepare_batch_rows_reason="preparation is owned by the shared fused method worker",
            native_engine_abort_supported=False,
            cleanup_artifacts=None if closed or self.control is None else str(self.control),
        )
        if self._active is not None:
            self.last_close_report["peak_core_usage"] = self._active.peak_core_usage
            self._active.last_close_report = self.last_close_report
        if self.prepared_batching is not None:
            self.last_close_report['prepared_batching'] = asdict(self.prepared_batching)
            self.last_close_report['request_batch_count'] = len(self.request_batches)
            self.last_close_report['cross_read_token_batches'] = self._streaming_tokens
            if self._streaming_tokens:
                self.last_close_report.update(
                    pending_payload_and_message_byte_limit=self.capacity.item_input_bytes,
                    pending_lookahead_rows=1,
                    model_batch_mode='single_stage_cross_read_token_batches',
                    source_pull_batch_rows=self.resources.model_batch_rows,
                    model_batch_row_guard=self.prepared_batching.max_rows,
                    batch_concurrency=1,
                    prepare_batch_rows_reason='control-thread cost preparation; original worker preparation retained')
        if self.relation_barriers:
            self.last_close_report.update(
                relation_barriers=True, relation_phase_count=len(self._segments),
                current_phase=self._phase_index,
                phase_history=list(self.phase_history),
                phase_stage_ids=[list(stages) for stages in self._accountants],
                source_materialization=("full source expected occurrence/value declaration; incremental source "
                                        "iterator; complete Extract relation at each explicit barrier"),
                driver_retention=("full source expected values, source iterator, one pending raw batch, "
                                  "current complete Extract relation, call/stats/raw history and decoded results"),
                transport_byte_scope=("per-task logical raw input/result wire reservations; excludes driver "
                                      "declarations, complete relations, audit, history and worker/IPC copies"),
                whole_query_rss_bounded=False,
                model_batch_mode="finite_synchronous_fused_segments_with_extract_barriers",
            )
            if self._streaming_tokens:
                self.last_close_report['model_batch_mode'] = 'single_stage_cross_read_token_batches'
        if closed and self.control is not None:
            shutil.rmtree(self.control)
        return self.last_close_report

    def _evaluate_record_local(self, records, chain, expected, query_id, cancelled, columns):
        from src.semantic_methods.continuation import MethodLimits
        from .lotus_records_async_actor import AsyncRecordsActorOwner

        if self._closing or self._active is not None:
            raise RuntimeError("Core chain owner already used or closing")
        chain = LotusRecordChain.from_dict(chain.describe())
        if chain.query_id != query_id or len({s.method.model_name for s in chain.stages}) != 1:
            raise ValueError("record-local query requires one query and one model deployment")
        if any(s.generation.max_tokens > self.engine.max_model_len for s in chain.stages):
            raise ValueError("chain max_tokens exceeds max_model_len")
        if self.token_budget is not None:
            if any(stage.kind not in ("filter", "map") for stage in chain.stages):
                raise ValueError("query token_budget supports only Filter/Map plans")
            if len({stage.model_role for stage in chain.stages}) != 1:
                raise ValueError("query token_budget requires one model role")
            if set(self.token_budget.expected_output) != {stage.stage_id for stage in chain.stages}:
                raise ValueError("query stages must match token_budget output estimates")
        if self._ready_options:
            if any(stage.kind not in ("filter", "map") for stage in chain.stages):
                raise ValueError("ready selection options support only Filter/Map plans")
            if len({stage.model_role for stage in chain.stages}) != 1:
                raise ValueError("ready selection options require one model role")
        expected = tuple(RecordOccurrence.from_dict(r.describe()) for r in expected)
        run = ChainRun(chain, list(expected), columns=columns, _relation_execution=True)
        SessionSpec(query_id, "chain-section-0", "lotus-record-prepared-v1",
            task_profiles=tuple(TaskProfile(stage.stage_id, "prepared-chain") for stage in chain.stages))
        if self.method_limits is None:
            size = self.capacity.item_input_bytes + len(chain.stages) * self.capacity.item_result_bytes
            self.method_limits = MethodLimits(self.capacity.item_input_bytes, 2 * size, size, len(chain.stages))
        if self.method_limits.stages < len(chain.stages):
            raise ValueError("method stage reservation must cover the complete finite plan")
        self.query_chain = self.chain = chain
        self._stage_bindings = {stage.stage_id: stage for stage in chain.stages}
        self._expected_bindings = {record.row_id: record for record in expected}
        self._root_run, self._source_expected = run, expected
        self._expected_count = len(expected)
        self.deadline = time.monotonic() + self.resources.query_timeout_s
        _check(self.deadline, cancelled)
        if self._actors is None:
            self._actors = AsyncRecordsActorOwner.for_prepared(
                self.engine, chain.stages[0].generation, self.resources)
        elif self._actors.generation != chain.stages[0].generation:
            raise ValueError("prepared owner's default generation differs from the initial stage")
        limits = self.capacity.limits(self.resources)
        if self.token_budget is not None:
            limits = replace(limits, active_work=self.token_budget.active_tokens)
        endpoint = EndpointSnapshot("local-chain-model", "local://model", "local", "0",
                                    True, 0, 0, None, time.monotonic())
        policies = SessionPolicies(StaticAdmissionController(limits.active_requests),
            RoundRobinEndpointRouter(), TopologySnapshot((endpoint,), time.monotonic()), "local")
        self.transport = _SectionTransport(self, limits.held_tasks)
        self.core = SessionEngine(limits, self.transport, policies)
        try:
            self._active = _RecordLocalRows(self, records, run, cancelled)
            if expected:
                self._actors.start(min(self.deadline, time.monotonic() + self.resources.startup_timeout_s), cancelled)
        except BaseException as error:
            error.add_note("record-local startup cleanup: " + str(self.close()))
            raise
        return self._active

    def _register_local_task(self, task, value):
        stage = self._stage_bindings[task.task.profile_name]
        record = self._expected_bindings[value["row"]["row_id"]]
        canonical = stage_identity(self.query_chain.query_id, record, stage.stage_id, model_role=stage.model_role)
        if (any(value["row"].get(name) != canonical[name] for name in IDENTITY_FIELDS)
                or GenerationSpec.from_dict(value["generation"]) != stage.generation):
            raise ValueError("prepared request differs from the immutable query stage binding")
        context = dict(identity=canonical, parent_call_ids=list(value["parent_call_ids"]))
        with self._receipt_lock:
            if task.key in self._contexts and self._contexts[task.key] != context:
                raise ValueError("prepared task key changed its query association")
            self._contexts[task.key] = context

    def _record_local_receipt(self, key, value):
        with self._receipt_lock:
            context = self._contexts[key]
            if any(value.get(name) != context["identity"][name] for name in IDENTITY_FIELDS):
                raise ValueError("prepared receipt changed its registered query task")
            receipt = dict(deepcopy(value), parent_call_ids=list(context["parent_call_ids"]))
            previous = self._raw_receipts.get(key)
            if previous is not None:
                for name in (*IDENTITY_FIELDS, "status", "text", "prompt_tokens", "completion_tokens",
                             "total_tokens", "engine_uid", "engine_request_id", "parent_call_ids"):
                    if previous.get(name) != receipt.get(name):
                        raise ValueError("late prepared receipt conflicts with its original outcome")
            self._raw_receipts[key] = receipt

    def _collect_local_receipts(self):
        for key, payload in self.transport.failure_receipts():
            self._record_local_receipt(key, json.loads(payload))

    def _local_calls(self):
        if self._root_run is None:
            return []
        with self._receipt_lock:
            calls = {v["call_id"]: deepcopy(v) for v in self._raw_receipts.values()}
        parsed = list(self._root_run.calls)
        if self._active is not None and self._active.section is not None:
            parsed += self._active.section._results.calls
        for value in parsed:
            if value.get("error_type") == "NativeTaskUnconfirmed" and value["call_id"] in calls:
                continue  # A real receipt supplements a declaration, never a model unknown.
            calls[value["call_id"]] = {**calls.get(value["call_id"], {}), **deepcopy(value)}
        order = {stage.stage_id: i for i, stage in enumerate(self.query_chain.stages)}
        return sorted(calls.values(), key=lambda v: (order[v["algorithm_stage"]], v["source_position"], v["call_index"]))

    def _close_record_local(self, *, timeout=None):
        self._closing = True
        if self.last_close_report is not None and self.last_close_report["status"] == "closed":
            return self.last_close_report
        errors = [] if self._active is None else self._active._close_query()
        if self._actors is None:
            actor = dict(status="closed", cleanup_errors=[], gpu_exit_confirmed=None)
            closed = dict(model_owner=actor, transport_closed=True, producer_retirement=None, cleanup_errors=[])
        else:
            closed = close_prepared_core(self._actors, self.transport, self.core, self.capacity,
                                         deadline=self.deadline, timeout=timeout)
        errors += closed["cleanup_errors"]
        if self.transport is not None:
            try:
                self._collect_local_receipts()
            except BaseException as error:
                errors.append("late_receipt: " + type(error).__name__)
        stream = self._active
        if stream is not None and stream.error is not None and not stream._root_failed:
            prepared_ids = {v["call_id"] for v in stream.run.prepared}
            with self._receipt_lock:
                submitted = {v["identity"]["call_id"] for v in self._contexts.values()} & prepared_ids
                stream.run.raw_results = [deepcopy(v) for v in self._raw_receipts.values() if v["call_id"] in submitted]
            stream.run.failure(stream.error, submitted_call_ids=submitted)
            stream._root_failed = True
        usage = None if self.core is None else asdict(self.core.capacity.usage())
        actor = closed["model_owner"]
        status = "closed" if (actor["status"] == "closed" and closed["transport_closed"]
                              and not errors and (usage is None or not usage["held_tasks"])) else "unknown"
        self.last_close_report = dict(implementation="semloom_record_chain_core", execution=self.execution,
            status=status, query_completed=bool(stream is not None and stream._output_exhausted
                                               and stream.error is None and status == "closed"),
            production_core_connected=self.production_core_connected,
            cleanup_scope="core_transport_actor_and_engine_api", physical_gpu_exit_checked=False,
            gpu_exit_confirmed=actor.get("gpu_exit_confirmed"), model_owner=actor,
            transport_closed=closed["transport_closed"], producer_retirement=closed["producer_retirement"],
            retained_remote_inputs=0 if self.transport is None else self.transport.retained_refs(),
            core_usage=usage, cleanup_errors=list(dict.fromkeys(errors)), calls=self.calls,
            phase_history=deepcopy(self.phase_history), expected_occurrences=self._expected_count,
            received_occurrences=0 if stream is None else len(stream.offered),
            peak_core_usage=None if stream is None else stream.peak_core_usage,
            source_exhausted=bool(stream and stream.source_eof), source_closed=bool(stream and stream.source_closed),
            scheduler_owner="SemLoom SessionEngine", method_owner="shared ChainRun/MethodRun",
            model_batch_mode="independent_final_results", model_batch_rows=1,
            cross_query_model_reuse=False, whole_query_rss_bounded=False,
            driver_retention="finite expected relation, current full relation, parsed call and raw receipt audit",
            prepare_actor_count=0, prepare_worker_pool_used=False,
            work_unit="model_calls" if self.token_budget is None else "tokens")
        if self._ready_options:
            policy = self._ready_options["successor_first"]
            self.last_close_report["ready_selection"] = dict(
                order=self._ready_options["ready_order"],
                successor_first=None if policy is None else asdict(policy),
                caller_clock=self._ready_options["clock"] is not None,
                observer_configured=self._ready_options["on_ready_event"] is not None,
                scope="local method-ready to Core offer; no native preemption")
        if stream is not None:
            stream.last_close_report = self.last_close_report
            if stream.error is not None:
                stream.error.chain_calls = self.calls
        return self.last_close_report
