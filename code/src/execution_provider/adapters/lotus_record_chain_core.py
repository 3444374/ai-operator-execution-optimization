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
import weakref

from src.infrastructure.private_artifacts import require_outside_git
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.models import EndpointSnapshot, TopologySnapshot
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import (
    Acceptance, OfferedTask, SessionLimits, SessionSpec, SessionTimeouts, State, TaskInfo, TaskProfile,
)
from src.scheduling.core.session_policy import SessionPolicies
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.runtime.async_backend import BoundedAsyncBackend, ConfirmedTaskFailure
from src.scheduling.submission_control.admission import StaticAdmissionController
from src.semantic_methods.lotus.record_chain import LotusRecordChain, RecordOccurrence, ChainRun, stage_identity
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec, IDENTITY_FIELDS

from .lotus_chain_actor_backend import LotusChainActorBackend
from .records_capacity import CoreRecordsCapacity
from .records_query_runtime import _check, _RecordLocalRows
from .records_transport import close_prepared_core


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
        options = {}
        if owner._streaming_tokens:
            prepared_info = json.loads(task.task.metadata)
            options = dict(source_flush_reason=prepared_info['reason'], prepared_info=prepared_info)
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
        from src.semantic_methods.lotus.prepared_batching import PreparedRequestBatcher, prepared_wire_row
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
            # Single-row arrays conservatively cover the final array framing.
            # Prepared messages and identities remain charged input, not metadata.
            return len(_encode([prepared_wire_row(row)], owner.capacity.item_input_bytes))

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
                from src.semantic_methods.lotus.prepared_batching import (
                    preparation_signature, prepared_wire_row,
                )

                if self._token_batches is None:
                    self._token_batches = self._pack_source()
                try:
                    group = next(self._token_batches)
                    rows = [prepared_wire_row(row) for row in group.rows]
                    metadata = _encode(dict(reason=group.reason, input_tokens=group.input_tokens,
                        output_cap_tokens=group.output_cap_tokens, signature=preparation_signature(
                            owner.chain.stages[0], owner.engine, owner.prepared_batching)),
                        owner.capacity.metadata_bytes)
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
        self._audit_pending = {}
        super().__init__(owner._actors, maximum, lambda: owner.core.wake.notify(),
                         deadline_monotonic=owner.deadline, on_result=owner._record_local_receipt,
                         token_budget=owner.token_budget, bulk=owner._async_token_batches,
                         handoff_progress=getattr(owner, '_event_driven_preparation', False))
        self.poll_interval_s = owner.capacity.poll_interval_s

    def try_submit_batch(self, tasks, endpoint):
        values = []
        for task in tasks:
            value = self._request(task)
            self.owner._register_local_task(task, value)
            values.append(value)
        # Task bytes and the stage binding are immutable; the owner registration
        # above only reads this parse. Reuse it within this handoff, not a cache.
        outcome = super()._try_submit_batch(tasks, endpoint, values=values)
        if outcome.acceptance == Acceptance.NOT_ACCEPTED:
            with self.owner._receipt_lock:
                for task in tasks:
                    self.owner._contexts.pop(task.key, None)
        return outcome

    def _submit_task(self, task, timings, prepare_method):
        self.owner._register_local_task(task, self._request(task))
        return super()._submit_task(task, timings, prepare_method)

    def recover_actor_receipts(self, receipts):
        errors = super().recover_actor_receipts(receipts)
        # A failed bulk reply may have ended every local result reader. Core
        # can then ACK retirement and remove these transport receipts without
        # _result_transferred running. Transfer the validated audit first.
        return errors + self.transfer_audit_receipts()

    def transfer_audit_receipts(self):
        for key, payload in self.failure_receipts():
            try:
                self.on_result(key, json.loads(payload))
            except BaseException as error:
                with self._lock:
                    pending = self._audit_pending.setdefault(key, dict(ack_deferred=False))
                    pending['error'] = 'actor_close_receipt_sink: '+type(error).__name__
            else:
                with self._lock:
                    pending = self._audit_pending.pop(key, None)
                if pending is not None and pending['ack_deferred']:
                    super().acknowledge(key)
        with self._lock:
            return [v['error'] for v in self._audit_pending.values()]

    def acknowledge(self, key):
        with self._lock:
            pending = self._audit_pending.get(key)
            if pending is not None:
                # Core may retire compute while the same bounded result and
                # registered byte/identity context still await the audit sink.
                pending['ack_deferred'] = True
                return
        super().acknowledge(key)

    def audit_handoff_report(self):
        with self._lock:
            return dict(pending_receipts=len(self._audit_pending),
                deferred_core_acks=sum(v['ack_deferred'] for v in self._audit_pending.values()),
                retained_result_bytes=sum(len(self._outcomes[key]) for key in self._audit_pending),
                registered_requests=sum(key in self._requests for key in self._audit_pending),
                scope='encoded retained audit receipts; not compute ownership or RSS')


class SemLoomRecordChainCore:
    """Single-control-thread owner for finite queries; fused is the default.

    This profile requires one CPU worker and one active fused batch. Core owns
    admission and delivery leases; the shared kernel owns the original method
    loop. A fresh owner/query ID is required for independent repeat work.
    record_local_sections instead reuses a prepared async model across legal
    F/M intervals and complete Extracts; it never starts the fused worker.
    Explicit resident rolling refill also retains that model across drained
    queries, with fresh per-query accounting and controller state.
    """

    _batch_type = None

    def __init__(self, engine: EngineSpec, resources: RecordsResources, *,
                 capacity: CoreChainCapacity | CoreRecordsCapacity | None = None, control_dir=None,
                 execution="fused", actor_owner=None, method_limits=None,
                 release_completed_audit=False, token_budget=None, ready_order="fifo", successor_first=None,
                 ready_clock=None, on_ready_event=None, progression="record", progression_batch_rows=None,
                 prepared_batching=None, batch_concurrency=None, batching_tokenizer=None,
                 batch_feedback=None, rolling_refill=False, request_feedback=None, resident=False,
                 experimental_skip_token_packing=False, request_window=None):
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
        if progression not in ("record", "batch_stage"):
            raise ValueError("unknown chain completion progression")
        if progression == "record" and progression_batch_rows is not None:
            raise ValueError("progression_batch_rows requires batch_stage progression")
        if progression == "batch_stage":
            if execution != "record_local_sections":
                raise ValueError("completion progression requires record_local_sections")
            if (type(progression_batch_rows) is not int or
                    not 1 <= progression_batch_rows <= self.resources.max_active_batches):
                raise ValueError("completion batch must fit the retained row capacity")
            if token_budget is not None or ready_order != "fifo":
                raise ValueError("batch-stage progression currently requires count admission and FIFO")
        self.progression, self.progression_batch_rows = progression, progression_batch_rows
        self.prepared_batching = None
        if prepared_batching is not None:
            from src.semantic_methods.lotus.prepared_batching import PreparedBatching
            if type(prepared_batching) is not PreparedBatching:
                raise TypeError('prepared_batching must be PreparedBatching or None')
            if (prepared_batching.policy == 'fixed_rows'
                    and prepared_batching.max_rows > self.resources.model_batch_rows):
                raise ValueError('prepared batching max_rows cannot exceed model_batch_rows')
            if self.engine.chat_template is not None:
                raise ValueError('prepared_batching currently requires the tokenizer-owned chat template')
            self.prepared_batching = PreparedBatching(**asdict(prepared_batching))
        self._streaming_tokens = (self.prepared_batching is not None
                                  and self.prepared_batching.policy == 'token_budget' and execution == 'fused')
        self._async_token_batches = self.prepared_batching is not None and execution == 'record_local_sections'
        if type(rolling_refill) is not bool:
            raise ValueError('rolling_refill must be boolean')
        self.rolling_refill = rolling_refill
        self.batch_concurrency, self._batching_tokenizer = batch_concurrency, batching_tokenizer
        if self._async_token_batches:
            if (self.prepared_batching.policy != 'token_budget' or self.prepared_batching.max_rows is not None
                    or progression != 'record' or token_budget is not None or ready_order != 'fifo'):
                raise ValueError('async token batches require uncapped token B, record progression, FIFO and W off')
            if rolling_refill and (batch_concurrency is not None or batch_feedback is not None):
                raise ValueError('rolling_refill uses request R, not batch_concurrency or batch_feedback')
            self.batch_concurrency = None if rolling_refill else 1 if batch_concurrency is None else batch_concurrency
            if not rolling_refill and (type(self.batch_concurrency) is not int or not 1 <= self.batch_concurrency <= resources.max_active_batches):
                raise ValueError('batch_concurrency must fit the member request grant')
        elif batch_concurrency is not None or batching_tokenizer is not None or rolling_refill:
            raise ValueError('batch controls require record-local token batching')
        if type(experimental_skip_token_packing) is not bool:
            raise TypeError('experimental_skip_token_packing must be boolean')
        if experimental_skip_token_packing and not (self._async_token_batches and rolling_refill):
            raise ValueError('experimental token packing bypass requires record-local rolling batches')
        self.experimental_skip_token_packing = experimental_skip_token_packing
        if batch_feedback is not None:
            from src.scheduling.submission_control.stage_work import SupplyFeedbackConfig
            if type(batch_feedback) is not SupplyFeedbackConfig:
                raise TypeError('batch_feedback must be SupplyFeedbackConfig')
            if (not self._async_token_batches or self.batch_concurrency != 1
                    or batch_feedback.max_batches > resources.max_active_batches
                    or ready_clock is not None or on_ready_event is not None):
                raise ValueError('batch feedback requires token batches starting at one, a ceiling within R and native ready clocks')
        self.batch_feedback, self._supply_feedback = batch_feedback, None
        if request_feedback is not None:
            from src.scheduling.submission_control.adaptive import ResourceFeedbackConfig, ResourceRequestConfig
            if type(request_feedback) not in (ResourceFeedbackConfig, ResourceRequestConfig):
                raise TypeError('request_feedback must be a typed current or explicit legacy request policy')
            if type(request_feedback) is ResourceFeedbackConfig and request_feedback.memory_floor_bytes:
                raise ValueError('rolling memory_floor_bytes requires a memory signal source; none is connected')
            if type(request_feedback) is ResourceRequestConfig and request_feedback.require_memory_feedback:
                raise ValueError('an independent memory-pressure source is not connected')
            if experimental_skip_token_packing:
                raise ValueError('experimental token packing bypass requires fixed rolling request capacity')
            if (not self._async_token_batches or not rolling_refill
                    or ready_clock is not None or on_ready_event is not None
                    or request_feedback.poll_interval_s > request_feedback.window_s):
                raise ValueError('request feedback requires rolling token batches and native ready clocks')
        self.request_feedback = request_feedback
        self._interleaved_preparation = False
        self._event_driven_preparation = False
        self.request_window = resources.max_active_batches if request_window is None else request_window
        if (type(self.request_window) is not int or not 1 <= self.request_window <= resources.max_active_batches
                or request_window is not None and not rolling_refill):
            raise ValueError('request_window must be a positive rolling window within the hard request capacity')
        if type(resident) is not bool or resident and not (self._async_token_batches and rolling_refill):
            raise ValueError('resident token owner requires record-local rolling refill')
        self.resident = resident
        self._query_count = 0
        self._request_scan_hints = {}
        self._request_scan_pending = {}
        self._request_scan_acknowledged = True
        self._feedback_sink_installed = False
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
            from .records_profile import resolve_core_profile
            if resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1:
                raise ValueError("record-local sections require prepare_batch_rows=model_batch_rows=1")
            resolve_core_profile(self.engine, self.resources, dict(mode="async"))
            self.capacity = CoreRecordsCapacity() if capacity is None else capacity
            if type(self.capacity) is not CoreRecordsCapacity:
                raise TypeError("record-local sections capacity must be CoreRecordsCapacity")
            if self.capacity.budget is not None:
                if not self._async_token_batches or not rolling_refill or progression != 'record':
                    raise ValueError('independent resource budgets require rolling record token execution')
                if resources.max_active_batches != self.capacity.budget.active_requests:
                    raise ValueError('owner resources must match the declared hard request budget')
                if request_window is None and request_feedback is None:
                    raise ValueError('independent rolling resources require an explicit request_window')
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
        self._service_runtime_binding = self._service_ready_observer = None
        self._service_wrapper_ref = self._service_retired_stream_ref = None
        self._service_payloads_retired = False
        self._service_payloads_certificate = None

    @property
    def production_core_connected(self):
        return isinstance(self.core, SessionEngine)

    def _control(self):
        if threading.get_ident() != self._thread:
            raise RuntimeError("Core chain owner requires one control thread")

    def _configure_record_feedback(self):
        options = (self.request_feedback if self.request_feedback is not None else
                   self.batch_feedback.options if self.batch_feedback is not None else None)
        if options is not None:
            interval = self._actors.limits.feedback_interval_s or options.poll_interval_s
            if interval > options.window_s:
                raise ValueError('native feedback interval must fit the measurement window')
            if self._actors.model_actor is not None:
                if not self.resident or self._actors.limits.feedback_interval_s != interval:
                    raise ValueError('feedback must bind before the model owner starts')
            else:
                self._actors.limits = replace(self._actors.limits, feedback_interval_s=interval)
        return options

    def start(self):
        """Preload the resident prepared model; no query source is read here."""
        self._control()
        if not self.resident or self._closing or self._actors is None:
            raise RuntimeError('only an open resident prepared owner can preload its model')
        self._configure_record_feedback()
        ready = self._start_record_actors(time.monotonic()+self.resources.startup_timeout_s)
        if self.token_budget is not None:
            self.token_budget.bind_model_context(ready)
        return ready

    def _start_record_actors(self, deadline, cancelled=None):
        self._service_before_start()
        ready = self._actors.start(deadline, cancelled)
        self._service_after_start()
        return ready

    def _record_startup_cleanup(self, error):
        try:
            error.add_note('record-local startup cleanup: ' + str(self.close()))
        except BaseException as cleanup:
            error.add_note('record-local startup cleanup failed: ' + type(cleanup).__name__ + ': ' + str(cleanup))

    def _service_before_start(self):
        if self._service_runtime_binding is not None:
            self._service_runtime_binding.bind()
            self._service_runtime_binding.check(require_identity=True)

    def _service_after_start(self):
        if self._service_runtime_binding is not None:
            self._service_runtime_binding.check(require_identity=True)
            self._service_ready_observer()

    def _service_cleanup_confirmed(self, *, expected_identity=None):
        from .records_runtime_identity import prepared_cleanup_confirmed
        return prepared_cleanup_confirmed(self._actors, self.transport, self.core, self.last_close_report,
            closing=self._closing, feedback=self._supply_feedback, expected_identity=expected_identity)

    def _service_retire_stream(self, stream, report):
        for name in ('source', 'run', 'section', 'error', 'driver', 'session', 'pending',
                     'pool', '_output', '_phase_source', 'cancelled', 'owner'):
            setattr(stream, name, None)
        stream.bindings, stream.offered = {}, set()
        stream._phase_bindings, stream._prepared, stream._extract_results = {}, {}, []
        stream.last_close_report = report

    def _service_retire_payloads(self, metadata, certificate):
        """Detach private query data only after its exact service handoff."""
        self._control()
        wrapper = None if self._service_wrapper_ref is None else self._service_wrapper_ref()
        if (wrapper is None or wrapper.owner is not self or not wrapper._closed
                or wrapper._retired_report is not metadata
                or wrapper._service_retirement is not certificate
                or certificate is None or not wrapper.cleanup_confirmed() or wrapper.has_unconsumed_results()
                or self._service_payloads_certificate is not None
                   and self._service_payloads_certificate is not certificate):
            raise RuntimeError('private payload retirement requires the original certified result transfer')
        from .records_runtime_identity import retired_close_metadata
        # The actual ready receipt contains model/control metadata, not query
        # results. Keep it complete for the existing exact cleanup comparison.
        report = retired_close_metadata(metadata)
        report.update(payload_transferred=True, cleanup_confirmed=True,
                      calls_count=metadata['calls_count'], unconsumed_call_results=[])
        actor = retired_close_metadata(dict(model_owner=self._actors.last_close_report))['model_owner']
        actor.update(model_ready=self._actors.model_ready, cleanup_errors=[],
                     gpu_exit_confirmed=self._actors.last_close_report.get('gpu_exit_confirmed'))
        report['model_owner'] = actor
        stream = self._active
        if stream is not None:
            self._service_retired_stream_ref = weakref.ref(stream)
        elif self._service_retired_stream_ref is not None:
            stream = self._service_retired_stream_ref()
        self._service_payloads_certificate = certificate
        self.last_close_report = report
        if stream is not None:
            self._service_retire_stream(stream, report)
        with self._receipt_lock:
            self._raw_receipts, self._contexts = {}, {}
        self._root_run = self._active = self.results = None
        self._expected_bindings, self._accountants = {}, {}
        self._source_expected, self._current_relation = (), ()
        self._phase_parents = self._phase_columns = None
        self.phase_history = []
        if self.transport is not None:
            with self.transport._lock:
                self.transport._outcomes, self.transport._requests, self.transport._refs = {}, {}, {}
                self.transport._consumed_receipts, self.transport._bulk = set(), {}
        organizer = getattr(self, '_token_organizer', None)
        if organizer is not None:
            organizer._prefix = organizer._prefix_context = None
            organizer._estimates, organizer._frontier, organizer._frontier_set = {}, [], set()
            organizer._header_estimate = organizer._header_context = None
            self._token_organizer = None
            self.core.policies = replace(self.core.policies, organize=None)
        self._actors.last_close_report = actor
        self._actors._model_close_report = actor.get('engine_close_report')
        self._actors._model_close_ref = None
        self._service_payloads_retired = True

    def _service_payloads_retired_confirmed(self):
        """Read the actual detached fields; a marker alone never returns bytes."""
        if (not self._service_payloads_retired or self._raw_receipts or self._contexts
                or self._root_run is not None or self._active is not None
                or self._source_expected or self._current_relation or self._expected_bindings
                or 'calls' in (self.last_close_report or {})):
            return False
        stream = None if self._service_retired_stream_ref is None else self._service_retired_stream_ref()
        if stream is not None:
            if (any(getattr(stream, name, None) is not None for name in
                    ('source', 'run', 'section', 'error', 'driver', 'session', 'pending',
                     'pool', '_output', '_phase_source', 'owner'))
                    or stream.bindings or getattr(stream, '_phase_bindings', {})
                    or getattr(stream, '_prepared', {}) or getattr(stream, '_extract_results', [])
                    or 'calls' in (stream.last_close_report or {})):
                return False
        return all(not ({'unconsumed_call_results', 'unconsumed_core_results', 'calls'} & set(value or {}))
                   for value in (self._actors.last_close_report, self._actors._model_close_report))

    def finish_query(self, *, timeout=None):
        self._control()
        if self._active is None or not self._active._output_exhausted or self._active.error is not None:
            return self.close(timeout=timeout)
        return self._close_record_local(timeout=timeout, retain_model=self.resident)

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
        limits = self.capacity.limits(self.resources, stage_count)
        if self._streaming_tokens:
            rows = self.prepared_batching.max_rows
            # Core accounts stage-call upper bounds. With no batch row cap,
            # the immutable whole-query declaration bounds this counter; it
            # cannot close or shrink a batch and is not passed to the batcher.
            rows = max(1, self._expected_count) if rows is None else rows
            limits = replace(limits, active_work=rows * stage_count)
        return limits

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
        if self.engine.max_model_len is not None and any(
                stage.generation.max_tokens > self.engine.max_model_len for stage in chain.stages):
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
        from .lotus_records_async_actor import AsyncRecordsActorOwner

        if self._service_runtime_binding is not None:
            self._service_runtime_binding.check()
        if self._closing or self._active is not None and not (
                self.resident and (self.last_close_report or {}).get('query_completed')
                and self.last_close_report.get('model_retained')):
            raise RuntimeError("Core chain owner already used or closing")
        if not self._request_scan_acknowledged:
            # A prior model EOF is not proof that its complete query callback
            # succeeded. Uncommitted pending evidence cannot seed another query.
            self._request_scan_hints.clear()
            self._request_scan_pending.clear()
        self._request_scan_acknowledged = False
        chain = LotusRecordChain.from_dict(chain.describe())
        if chain.query_id != query_id or len({s.method.model_name for s in chain.stages}) != 1:
            raise ValueError("record-local query requires one query and one model deployment")
        if self.engine.max_model_len is not None and any(s.generation.max_tokens > self.engine.max_model_len for s in chain.stages):
            raise ValueError("chain max_tokens exceeds max_model_len")
        if self.progression == "batch_stage" and (any(s.kind not in ("filter", "map") for s in chain.stages)
                or len({s.model_role for s in chain.stages}) != 1):
            raise ValueError("batch-stage progression requires same-role Filter/Map plans")
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
            self.method_limits = self.capacity.method_limits(len(chain.stages))
        if self.method_limits.stages < len(chain.stages):
            raise ValueError("method stage reservation must cover the complete finite plan")
        self.capacity.method_capacity(self.resources, self.method_limits)
        if self._active is not None:
            self._active = None
            self._contexts.clear()
            self._raw_receipts.clear()
            self.phase_history = []
            self.last_close_report = None
        self.query_chain = self.chain = chain
        self._stage_bindings = {stage.stage_id: stage for stage in chain.stages}
        self._expected_bindings = {record.row_id: record for record in expected}
        self._root_run, self._source_expected = run, expected
        self._expected_count = len(expected)
        self.deadline = time.monotonic() + self.resources.query_timeout_s
        _check(self.deadline, cancelled)
        if self._actors is None:
            self._actors = AsyncRecordsActorOwner.for_prepared(
                self.engine, chain.stages[0].generation, self.resources,
                **({} if self.capacity.budget is None else dict(capacity=self.capacity)))
        elif self._actors.generation != chain.stages[0].generation:
            raise ValueError("prepared owner's default generation differs from the initial stage")
        self._actors.close_deadline = None
        self._actors.peak_prepared_batch_rpc_bytes = 0
        feedback_options = self._configure_record_feedback()
        if expected and self._service_runtime_binding is not None:
            # No task producer exists before ready. A failed ready receipt can
            # therefore close its actual actor without retiring an empty Core
            # against a model identity which the caller never received.
            try:
                self._start_record_actors(min(self.deadline,
                    time.monotonic() + self.resources.startup_timeout_s), cancelled)
            except BaseException as error:
                self._record_startup_cleanup(error)
                raise
        limits = self.capacity.limits(self.resources)
        if self.token_budget is not None:
            limits = replace(limits, active_work=self.token_budget.active_tokens)
        endpoint = EndpointSnapshot("local-chain-model", "local://model", "local", "0",
                                    True, 0, 0, None, time.monotonic())
        policies = SessionPolicies(StaticAdmissionController(limits.active_requests),
            RoundRobinEndpointRouter(), TopologySnapshot((endpoint,), time.monotonic()), "local")
        self.transport = _SectionTransport(self, limits.held_tasks)
        if self._async_token_batches:
            from .lotus_chain_token_batches import PreparedTokenOrganizer
            if expected and self._batching_tokenizer is None:
                from transformers import AutoTokenizer
                self._batching_tokenizer = AutoTokenizer.from_pretrained(
                    self.engine.tokenizer_path or self.engine.model_path, local_files_only=True, trust_remote_code=False)
            self._token_organizer = PreparedTokenOrganizer(self, self._batching_tokenizer, self.batch_concurrency)
            policies = replace(policies, organize=self._token_organizer)
        if self.core is None:
            self.core = SessionEngine(limits, self.transport, policies)
        else:
            # Preserve SessionEngine's monotonically allocated session identities.
            # The previous query has closed its transport and released every credit.
            self.core.backend, self.core.policies = self.transport, policies
        if feedback_options is not None:
            from .lotus_chain_token_batches import BatchSupplyFeedback, RequestSupplyFeedback
            from .records_request_feedback import ResourceRequestSupplyFeedback
            from src.scheduling.submission_control.adaptive import ResourceRequestConfig
            request_type = (ResourceRequestSupplyFeedback if type(self.request_feedback) is ResourceRequestConfig
                            else RequestSupplyFeedback)
            self._supply_feedback = (request_type(self, self.request_feedback)
                if self.request_feedback is not None else BatchSupplyFeedback(self, self.batch_feedback))
            if not self._feedback_sink_installed:
                previous_sink = self.core.sink
                def sink(action, key):
                    if previous_sink is not None:
                        previous_sink(action, key)
                    self._supply_feedback.event(action, key)
                self.core.sink = sink
                self._feedback_sink_installed = True
            self._ready_options = dict(ready_order='fifo', successor_first=None,
                clock=None, on_ready_event=self._supply_feedback.ready)
        try:
            self._active = _RecordLocalRows(self, records, run, cancelled)
            if expected:
                if self._service_runtime_binding is None:
                    self._start_record_actors(min(self.deadline,
                        time.monotonic() + self.resources.startup_timeout_s), cancelled)
                from src.serving.backends.model_context import model_context_limit
                context_limit = model_context_limit(self.engine, self._actors.model_ready)
                if any(stage.generation.max_tokens > context_limit for stage in chain.stages):
                    raise ValueError('chain output cap exceeds effective model context')
                if self.token_budget is not None:
                    self.token_budget.bind_model_context(self._actors.model_ready)
            self._query_count += 1
        except BaseException as error:
            self._record_startup_cleanup(error)
            raise
        return self._active

    def _register_local_task(self, task, value):
        stage = self._stage_bindings[task.task.profile_name]
        record = self._expected_bindings[value["row"]["row_id"]]
        canonical = stage_identity(self.query_chain.query_id, record, stage.stage_id, model_role=stage.model_role)
        if (any(value["row"].get(name) != canonical[name] for name in IDENTITY_FIELDS)
                or GenerationSpec.from_dict(value["generation"]) != stage.generation):
            raise ValueError("prepared request differs from the immutable query stage binding")
        context = dict(identity=canonical, parent_call_ids=list(value["parent_call_ids"]),
                       core_task_key=dict(session_id=task.key.session_id, sequence=task.key.sequence))
        if "token_cost" in value:
            context["token_estimate"] = deepcopy(value["token_cost"])
        if task.member is not None:
            context["transport_batch"] = asdict(task.member)
        with self._receipt_lock:
            if task.key in self._contexts and self._contexts[task.key] != context:
                raise ValueError("prepared task key changed its query association")
            self._contexts[task.key] = context

    def _record_local_receipt(self, key, value):
        with self._receipt_lock:
            context = self._contexts[key]
            if any(value.get(name) != context["identity"][name] for name in IDENTITY_FIELDS):
                raise ValueError("prepared receipt changed its registered query task")
            receipt = dict(deepcopy(value), **{name:deepcopy(item) for name,item in context.items()
                                             if name != "identity"})
            previous = self._raw_receipts.get(key)
            if previous is not None:
                for name in (*IDENTITY_FIELDS, "status", "text", "prompt_tokens", "completion_tokens",
                             "total_tokens", "engine_uid", "engine_request_id", "parent_call_ids"):
                    if previous.get(name) != receipt.get(name):
                        raise ValueError("late prepared receipt conflicts with its original outcome")
            self._raw_receipts[key] = receipt

    def _collect_local_receipts(self):
        return self.transport.transfer_audit_receipts()

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

    def _close_record_local(self, *, timeout=None, retain_model=False):
        binding = getattr(self, '_service_runtime_binding', None)
        runtime_owned = (binding is not None and self._actors is not None and (self._actors.ray is not None
                         or self._actors.model_actor is not None or self._actors.model_ready is not None))
        if (binding is not None and not self._service_cleanup_confirmed()
                and runtime_owned):
            binding.check(require_identity=True)
        if not retain_model:
            self._closing = True
        if self._supply_feedback is not None:
            self._supply_feedback.close()
        if (self.last_close_report is not None and self.last_close_report["status"] == "closed"
                and (retain_model or not self.last_close_report.get('model_retained'))
                and (binding is None or retain_model or self._service_cleanup_confirmed())):
            if self._supply_feedback is not None:
                self.last_close_report = dict(self.last_close_report,
                    **{self._supply_feedback.report_field: self._supply_feedback.report()})
            return self.last_close_report
        errors = [] if self._active is None else self._active._close_query()
        if self._actors is None:
            actor = dict(status="closed", cleanup_errors=[], gpu_exit_confirmed=None)
            closed = dict(model_owner=actor, transport_closed=True, producer_retirement=None, cleanup_errors=[])
        else:
            closed = close_prepared_core(self._actors, self.transport, self.core, self.capacity,
                deadline=self.deadline, timeout=timeout, retain_model=retain_model)
            if binding is not None and runtime_owned:
                binding.check(require_identity=True)
        errors += closed["cleanup_errors"]
        if self.transport is not None:
            try:
                errors += self._collect_local_receipts()
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
        if retain_model and self._supply_feedback is not None:
            if not self._supply_feedback.report()['snapshot_rpc_terminal_evidence_complete']:
                errors.append('resident_query_feedback_rpc_unconfirmed')
        status = "closed" if (actor["status"] == ('retained' if retain_model else 'closed') and closed["transport_closed"]
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
            audit_handoff=None if self.transport is None else self.transport.audit_handoff_report(),
            phase_history=deepcopy(self.phase_history), expected_occurrences=self._expected_count,
            received_occurrences=0 if stream is None else len(stream.offered),
            peak_core_usage=None if stream is None else stream.peak_core_usage,
            source_exhausted=bool(stream and stream.source_eof), source_closed=bool(stream and stream.source_closed),
            scheduler_owner="SemLoom SessionEngine", method_owner="shared ChainRun/MethodRun",
            model_batch_mode="independent_final_results", model_batch_rows=1,
            cross_query_model_reuse=self.resident and self._query_count > 1,
            model_retained=retain_model and status == 'closed', model_owner_query_count=self._query_count,
            whole_query_rss_bounded=False,
            driver_retention="finite expected relation, current full relation, parsed call and raw receipt audit",
            **({} if self.capacity.budget is None else dict(resource_budget=asdict(self.capacity.budget),
                resource_budget_source='explicit logical deployment declaration; excludes Python/Ray RSS overhead')),
            prepare_actor_count=0, prepare_worker_pool_used=False,
            progression=dict(policy=self.progression, batch_rows=self.progression_batch_rows,
                scope="successor submission over the same single-row async model path"),
            work_unit="model_calls" if self.token_budget is None else "tokens")
        if self._async_token_batches and getattr(self, '_token_organizer', None) is not None:
            self.last_close_report['async_token_batches'] = dict(self._token_organizer.report(),
                data_batch_rpcs=self.transport.data_batch_rpcs,
                batch_handoff=self.transport.batch_handoff_report(),
                peak_data_rpc_bytes=getattr(self._actors, 'peak_prepared_batch_rpc_bytes', 0),
                peak_result_rpc_bytes=self.transport.peak_result_frame_bytes,
                engine_max_num_seqs=self.engine.max_num_seqs,
                engine_max_num_batched_tokens=self.engine.max_num_batched_tokens)
        if self._supply_feedback is not None:
            self.last_close_report[self._supply_feedback.report_field] = self._supply_feedback.report()
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

    def confirm_query_feedback(self, *, completed):
        """Commit only after the caller's complete result interpretation succeeds.

        Model/transport EOF only stages a hint. The caller owns parsing,
        projection and final result validation; failure explicitly discards it.
        This state is in-memory and scoped to this compatible resident owner.
        """
        if type(completed) is not bool:
            raise TypeError('completed must be the explicit complete-query outcome')
        close = self.last_close_report or {}
        feedback = close.get('request_supply_feedback') or {}
        eligible = (completed and self.resident and close.get('query_completed') is True
            and close.get('status') == 'closed' and close.get('model_retained') is True
            and close.get('source_closed') is True and not close.get('cleanup_errors')
            and (close.get('core_usage') or {}).get('held_tasks') == 0
            and feedback.get('snapshot_rpc_terminal_evidence_complete') is True
            and all(call.get('status') == 'succeeded' for call in self.calls))
        committed = bool(eligible and self._request_scan_pending)
        if committed:
            self._request_scan_hints = deepcopy(self._request_scan_pending)
        elif not eligible or any(phase.get('feedback_gaps', 0)
                or phase.get('stop_reason') == 'resource_pressure'
                or (phase.get('reasons') or {}).get('at_minimum', 0) or any(
                row.get('reason') == 'resource_pressure' for row in phase.get('history', ()))
                for phase in feedback.get('phases', ())):
            self._request_scan_hints.clear()
        self._request_scan_pending.clear()
        self._request_scan_acknowledged = True
        receipt = dict(scope='compatible resident model owner; no disk cache or historical rate reuse',
            full_query_completed=completed, reuse_eligible=eligible, committed=committed,
            qualified_phase_hints=len(self._request_scan_hints),
            policy='ascending_request_work_v1')
        if self.last_close_report is not None:
            self.last_close_report = dict(self.last_close_report, request_scan_reuse=receipt)
        return deepcopy(receipt)
