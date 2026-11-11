"""Fused batch transfer and result iteration over the existing Core.

The query owner assembles the model, stages and resources. This module carries
its fused batches and consumes their results without another execution owner.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import Future, TimeoutError as FutureTimeout
from dataclasses import asdict
import hashlib
import json
import socket
import threading
import time

from src.infrastructure.deadlines import remaining_time
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import OfferedTask, State, TaskInfo
from src.scheduling.runtime.async_backend import BoundedAsyncBackend, ConfirmedTaskFailure
from src.semantic_methods.lotus.record_types import RecordOccurrence

from .records_query_runtime import _check


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
    def __init__(self, owner, records, spec, cancelled, *, close_on_eof=True):
        self.close_on_eof = close_on_eof
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

    def finish_relation(self):
        """Retire the consumed input and assemble its complete Extract relation."""
        from src.semantic_methods.lotus.record_relation import complete_extract_relation

        owner = self.owner
        _check(owner.deadline, self.cancelled)
        usage = asdict(owner.core.capacity.usage())
        if (not self.source_eof or self.pending is not None
                or owner.results.pending or any(usage.values())
                or not owner.transport.phase_drained()):
            raise RuntimeError("Core relation phase still owns source, delivery or transport work")
        owner.results.verify_success(source_exhausted=True)
        errors = self._close_source()
        if errors:
            raise self._source_close_error
        retired = self.session.close()
        if retired.status != "CLOSED" or any(asdict(retired.usage).values()):
            raise RuntimeError("Core relation session did not retire completely")
        completed = complete_extract_relation(
            self._phase_records, columns=owner.results.columns,
            extract_stage=owner.chain.stages[-1], query_id=owner.query_chain.query_id,
            calls=owner.calls, source_exhausted=True,
        )
        _check(owner.deadline, self.cancelled)
        history = dict(
            phase_stage_ids=list(owner.phase_stage_ids), source_exhausted=True,
            session_id=self.session.session_id, received_occurrences=len(owner.results.received),
            output_occurrences=len(completed.records), core_usage=usage,
            barrier="complete_extract_relation", verified_success=True,
        )
        return completed, history

    def begin_relation(self, records):
        self.source = iter(records)
        self.source_eof = self.source_closed = False
        self.offered, self.sequence, self.pending = set(), 0, None
        self._phase_records = []
        self.session = self.owner.core.open(
            self.owner._phase_spec(), self.owner._fused_limits(len(self.owner.chain.stages)))

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
                    if self.close_on_eof:
                        report = self.owner.close()
                        if report["status"] != "closed":
                            raise RuntimeError("Core chain cleanup remains unconfirmed")
                        report["query_completed"] = True
                    elif self._close_query():
                        raise self._source_close_error
                    self.completed = True
                    raise StopIteration
                if not progress.has_immediate_work:
                    wait = remaining_time(self.owner.deadline, self.owner.capacity.poll_interval_s)
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
