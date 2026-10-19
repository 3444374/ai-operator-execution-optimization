"""Finite LOTUS chains carried by the existing production incremental Core.

Core schedules one fused batch at a time. Its work descriptor is a conservative
stage-call upper bound, not a count of GPU requests currently executing. The
shared worker owns methods, original parsers, and the private model process.
Logical transport bytes exclude worker Python copies, audit files and the
consumer's retained relation/call ledger; they are not RSS or GPU-memory limits.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future, TimeoutError as FutureTimeout
from dataclasses import asdict, dataclass
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

from src.baselines.common.private_artifacts import require_outside_git
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.models import EndpointSnapshot, TopologySnapshot
from src.scheduling.core.session import SessionEngine
from src.scheduling.core.session_contract import (
    OfferedTask, SessionLimits, SessionSpec, SessionTimeouts, State, TaskInfo,
)
from src.scheduling.core.session_policy import SessionPolicies
from src.scheduling.endpoint_routing.policies import RoundRobinEndpointRouter
from src.scheduling.runtime.async_backend import BoundedAsyncBackend, ConfirmedTaskFailure
from src.scheduling.submission_control.admission import StaticAdmissionController
from src.semantic_methods.lotus.record_chain import LotusRecordChain, RecordOccurrence
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec


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
                from src.baselines.text.frameworks.lotus_chain_batch import FusedChainBatch

                worker_type = owner._batch_type or FusedChainBatch
                self.worker = worker_type(
                    owner.chain.describe(), asdict(owner.engine),
                    list(owner.results.bindings.values()), str(owner.control),
                    owner.deadline, owner.resources.startup_timeout_s,
                    socket.gethostname(), owner.control_token,
                )
            if self._stopping.is_set():
                raise InterruptedError("Core chain transport closed during worker construction")
        value = self.worker.run_rows(json.loads(task.task.payload))
        # Worker materialization and its audit are separate from the bounded
        # Core response. Overflow is an uncertain task, never a fake terminal;
        # close recovers the independent call audit without releasing its credit.
        return _encode(value, task.task.max_result_bytes)

    async def _execute(self, task, endpoint):
        from src.baselines.text.frameworks.lotus_chain_results import (
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
        audit = ChainBatchResults(records, self.owner.chain)
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
        self.session = owner.core.open(spec)
        self.offered, self.sequence = set(), 0
        self.pending = None
        self.source_eof = self.source_closed = self.finished = self.completed = False
        self.last_close_report = None
        self.peak_core_usage = asdict(owner.core.capacity.usage())

    @property
    def production_core_connected(self):
        return isinstance(self.owner.core, SessionEngine)

    @property
    def calls(self):
        return self.owner.results.calls

    @property
    def columns(self):
        return self.owner.results.columns

    @property
    def batch_stage_stats(self):
        return self.owner.results.batch_stage_stats

    @property
    def raw_result_batches(self):
        return self.owner.results.raw_result_batches

    @property
    def unassociated_results(self):
        return self.owner.results.unassociated_results

    def __iter__(self):
        return self

    def _observe(self):
        for key, value in asdict(self.owner.core.capacity.usage()).items():
            self.peak_core_usage[key] = max(value, self.peak_core_usage[key])

    def _fill(self):
        from src.baselines.text.frameworks.lotus_chain_results import _source_binding, record_row

        owner = self.owner
        if self.source_eof or owner.core.capacity.usage().held_tasks:
            return
        if self.pending is None:
            rows, batch_ids = [], set()
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
                self.sequence, payload, upper, owner.capacity.item_result_bytes,
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
                    return self.owner.results.pop()
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
                    self.owner.results.verify_coverage()
                    self.owner.results.finalize_columns()
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
        errors = []
        if not self.source_closed:
            try:
                close = getattr(self.source, "close", None)
                if close is not None:
                    close()
                self.source_closed = True
            except BaseException as error:
                errors.append("source_iterator_close: " + type(error).__name__)
        self.session.close()
        self.owner.core.reap(self.owner.capacity.step_actions)
        return errors

    def close(self, *, timeout=None):
        return self.owner.close(timeout=timeout)


class SemLoomRecordChainCore:
    """Single-control-thread owner for one finite query and one fused worker.

    This profile requires one CPU worker and one active fused batch. Core owns
    admission and delivery leases; the shared kernel owns the original method
    loop. A fresh owner/query ID is required for independent repeat work.
    """

    _batch_type = None

    def __init__(self, engine: EngineSpec, resources: RecordsResources, *,
                 capacity: CoreChainCapacity | None = None, control_dir=None):
        self.engine = EngineSpec.from_dict(asdict(engine))
        self.resources = RecordsResources(**asdict(resources))
        if resources.cpu_workers != 1 or resources.max_active_batches != 1:
            raise ValueError("Core fused chain requires cpu_workers=max_active_batches=1")
        self.capacity = CoreChainCapacity() if capacity is None else capacity
        if type(self.capacity) is not CoreChainCapacity:
            raise TypeError("Core chain capacity must be CoreChainCapacity")
        self._thread = threading.get_ident()
        self._closing = False
        self.control_dir = control_dir
        self.core = self.transport = self.chain = self.results = self._active = None
        self.control = self.deadline = self.control_token = None
        self.last_close_report = None

    @property
    def production_core_connected(self):
        return isinstance(self.core, SessionEngine)

    def _control(self):
        if threading.get_ident() != self._thread:
            raise RuntimeError("Core chain owner requires one control thread")

    def evaluate(self, records, chain, *, expected, query_id, cancelled=None):
        """Return a single-pass relation iterator; publish its frame after completed EOF.

        ``expected`` binds each source occurrence and its values without prompts.
        ``model_batch_rows`` bounds one fused input batch, including preparation;
        no separate preparation pool or per-stage Core dispatch is introduced.
        Calls/usage remain inspectable on failure. Unknown work retains the Core
        task and source bytes, even when a later private-engine cleanup succeeds.
        """
        self._control()
        if self._closing or self._active is not None:
            raise RuntimeError("Core chain owner already used or closing")
        chain = LotusRecordChain.from_dict(chain.describe())
        if chain.query_id != query_id:
            raise ValueError("chain and query_id must identify the same query")
        if len({stage.method.model_name for stage in chain.stages}) != 1:
            raise ValueError("Core fused chain requires one model deployment")
        if any(stage.generation.max_tokens > self.engine.max_model_len for stage in chain.stages):
            raise ValueError("chain max_tokens exceeds max_model_len")
        expected = tuple(RecordOccurrence.from_dict(record.describe()) for record in expected)
        for name in ("row_id", "source_position"):
            if len({getattr(record, name) for record in expected}) != len(expected):
                raise ValueError("Core chain requires globally unique " + name)
        if any(record.index_updates for record in expected):
            raise ValueError("assemble final Extract index updates before another query")
        self.engine.validate_driver_environment()
        spec = SessionSpec(query_id, "lotus-record-chain", "lotus-record-chain-fused-v1",
                           work_unit="stage_call_upper_bound")
        self.deadline = time.monotonic() + self.resources.query_timeout_s
        _check(self.deadline, cancelled)
        from src.baselines.text.frameworks.lotus_chain_results import ChainBatchResults

        self.chain, self.results = chain, ChainBatchResults(expected, chain)
        self.control = Path(tempfile.mkdtemp(prefix="core-lotus-chain-", dir=self.control_dir))
        try:
            require_outside_git(self.control)
            os.chmod(self.control, 0o700)
            self.control_token = uuid.uuid4().hex
            descriptor = os.open(self.control / "owner", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w") as output:
                output.write(self.control_token)
            limits = self.capacity.limits(self.resources, len(chain.stages))
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
            if accept:
                self.results.accept(value)
            else:
                self.results.recover(value)

    def close(self, *, timeout=None):
        self._control()
        if timeout is not None and (type(timeout) not in (int, float)
                                    or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("cleanup timeout must be finite and positive")
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
            from src.baselines.text.frameworks.daft_engine_process import MAX_MESSAGE_BYTES

            maximum = MAX_MESSAGE_BYTES * (len(self.chain.stages) + 2) + self.capacity.item_input_bytes
            for path in [*self.control.glob("chain-audit-*.json"),
                         *self.control.glob("chain-failure-*.json")]:
                try:
                    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
                        raise ValueError("invalid or oversized chain audit")
                    self.results.recover(json.loads(path.read_text()))
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
                                          else len(self.results.expected) * len(self.chain.stages)),
            cpu_workers_applied=True, prepare_batch_rows_applied=False,
            declared_model_evaluations=0 if self.results is None else len(self.results.calls),
            expected_occurrences=0 if self.results is None else len(self.results.expected),
            received_occurrences=0 if self.results is None else len(self.results.received),
            prepare_batch_rows_reason="preparation is owned by the shared fused method worker",
            native_engine_abort_supported=False,
            cleanup_artifacts=None if closed or self.control is None else str(self.control),
        )
        if self._active is not None:
            self.last_close_report["peak_core_usage"] = self._active.peak_core_usage
            self._active.last_close_report = self.last_close_report
        if closed and self.control is not None:
            shutil.rmtree(self.control)
        return self.last_close_report
