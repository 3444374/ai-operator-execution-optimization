"""Finite LOTUS record chains in one Daft Native process class.

Daft owns native batches and buffering. The shared fused kernel calls the sole
LotusRecordChain stage loop and owns one isolated private model process. The
consumer validates compact batch envelopes; it does not schedule method stages.
"""

from dataclasses import asdict
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import tempfile
import threading
import time
import uuid

from src.semantic_methods.lotus.record_chain import LotusRecordChain, RecordOccurrence, _ordered_columns
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec
from .daft_engine_process import DEFAULT_CLOSE_TIMEOUT_S
from .lotus_chain_batch import FusedChainBatch
from .lotus_chain_results import ChainBatchResults, ChainExecutionError, _json, _source_binding, record_row


DAFT_VERSION = "0.7.21"


class DaftChainExecutionError(ChainExecutionError):
    def __init__(self, remote_error_type):
        self.remote_error_type = remote_error_type
        RuntimeError.__init__(self, "Daft LOTUS chain failed: " + remote_error_type)


def _check(deadline, control):
    if (Path(control) / "stop").exists():
        raise InterruptedError("Daft LOTUS chain cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("Daft LOTUS chain query deadline expired")


def _native_runner(runner_threads):
    from daft.runners import _get_runner, get_or_create_runner
    from src.infrastructure import daft_runtime

    if _get_runner() is None:
        if os.environ.get("DAFT_RUNNER") not in (None, "native"):
            raise RuntimeError("chain requires an explicit Daft Native configuration")
        daft_runtime.configure_daft_runner("native", num_threads=runner_threads)
    elif daft_runtime._configured is not None:
        daft_runtime.configure_daft_runner("native", num_threads=runner_threads)
    elif runner_threads is not None:
        raise RuntimeError("cannot apply thread count to an externally selected Daft runner")
    if get_or_create_runner().name != "native":
        raise RuntimeError("chain requires Daft Native " + DAFT_VERSION)


def _graph(frame, chain, engine, resources, expected, control, deadline, control_token,
           *, runner_threads=None, batch_type=None, model_gpus=1,
           initial_parents=None, columns=None, allow_nulls=False):
    """Only CPU qualification may substitute a kernel or GPU resource request."""
    import daft

    if daft.__version__ != DAFT_VERSION:
        raise RuntimeError("chain requires exactly Daft Native " + DAFT_VERSION)
    _native_runner(runner_threads)
    if not expected:
        @daft.func.batch(return_dtype=daft.DataType.string(),
                         batch_size=resources.model_batch_rows, max_retries=0)
        def empty_source(records):
            if len(records):
                raise ValueError("Daft graph contains occurrences absent from the empty source declaration")
            return daft.Series.from_pylist([])

        return frame.select(empty_source(daft.col("record_json")).alias("batch_json"))

    chain_config, engine_config = chain.describe(), asdict(engine)
    bindings = [_source_binding(record) for record in expected]
    driver_host, control_path = socket.gethostname(), str(control)
    startup_timeout_s = resources.startup_timeout_s

    @daft.cls(cpus=1, gpus=model_gpus, use_process=True, max_concurrency=1,
              max_retries=0, on_error="raise")
    class Chain:
        def __init__(self):
            self.kernel = (batch_type or FusedChainBatch)(
                chain_config, engine_config, bindings, control_path, deadline,
                startup_timeout_s, driver_host, control_token,
                initial_parents=initial_parents, columns=columns, allow_nulls=allow_nulls)

        @daft.method.batch(return_dtype=daft.DataType.string(), batch_size=resources.model_batch_rows)
        def run(self, records):
            values = records.to_pylist()
            if not values:
                return daft.Series.from_pylist([])
            envelope = self.kernel.run_rows([{"record_json": value} for value in values])
            # A native batch UDF preserves its input length. Only its first row
            # carries the full ledger; a native filter drops padding nulls.
            return daft.Series.from_pylist([_json(envelope)] + [None] * (len(values) - 1))

    frame = frame.select(Chain().run(daft.col("record_json")).alias("batch_json"))
    return frame.where(daft.col("batch_json").not_null())


class _DaftChainRows(ChainBatchResults):
    _backend_name = "Daft"

    def _execution_error(self, remote_error_type):
        return DaftChainExecutionError(remote_error_type)

    def __init__(self, frame, expected, chain, deadline, cancelled, control, *,
                 initial_parents=None, columns=None):
        super().__init__(expected, chain, initial_parents=initial_parents, columns=columns)
        self.frame, self.deadline, self.cancelled = frame, deadline, cancelled
        self.control = Path(control)
        self._iterator = None
        self._done = self._started = self.completed = False
        self._stop = threading.Event()
        self._watchdog = self._failure = None
        self.last_close_report = None

    def __iter__(self):
        return self

    def _check(self):
        if self._failure is not None:
            raise self._failure
        if self.cancelled is not None and self.cancelled():
            raise InterruptedError("Daft LOTUS chain cancelled")
        _check(self.deadline, self.control)

    def _watch(self):
        while not self._stop.wait(0.02):
            try:
                self._check()
            except BaseException as exc:
                self._failure = exc
                (self.control / "stop").touch(mode=0o600)
                return

    def __next__(self):
        if self._done:
            raise StopIteration
        try:
            if self._iterator is None:
                self._check()
                self._started = self.frame is not None
                self._watchdog = threading.Thread(target=self._watch, daemon=True,
                                                  name="daft-lotus-chain-stop")
                self._watchdog.start()
                self._iterator = iter(()) if self.frame is None else iter(self.frame.iter_rows())
            while not self.pending:
                self.accept(json.loads(next(self._iterator)["batch_json"]))
            return self.pop()
        except StopIteration:
            try:
                self._check()
                self.verify_coverage()
                report = self.close()
                if report["status"] != "closed":
                    raise RuntimeError("Daft chain cleanup remains unconfirmed")
                self.completed = True
                report["query_completed"] = True
                self.finalize_columns()
            except BaseException as exc:
                self._finish_error(exc)
                raise
            raise
        except BaseException as exc:
            primary = self._failure or exc
            self._finish_error(primary)
            if primary is not exc:
                raise primary from exc
            raise

    def _finish_error(self, error):
        try:
            report = self.close()
            if report["status"] != "closed":
                error.add_note("Daft chain cleanup: " + _json(report))
        except BaseException as cleanup:
            error.add_note("Daft chain cleanup also failed: " + type(cleanup).__name__)
        error.chain_calls = list(self.calls)
        error.chain_stage_stats = list(self.batch_stage_stats)
        error.chain_raw_results = [row for batch in self.raw_result_batches.values() for row in batch]

    def close(self):
        self._done = True
        if self.last_close_report is not None and self.last_close_report["status"] == "closed":
            return self.last_close_report
        errors = list(self._cleanup_errors)
        self._stop.set()
        (self.control / "stop").touch(mode=0o600)
        if self._watchdog is not None:
            self._watchdog.join(timeout=0.2)
            if self._watchdog.is_alive():
                errors.append("stop_watchdog_still_running")
        try:
            close = getattr(self._iterator, "close", None)
            if close is not None:
                close()
        except BaseException as exc:
            errors.append("iterator_close: " + type(exc).__name__)
        reports = []
        for path in self.control.glob("engine-*.json"):
            try:
                report = json.loads(path.read_text())
                if not report.get("process_exit_confirmed") and report.get("model_pid"):
                    # Allow the existing private close budget to finish before
                    # supervising only that registered process identity.
                    until = time.monotonic() + DEFAULT_CLOSE_TIMEOUT_S + 1
                    while time.monotonic() < until and not report.get("process_exit_confirmed"):
                        time.sleep(0.02)
                        report = json.loads(path.read_text())
                    if not report.get("process_exit_confirmed"):
                        report = _stop_registered_engine(report)
                reports.append(report)
                if report.get("status") != "closed" or not report.get("process_exit_confirmed"):
                    errors.append("private_engine_process_exit_unconfirmed")
                errors.extend(report.get("cleanup_errors", []))
            except Exception as exc:
                errors.append("engine_report_read: " + type(exc).__name__)
        for path in [*self.control.glob("chain-audit-*.json"), *self.control.glob("chain-failure-*.json")]:
            try:
                envelope = json.loads(path.read_text())
                self.recover(envelope)
            except Exception as exc:
                errors.append("failure_ledger_read: " + type(exc).__name__)
        if self._started and self.expected and not reports:
            workers = []
            for path in self.control.glob("chain-worker-*.json"):
                try:
                    workers.append(json.loads(path.read_text()))
                except Exception as exc:
                    errors.append("worker_report_read: " + type(exc).__name__)
            if not workers or any(worker["engine_allocation_started"] for worker in workers):
                errors.append("engine_process_cleanup_unconfirmed")
        self.last_close_report = dict(
            status="closed" if not errors else "unknown", cleanup_errors=errors,
            query_completed=self.completed, cleanup_scope="daft_native_iterator_and_private_engine_processes",
            engine_reports=reports, gpu_exit_confirmed=None if self._started and self.expected else True,
            received_occurrences=len(self.received), expected_occurrences=len(self.expected),
            model_evaluations=len(self.calls), application_call_upper_bound=len(self.expected) * len(self.chain.stages),
            received_native_batch_envelopes=len(self._received_batches),
            scheduler_owner="Daft Native " + DAFT_VERSION, native_batch_owner="Daft class batch UDF",
            source_binding="canonical JSON SHA256 per occurrence", layout="fused_record_methods",
            stage_kinds=[stage.kind for stage in self.chain.stages], engine_report_count=len(reports),
            stage_stats_scope="worker cumulative snapshots", use_process=True,
            prepare_batch_rows_applied=False, max_active_batches_applied=False,
            unused_configuration_reason="one fused Native class prepares its model batch; Daft owns buffering",
            cleanup_artifacts=str(self.control) if errors else None)
        if not errors:
            shutil.rmtree(self.control)
        return self.last_close_report


def _stop_registered_engine(report):
    from src.experiments.lotus_records_supervisor import _live, _observe, _signal, _wait

    pid, created = report["model_pid"], report["process_created"]
    owned = {tuple(item) for item in report.get("owned_processes", [(pid, created)])}
    _observe(owned, pid, created)
    for signum in (signal.SIGTERM, signal.SIGKILL):
        if not _live(owned):
            break
        _signal(owned, pid, created, signum)
        _wait(owned, pid, created, time.monotonic() + 0.5)
    remaining = _live(owned)
    return dict(report, status="unknown", process_exit_confirmed=not remaining,
                live_observed_processes=len(remaining), gpu_exit_confirmed=None,
                cleanup_errors=["worker_cleanup_unconfirmed_driver_supervised_os_exit"])


def evaluate(frame, chain, engine: EngineSpec, resources: RecordsResources, *,
             expected, query_id, control_dir=None, cancelled=None, runner_threads=None, columns=None):
    """Execute once, under an outer query-process supervisor for a hard deadline.

    The lazy input must contain exactly one string record_json column; each row
    encodes RecordOccurrence.describe(). Final relation assembly follows verified
    EOF. Source values, worker prompts and per-stage call results stay distinct.
    Only one local deployment is supported. Explicit relation_barriers plans
    complete the whole Extract relation between native graphs; each nonempty
    segment initializes a separate model. EngineSpec.max_model_calls is per
    engine. This adapter does not provide Join, branches or global aggregation.
    """
    resources = RecordsResources(**asdict(resources))
    if resources.cpu_workers != 1:
        raise ValueError("the fused Daft chain supports cpu_workers=1 only")
    if runner_threads is not None and (type(runner_threads) is not int or runner_threads < 1):
        raise ValueError("runner_threads must be a positive integer or None")
    deadline = time.monotonic() + resources.query_timeout_s
    engine = EngineSpec.from_dict(asdict(engine))
    chain = LotusRecordChain.from_dict(chain.describe())
    if len({stage.method.model_name for stage in chain.stages}) != 1:
        raise ValueError("the fused Daft chain requires one model deployment across all stages")
    if chain.query_id != query_id:
        raise ValueError("chain and query_id must identify the same query")
    expected = tuple(RecordOccurrence.from_dict(record.describe()) for record in expected)
    for name in ("row_id", "source_position"):
        if len({getattr(record, name) for record in expected}) != len(expected):
            raise ValueError("streaming chain requires globally unique " + name)
    if chain.relation_barriers or columns is not None:
        columns = _ordered_columns(sorted(expected, key=lambda row: row.source_position), columns)
    if any(stage.generation.max_tokens > engine.max_model_len for stage in chain.stages):
        raise ValueError("chain max_tokens exceeds max_model_len")
    engine.validate_driver_environment()
    if cancelled is not None and cancelled():
        raise InterruptedError("Daft LOTUS chain cancelled before graph allocation")
    import daft

    if daft.__version__ != DAFT_VERSION:
        raise RuntimeError("chain requires exactly Daft Native " + DAFT_VERSION)
    if frame.column_names != ["record_json"] or frame.schema()["record_json"].dtype != daft.DataType.string():
        raise ValueError("Daft chain input requires exactly one string record_json column")
    control = Path(tempfile.mkdtemp(prefix="daft-lotus-chain-", dir=control_dir))
    try:
        from src.baselines.common.private_artifacts import require_outside_git

        require_outside_git(control)
        os.chmod(control, 0o700)
        token = uuid.uuid4().hex
        (control / "owner").write_text(token)
        _check(deadline, control)
        if chain.relation_barriers:
            from .lotus_chain_native_relation import NativeExtractRelationRows

            def open_phase(index, segment, records, *, initial_parents, columns, allow_nulls):
                phase_control = control / ("phase-" + str(index))
                phase_control.mkdir(mode=0o700)
                phase_token = uuid.uuid4().hex
                (phase_control / "owner").write_text(phase_token)
                if index == 0:
                    source = frame
                elif records:
                    source = daft.from_pylist([record_row(record) for record in records])
                else:
                    import pyarrow as pa
                    source = daft.from_arrow(pa.table({"record_json": pa.array([], type=pa.string())}))
                graph = _graph(source, segment, engine, resources, records, phase_control,
                    deadline, phase_token, runner_threads=runner_threads,
                    initial_parents=initial_parents, columns=columns, allow_nulls=allow_nulls)
                return _DaftChainRows(graph, records, segment, deadline, cancelled, phase_control,
                    initial_parents=initial_parents, columns=columns)

            return NativeExtractRelationRows(chain, expected, open_phase, control, deadline,
                cancelled, scheduler_owner="Daft Native " + DAFT_VERSION, columns=columns)
        graph = _graph(frame, chain, engine, resources, expected, control, deadline, token,
                       runner_threads=runner_threads, columns=columns)
        return _DaftChainRows(graph, expected, chain, deadline, cancelled, control, columns=columns)
    except BaseException:
        shutil.rmtree(control)
        raise
