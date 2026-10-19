"""A fused finite LOTUS chain on a caller-owned Ray Data 2.56.1 Dataset.

Input rows contain only ``record_json`` (RecordOccurrence.describe() as JSON).
Ray owns batching and its single GPU actor. That actor prepares, generates and
parses each dependent stage; the consumer only checks and collects compact
results. The private model interpreter remains separate from LOTUS.
"""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import tempfile
import threading
import time
import uuid

from src.semantic_methods.lotus.record_chain import (
    LotusRecordChain, RecordOccurrence, stage_identity,
)
from src.semantic_methods.lotus.records import RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, IDENTITY_FIELDS


from .lotus_chain_batch import FusedChainBatch
from .lotus_chain_results import (
    ChainBatchResults, _json, _metadata, _source_binding, record_row,
)


RAY_VERSION = "2.56.1"
CODE_ROOT = Path(__file__).resolve().parents[4]


class RayChainExecutionError(RuntimeError):
    """A remote method failure; model call statuses remain in ``chain_calls``."""

    def __init__(self, remote_error_type):
        self.remote_error_type = remote_error_type
        super().__init__("Ray LOTUS chain failed: " + remote_error_type)


def _check(deadline, control):
    if (Path(control) / "stop").exists():
        raise InterruptedError("Ray LOTUS chain cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("Ray LOTUS chain query deadline expired")


def _empty_source_batch(batch):
    import pyarrow as pa

    if batch.num_rows:
        raise ValueError("Ray graph contains occurrences absent from the empty source declaration")
    return pa.Table.from_pylist([], schema=pa.schema([("batch_json", pa.string())]))


class _RayChainRows(ChainBatchResults):
    _backend_name = "Ray"

    def _execution_error(self, remote_error_type):
        return RayChainExecutionError(remote_error_type)

    def __init__(self, result, expected, chain, deadline, cancelled, control):
        super().__init__(expected, chain)
        self.result = result
        self.deadline, self.cancelled, self.control = deadline, cancelled, Path(control)
        self._iterator = None
        self._done = self._started = self._executor_closed = False
        self.completed = False
        self._stop = threading.Event()
        self._watchdog = self._failure = None
        self.last_close_report = None

    def __iter__(self):
        return self

    def _check(self):
        if self._failure is not None:
            raise self._failure
        if self.cancelled is not None and self.cancelled():
            raise InterruptedError("Ray LOTUS chain cancelled")
        _check(self.deadline, self.control)

    def _watch(self):
        while not self._stop.wait(0.02):
            try:
                self._check()
            except BaseException as exc:
                self._failure = exc
                (self.control / "stop").touch(mode=0o600)
                executor = getattr(self.result, "_current_executor", None)
                if executor is not None:
                    try:
                        executor.shutdown(force=True)
                        self._executor_closed = True
                    except BaseException as cleanup:
                        exc.add_note("Ray shutdown: " + type(cleanup).__name__)
                return


    def __next__(self):
        if self._done:
            raise StopIteration
        try:
            if self._iterator is None:
                self._check()
                self._started = self.result is not None
                self._watchdog = threading.Thread(target=self._watch, daemon=True,
                                                  name="ray-lotus-chain-stop")
                self._watchdog.start()
                self._iterator = iter(()) if self.result is None else iter(self.result.iter_rows())
            while not self._pending:
                self._accept(json.loads(next(self._iterator)["batch_json"]))
            return self._pending.popleft()
        except StopIteration:
            try:
                self._check()
                if self.received != set(self.expected):
                    raise ValueError("Ray graph omitted expected input occurrences")
                report = self.close()
                if report["status"] != "closed":
                    raise RuntimeError("Ray chain cleanup remains unconfirmed")
                self.completed = True
                report["query_completed"] = True
                self.columns = list(dict.fromkeys(self._base_columns + [
                    name for position in sorted(self._derived_columns) for name in self._derived_columns[position]]))
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
                error.add_note("Ray chain cleanup: " + _json(report))
        except BaseException as cleanup:
            error.add_note("Ray chain cleanup also failed: " + type(cleanup).__name__)
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
        watchdog_stopped = self._watchdog is None or not self._watchdog.is_alive()
        if not self._executor_closed and watchdog_stopped:
            try:
                executor = getattr(self.result, "_current_executor", None)
                if executor is not None:
                    executor.shutdown(force=True)
                self._executor_closed = True
            except BaseException as exc:
                errors.append("executor_shutdown: " + type(exc).__name__)
        reports = []
        for path in self.control.glob("engine-*.json"):
            try:
                report = json.loads(path.read_text())
                if not report.get("process_exit_confirmed") and report.get("model_pid"):
                    until = time.monotonic() + 5.5
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
                failure = json.loads(path.read_text())
                self._record_calls(failure["calls"], recovering=True)
                if "stage_stats" in failure and failure["stage_stats"] not in self.batch_stage_stats:
                    self.batch_stage_stats.append(failure["stage_stats"])
                self._recover_raw(failure, priority=2 if "error_type" in failure else 1)
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
            query_completed=self.completed,
            cleanup_scope="ray_data_graph_and_local_private_engine_processes",
            executor_shutdown_confirmed=self._executor_closed, engine_reports=reports,
            gpu_exit_confirmed=None if self._started and self.expected else True,
            received_occurrences=len(self.received), expected_occurrences=len(self.expected),
            model_evaluations=len(self.calls), scheduler_owner="Ray Data 2.56.1",
            unassociated_result_count=len(self.unassociated_results),
            application_call_upper_bound=len(self.expected) * len(self.chain.stages),
            received_native_batch_envelopes=len(self._received_batches),
            source_binding="canonical JSON SHA256 per occurrence",
            layout="fused_record_methods", stage_kinds=[stage.kind for stage in self.chain.stages],
            engine_report_count=len(reports),
            native_batch_owner="Ray Data map_batches", stage_stats_scope="worker cumulative snapshots",
            prepare_batch_rows_applied=False, cpu_workers_applied=False,
            unused_configuration_reason="fused single actor prepares its native model batch",
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


def _graph(dataset, chain, engine, resources, expected, control, deadline, control_token,
           *, batch_type=None, model_gpus=1):
    """CPU library tests inject fixtures; public evaluation fixes the real kernel."""
    import ray.data

    if not expected:
        return dataset.map_batches(_empty_source_batch, batch_size=resources.model_batch_rows,
                                   batch_format="pyarrow", max_retries=0, num_cpus=1)
    return dataset.map_batches(
        batch_type or FusedChainBatch, batch_size=resources.model_batch_rows, batch_format="pyarrow",
        compute=ray.data.ActorPoolStrategy(size=1,
            max_tasks_in_flight_per_actor=resources.max_active_batches),
        fn_constructor_kwargs=dict(chain=chain.describe(), engine=asdict(engine),
            expected=[_source_binding(record) for record in expected], control=str(control),
            deadline=deadline, startup_timeout_s=resources.startup_timeout_s,
            driver_host=socket.gethostname(), control_token=control_token),
        num_cpus=1, num_gpus=model_gpus, max_concurrency=1, max_restarts=0, max_task_retries=0,
        runtime_env={"py_executable": shlex.quote(engine.driver_python), "env_vars": {
            "PYTHONPATH": str(CODE_ROOT), "LITELLM_LOCAL_MODEL_COST_MAP": "True",
            "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1"}})


def evaluate(dataset, chain, engine: EngineSpec, resources: RecordsResources, *,
             expected, query_id, control_dir=None, cancelled=None):
    """Build one native graph, then consume it once with identity-only collection.

    ``expected`` contains source occurrence metadata, never prepared prompts.
    Only a single local Ray node and a shared local control directory are
    supported. Physical GPU release still needs independent GPU qualification;
    a query-process supervisor is required for uninterruptible native startup.
    Assemble the final relation after successful EOF (``stream.completed``).
    Remote method exceptions use RayChainExecutionError.remote_error_type;
    succeeded model calls remain succeeded even when their parser fails.
    This fused layout requires cpu_workers=1 and uses model_batch_rows as Ray's
    input batch size; prepare_batch_rows has no separate preparation stage.
    The returned iterator is single-pass: a second iteration is empty EOF.
    Re-reading collected records does not run models; use a new evaluate call
    with a distinct query_id for independent work. Actions on the private native Dataset
    are unsupported, and this path does not use LOTUS LazyFrame content caching.
    """
    resources = RecordsResources(**asdict(resources))
    if resources.cpu_workers != 1:
        raise ValueError("the fused Ray chain supports cpu_workers=1 only")
    deadline = time.monotonic() + resources.query_timeout_s
    engine = EngineSpec.from_dict(asdict(engine))
    chain = LotusRecordChain.from_dict(chain.describe())
    if chain.relation_barriers:
        raise ValueError("Extract relation barriers require the explicit Core profile")
    if len({stage.method.model_name for stage in chain.stages}) != 1:
        raise ValueError("this fused Ray chain requires one model deployment across all stages")
    if chain.query_id != query_id:
        raise ValueError("chain and query_id must identify the same query")
    expected = tuple(RecordOccurrence.from_dict(record.describe()) for record in expected)
    for name in ("row_id", "source_position"):
        if len({getattr(record, name) for record in expected}) != len(expected):
            raise ValueError("streaming chain requires globally unique " + name)
    if any(stage.generation.max_tokens > engine.max_model_len for stage in chain.stages):
        raise ValueError("chain max_tokens exceeds max_model_len")
    engine.validate_driver_environment()
    if cancelled is not None and cancelled():
        raise InterruptedError("Ray LOTUS chain cancelled before graph allocation")
    import ray
    import ray.data

    if ray.__version__ != RAY_VERSION or not ray.is_initialized():
        raise RuntimeError("chain requires caller-owned Ray Data " + RAY_VERSION)
    if len([node for node in ray.nodes() if node["Alive"]]) != 1:
        raise ValueError("Ray LOTUS chain requires a single local Ray node")
    control = Path(tempfile.mkdtemp(prefix="ray-lotus-chain-", dir=control_dir))
    try:
        from src.baselines.common.private_artifacts import require_outside_git

        require_outside_git(control)
        os.chmod(control, 0o700)
        token = uuid.uuid4().hex
        (control / "owner").write_text(token)
        _check(deadline, control)
        result = _graph(
            dataset, chain, engine, resources, expected, control, deadline, token)
        return _RayChainRows(result, expected, chain, deadline, cancelled, control)
    except BaseException:
        shutil.rmtree(control)
        raise
