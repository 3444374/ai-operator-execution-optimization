"""A fused finite LOTUS chain on a caller-owned Ray Data 2.56.1 Dataset.

Input rows contain only ``record_json`` (RecordOccurrence.describe() as JSON).
Ray owns batching and its single GPU actor. That actor prepares, generates and
parses each dependent stage; the consumer only checks and collects compact
results. The private model interpreter remains separate from LOTUS.
"""

from __future__ import annotations

from collections import deque
from copy import deepcopy
from dataclasses import asdict
import hashlib
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


RAY_VERSION = "2.56.1"
CODE_ROOT = Path(__file__).resolve().parents[4]


class RayChainExecutionError(RuntimeError):
    """A remote method failure; model call statuses remain in ``chain_calls``."""

    def __init__(self, remote_error_type):
        self.remote_error_type = remote_error_type
        super().__init__("Ray LOTUS chain failed: " + remote_error_type)


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def record_row(record):
    """Encode source values, without constructing prompts or model requests."""
    return {"record_json": _json(record.describe())}


def _metadata(record):
    return {key: deepcopy(getattr(record, key))
            for key in ("row_id", "source_position", "source_index", "source_refs")}


def _source_binding(record):
    canonical = json.dumps(json.loads(_json(record.describe())), ensure_ascii=False,
                           sort_keys=True, separators=(",", ":"), allow_nan=False)
    return dict(_metadata(record), record_sha256=hashlib.sha256(canonical.encode("utf-8")).hexdigest())


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


class FusedChainBatch:
    """One native actor, with worker-local methods and one private model process."""

    _engine_type = None

    def __init__(self, chain, engine, expected, control, deadline, startup_timeout_s,
                 driver_host, control_token):
        from .daft_engine_process import _publish

        if socket.gethostname() != driver_host:
            raise RuntimeError("Ray LOTUS chain supports a single local host only")
        self.control = Path(control)
        if (self.control / "owner").read_text() != control_token:
            raise RuntimeError("Ray LOTUS chain requires a shared local control directory")
        _check(deadline, self.control)
        self.chain = LotusRecordChain.from_dict(chain)
        if len({stage.method.model_name for stage in self.chain.stages}) != 1:
            raise ValueError("this fused Ray chain requires one model deployment across all stages")
        self.engine_spec = EngineSpec.from_dict(engine)
        if any(stage.generation.max_tokens > self.engine_spec.max_model_len for stage in self.chain.stages):
            raise ValueError("chain max_tokens exceeds max_model_len")
        self.expected = {row["row_id"]: row for row in expected}
        self.received = set()
        self.deadline, self.startup_timeout_s = deadline, startup_timeout_s
        self.engine = None
        self.failed = False
        self.worker_report = self.control / ("chain-worker-" + uuid.uuid4().hex + ".json")
        _publish(self.worker_report, dict(engine_allocation_started=False, worker_pid=os.getpid()))

    def _infer(self, prepared, *, generation):
        from .daft_engine_process import _publish
        from .lotus_chain_engine_process import ChainEngineProcess

        _check(self.deadline, self.control)
        prior = {call["row_id"]: call["call_id"] for call in self._audit_calls}
        declared = [dict({key: row[key] for key in IDENTITY_FIELDS}, status="unknown",
                         parent_call_ids=[] if row["row_id"] not in prior else [prior[row["row_id"]]],
                         submission_confirmed=False, error_type="NativeTaskUnconfirmed",
                         prompt_tokens=None, completion_tokens=None, total_tokens=None)
                    for row in prepared]
        self._audit_calls.extend(declared)
        self._raw_stage_id = prepared[0]["algorithm_stage"]
        audit = dict(batch_id=self._batch_id, raw_stage_id=self._raw_stage_id, calls=self._audit_calls)
        _publish(self._audit_path, audit)
        if self.engine is None:
            _publish(self.worker_report, dict(engine_allocation_started=True, worker_pid=os.getpid()))
            self.engine = (self._engine_type or ChainEngineProcess)(
                self.engine_spec, generation, self.control, self.deadline,
                self.startup_timeout_s)
        results = self.engine.run_prepared(prepared, generation=generation)
        for declaration in declared:
            matches = [row for row in results if type(row) is dict and all(
                row.get(key) == declaration[key] for key in IDENTITY_FIELDS)]
            if len(matches) == 1:
                parents = declaration["parent_call_ids"]
                declaration.clear()
                declaration.update(deepcopy(matches[0]), parent_call_ids=parents)
        raw = [row for row in results if type(row) is not dict or sum(
            type(other) is dict and all(other.get(key) == row.get(key) for key in IDENTITY_FIELDS)
            for other in results) != 1 or not any(
                all(row.get(key) == item[key] for key in IDENTITY_FIELDS) for item in declared)]
        try:
            _publish(self._audit_path, dict(audit, raw_results=raw))
        except BaseException as exc:
            self._audit_errors.append("call_ledger_write: " + type(exc).__name__)
        return results

    def __call__(self, batch):
        import pyarrow as pa
        from .daft_engine_process import _publish

        if self.failed:
            # Ray may already have queued another batch before the consumer
            # sees the first error. Preserve that error without another call.
            return pa.Table.from_pylist([{"batch_json": self._failure_json}])
        self._audit_calls, self._audit_errors = [], []
        self._batch_id, self._raw_stage_id = uuid.uuid4().hex, None
        self._audit_path = self.control / ("chain-audit-" + self._batch_id + ".json")
        records = []
        envelope = dict(inputs=[],
                        batch_id=self._batch_id, raw_stage_id=None,
                        records=[], calls=[], stage_stats={}, columns=[], raw_results=[], error_type=None,
                        native_input_rows=0, worker_pid=os.getpid(), cleanup_errors=[])
        try:
            _check(self.deadline, self.control)
            batch_ids = set()
            for row in batch.to_pylist():
                if set(row) != {"record_json"} or type(row["record_json"]) is not str:
                    raise ValueError("Ray chain input requires one record_json string column")
                record = RecordOccurrence.from_dict(json.loads(row["record_json"]))
                if (self.expected.get(record.row_id) != _source_binding(record)
                        or record.row_id in self.received or record.row_id in batch_ids):
                    raise ValueError("Ray chain input changed or duplicated an occurrence")
                batch_ids.add(record.row_id)
                records.append(record)
            self.received.update(batch_ids)
            envelope.update(inputs=[_source_binding(record) for record in records], native_input_rows=len(records))
            result = self.chain.run_batch(records, self._infer)
            envelope.update(records=[record.describe() for record in result["records"]],
                            calls=result["calls"], stage_stats=result["stage_stats"],
                            columns=result["columns"])
            _check(self.deadline, self.control)
        except BaseException as exc:
            self.failed = True
            # The method attaches calls before parsing, including known usage
            # when a later parse or dependent generation fails.
            envelope.update(calls=getattr(exc, "chain_calls", envelope["calls"]),
                            stage_stats=getattr(exc, "chain_stage_stats", envelope["stage_stats"]),
                            raw_results=getattr(exc, "chain_raw_results", []),
                            records=[], error_type=type(exc).__name__)
            envelope["raw_stage_id"] = self._raw_stage_id
            try:
                _publish(self.control / ("chain-failure-" + self._batch_id + ".json"), envelope)
            except BaseException as recording:
                envelope["cleanup_errors"].append("failure_ledger_write: " + type(recording).__name__)
        finally:
            envelope["cleanup_errors"].extend(self._audit_errors)
            if self.engine is not None and (self.failed or len(self.received) == len(self.expected)):
                try:
                    envelope["engine_close"] = self.engine.close()
                except BaseException as exc:
                    envelope["cleanup_errors"].append("engine_close: " + type(exc).__name__)
                    try:
                        _publish(self.control / "engine-close-failure.json", dict(
                            status="unknown", process_exit_confirmed=False,
                            cleanup_errors=envelope["cleanup_errors"]))
                    except BaseException:
                        pass
        # Even a wholly discarded batch returns its preceding model-call ledger.
        encoded = _json(envelope)
        if self.failed:
            self._failure_json = encoded
        return pa.Table.from_pylist([{"batch_json": encoded}])


class _RayChainRows:
    def __init__(self, result, expected, chain, deadline, cancelled, control):
        self.result, self.chain = result, chain
        self.expected = {record.row_id: _metadata(record) for record in expected}
        self.bindings = {record.row_id: _source_binding(record) for record in expected}
        self.deadline, self.cancelled, self.control = deadline, cancelled, Path(control)
        self.received = set()
        self.calls, self.batch_stage_stats, self.unassociated_results = [], [], []
        self.raw_result_batches, self._raw_priorities, self._received_batches = {}, {}, set()
        self.columns = list(dict.fromkeys(
            [key for record in sorted(expected, key=lambda row: row.source_position) for key in record.values]
            + [stage.method.output_column for stage in chain.stages if stage.kind == "map"]))
        self._base_columns, self._derived_columns = list(self.columns), {}
        self._call_records, self._pending = {}, deque()
        self._iterator = None
        self._done = self._started = self._executor_closed = False
        self.completed = False
        self._stop = threading.Event()
        self._watchdog = self._failure = None
        self._cleanup_errors = []
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

    def _record_calls(self, calls, *, recovering=False):
        stages = {stage.stage_id: stage for stage in self.chain.stages}
        for call in calls:
            expected = self.expected.get(call.get("row_id"))
            if (expected is None or any(key not in call for key in IDENTITY_FIELDS)
                    or call["source_position"] != expected["source_position"]
                    or call["query_id"] != self.chain.query_id
                    or call["algorithm_stage"] not in stages
                    or call.get("status") not in {"succeeded", "failed", "unknown"}):
                raise ValueError("Ray chain returned an undeclared or invalid model call")
            canonical = stage_identity(
                self.chain.query_id, RecordOccurrence.from_dict({**expected, "values": {}}),
                call["algorithm_stage"], model_role=stages[call["algorithm_stage"]].model_role)
            if any(call[name] != canonical[name] for name in IDENTITY_FIELDS):
                raise ValueError("Ray chain changed a model evaluation identity")
            if call["call_id"] in self._call_records:
                if recovering:
                    from src.baselines.common.redact import redact_json_values

                    previous = self._call_records[call["call_id"]]
                    if call.get("error_type") == "NativeTaskUnconfirmed":
                        continue
                    if previous.get("error_type") == "NativeTaskUnconfirmed":
                        previous.update(call)
                        continue
                    comparable = redact_json_values(previous)
                    for key in ("status", "text", "submission_confirmed", "prompt_tokens",
                                "completion_tokens", "total_tokens", "engine_uid", "engine_request_id",
                                "finish_reason", "parent_call_ids"):
                        if comparable.get(key) is not None and call.get(key) is not None and comparable[key] != call[key]:
                            raise ValueError("conflicting recovered model response for " + call["call_id"])
                    continue
                raise ValueError("Ray chain duplicated a model evaluation")
            self._call_records[call["call_id"]] = call
            self.calls.append(call)

    def _accept(self, envelope):
        batch_id = envelope["batch_id"]
        if type(batch_id) is not str or not batch_id or batch_id in self._received_batches:
            raise ValueError("Ray graph returned a missing or duplicate native batch identity")
        inputs = envelope["inputs"]
        batch_ids = set()
        for item in inputs:
            row_id = item["row_id"]
            if (self.bindings.get(row_id) != item or row_id in self.received or row_id in batch_ids):
                raise ValueError("Ray graph changed or duplicated an input occurrence")
            batch_ids.add(row_id)
        self.received.update(batch_ids)
        self._received_batches.add(batch_id)
        if any(call.get("row_id") not in batch_ids for call in envelope["calls"]):
            raise ValueError("Ray chain call belongs to a different native batch")
        self._record_calls(envelope["calls"])
        self._cleanup_errors.extend(envelope.get("cleanup_errors", []))
        self.batch_stage_stats.append(envelope["stage_stats"])
        self._recover_raw(envelope, priority=2)
        for column in envelope["columns"]:
            if column not in self.columns:
                self.columns.append(column)
        if envelope.get("error_type"):
            error = RayChainExecutionError(envelope["error_type"])
            error.chain_calls = list(self.calls)
            error.chain_stage_stats = list(self.batch_stage_stats)
            raise error
        rows = [RecordOccurrence.from_dict(row) for row in envelope["records"]]
        final_ids = [record.row_id for record in rows]
        previous = {}
        expected_stage = batch_ids
        for stage in self.chain.stages:
            stage_calls = [call for call in envelope["calls"]
                           if call["algorithm_stage"] == stage.stage_id]
            if (len(stage_calls) != len(expected_stage)
                    or {call["row_id"] for call in stage_calls} != expected_stage
                    or any(call["status"] != "succeeded" for call in stage_calls)):
                raise ValueError("Ray chain omitted or repeated a dependent stage evaluation")
            for call in stage_calls:
                if call.get("parent_call_ids") != previous.get(call["row_id"], []):
                    raise ValueError("Ray chain changed a parent evaluation association")
            previous = {call["row_id"]: [call["call_id"]] for call in stage_calls}
            if stage.kind == "filter":
                if any(type(call.get("decision")) is not bool for call in stage_calls):
                    raise ValueError("Ray chain lacks Filter parser decisions")
                expected_stage = {call["row_id"] for call in stage_calls if call["decision"]}
        if len(set(final_ids)) != len(final_ids) or set(final_ids) != expected_stage:
            raise ValueError("Ray chain final rows differ from terminal stage occurrences")
        for record in rows:
            if record.row_id not in batch_ids or _metadata(record) != self.expected[record.row_id]:
                raise ValueError("Ray chain changed a final occurrence identity")
            self._derived_columns[record.source_position] = [key for key in record.values if key not in self._base_columns]
        self._pending.extend(rows)

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

    def _recover_raw(self, record, *, priority):
        if not record.get("raw_results"):
            return
        key = (record["batch_id"], record["raw_stage_id"])
        previous_priority = self._raw_priorities.get(key, 0)
        if priority == previous_priority:
            from src.baselines.common.redact import redact_json_values

            if redact_json_values(self.raw_result_batches[key]) != redact_json_values(record["raw_results"]):
                raise ValueError("conflicting recovered raw response snapshot")
        if priority > previous_priority:
            self.raw_result_batches[key] = record["raw_results"]
            self._raw_priorities[key] = priority
        self.unassociated_results = []
        for batch in self.raw_result_batches.values():
            for row in batch:
                matches = [] if type(row) is not dict else [call for call in self.calls if all(
                    row.get(name) == call[name] for name in IDENTITY_FIELDS)]
                duplicates = 0 if type(row) is not dict else sum(
                    type(other) is dict and all(other.get(name) == row.get(name) for name in IDENTITY_FIELDS)
                    for other in batch)
                if len(matches) != 1 or duplicates != 1:
                    self.unassociated_results.append(row)

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
