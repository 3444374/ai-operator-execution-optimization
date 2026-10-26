"""Original global helper cascade, with independent Ray Data records stages.

Ray Data owns scan, selection, duplicate draws, CPU preparation, GPU callable
actors and physical batching. Stages run serially to support different models on
one GPU. The main model may be loaded separately for sampling and final review;
all scans, initialization, IPC and shutdown belong to this configuration's cost.
"""

from copy import deepcopy
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import tempfile
import time

from src.semantic_methods.lotus.cascade import (
    CascadePreparer, CascadeStage, IDENTITY_FIELDS, _StageCalls, local_generation,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec

from .ray_data_lotus_records import _RayRows, _CLOSE_COLUMN, _check_deadline, _runtime_env, _table
from .lotus_cascade_process import StageEngineCleanup


def _selected(row, *, selected, input_columns):
    """A repeated sample creates separate logical calls before any native batching."""
    calls = selected.get((row["row_id"], int(row["source_position"])), ())
    for call in calls:
        if any(row[name] != call[name] or type(row[name]) is not str for name in input_columns):
            raise ValueError("cascade source text differs from its checked occurrence")
        yield dict(call)


class PrepareCascade:
    def __init__(self, stage, deadline):
        _check_deadline(deadline)
        self.preparer = CascadePreparer(CascadeStage(**stage))
        self.deadline = deadline

    def __call__(self, batch):
        _check_deadline(self.deadline)
        rows = self.preparer.prepare_rows(batch.to_pylist())
        _check_deadline(self.deadline)
        for row in rows:
            row["prepare_worker_pid"] = os.getpid()
        return _table(rows)


class InferCascade:
    """Ray Data's GPU callable owns a private, supervised local engine process."""

    def __init__(self, engine, generation, expected_count, deadline, startup_timeout_s, control):
        from .daft_engine_process import EngineProcess
        from src.serving.backends.vllm_engine import GenerationSpec

        _check_deadline(deadline)
        self.deadline, self.expected_count = deadline, expected_count
        self.completed = 0
        self.kernel = None
        self.control = Path(control)
        # This transport creates no shared pool, policy, physical queue or model
        # scheduler. Ray Data owns the callable and its single-GPU placement.
        self.kernel = EngineProcess(
            EngineSpec.from_dict(engine), GenerationSpec.from_dict(generation),
            self.control, deadline, startup_timeout_s,
        )

    def __call__(self, batch):
        inputs = batch.to_pylist()
        try:
            _check_deadline(self.deadline)
            rows = self.kernel.run_prepared(inputs)
        except BaseException as error:
            try:
                report = self.kernel.close()
                error.add_note("cascade engine close: " + json.dumps(report, sort_keys=True))
            except BaseException as cleanup:
                error.add_note("cascade engine close failed: " + type(cleanup).__name__)
            raise
        for row, prepared in zip(rows, inputs, strict=True):
            row.update(prepare_worker_pid=prepared["prepare_worker_pid"],
                       infer_worker_pid=os.getpid(), native_infer_batch_rows=len(inputs))
            row[_CLOSE_COLUMN] = ""
        self.completed += len(rows)
        if self.completed >= self.expected_count and rows:
            try:
                report = self.kernel.close()
            except BaseException as error:
                report = dict(status="unknown", process_exit_confirmed=False,
                              cleanup_errors=[type(error).__name__])
            report["engine_api_shutdown_confirmed"] = bool(
                (report.get("engine_api") or {}).get("engine_api_shutdown_confirmed")
            )
            # Preserve the report in its owning external temporary directory.
            # Physical GPU exit remains unavailable until independent observation.
            report["control_dir"] = str(self.control)
            rows[-1][_CLOSE_COLUMN] = json.dumps(report, sort_keys=True)
        return _table(rows)


def _stage_graph(source, stage, resources, deadline, *, scan, prepare_python,
                 infer_class, infer_kwargs, infer_python, num_gpus):
    """Shared graph construction; CPU qualification supplies a named model fixture."""
    import pyarrow as pa
    from pyarrow import csv
    import ray
    import ray.data

    if ray.__version__ != "2.56.1" or not ray.is_initialized():
        raise RuntimeError("cascade records graph requires caller-owned Ray 2.56.1")
    if (not isinstance(scan, RecordScan)
            or not set(stage.input_columns) <= set(scan.payload_columns)):
        raise ValueError("cascade scan does not cover this stage's input columns")
    scan.describe()
    _check_deadline(deadline)
    selected = {}
    for row in stage.records:
        selected.setdefault((row["row_id"], row["source_position"]), []).append(dict(row))
    types = {"string": pa.string(), "int64": pa.int64()}
    dataset = ray.data.read_csv(
        str(source.csv_path), ray_remote_args={"max_retries": 0},
        convert_options=csv.ConvertOptions(column_types={
            name: types[dtype] for name, dtype in scan.csv_schema
        }, strings_can_be_null=False, null_values=[]),
    )
    if scan.predicate is not None:
        column, value = scan.predicate
        dataset = dataset.filter(lambda row: row[column] == value, max_retries=0)
    dataset = dataset.select_columns(["row_id", "source_position", *scan.payload_columns])
    dataset = dataset.flat_map(_selected, fn_kwargs={
        "selected": selected, "input_columns": stage.input_columns,
    }, max_retries=0)
    # Do not serialize the entire source into every formatter instance.
    method = asdict(replace(stage, records=()))
    dataset = dataset.map_batches(
        PrepareCascade, batch_size=resources.prepare_batch_rows, batch_format="pyarrow",
        compute=ray.data.ActorPoolStrategy(size=resources.cpu_workers,
            max_tasks_in_flight_per_actor=resources.max_active_batches),
        fn_constructor_kwargs={"stage": method, "deadline": deadline},
        num_cpus=1, max_concurrency=1, max_restarts=0, max_task_retries=0,
        runtime_env=_runtime_env(prepare_python),
    )
    return dataset.map_batches(
        infer_class, batch_size=resources.model_batch_rows, batch_format="pyarrow",
        compute=ray.data.ActorPoolStrategy(size=1,
            max_tasks_in_flight_per_actor=resources.max_active_batches),
        fn_constructor_kwargs=infer_kwargs, num_cpus=1, num_gpus=num_gpus,
        max_concurrency=1, max_restarts=0, max_task_retries=0,
        runtime_env=_runtime_env(infer_python),
    )


class _CascadeRayRows(_RayRows):
    def __init__(self, *args, control, stage=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine_cleanup = StageEngineCleanup(control)
        self.stage, self._result_receipts = stage, {}

    def _recover_result_receipts(self, report):
        failure = report.get("result_failure")
        if failure is None:
            return []
        if (self.stage is None or type(failure) is not dict
                or type(failure.get("verified_call_results")) is not list):
            return ["result_receipt_recovery: ValueError"]
        errors = []
        ready_uid = (report.get("engine_ready") or {}).get("engine_uid")
        for row in failure["verified_call_results"]:
            try:
                call_id = row.get("call_id") if type(row) is dict else None
                expected = self.expected.get(call_id) if type(call_id) is str else None
                if expected is None or any(row.get(key) != value for key, value in expected.items()):
                    raise ValueError("engine receipt belongs to another cascade occurrence")
                if call_id in self.received:
                    continue
                if (sum(type(other) is dict and other.get("call_id") == call_id
                        for other in failure["verified_call_results"]) != 1
                        or type(row.get("engine_protocol_error")) is not str
                        or not row["engine_protocol_error"].strip()
                        or type(ready_uid) is not str or not ready_uid or row.get("engine_uid") != ready_uid):
                    raise ValueError("engine receipt lacks its cascade result-failure identity")
                audit = _StageCalls(replace(self.stage, records=(expected,)))
                audit.record(row)
                receipt = audit.ordered()[0]
                if call_id in self._result_receipts and self._result_receipts[call_id] != receipt:
                    raise ValueError("engine receipt conflicts with an existing terminal")
                self._result_receipts[call_id] = receipt
            except (ValueError, TypeError, KeyError, AttributeError) as error:
                errors.append("result_receipt_recovery: " + type(error).__name__)
        return errors

    def _watch(self):
        cleanup_done = False
        while not self._stop.wait(0.02):
            try:
                self._check()
            except BaseException as error:
                self._failure = error
                if not cleanup_done:
                    self.engine_cleanup.stop(started=self._started)
                    cleanup_done = True
                executor = getattr(self.result, "_current_executor", None)
                if executor is None:
                    continue
                try:
                    executor.shutdown(force=True)
                    self._executor_closed = True
                except BaseException as cleanup:
                    self._cleanup_errors.append(type(cleanup).__name__)
                return

    def close(self):
        # An actor's model child owns a separate OS session. Stop and supervise
        # that registered session before Ray is allowed to terminate its actor.
        self.engine_cleanup.request_stop()
        self._stop.set()
        if self._watchdog is not None:
            self._watchdog.join(timeout=7.5)
        cleanup = self.engine_cleanup.stop(started=self._started)
        initializing = self._started and self.iterator is None and getattr(
            self.result, "_current_executor", None) is None
        error = None
        try:
            report = super().close()
        except BaseException as primary:
            error = primary
            report = dict(status="unknown", cleanup_errors=["graph_close: " + type(primary).__name__])
        report["registered_engine_cleanup"] = cleanup
        report["cleanup_errors"].extend(cleanup["errors"])
        report["process_exit_confirmed"] = cleanup["process_exit_confirmed"]
        if initializing:
            report["executor_shutdown_confirmed"] = False
            report["cleanup_errors"].append("graph_executor_initialization_unconfirmed")
        for engine in [*cleanup["reports"], report.get("engine_close_report") or {}]:
            report["cleanup_errors"].extend(self._recover_result_receipts(engine))
        report["unconsumed_call_results"] = [self._result_receipts[call_id] for call_id in self.expected
            if call_id in self._result_receipts and call_id not in self.received]
        self.last_close_report = report
        if error is not None:
            raise error
        return report


class _StageRows:
    """Completed usage survives cleanup failure; next stage requires process exit."""

    def __init__(self, rows, stage, reports):
        self.rows, self.stage, self.reports = rows, stage, reports
        self.last_close_report = None

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.rows)

    def close(self):
        error = None
        try:
            report = self.rows.close()
        except BaseException as primary:
            error = primary
            report = getattr(self.rows, "last_close_report", None) or dict(
                status="unknown", cleanup_errors=["stage_close: " + type(primary).__name__])
        engine = report.get("engine_close_report") or {}
        settled = (error is None and report.get("iterator_closed") and report.get("executor_shutdown_confirmed")
                   and report.get("watchdog_stopped") and not report.get("cleanup_errors")
                   and engine.get("process_exit_confirmed") is True
                   and engine.get("engine_api_shutdown_confirmed") is True
                   and not engine.get("cleanup_errors"))
        registered = report.get("registered_engine_cleanup")
        if registered is not None:
            settled = settled and registered.get("process_exit_confirmed") is True
        self.last_close_report = dict(report, algorithm_stage=self.stage.name,
                                      model_role=self.stage.model_role,
                                      stage_process_exit_confirmed=bool(settled))
        if self.reports and self.reports[-1].get("algorithm_stage") == self.stage.name:
            self.reports[-1] = deepcopy(self.last_close_report)
        else:
            self.reports.append(deepcopy(self.last_close_report))
        if error is not None:
            raise error
        if not settled:
            raise RuntimeError("cascade stage engine/process cleanup is unconfirmed")
        return self.last_close_report


class RayCascadeBackend:
    """No physical execution is shared with Daft, IMLane, or SemLoom."""

    def __init__(self, source, *, engines, resources, cancelled=None):
        if type(engines) is not dict or set(engines) != {"main", "helper"}:
            raise ValueError("both main and helper engine deployments are required")
        self.engines = {role: EngineSpec.from_dict(asdict(spec)) for role, spec in engines.items()}
        if Path(self.engines["main"].model_path).resolve() == Path(self.engines["helper"].model_path).resolve():
            raise ValueError("this cascade deployment requires distinct explicit model paths")
        self.resources = RecordsResources(**asdict(resources))
        self.source, self.cancelled = source, cancelled
        self.deadline = time.monotonic() + resources.query_timeout_s
        self.close_reports = []
        self._checked = False
        self._methods, self._instructions, self._effective = {}, {}, {}

    def preflight(self, main_lm, helper_lm, effective, *, max_possible_calls, method, helper_method):
        from lotus.nl_expression import nle2str
        from tokenizers import Tokenizer

        if (type(max_possible_calls) is not int or max_possible_calls < 0
                or not isinstance(method, FilterMethod) or not isinstance(helper_method, FilterMethod)
                or not method.resolved_input_columns or not helper_method.resolved_input_columns):
            raise ValueError("cascade requires a finite call bound and bound FilterMethod configurations")
        self.source.preflight(method)
        self.scan = self.source.scan_spec(method)
        required = set(method.resolved_input_columns) | set(helper_method.resolved_input_columns)
        if not isinstance(self.scan, RecordScan) or not required <= set(self.scan.payload_columns):
            raise ValueError("cascade scan must cover main and helper referenced text columns")
        import ray
        self.check_lifecycle()
        if self._checked:
            raise RuntimeError("cascade backend owns one query; create a new backend for another query")
        if ray.__version__ != "2.56.1" or not ray.is_initialized():
            raise RuntimeError("cascade requires caller-owned Ray 2.56.1")
        alive = [node for node in ray.nodes() if node["Alive"]]
        if len(alive) != 1 or alive[0]["NodeID"] != ray.get_runtime_context().get_node_id():
            raise ValueError("first cascade process supervision requires one local Ray node")
        for role, lm in (("main", main_lm), ("helper", helper_lm)):
            spec = self.engines[role]
            binding = FilterMethod.from_dict((method if role == "main" else helper_method).describe())
            if binding.model_name != lm.model or not binding.resolved_input_columns:
                raise ValueError("cascade method must bind its model and input columns")
            spec.validate_driver_environment()
            # The same local tokenizer files must back original LM label checks
            # and the actual engine; a model-name tokenizer guess is insufficient.
            path = Path(spec.tokenizer_path or spec.model_path) / "tokenizer.json"
            tokenizer = Tokenizer.from_file(str(path))
            for token in ("True", "False"):
                expected = tokenizer.encode(token, add_special_tokens=False).ids
                if len(expected) != 1 or lm.encode_text(token) != expected:
                    raise ValueError("original LM and deployed tokenizer label IDs differ")
            dummy = CascadeStage("helper_score" if role == "helper" else "fallback",
                                 role, lm.model, binding.instruction, binding.default,
                                 binding.system_prompt, binding.output_tokens,
                                 effective[role], (), binding.resolved_input_columns)
            if local_generation(dummy).max_tokens > spec.max_model_len:
                raise ValueError("cascade max_tokens exceeds max_model_len")
            self._methods[role] = binding
            self._instructions[role] = nle2str(binding.instruction, list(binding.resolved_input_columns))
        self._effective = deepcopy(effective)
        self._checked = True

    def check_lifecycle(self):
        _check_deadline(self.deadline)
        if self.cancelled is not None and self.cancelled():
            raise InterruptedError("cascade query cancelled")

    def __call__(self, stage):
        if not self._checked:
            raise RuntimeError("cascade local engines require method preflight")
        if self.close_reports and not self.close_reports[-1]["stage_process_exit_confirmed"]:
            raise RuntimeError("previous cascade stage still owns an unconfirmed model process")
        self.check_lifecycle()
        binding = self._methods[stage.model_role]
        if (stage.model_name != binding.model_name or stage.instruction != self._instructions[stage.model_role]
                or tuple(stage.input_columns) != binding.resolved_input_columns
                or stage.default != binding.default or stage.system_prompt != binding.system_prompt
                or stage.output_tokens != binding.output_tokens
                or stage.effective_kwargs != self._effective[stage.model_role]):
            raise ValueError("cascade stage differs from its preflight method binding")
        spec = self.engines[stage.model_role]
        generation = local_generation(stage)
        calls = [{key: row[key] for key in IDENTITY_FIELDS} for row in stage.records]
        if not calls:
            return iter(())
        allowed = {(row["row_id"], row["source_position"])
                   for row in self.source.expected(calls[0]["query_id"])}
        if any((row["row_id"], row["source_position"]) not in allowed for row in calls):
            raise ValueError("cascade stage includes an occurrence outside the selected relation")
        if spec.max_model_calls is not None and spec.max_model_calls < len(calls):
            raise ValueError("engine lifetime budget cannot cover this cascade stage")
        # Each independently constructed model process has a finite application
        # request budget. The controller separately checks the total query bound.
        spec = replace(spec, max_model_calls=len(calls))
        control = Path(tempfile.mkdtemp(prefix="lotus-cascade-stage-"))
        graph = _stage_graph(
            self.source, stage, self.resources, self.deadline, scan=self.scan,
            prepare_python=spec.driver_python,
            infer_class=InferCascade, infer_kwargs={
                "engine": asdict(spec), "generation": asdict(generation),
                "expected_count": len(calls), "deadline": self.deadline,
                "startup_timeout_s": self.resources.startup_timeout_s,
                "control": str(control),
            }, infer_python=spec.driver_python, num_gpus=1,
        )
        return _StageRows(_CascadeRayRows(graph, calls, self.deadline, self.cancelled,
                                         control=control, stage=stage),
                          stage, self.close_reports)
