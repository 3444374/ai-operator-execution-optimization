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
    CascadePreparer, CascadeStage, IDENTITY_FIELDS, local_generation,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec

from .ray_data_lotus_records import _RayRows, _CLOSE_COLUMN, _check_deadline, _runtime_env, _table
from .lotus_cascade_process import StageEngineCleanup


def _selected(row, *, selected):
    """A repeated sample creates separate logical calls before any native batching."""
    calls = selected.get((row["row_id"], int(row["source_position"])), ())
    for call in calls:
        if row["reviewText"] != call["reviewText"]:
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


def _stage_graph(source, stage, resources, deadline, *, prepare_python,
                 infer_class, infer_kwargs, infer_python, num_gpus):
    """Shared graph construction; CPU qualification supplies a named model fixture."""
    import pyarrow as pa
    from pyarrow import csv
    import ray
    import ray.data

    if ray.__version__ != "2.56.1" or not ray.is_initialized():
        raise RuntimeError("cascade records graph requires caller-owned Ray 2.56.1")
    _check_deadline(deadline)
    selected = {}
    for row in stage.records:
        selected.setdefault((row["row_id"], row["source_position"]), []).append(dict(row))
    dataset = ray.data.read_csv(
        str(source.csv_path), ray_remote_args={"max_retries": 0},
        convert_options=csv.ConvertOptions(column_types={
            "row_id": pa.string(), "source_position": pa.int64(),
            "reviewId": pa.string(), "id": pa.string(), "reviewText": pa.string(),
        }, strings_can_be_null=False, null_values=[]),
    )
    dataset = dataset.filter(lambda row: row["id"] == "taken_3", max_retries=0)
    dataset = dataset.select_columns(["row_id", "source_position", "reviewText"])
    dataset = dataset.flat_map(_selected, fn_kwargs={"selected": selected}, max_retries=0)
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
    def __init__(self, *args, control, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine_cleanup = StageEngineCleanup(control)

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
        report = super().close()
        report["registered_engine_cleanup"] = cleanup
        report["cleanup_errors"].extend(cleanup["errors"])
        report["process_exit_confirmed"] = cleanup["process_exit_confirmed"]
        if initializing:
            report["executor_shutdown_confirmed"] = False
            report["cleanup_errors"].append("graph_executor_initialization_unconfirmed")
        self.last_close_report = report
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
        report = self.rows.close()
        engine = report.get("engine_close_report") or {}
        settled = (report.get("iterator_closed") and report.get("executor_shutdown_confirmed")
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

    def preflight(self, main_lm, helper_lm, effective, *, max_possible_calls):
        from tokenizers import Tokenizer

        source_info = self.source.preflight(FilterMethod(main_lm.model))
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
            if spec.max_model_calls is not None and spec.max_model_calls < source_info["eligible_rows"]:
                raise ValueError("each model deployment must cover any complete logical stage")
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
                                 role, lm.model, "", True, None, ("True", "False"),
                                 effective[role], ())
            local_generation(dummy)
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
            self.source, stage, self.resources, self.deadline, prepare_python=spec.driver_python,
            infer_class=InferCascade, infer_kwargs={
                "engine": asdict(spec), "generation": asdict(generation),
                "expected_count": len(calls), "deadline": self.deadline,
                "startup_timeout_s": self.resources.startup_timeout_s,
                "control": str(control),
            }, infer_python=spec.driver_python, num_gpus=1,
        )
        return _StageRows(_CascadeRayRows(graph, calls, self.deadline, self.cancelled, control=control),
                          stage, self.close_reports)
