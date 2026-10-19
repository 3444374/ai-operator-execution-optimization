"""Global LOTUS cascade stages executed by independent Daft Native graphs.

The original controller owns stage selection and its global barriers. Daft owns
each CSV scan, repeated-draw expansion, preparation processes and model batches.
Distinct model roles run serially; scans, model initialization and shutdown are
part of this deployment's cost, including another main load after calibration.
"""

from copy import deepcopy
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time

from src.semantic_methods.lotus.cascade import (
    CascadePreparer, CascadeStage, IDENTITY_FIELDS, _StageCalls, local_generation,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

from .daft_lotus_records import DEFAULT_PREPARED_BATCH_BYTES, _DaftRows, _check
from .lotus_cascade_process import StageEngineCleanup


DAFT_VERSION = "0.7.21"


def _selected(row, *, selected, input_columns):
    """Keep every sampled call, checking its values against the native CSV row."""
    calls = selected.get((row["row_id"], int(row["source_position"])), ())
    for call in calls:
        if any(row[name] != call[name] or type(row[name]) is not str for name in input_columns):
            raise ValueError("cascade source text differs from its checked occurrence")
        yield dict(call)


def _stage_graph(source, stage, engine, generation, resources, deadline, control, *,
                 scan, runner_threads=None,
                 max_prepared_batch_bytes=DEFAULT_PREPARED_BATCH_BYTES,
                 prepare_type=None, engine_type=None, model_gpus=1):
    """Only CPU qualification may substitute kernels and a zero-GPU resource request."""
    import daft
    from daft.functions import to_struct
    from src.infrastructure.daft_runtime import configure_daft_runner

    if daft.__version__ != DAFT_VERSION:
        raise RuntimeError("cascade requires exactly Daft Native " + DAFT_VERSION)
    if not isinstance(scan, RecordScan):
        raise TypeError("source.scan_spec(method) must return RecordScan")
    scan.describe()
    columns = tuple(stage.input_columns)
    if not columns or not set(columns).issubset(scan.payload_columns):
        raise ValueError("record scan omits cascade stage input columns")
    configure_daft_runner("native", num_threads=runner_threads)
    selected = {}
    for row in stage.records:
        selected.setdefault((row["row_id"], row["source_position"]), []).append(dict(row))
    fields = {name: daft.DataType.string() for name in IDENTITY_FIELDS}
    for name in ("source_position", "call_index"):
        fields[name] = daft.DataType.int64()
    call_dtype = daft.DataType.struct(dict(fields, **{
        name: daft.DataType.string() for name in columns
    }))
    fields.update(messages_json=daft.DataType.string(), prepare_worker_pid=daft.DataType.int64(),
                  prepare_started_ns=daft.DataType.int64(), prepare_finished_ns=daft.DataType.int64())
    prepared_dtype = daft.DataType.struct(fields)
    stage_config = asdict(replace(stage, records=()))
    expected_count = len(stage.records)
    engine_config, generation_config = asdict(engine), asdict(generation)

    @daft.func.batch(return_dtype=daft.DataType.list(call_dtype),
                     batch_size=resources.prepare_batch_rows, max_retries=0)
    def expand(records):
        _check(deadline, control)
        return daft.Series.from_pylist([
            list(_selected(row, selected=selected, input_columns=columns))
            for row in records.to_pylist()
        ])

    @daft.cls(cpus=1, use_process=True, max_concurrency=resources.cpu_workers,
              max_retries=0, on_error="raise")
    class Prepare:
        def __init__(self):
            _check(deadline, control)
            self.work = (prepare_type or CascadePreparer)(CascadeStage(**stage_config))

        @daft.method.batch(return_dtype=prepared_dtype, batch_size=resources.prepare_batch_rows)
        def run(self, records):
            _check(deadline, control)
            started = time.monotonic_ns()
            prepared = self.work.prepare_rows(records.to_pylist())
            finished = time.monotonic_ns()
            for row in prepared:
                row.update(prepare_worker_pid=os.getpid(), prepare_started_ns=started,
                           prepare_finished_ns=finished)
            _check(deadline, control)
            return daft.Series.from_pylist(prepared)

    @daft.cls(cpus=1, gpus=model_gpus, use_process=True, max_concurrency=1,
              max_retries=0, on_error="raise")
    class Infer:
        def __init__(self):
            _check(deadline, control)
            self.kernel, self.completed = None, 0

        @daft.method.batch(return_dtype=daft.DataType.string(), batch_size=resources.model_batch_rows)
        def run(self, prepared):
            inputs = prepared.to_pylist()
            if not inputs:
                return daft.Series.from_pylist([])
            input_bytes = sum(len(row["messages_json"].encode("utf-8")) for row in inputs)
            if len(inputs) > resources.model_batch_rows or input_bytes > max_prepared_batch_bytes:
                raise BufferError("actual Daft cascade batch exceeds its logical input allowance")
            _check(deadline, control)
            if self.kernel is None:
                from .daft_engine_process import EngineProcess

                self.kernel = (engine_type or EngineProcess)(
                    EngineSpec.from_dict(engine_config), GenerationSpec.from_dict(generation_config),
                    control, deadline, resources.startup_timeout_s,
                )
            try:
                rows = self.kernel.run_prepared(inputs)
            except BaseException as error:
                try:
                    self.kernel.close()
                except BaseException as cleanup:
                    error.add_note("cascade engine close failed: " + type(cleanup).__name__)
                raise
            for row, prepared_row in zip(rows, inputs, strict=True):
                for name in ("prepare_worker_pid", "prepare_started_ns", "prepare_finished_ns"):
                    row[name] = prepared_row[name]
                row.update(infer_worker_pid=os.getpid(), native_infer_batch_rows=len(inputs),
                           native_infer_message_bytes=input_bytes)
            self.completed += len(rows)
            if self.completed >= expected_count:
                try:
                    self.kernel.close()
                except BaseException as error:
                    # A cleanup observer cannot discard completed rows or usage.
                    from .daft_engine_process import _publish

                    try:
                        _publish(Path(control) / "engine-close-failure.json", dict(
                            status="unknown", process_exit_confirmed=False,
                            cleanup_errors=["engine_close: " + type(error).__name__]))
                    except BaseException:
                        pass
            return daft.Series.from_pylist([
                json.dumps(row, ensure_ascii=False, allow_nan=False) for row in rows
            ])

    types = {"string": daft.DataType.string(), "int64": daft.DataType.int64()}
    frame = daft.read_csv(str(source.csv_path), schema={
        name: types[dtype] for name, dtype in scan.csv_schema
    })
    if scan.predicate is not None:
        name, value = scan.predicate
        frame = frame.where(daft.col(name) == value)
    source_columns = ["row_id", "source_position", *columns]
    frame = frame.select(*source_columns)
    frame = frame.select(expand(to_struct(*(daft.col(name) for name in source_columns))).alias("_call"))
    frame = frame.explode("_call").where(daft.col("_call").not_null())
    frame = frame.select(Prepare().run(daft.col("_call")).alias("_prepared"))
    return frame.select(Infer().run(daft.col("_prepared")).alias("engine_result_json"))


class _CascadeDaftRows(_DaftRows):
    """Keep native lifecycle handling with the cascade's distinct call namespace."""

    def __init__(self, *args, stage, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage = stage
        self.engine_cleanup = StageEngineCleanup(self.control)
        self._watch_cleanup_started = threading.Event()
        self._watch_cleanup = None
        self._watch_cleanup_errors = []

    def _watch(self):
        while not self._stop.wait(0.02):
            try:
                self._check()
            except BaseException as primary:
                self._failure = primary
                try:
                    (self.control / "stop").touch(mode=0o600)
                except BaseException as error:
                    # The driver can be blocked in native iteration. Stop the
                    # registered model here; waiting for close would defer an
                    # early cancellation until the query deadline.
                    self._watch_cleanup_started.set()
                    self._watch_cleanup_errors.append("stop_request: " + type(error).__name__)
                    self._watch_cleanup = self.engine_cleanup.stop(started=self._started)
                    self._watch_cleanup_errors.extend(self._watch_cleanup["errors"])
                    for report in self._watch_cleanup["reports"]:
                        try:
                            self._watch_cleanup_errors.extend(self._recover_result_receipts(report))
                        except BaseException as recovery:
                            self._watch_cleanup_errors.append("result_receipt_recovery: " + type(recovery).__name__)
                return

    def close(self):
        if self._watch_cleanup_started.is_set() and self._watchdog is not None:
            self._watchdog.join(timeout=8)
            if self._watchdog.is_alive():
                self._watch_cleanup_errors.append("stop_watchdog_still_running")
        try:
            report = super().close()
            if self._watch_cleanup_started.is_set():
                report = dict(report, status="unknown",
                    cleanup_errors=list(dict.fromkeys([*report.get("cleanup_errors", []),
                                                       *self._watch_cleanup_errors])),
                    registered_engine_cleanup=self._watch_cleanup,
                    unconsumed_call_results=[self._result_receipts[call_id] for call_id in self.expected
                        if call_id in self._result_receipts and call_id not in self.received])
                self.last_close_report = report
            return report
        except BaseException as primary:
            # The ordinary close starts with a stop-file write. I/O failure at
            # that point must not skip iterator close, registered PID cleanup
            # or recovery of authoritative receipts. Retain its original error.
            self._done = True
            self._stop.set()
            errors = ["native_close: " + type(primary).__name__, *self._watch_cleanup_errors]
            if self._watchdog is not None:
                self._watchdog.join(timeout=0.2)
                if self._watchdog.is_alive():
                    errors.append("stop_watchdog_still_running")
            cleanup = self.engine_cleanup.stop(started=self._started)
            errors.extend(cleanup["errors"])
            reports = cleanup["reports"]
            try:
                close = getattr(self.iterator, "close", None)
                if close is not None:
                    close()
            except BaseException as error:
                errors.append("iterator_close: " + type(error).__name__)
            for report in reports:
                try:
                    errors.extend(self._recover_result_receipts(report))
                except BaseException as error:
                    errors.append("result_receipt_recovery: " + type(error).__name__)
            self.last_close_report = dict(
                status="unknown", cleanup_scope="daft_iterator_and_private_engines",
                cleanup_errors=list(dict.fromkeys(errors)), cleanup_artifacts=str(self.control),
                engine_reports=reports, registered_engine_cleanup=cleanup,
                received_occurrences=len(self.received), expected_occurrences=len(self.expected),
                gpu_exit_confirmed=None, scheduler_owner="Daft Native", use_process=True,
                max_active_batches_applied=False,
                unconsumed_call_results=[self._result_receipts[call_id] for call_id in self.expected
                    if call_id in self._result_receipts and call_id not in self.received])
            primary.add_note("Daft cascade emergency cleanup: " + json.dumps(
                dict(errors=self.last_close_report["cleanup_errors"],
                     process_exit_confirmed=cleanup["process_exit_confirmed"]), sort_keys=True))
            raise

    def _recover_result_receipts(self, report):
        # Ordinary CallLedger validates another canonical call-ID namespace.
        # Reuse the method's own journal for these verified cascade terminals.
        failure = report.get("result_failure")
        if failure is None:
            return []
        if type(failure) is not dict or type(failure.get("verified_call_results")) is not list:
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
                checked = _StageCalls(replace(self.stage, records=(expected,)))
                checked.record(row)
                receipt = checked.ordered()[0]
                if call_id in self._result_receipts and self._result_receipts[call_id] != receipt:
                    raise ValueError("engine receipt conflicts with an existing terminal")
                self._result_receipts[call_id] = receipt
            except (ValueError, TypeError, KeyError, AttributeError) as error:
                errors.append("result_receipt_recovery: " + type(error).__name__)
        return errors

    @staticmethod
    def _stop_registered_engine(report):
        # Unlike the ordinary helper, this still attempts the registered PID
        # when descendant enumeration is independently permission-restricted.
        return StageEngineCleanup._stop_registered(report)


class _StageRows:
    """Preserve terminal rows while requiring confirmed cleanup before a role switch."""

    def __init__(self, rows, stage, reports):
        self.rows, self.stage, self.reports = rows, stage, reports
        self.last_close_report = None

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.rows)

    def close(self):
        try:
            report = self.rows.close()
        except BaseException as primary:
            report = getattr(self.rows, "last_close_report", None) or dict(
                status="unknown", cleanup_errors=["stage_close: " + type(primary).__name__])
            self._record_report(report, force_unknown=True)
            raise
        settled = self._record_report(report)
        if not settled:
            raise RuntimeError("cascade stage engine/process cleanup is unconfirmed")
        return self.last_close_report

    def _record_report(self, report, *, force_unknown=False):
        engines = report.get("engine_reports", [])
        settled = (not force_unknown and report.get("status") == "closed" and bool(engines)
                   and not report.get("cleanup_errors")
                   and all(engine.get("process_exit_confirmed") is True
                           and (engine.get("engine_api") or {}).get("engine_api_shutdown_confirmed") is True
                           and not engine.get("cleanup_errors") for engine in engines))
        self.last_close_report = dict(report, algorithm_stage=self.stage.name,
                                      model_role=self.stage.model_role,
                                      stage_process_exit_confirmed=bool(settled))
        if self.reports and self.reports[-1].get("algorithm_stage") == self.stage.name:
            self.reports[-1] = deepcopy(self.last_close_report)
        else:
            self.reports.append(deepcopy(self.last_close_report))
        return settled


class DaftCascadeBackend:
    """One query owner; no physical scheduler, queue or model pool is shared."""

    def __init__(self, source, *, engines, resources, cancelled=None, runner_threads=None,
                 max_prepared_batch_bytes=DEFAULT_PREPARED_BATCH_BYTES):
        if type(engines) is not dict or set(engines) != {"main", "helper"}:
            raise ValueError("both main and helper engine deployments are required")
        self.engines = {role: EngineSpec.from_dict(asdict(spec)) for role, spec in engines.items()}
        if Path(self.engines["main"].model_path).resolve() == Path(self.engines["helper"].model_path).resolve():
            raise ValueError("this cascade deployment requires distinct explicit model paths")
        if runner_threads is not None and (type(runner_threads) is not int or runner_threads < 1):
            raise ValueError("runner_threads must be a positive integer or None")
        if type(max_prepared_batch_bytes) is not int or max_prepared_batch_bytes < 1:
            raise ValueError("max_prepared_batch_bytes must be a positive integer")
        self.resources = RecordsResources(**asdict(resources))
        self.source, self.cancelled = source, cancelled
        self.deadline = time.monotonic() + resources.query_timeout_s
        self.runner_threads, self.max_prepared_batch_bytes = runner_threads, max_prepared_batch_bytes
        self.close_reports, self._checked = [], False
        self._methods, self._scans = {}, {}
        self._instructions, self._effective = {}, {}

    def preflight(self, main_lm, helper_lm, effective, *, max_possible_calls, method, helper_method):
        import daft
        from lotus.nl_expression import nle2str
        from tokenizers import Tokenizer

        self.check_lifecycle()
        if self._checked:
            raise RuntimeError("cascade backend owns one query; create a new backend for another query")
        if daft.__version__ != DAFT_VERSION:
            raise RuntimeError("cascade requires exactly Daft Native " + DAFT_VERSION)
        if type(max_possible_calls) is not int or max_possible_calls < 0:
            raise ValueError("cascade max_possible_calls must be a nonnegative integer")
        if not isinstance(method, FilterMethod):
            raise TypeError("cascade requires bound FilterMethod configurations")
        self.source.preflight(method)
        scan = self.source.scan_spec(method)
        if not isinstance(scan, RecordScan):
            raise TypeError("source.scan_spec(method) must return RecordScan")
        scan.describe()
        methods, scans, instructions = {}, {}, {}
        for role, lm, binding in (("main", main_lm, method), ("helper", helper_lm, helper_method)):
            if not isinstance(binding, FilterMethod):
                raise TypeError("cascade requires bound FilterMethod configurations")
            binding = FilterMethod.from_dict(binding.describe())
            if binding.model_name != lm.model or not binding.resolved_input_columns:
                raise ValueError("cascade method must bind its model and input columns")
            if not set(binding.resolved_input_columns).issubset(scan.payload_columns):
                raise ValueError("record scan omits cascade input columns")
            spec = self.engines[role]
            spec.validate_driver_environment()
            tokenizer = Tokenizer.from_file(str(Path(spec.tokenizer_path or spec.model_path) / "tokenizer.json"))
            for token in ("True", "False"):
                expected = tokenizer.encode(token, add_special_tokens=False).ids
                if len(expected) != 1 or lm.encode_text(token) != expected:
                    raise ValueError("original LM and deployed tokenizer label IDs differ")
            dummy = CascadeStage("helper_score" if role == "helper" else "fallback", role,
                lm.model, binding.instruction, binding.default, binding.system_prompt,
                binding.output_tokens, effective[role], (), binding.resolved_input_columns)
            generation = local_generation(dummy)
            if generation.max_tokens > spec.max_model_len:
                raise ValueError("cascade max_tokens exceeds max_model_len")
            methods[role], scans[role] = binding, scan
            instructions[role] = nle2str(binding.instruction, list(binding.resolved_input_columns))
        self._methods, self._scans = methods, scans
        self._instructions, self._effective = instructions, deepcopy(effective)
        self._checked = True

    def check_lifecycle(self):
        _check(self.deadline)
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
        generation = local_generation(stage)
        calls = [{key: row[key] for key in IDENTITY_FIELDS} for row in stage.records]
        if not calls:
            return iter(())
        allowed = {(row["row_id"], row["source_position"])
                   for row in self.source.expected(calls[0]["query_id"])}
        if any((row["row_id"], row["source_position"]) not in allowed for row in calls):
            raise ValueError("cascade stage includes an occurrence outside the selected relation")
        spec = self.engines[stage.model_role]
        if spec.max_model_calls is not None and spec.max_model_calls < len(calls):
            raise ValueError("engine lifetime budget cannot cover this cascade stage")
        if generation.max_tokens > spec.max_model_len:
            raise ValueError("cascade max_tokens exceeds max_model_len")
        spec = replace(spec, max_model_calls=len(calls))
        control = Path(tempfile.mkdtemp(prefix="daft-cascade-stage-"))
        os.chmod(control, 0o700)
        try:
            frame = _stage_graph(self.source, stage, spec, generation, self.resources,
                self.deadline, control, scan=self._scans[stage.model_role],
                runner_threads=self.runner_threads, max_prepared_batch_bytes=self.max_prepared_batch_bytes)
            rows = _CascadeDaftRows(frame, calls, self.deadline, self.cancelled, control, stage=stage)
            return _StageRows(rows, stage, self.close_reports)
        except BaseException:
            shutil.rmtree(control)
            raise
