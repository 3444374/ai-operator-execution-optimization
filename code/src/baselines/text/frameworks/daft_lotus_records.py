"""Daft Native 0.7.21 CSV → LOTUS preparation → private engine → result rows.

Daft owns its class instances, batches and scheduling. The model process belongs
to the inference class; no SemLoom pool, submission policy or HTTP is involved.
"""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import shutil
import signal
import tempfile
import threading
import time

from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS


DEFAULT_PREPARED_BATCH_BYTES = 64 * 1024**2


def _check(deadline, control=None):
    if control is not None and (Path(control) / "stop").exists():
        raise InterruptedError("Daft records query cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("Daft records query deadline expired")


def _graph(source, method, engine, generation, resources, query_id, expected, deadline,
           control, *, runner_threads=None, max_prepared_batch_bytes=DEFAULT_PREPARED_BATCH_BYTES,
           prepare_type=None, engine_type=None, model_gpus=1, scan=None):
    """The injectable kernels serve CPU library tests; public evaluate fixes both."""
    import daft
    from daft.functions import to_struct
    from src.infrastructure.daft_runtime import configure_daft_runner
    from src.semantic_methods.lotus.engine import bind_filter_method

    scan = source.scan_spec(method) if scan is None else scan
    if not isinstance(scan, RecordScan):
        raise TypeError("source.scan_spec(method) must return RecordScan")
    scan.describe()
    method = bind_filter_method(method)
    if not set(method.resolved_input_columns).issubset(scan.payload_columns):
        raise ValueError("record scan omits Filter input columns")
    if daft.__version__ != "0.7.21":
        raise RuntimeError("records native graph requires exactly Daft 0.7.21")
    configure_daft_runner("native", num_threads=runner_threads)
    fields = {name: daft.DataType.string() for name in IDENTITY_FIELDS}
    for name in ("source_position", "call_index"):
        fields[name] = daft.DataType.int64()
    fields.update(messages_json=daft.DataType.string(), prepare_worker_pid=daft.DataType.int64(),
                  prepare_started_ns=daft.DataType.int64(), prepare_finished_ns=daft.DataType.int64())
    prepared_dtype = daft.DataType.struct(fields)
    # Keep the engine's entire versioned row, including unknown/failed usage,
    # rather than duplicate a narrower result schema in each vendor adapter.
    result_dtype = daft.DataType.struct({"engine_result_json": daft.DataType.string()})
    method_config = asdict(method)
    engine_config, generation_config = asdict(engine), asdict(generation)

    @daft.cls(cpus=1, use_process=True, max_concurrency=resources.cpu_workers,
              max_retries=0, on_error="raise")
    class Prepare:
        def __init__(self):
            from src.semantic_methods.lotus.engine import LotusFilterMethod

            _check(deadline, control)
            self.work = (prepare_type or LotusFilterMethod)(FilterMethod.from_dict(method_config))

        @daft.method.batch(return_dtype=prepared_dtype, batch_size=resources.prepare_batch_rows)
        def run(self, records):
            _check(deadline, control)
            rows = [dict(row, **identity(query_id, row["row_id"], row["source_position"]))
                    for row in records.to_pylist()]
            prepared = self.work.prepare_rows(rows)
            _check(deadline, control)
            return daft.Series.from_pylist(prepared)

    @daft.cls(cpus=1, gpus=model_gpus, use_process=True, max_concurrency=1,
              max_retries=0, on_error="raise")
    class Infer:
        def __init__(self):
            _check(deadline, control)
            self.kernel = None
            self.completed = 0

        @daft.method.batch(return_dtype=result_dtype, batch_size=resources.model_batch_rows)
        def run(self, prepared):
            inputs = prepared.to_pylist()
            input_bytes = sum(len(row["messages_json"].encode("utf-8")) for row in inputs)
            if len(inputs) > resources.model_batch_rows or input_bytes > max_prepared_batch_bytes:
                raise BufferError("actual Daft inference batch exceeds its logical input allowance")
            _check(deadline, control)
            if self.kernel is None:
                from .daft_engine_process import EngineProcess

                self.kernel = (engine_type or EngineProcess)(
                    EngineSpec.from_dict(engine_config), GenerationSpec.from_dict(generation_config),
                    control, deadline, resources.startup_timeout_s,
                )
            rows = self.kernel.run_prepared(inputs)
            for row, prepared_row in zip(rows, inputs, strict=True):
                for name in ("prepare_worker_pid", "prepare_started_ns", "prepare_finished_ns"):
                    row[name] = prepared_row[name]
                row.update(infer_worker_pid=os.getpid(), native_infer_batch_rows=len(inputs),
                           native_infer_message_bytes=input_bytes)
            self.completed += len(rows)
            if self.completed >= len(expected):
                # Preserve completed rows and usage even if closing reports an
                # error. The consumer inspects the separate cleanup report.
                try:
                    self.kernel.close()
                except BaseException as exc:
                    from .daft_engine_process import _publish

                    try:
                        _publish(Path(control) / "engine-close-failure.json", dict(
                            status="unknown", process_exit_confirmed=False,
                            cleanup_errors=["engine_close: " + type(exc).__name__]))
                    except BaseException:
                        # The registered process report remains unconfirmed;
                        # reporting I/O must not discard authoritative rows.
                        pass
            return daft.Series.from_pylist([
                dict(engine_result_json=json.dumps(row, ensure_ascii=False, allow_nan=False))
                for row in rows
            ])

    types = {"string": daft.DataType.string(), "int64": daft.DataType.int64()}
    frame = daft.read_csv(str(source.csv_path), schema={
        name: types[dtype] for name, dtype in scan.csv_schema
    })
    if scan.predicate is not None:
        column, value = scan.predicate
        frame = frame.where(daft.col(column) == value)
    columns = ["row_id", "source_position", *scan.payload_columns]
    frame = frame.select(*columns)
    prepare, infer = Prepare(), Infer()
    frame = frame.with_column("_prepared", prepare.run(
        to_struct(*(daft.col(column) for column in columns)))).select("_prepared")
    frame = frame.with_column("_result", infer.run(daft.col("_prepared")))
    return frame.select(daft.col("_result").get("engine_result_json").alias("engine_result_json"))


class _DaftRows:
    def __init__(self, frame, expected, deadline, cancelled, control):
        self.frame, self.deadline, self.cancelled = frame, deadline, cancelled
        self.expected = {row["call_id"]: row for row in expected}
        if len(self.expected) != len(expected):
            raise ValueError("source has duplicate call identities")
        self.received = set()
        self.control = Path(control)
        self.iterator = None
        self._stop = threading.Event()
        self._watchdog = None
        self._failure = None
        self._done = False
        self._started = False
        self.last_close_report = None

    def __iter__(self):
        return self

    def _check(self):
        if self._failure is not None:
            raise self._failure
        if self.cancelled is not None and self.cancelled():
            raise InterruptedError("Daft records query cancelled")
        _check(self.deadline)

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
            if self.iterator is None:
                self._check()
                self._started = self.frame is not None
                self._watchdog = threading.Thread(target=self._watch, daemon=True,
                                                  name="daft-records-stop")
                self._watchdog.start()
                self.iterator = iter(()) if self.frame is None else iter(self.frame.iter_rows())
            row = json.loads(next(self.iterator)["engine_result_json"])
            expected = self.expected.get(row.get("call_id"))
            if expected is None or row["call_id"] in self.received:
                raise ValueError("Daft returned an undeclared or duplicate occurrence")
            if any(row.get(key) != value for key, value in expected.items()):
                raise ValueError("Daft changed call identity")
            self.received.add(row["call_id"])
            return row
        except StopIteration:
            self._done = True
            try:
                self._check()
                if self.received != set(self.expected):
                    raise ValueError("Daft omitted expected occurrences")
                report = self.close()
                if report["status"] != "closed":
                    raise RuntimeError("Daft records cleanup remains unconfirmed")
            except BaseException as exc:
                self._finish_error(exc)
                raise
            raise
        except BaseException as exc:
            primary = self._failure or exc
            if primary is not exc:
                primary.add_note("Daft execution also failed: " + type(exc).__name__)
            self._finish_error(primary)
            if primary is not exc:
                raise primary from exc
            raise

    def _finish_error(self, error):
        self._done = True
        try:
            report = self.close()
            if report["status"] != "closed":
                error.add_note("Daft cleanup: " + json.dumps(report, sort_keys=True))
        except BaseException as cleanup:
            error.add_note("Daft cleanup also failed: " + type(cleanup).__name__)

    def close(self):
        self._done = True
        if self.last_close_report is not None and self.last_close_report["status"] == "closed":
            return self.last_close_report
        errors = []
        self._stop.set()
        (self.control / "stop").touch(mode=0o600)
        if self._watchdog is not None:
            self._watchdog.join(timeout=0.2)
            if self._watchdog.is_alive():
                errors.append("stop_watchdog_still_running")
        try:
            close = getattr(self.iterator, "close", None)
            if close is not None:
                close()
        except BaseException as exc:
            errors.append("iterator_close: " + type(exc).__name__)
        reports = []
        for path in self.control.glob("engine-*.json"):
            try:
                report = json.loads(path.read_text())
                if not report.get("process_exit_confirmed") and report.get("model_pid"):
                    # The class may already be idle or its Daft worker may have
                    # exited. Wait for its own reaper, then supervise only the
                    # registered private process with matching OS identity.
                    wait_until = time.monotonic() + 5.5
                    while time.monotonic() < wait_until and not report.get("process_exit_confirmed"):
                        time.sleep(0.02)
                        report = json.loads(path.read_text())
                    if not report.get("process_exit_confirmed"):
                        report = self._stop_registered_engine(report)
                reports.append(report)
            except Exception as exc:
                errors.append("engine_report_read: " + type(exc).__name__)
        if self._started and self.expected and not reports:
            errors.append("engine_process_cleanup_unconfirmed")
        for report in reports:
            errors.extend(report.get("cleanup_errors", []))
            if report.get("status") != "closed" or not report.get("process_exit_confirmed"):
                errors.append("private_engine_process_exit_unconfirmed")
        self.last_close_report = dict(
            status="closed" if not errors else "unknown", cleanup_scope="daft_iterator_and_private_engines",
            engine_reports=reports, cleanup_errors=errors,
            received_occurrences=len(self.received), expected_occurrences=len(self.expected),
            gpu_exit_confirmed=None if self._started else True,
            scheduler_owner="Daft Native", use_process=True,
            max_active_batches_applied=False,
            max_active_batches_reason="Ray actor setting; Daft owns native buffering",
            result_transport="compact complete engine rows as JSON within a native struct",
            cleanup_artifacts=None if not errors else str(self.control),
        )
        if not errors:
            shutil.rmtree(self.control)
        return self.last_close_report

    @staticmethod
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
                    cleanup_errors=["engine_worker_cleanup_unconfirmed_driver_supervised_os_exit"])


def evaluate(source, method: FilterMethod, engine: EngineSpec, generation: GenerationSpec,
             resources: RecordsResources, *, query_id: str, cancelled=None,
             runner_threads=None, max_prepared_batch_bytes=DEFAULT_PREPARED_BATCH_BYTES):
    """Validate before allocation; the caller supervises the whole query process.

    Cancellation interrupts private engine waits. A synchronous native scan or
    formatter must return before its cooperative check can run; a hard wall
    deadline for those phases requires lotus_records_supervisor.supervise().
    """
    resources = RecordsResources(**asdict(resources))
    if runner_threads is not None and (type(runner_threads) is not int or runner_threads < 1):
        raise ValueError("runner_threads must be a positive integer or None")
    if type(max_prepared_batch_bytes) is not int or max_prepared_batch_bytes < 1:
        raise ValueError("max_prepared_batch_bytes must be a positive integer")
    deadline = time.monotonic() + resources.query_timeout_s
    method = FilterMethod.from_dict(asdict(method))
    generation = GenerationSpec.from_dict(asdict(generation))
    engine = EngineSpec.from_dict(asdict(engine))
    from src.semantic_methods.lotus.engine import (
        bind_filter_method, make_lm, validate_generation, validate_lm,
    )

    source.preflight(method)
    scan = source.scan_spec(method)
    if not isinstance(scan, RecordScan):
        raise TypeError("source.scan_spec(method) must return RecordScan")
    scan.describe()
    validate_generation(method, generation)
    method = bind_filter_method(method)
    if not set(method.resolved_input_columns).issubset(scan.payload_columns):
        raise ValueError("record scan omits Filter input columns")
    validate_lm(make_lm(method, generation), method, generation)
    if generation.max_tokens > engine.max_model_len:
        raise ValueError("max_tokens exceeds max_model_len")
    engine.validate_driver_environment()
    expected = source.expected(query_id)
    _check(deadline)
    if cancelled is not None and cancelled():
        raise InterruptedError("Daft records query cancelled before graph allocation")
    control = tempfile.mkdtemp(prefix="daft-records-")
    os.chmod(control, 0o700)
    try:
        frame = None if not expected else _graph(source, method, engine, generation, resources,
                                                query_id, expected, deadline, control,
                                                runner_threads=runner_threads,
                                                max_prepared_batch_bytes=max_prepared_batch_bytes,
                                                scan=scan)
        return _DaftRows(frame, expected, deadline, cancelled, control)
    except BaseException:
        shutil.rmtree(control)
        raise
