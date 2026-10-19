"""LOTUS text Filter records on the caller-owned Ray Data 2.56.1 graph.

The native graph owns batching and actor scheduling. Only primitive method and
engine configuration crosses its workers; there is no HTTP replay or Core flow.
"""

from __future__ import annotations

import json
import shlex
import threading
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Iterator

from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


_CODE_ROOT = Path(__file__).resolve().parents[4]
_CLOSE_COLUMN = "__engine_close_report_json"


def _runtime_env(python: str) -> dict[str, Any]:
    return {
        "py_executable": shlex.quote(python),
        "env_vars": {"PYTHONPATH": str(_CODE_ROOT)},
    }


def _check_deadline(deadline: float) -> None:
    if time.monotonic() >= deadline:
        raise TimeoutError("LOTUS records query deadline expired")


def _add_identity(row: dict[str, Any], *, query_id: str) -> dict[str, Any]:
    return {
        **row,
        **identity(query_id, row["row_id"], int(row["source_position"])),
    }


def _table(rows: list[dict[str, Any]]) -> Any:
    import pyarrow as pa

    return pa.Table.from_pylist(rows)


class PrepareBatch:
    """A worker-local LOTUS formatter, never a serialized driver LM."""

    def __init__(self, method: dict[str, Any], deadline: float):
        from src.semantic_methods.lotus.engine import LotusFilterMethod

        _check_deadline(deadline)
        self.method = LotusFilterMethod(FilterMethod.from_dict(method))
        self.deadline = deadline

    def __call__(self, batch: Any) -> Any:
        _check_deadline(self.deadline)
        rows = self.method.prepare_rows(batch.to_pylist())
        _check_deadline(self.deadline)
        return _table(rows)


class InferBatch:
    """One single-GPU synchronous engine for this finite native query."""

    def __init__(
        self,
        engine: dict[str, Any],
        generation: dict[str, Any],
        expected_count: int,
        deadline: float,
        startup_timeout_s: float,
    ):
        from src.serving.backends.vllm_engine import VllmEngine

        _check_deadline(deadline)
        self.deadline = deadline
        self.expected_count = expected_count
        self.completed = 0
        self.engine = None
        startup_deadline = min(deadline, time.monotonic() + startup_timeout_s)
        try:
            self.engine = VllmEngine(
                EngineSpec.from_dict(engine), GenerationSpec.from_dict(generation)
            )
            _check_deadline(startup_deadline)
        except BaseException as exc:
            self._close_on_error(exc)
            raise

    def _close_on_error(self, exc: BaseException) -> None:
        if self.engine is not None:
            try:
                report = self.engine.close()
                exc.add_note("model cleanup: " + json.dumps(report, sort_keys=True))
            except BaseException as close_exc:
                exc.add_note(f"model cleanup failed: {type(close_exc).__name__}")

    def __call__(self, batch: Any) -> Any:
        try:
            _check_deadline(self.deadline)
            rows = self.engine.run_prepared(
                batch.to_pylist(), deadline_monotonic=self.deadline
            )
        except BaseException as exc:
            self._close_on_error(exc)
            raise
        self.completed += len(rows)
        for row in rows:
            row[_CLOSE_COLUMN] = ""
        if self.completed >= self.expected_count and rows:
            try:
                report = self.engine.close()
            except BaseException as exc:
                report = dict(
                    status="unknown", engine_api_shutdown_confirmed=False,
                    gpu_exit_confirmed=None, error_type=type(exc).__name__,
                    cleanup_scope="engine_core_api",
                )
            rows[-1][_CLOSE_COLUMN] = json.dumps(report, sort_keys=True)
        # Closing failure cannot discard a batch that already has real usage.
        return _table(rows)


class _RayRows(Iterator[dict[str, Any]]):
    """Consume the vendor iterator and retain unconfirmed cleanup for retry."""

    def __init__(self, result: Any, expected: list[dict[str, Any]], deadline: float, cancelled: Callable[[], bool] | None):
        self.result = result
        self.expected = {row["call_id"]: row for row in expected}
        if len(self.expected) != len(expected):
            raise ValueError("source has duplicate call identities")
        self.deadline = deadline
        self.cancelled = cancelled
        self.iterator = None if result is not None else iter(())
        self._started = False
        self.received: set[str] = set()
        self.engine_close_report: dict[str, Any] | None = None
        self.last_close_report: dict[str, Any] | None = None
        self._iterator_closed = False
        self._executor_closed = result is None
        self._stop = threading.Event()
        self._watchdog: threading.Thread | None = None
        self._failure: BaseException | None = None
        self._cleanup_errors: list[str] = []
        self._done = False

    def __iter__(self) -> _RayRows:
        return self

    def _check(self) -> None:
        if self._failure is not None:
            raise self._failure
        if self.cancelled is not None and self.cancelled():
            raise InterruptedError("LOTUS records query cancelled")
        _check_deadline(self.deadline)

    def _watch(self) -> None:
        while not self._stop.wait(0.02):
            try:
                self._check()
            except BaseException as exc:
                self._failure = exc
                executor = getattr(self.result, "_current_executor", None)
                if executor is None:
                    # Execution can still be constructing its executor.
                    continue
                try:
                    executor.shutdown(force=True)
                    self._executor_closed = True
                except BaseException as close_exc:
                    self._cleanup_errors.append(type(close_exc).__name__)
                return

    def __next__(self) -> dict[str, Any]:
        if self._done:
            raise StopIteration
        try:
            if not self._started:
                self._check()
            if self.result is not None and self._watchdog is None:
                self._watchdog = threading.Thread(
                    target=self._watch, name="lotus-records-ray-deadline", daemon=True
                )
                self._watchdog.start()
            if self.iterator is None:
                self._started = True
                self.iterator = iter(self.result.iter_rows())
            row = dict(next(self.iterator))
            close_report = row.pop(_CLOSE_COLUMN, "")
            if close_report:
                self.engine_close_report = json.loads(close_report)
            call_id = row.get("call_id")
            expected = self.expected.get(call_id)
            if expected is None or call_id in self.received:
                raise ValueError("native graph returned an unexpected or duplicate occurrence")
            if any(row.get(key) != value for key, value in expected.items()):
                raise ValueError("native graph changed occurrence identity")
            self.received.add(call_id)
            # After a stop request, drain compact results already returned by
            # the graph before raising the stop at EOF; their actual usage is
            # independent of whether this query may publish a final answer.
            return row
        except StopIteration:
            self._done = True
            try:
                self._check()
                if self.received != set(self.expected):
                    raise ValueError("native graph omitted expected occurrences")
            except BaseException as exc:
                self._finish_error(exc)
                raise
            report = self.close()
            if report["cleanup_errors"]:
                raise RuntimeError("native graph cleanup is unconfirmed: " + "; ".join(report["cleanup_errors"]))
            raise
        except BaseException as exc:
            self._finish_error(exc)
            raise

    def _finish_error(self, exc: BaseException) -> None:
        self._done = True
        report = self.close()
        if report["status"] != "closed":
            exc.add_note("native graph cleanup: " + json.dumps(report, sort_keys=True))

    def close(self) -> dict[str, Any]:
        self._done = True
        self._stop.set()
        if self._watchdog is not None:
            self._watchdog.join(timeout=0.2)
        watchdog_stopped = self._watchdog is None or not self._watchdog.is_alive()
        errors: list[str] = []
        if not self._iterator_closed:
            close = getattr(self.iterator, "close", None)
            try:
                if close is not None:
                    close()
                self._iterator_closed = True
            except BaseException as exc:
                errors.append("iterator_close: " + type(exc).__name__)
        # A blocked watchdog owns shutdown until it returns; do not start a
        # second blocking shutdown or call that background thread settled.
        if not self._executor_closed and watchdog_stopped:
            executor = getattr(self.result, "_current_executor", None)
            try:
                if executor is not None:
                    executor.shutdown(force=True)
                self._executor_closed = True
            except BaseException as exc:
                errors.append("executor_shutdown: " + type(exc).__name__)
        if not watchdog_stopped:
            errors.append("deadline_watchdog_still_running")
        empty = not self.expected or not self._started
        engine_closed = empty or bool(
            self.engine_close_report
            and self.engine_close_report.get("engine_api_shutdown_confirmed")
        )
        if not engine_closed:
            errors.append("model_engine_shutdown_unconfirmed")
        self._cleanup_errors.extend(errors)
        gpu_exit = True if empty else (
            self.engine_close_report or {}
        ).get("gpu_exit_confirmed")
        settled = self._iterator_closed and self._executor_closed and watchdog_stopped and engine_closed and gpu_exit is True
        self.last_close_report = {
            "status": "closed" if settled else "unknown",
            "cleanup_scope": "ray_data_graph_and_engine_api",
            "iterator_closed": self._iterator_closed,
            "executor_shutdown_confirmed": self._executor_closed,
            "watchdog_stopped": watchdog_stopped,
            "engine_close_report": self.engine_close_report,
            "gpu_exit_confirmed": gpu_exit,
            "received_occurrences": len(self.received),
            "expected_occurrences": len(self.expected),
            "cleanup_errors": errors,
            "cleanup_error_history": list(self._cleanup_errors),
        }
        return self.last_close_report


def evaluate(
    source: Any,
    method: FilterMethod,
    engine: EngineSpec,
    generation: GenerationSpec,
    resources: RecordsResources,
    *,
    query_id: str,
    cancelled: Callable[[], bool] | None = None,
) -> _RayRows:
    """Build CSV→filter→prepare→single-GPU model, then stream compact rows.

    A separate process supervisor remains required to establish physical GPU
    exit after cancellation, actor loss or a synchronous model call that stalls.
    """

    resources = RecordsResources(**asdict(resources))
    deadline = time.monotonic() + resources.query_timeout_s
    method = FilterMethod.from_dict(asdict(method))
    generation = GenerationSpec.from_dict(asdict(generation))
    engine = EngineSpec.from_dict(asdict(engine))
    from src.semantic_methods.lotus.engine import bind_filter_method, validate_generation

    source.preflight(method)
    scan = source.scan_spec(method)
    if not isinstance(scan, RecordScan):
        raise TypeError("source.scan_spec(method) must return RecordScan")
    scan.describe()
    validate_generation(method, generation)
    method = bind_filter_method(method)
    if not set(method.resolved_input_columns).issubset(scan.payload_columns):
        raise ValueError("record scan omits Filter input columns")
    engine.validate_driver_environment()
    expected = source.expected(query_id)
    _check_deadline(deadline)
    if cancelled is not None and cancelled():
        raise InterruptedError("LOTUS records query cancelled before graph allocation")
    if not expected:
        return _RayRows(None, expected, deadline, cancelled)

    import ray
    import ray.data
    import pyarrow as pa
    from pyarrow import csv

    if ray.__version__ != "2.56.1" or not ray.is_initialized():
        raise RuntimeError("records native graph requires caller-owned Ray 2.56.1")
    _check_deadline(deadline)
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
    dataset = dataset.map(_add_identity, fn_kwargs={"query_id": query_id}, max_retries=0)
    dataset = dataset.map_batches(
        PrepareBatch,
        batch_size=resources.prepare_batch_rows,
        batch_format="pyarrow",
        compute=ray.data.ActorPoolStrategy(
            size=resources.cpu_workers,
            max_tasks_in_flight_per_actor=resources.max_active_batches,
        ),
        fn_constructor_kwargs={"method": asdict(method), "deadline": deadline},
        num_cpus=1,
        max_concurrency=1,
        max_restarts=0,
        max_task_retries=0,
        runtime_env=_runtime_env(engine.driver_python),
    )
    result = dataset.map_batches(
        InferBatch,
        batch_size=resources.model_batch_rows,
        batch_format="pyarrow",
        compute=ray.data.ActorPoolStrategy(
            size=1,
            max_tasks_in_flight_per_actor=resources.max_active_batches,
        ),
        fn_constructor_kwargs={
            "engine": asdict(engine),
            "generation": asdict(generation),
            "expected_count": len(expected),
            "deadline": deadline,
            "startup_timeout_s": resources.startup_timeout_s,
        },
        num_gpus=1,
        max_concurrency=1,
        max_restarts=0,
        max_task_retries=0,
        runtime_env=_runtime_env(engine.model_python),
    )
    return _RayRows(result, expected, deadline, cancelled)
