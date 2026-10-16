"""Finite Movie Q3 static reference; this path does not use production Core.

Daft Native scans raw records. Ray Core preparation actors return ObjectRefs
directly to a single model actor. The driver receives only compact terminal rows.
"""

from __future__ import annotations

import os
import shlex
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Iterator

from src.semantic_methods.lotus.records import FilterMethod, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


_CODE_ROOT = Path(__file__).resolve().parents[3]


def _runtime_env(python: str) -> dict[str, Any]:
    return {
        "py_executable": shlex.quote(python),
        "env_vars": {"PYTHONPATH": str(_CODE_ROOT)},
    }


def _check(deadline: float, cancelled: Callable[[], bool] | None = None) -> None:
    if cancelled is not None and cancelled():
        raise InterruptedError("Movie Q3 static reference cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("Movie Q3 static reference deadline expired")


class _PrepareActor:
    def __init__(self, method: dict[str, Any]):
        from src.semantic_methods.lotus.engine import LotusFilterMethod

        self.method = LotusFilterMethod(FilterMethod.from_dict(method))

    def prepare(self, rows: list[dict[str, Any]], deadline: float) -> list[dict[str, Any]]:
        _check(deadline)
        prepared = self.method.prepare_rows(rows)
        _check(deadline)
        return prepared


class _ModelActor:
    def __init__(self, engine: dict[str, Any], generation: dict[str, Any], model_batch_rows: int):
        self.engine_spec = EngineSpec.from_dict(engine)
        self.generation = GenerationSpec.from_dict(generation)
        self.model_batch_rows = model_batch_rows
        self.engine = None

    def ready(self, deadline: float) -> dict[str, Any]:
        from src.serving.backends.vllm_engine import VllmEngine

        _check(deadline)
        if self.engine is None:
            self.engine = VllmEngine(self.engine_spec, self.generation)
        _check(deadline)
        return self.engine.ready()

    def run(self, rows: list[dict[str, Any]], deadline: float) -> Any:
        _check(deadline)
        results = []
        for start in range(0, len(rows), self.model_batch_rows):
            try:
                results.extend(self.engine.run_prepared(
                    rows[start:start + self.model_batch_rows], deadline_monotonic=deadline,
                ))
            except BaseException as exc:
                # Ray exception transport would otherwise hide successful
                # earlier sub-batches from the independent caller ledger.
                return dict(results=results, error_type=type(exc).__name__,
                            unresolved_calls=len(rows) - start)
        return results

    def close(self, timeout: float) -> dict[str, Any]:
        if self.engine is None:
            return dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=True)
        return self.engine.close(timeout=timeout)


class _StaticRows(Iterator[dict[str, Any]]):
    def __init__(
        self, owner: SemLoomRecordsStaticReference, source_rows: Any,
        expected: list[dict[str, Any]], query_id: str, deadline: float,
        cancelled: Callable[[], bool] | None,
    ):
        self.owner = owner
        self.source_rows = iter(source_rows)
        self.expected = {row["call_id"]: row for row in expected}
        if len(self.expected) != len(expected):
            raise ValueError("source has duplicate call identities")
        self.query_id, self.deadline, self.cancelled = query_id, deadline, cancelled
        self.received: set[str] = set()
        self.pending: list[tuple[Any, Any]] = []
        self.ready_rows: list[dict[str, Any]] = []
        self.source_eof = False
        self.finished = False
        self.source_closed = False
        self.submitted_batches = 0
        self._failure: BaseException | None = None
        self.last_close_report: dict[str, Any] | None = None

    def __iter__(self) -> _StaticRows:
        return self

    def _submit(self) -> None:
        while not self.source_eof and len(self.pending) < self.owner.resources.max_active_batches:
            _check(self.deadline, self.cancelled)
            rows = []
            for _ in range(self.owner.resources.prepare_batch_rows):
                try:
                    row = dict(next(self.source_rows))
                except StopIteration:
                    self.source_eof = True
                    break
                rows.append(dict(
                    identity(self.query_id, row["row_id"], int(row["source_position"])),
                    reviewText=row["reviewText"],
                ))
            _check(self.deadline, self.cancelled)
            if not rows:
                break
            actor = self.owner._prepare_actors[
                self.submitted_batches % len(self.owner._prepare_actors)
            ]
            prepared_ref = actor.prepare.remote(rows, self.deadline)
            # Top-level Ray arguments are dereferenced in the model worker;
            # prepared messages never return through ray.get on the driver.
            result_ref = self.owner._model_actor.run.remote(prepared_ref, self.deadline)
            self.pending.append((result_ref, prepared_ref))
            self.submitted_batches += 1

    def __next__(self) -> dict[str, Any]:
        if self.finished:
            raise StopIteration
        try:
            while not self.ready_rows:
                if self._failure is not None:
                    raise self._failure
                _check(self.deadline, self.cancelled)
                self._submit()
                if not self.pending:
                    if self.received != set(self.expected):
                        raise ValueError("static reference omitted expected occurrences")
                    report = self.close()
                    if report["cleanup_errors"]:
                        raise RuntimeError("static source cleanup is unconfirmed")
                    raise StopIteration
                refs = [item[0] for item in self.pending]
                ready, _ = self.owner.ray.wait(
                    refs, num_returns=1,
                    timeout=min(0.05, max(0.0, self.deadline - time.monotonic())),
                    fetch_local=False,
                )
                if not ready:
                    continue
                result_ref = ready[0]
                rows = self.owner.ray.get(result_ref)
                self.pending = [item for item in self.pending if item[0] != result_ref]
                if type(rows) is dict and "error_type" in rows:
                    self._failure = RuntimeError(
                        "static model batch failed: " + str(rows["error_type"])
                    )
                    self.owner._unsettled = True
                    rows = rows["results"]
                if type(rows) is not list:
                    raise ValueError("model actor must return compact terminal rows")
                self.ready_rows.extend(rows)
            row = self.ready_rows.pop(0)
            call_id = row.get("call_id")
            expected = self.expected.get(call_id)
            if expected is None or call_id in self.received:
                raise ValueError("static reference returned an unexpected or duplicate occurrence")
            if any(row.get(key) != value for key, value in expected.items()):
                raise ValueError("static reference changed occurrence identity")
            self.received.add(call_id)
            return row
        except StopIteration:
            raise
        except BaseException as exc:
            self.owner._unsettled = self.owner._unsettled or bool(self.pending)
            report = self.close()
            if report["status"] != "closed":
                exc.add_note("static query cleanup remains unconfirmed")
            raise

    def close(self) -> dict[str, Any]:
        self.finished = True
        errors = []
        if not self.source_closed:
            try:
                close = getattr(self.source_rows, "close", None)
                if close is not None:
                    close()
                self.source_closed = True
            except BaseException as exc:
                errors.append("source_iterator_close: " + type(exc).__name__)
        if self.pending:
            self.owner._unsettled = True
            # Cancellation is a stop request, never proof of model completion.
            for result_ref, prepared_ref in self.pending:
                for ref in (result_ref, prepared_ref):
                    try:
                        self.owner.ray.cancel(ref, force=False)
                    except BaseException as exc:
                        errors.append("cancel_request: " + type(exc).__name__)
        self.last_close_report = {
            "status": "closed" if self.source_closed and not self.pending else "unknown",
            "cleanup_scope": "static_query_input_and_refs",
            "source_closed": self.source_closed,
            "pending_batches": len(self.pending),
            "received_occurrences": len(self.received),
            "expected_occurrences": len(self.expected),
            "cleanup_errors": errors,
        }
        self.owner.last_query_report = self.last_close_report
        return self.last_close_report


class SemLoomRecordsStaticReference:
    """A bounded static comparison owner, explicitly outside production Core."""

    def __init__(
        self, method: FilterMethod, engine: EngineSpec,
        generation: GenerationSpec, resources: RecordsResources,
    ):
        self.method = FilterMethod.from_dict(asdict(method))
        self.engine = EngineSpec.from_dict(asdict(engine))
        self.generation = GenerationSpec.from_dict(asdict(generation))
        self.resources = RecordsResources(**asdict(resources))
        self.ray = None
        self._prepare_actors: list[Any] = []
        self._model_actor = None
        self._model_ready = None
        self._model_close_ref = None
        self._model_close_report = None
        self._killed: set[int] = set()
        self._active: _StaticRows | None = None
        self._unsettled = False
        self._closing = False
        self.last_query_report = None
        self.last_close_report = None

    def _wait_ready(self, ref: Any, deadline: float, cancelled: Callable[[], bool] | None) -> Any:
        while True:
            _check(deadline, cancelled)
            ready, _ = self.ray.wait(
                [ref], num_returns=1,
                timeout=min(0.05, max(0.0, deadline - time.monotonic())),
                fetch_local=False,
            )
            if ready:
                return self.ray.get(ref)

    def _start(self, deadline: float, cancelled: Callable[[], bool] | None) -> None:
        if self._model_actor is not None:
            return
        import ray

        self.ray = ray
        prepare = ray.remote(_PrepareActor)
        model = ray.remote(_ModelActor)
        for _ in range(self.resources.cpu_workers):
            self._prepare_actors.append(prepare.options(
                num_cpus=1, max_restarts=0, max_task_retries=0, max_concurrency=1,
                runtime_env=_runtime_env(self.engine.driver_python),
            ).remote(asdict(self.method)))
        self._model_actor = model.options(
            num_cpus=1, num_gpus=1, max_restarts=0, max_task_retries=0, max_concurrency=1,
            runtime_env=_runtime_env(self.engine.model_python),
        ).remote(asdict(self.engine), asdict(self.generation), self.resources.model_batch_rows)
        self._model_ready = self._wait_ready(
            self._model_actor.ready.remote(deadline), deadline, cancelled
        )

    def evaluate(
        self, source: Any, *, query_id: str,
        cancelled: Callable[[], bool] | None = None,
    ) -> _StaticRows:
        if self._closing or self._unsettled:
            raise RuntimeError("static reference has closing or unconfirmed model ownership")
        if self._active is not None and not self._active.finished:
            raise RuntimeError("static reference already owns an active query")
        deadline = time.monotonic() + self.resources.query_timeout_s
        from src.semantic_methods.lotus.engine import validate_filter_source, validate_generation

        source.preflight(self.method)
        validate_generation(self.method, self.generation)
        validate_filter_source()
        self.engine.validate_driver_environment()
        expected = source.expected(query_id)
        _check(deadline, cancelled)
        import daft
        import ray
        from daft.runners import _get_runner, get_or_create_runner
        from src.infrastructure.daft_runtime import configure_daft_runner

        if ray.__version__ != "2.56.1" or not ray.is_initialized():
            raise RuntimeError("static reference requires caller-owned Ray 2.56.1")
        if daft.__version__ != "0.7.21":
            raise RuntimeError("static reference requires Daft 0.7.21 Native")
        # get_or_create_runner infers Ray when Ray is already initialized.
        # Select Native first, while preserving an existing process runner.
        if _get_runner() is None:
            if os.environ.get("DAFT_RUNNER") not in (None, "native"):
                raise RuntimeError("static reference requires an explicit Daft Native configuration")
            configure_daft_runner("native")
        if get_or_create_runner().name != "native":
            raise RuntimeError("static reference requires Daft 0.7.21 Native")
        frame = daft.read_csv(str(source.csv_path), infer_schema=False, schema={
            "row_id": daft.DataType.string(),
            "source_position": daft.DataType.int64(),
            "reviewId": daft.DataType.string(),
            "id": daft.DataType.string(),
            "reviewText": daft.DataType.string(),
        })
        frame = frame.filter(daft.col("id") == "taken_3").select(
            "row_id", "source_position", "reviewText"
        )
        if expected:
            startup_deadline = min(deadline, time.monotonic() + self.resources.startup_timeout_s)
            try:
                self._start(startup_deadline, cancelled)
            except BaseException as exc:
                self._unsettled = self._model_actor is not None
                exc.add_note("static model startup cleanup: " + str(self.close()))
                raise
        else:
            self.ray = ray
        try:
            self._active = _StaticRows(
                self, frame.iter_rows(results_buffer_size=1), expected,
                query_id, deadline, cancelled,
            )
        except BaseException as exc:
            exc.add_note("static iterator setup cleanup: " + str(self.close()))
            raise
        return self._active

    def close(self) -> dict[str, Any]:
        """Retry failed API closure; retain unknown actor handles and reports."""

        self._closing = True
        errors = []
        if self._active is not None:
            query_report = self._active.close()
            errors.extend(query_report["cleanup_errors"])
        if self._model_actor is not None and not (
            self._model_close_report or {}
        ).get("engine_api_shutdown_confirmed"):
            try:
                if self._model_close_ref is None:
                    self._model_close_ref = self._model_actor.close.remote(10.0)
                deadline = time.monotonic() + min(10.0, self.resources.startup_timeout_s)
                report = self._wait_ready(self._model_close_ref, deadline, None)
                self._model_close_ref = None
                if type(report) is not dict:
                    raise ValueError("model close must return a compact report")
                self._model_close_report = report
                if not report.get("engine_api_shutdown_confirmed"):
                    errors.append("model_engine_shutdown_unconfirmed")
            except BaseException as exc:
                if not isinstance(exc, TimeoutError):
                    self._model_close_ref = None
                errors.append("model_close: " + type(exc).__name__)
        actors = list(self._prepare_actors)
        if (self._model_close_report or {}).get("engine_api_shutdown_confirmed"):
            actors.append(self._model_actor)
        for actor in actors:
            if id(actor) in self._killed:
                continue
            try:
                self.ray.kill(actor, no_restart=True)
                self._killed.add(id(actor))
            except BaseException as exc:
                errors.append("actor_kill_request: " + type(exc).__name__)
        empty = self._model_actor is None
        gpu_exit = True if empty else (self._model_close_report or {}).get("gpu_exit_confirmed")
        owned_actors = list(self._prepare_actors)
        if self._model_actor is not None:
            owned_actors.append(self._model_actor)
        actors_requested = all(id(actor) in self._killed for actor in owned_actors)
        settled = not errors and actors_requested and (empty or bool(
            (self._model_close_report or {}).get("engine_api_shutdown_confirmed")
        )) and gpu_exit is True
        self.last_close_report = {
            "implementation": "semloom_records_static_reference",
            "production_core_connected": False,
            "status": "closed" if settled else "unknown",
            "cleanup_scope": "ray_actor_and_engine_api",
            "model_ready": self._model_ready,
            "engine_close_report": self._model_close_report,
            "gpu_exit_confirmed": gpu_exit,
            "actor_kill_requests_confirmed": actors_requested,
            "model_actor_handle_retained": self._model_actor is not None,
            "last_query_report": self.last_query_report,
            "cleanup_errors": errors,
        }
        return self.last_close_report
