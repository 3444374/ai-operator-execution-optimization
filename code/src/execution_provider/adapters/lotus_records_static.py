"""Finite LOTUS text Filter reference; this path does not use production Core.

Daft Native scans raw records. Ray Core preparation actors return ObjectRefs
directly to a single model actor. The driver receives only compact terminal rows.
"""

from __future__ import annotations

import os
import time
from dataclasses import asdict
from typing import Any, Callable, Iterator

from src.execution_provider.adapters.lotus_records_actor_owner import (
    RecordsActorOwner, _ModelActor, _PrepareActor, _check, _runtime_env,
)
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, RecordsResources, identity
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


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
                rows.append(dict(row, **identity(
                    self.query_id, row["row_id"], int(row["source_position"]),
                )))
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
        self._actor_owner = RecordsActorOwner(
            self.method, self.engine, self.generation, self.resources,
        )
        self._active: _StaticRows | None = None
        self._unsettled = False
        self._closing = False
        self.last_query_report = None
        self.last_close_report = None

    @property
    def ray(self) -> Any:
        return self._actor_owner.ray

    @ray.setter
    def ray(self, value: Any) -> None:
        self._actor_owner.ray = value

    @property
    def _prepare_actors(self) -> list[Any]:
        return self._actor_owner.prepare_actors

    @property
    def _model_actor(self) -> Any:
        return self._actor_owner.model_actor

    @property
    def _model_ready(self) -> Any:
        return self._actor_owner.model_ready

    def _wait_ready(self, ref: Any, deadline: float, cancelled: Callable[[], bool] | None) -> Any:
        return self._actor_owner.wait_ready(ref, deadline, cancelled)

    def _start(self, deadline: float, cancelled: Callable[[], bool] | None) -> None:
        self._actor_owner.engine = self.engine
        self._actor_owner.generation = self.generation
        self._actor_owner.resources = self.resources
        self._actor_owner.start(deadline, cancelled)

    def evaluate(
        self, source: Any, *, query_id: str,
        cancelled: Callable[[], bool] | None = None,
    ) -> _StaticRows:
        if self._closing or self._unsettled:
            raise RuntimeError("static reference has closing or unconfirmed model ownership")
        if self._active is not None and not self._active.finished:
            raise RuntimeError("static reference already owns an active query")
        deadline = time.monotonic() + self.resources.query_timeout_s
        from src.semantic_methods.lotus.engine import bind_filter_method, validate_generation

        source.preflight(self.method)
        scan = source.scan_spec(self.method)
        if not isinstance(scan, RecordScan):
            raise TypeError("source.scan_spec(method) must return RecordScan")
        scan.describe()
        validate_generation(self.method, self.generation)
        self._actor_owner.method = bind_filter_method(self.method)
        if not set(self._actor_owner.method.resolved_input_columns).issubset(scan.payload_columns):
            raise ValueError("record scan omits Filter input columns")
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
        types = {"string": daft.DataType.string(), "int64": daft.DataType.int64()}
        frame = daft.read_csv(str(source.csv_path), infer_schema=False, schema={
            name: types[dtype] for name, dtype in scan.csv_schema
        })
        if scan.predicate is not None:
            column, value = scan.predicate
            frame = frame.filter(daft.col(column) == value)
        frame = frame.select("row_id", "source_position", *scan.payload_columns)
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
        actor_report = self._actor_owner.close()
        errors.extend(actor_report["cleanup_errors"])
        self.last_close_report = {
            "implementation": "semloom_records_static_reference",
            "production_core_connected": False,
            **actor_report,
            "status": "closed" if actor_report["status"] == "closed"
            and not self._unsettled and not errors else "unknown",
            "last_query_report": self.last_query_report,
            "cleanup_errors": errors,
        }
        return self.last_close_report
