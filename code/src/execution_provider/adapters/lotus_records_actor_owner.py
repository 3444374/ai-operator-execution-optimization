"""Own finite LOTUS preparation/model actors without owning query scheduling.

Callers retain their source, pending ObjectRefs, occurrence ledger and query
closure. This owner only allocates actors, waits for readiness/close metadata and
requests actor shutdown. Physical GPU/process exit belongs to the supervisor.
"""

from __future__ import annotations

import math
import shlex
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable

from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


_CODE_ROOT = Path(__file__).resolve().parents[3]


def _runtime_env(python: str) -> dict[str, Any]:
    return {
        "py_executable": shlex.quote(python),
        "env_vars": {"PYTHONPATH": str(_CODE_ROOT)},
    }


def _check(deadline: float, cancelled: Callable[[], bool] | None = None) -> None:
    # Keep the existing actors' exception semantics for the static caller.
    if cancelled is not None and cancelled():
        raise InterruptedError("Movie Q3 static reference cancelled")
    if time.monotonic() >= deadline:
        raise TimeoutError("Movie Q3 static reference deadline expired")


def _deadline(value: float) -> None:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError("actor deadline must be finite and positive; bool is not accepted")


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
        self._engine_start_attempted = False
        self._engine_start_failed = False

    def ready(self, deadline: float) -> dict[str, Any]:
        from src.serving.backends.vllm_engine import VllmEngine

        _check(deadline)
        if self.engine is None:
            if self._engine_start_attempted:
                raise RuntimeError("model startup is unconfirmed; owner must stop")
            self._engine_start_attempted = True
            try:
                self.engine = VllmEngine(self.engine_spec, self.generation)
            except BaseException:
                self._engine_start_failed = True
                raise
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
                # Preserve completed sub-batches for the caller's own ledger.
                return dict(results=results, error_type=type(exc).__name__,
                            unresolved_calls=len(rows) - start)
        return results

    def close(self, timeout: float) -> dict[str, Any]:
        if self.engine is None:
            if self._engine_start_attempted:
                return dict(status="unknown", engine_api_shutdown_confirmed=False, gpu_exit_confirmed=None)
            return dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=True)
        return self.engine.close(timeout=timeout)


class RecordsActorOwner:
    """A reusable model lifetime; callers own all query/occurrence state.

    ``prepare_actors`` return ObjectRefs for direct submission to ``model_actor``.
    ``wait_ready`` is for compact actor metadata, never prepared messages.
    Closing retains handles and an outstanding close ref when exit is unknown;
    it does not shut down the caller's Ray runtime.
    """

    def __init__(
        self, method: FilterMethod, engine: EngineSpec,
        generation: GenerationSpec, resources: RecordsResources,
    ):
        self.method = FilterMethod.from_dict(asdict(method))
        self.engine = EngineSpec.from_dict(asdict(engine))
        self.generation = GenerationSpec.from_dict(asdict(generation))
        self.resources = RecordsResources(**asdict(resources))
        self.ray = None
        self.prepare_actors: list[Any] = []
        self.model_actor = None
        self.model_ready = None
        self._model_close_ref = None
        self._model_close_report = None
        self._killed: set[int] = set()
        self._closing = False
        self._start_failed = False
        self.last_close_report = None

    def _model_actor_type(self) -> type[_ModelActor]:
        return _ModelActor

    def wait_ready(
        self, ref: Any, deadline: float, cancelled: Callable[[], bool] | None = None,
    ) -> Any:
        _deadline(deadline)
        while True:
            _check(deadline, cancelled)
            ready, _ = self.ray.wait(
                [ref], num_returns=1,
                timeout=min(0.05, max(0.0, deadline - time.monotonic())),
                fetch_local=False,
            )
            if ready:
                return self.ray.get(ref)

    def start(
        self, deadline: float, cancelled: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        if self._closing or self._start_failed:
            raise RuntimeError("records actors have closing or unconfirmed model ownership")
        _deadline(deadline)
        if self.model_actor is not None:
            return self.model_ready
        _check(deadline, cancelled)
        from src.semantic_methods.lotus.engine import validate_filter_source, validate_generation
        import ray

        validate_generation(self.method, self.generation)
        validate_filter_source()
        self.engine.validate_driver_environment()
        if ray.__version__ != "2.56.1" or not ray.is_initialized():
            raise RuntimeError("records actors require caller-owned Ray 2.56.1")
        _check(deadline, cancelled)
        self.ray = ray
        try:
            prepare = ray.remote(_PrepareActor)
            model = ray.remote(self._model_actor_type())
            for _ in range(self.resources.cpu_workers):
                self.prepare_actors.append(prepare.options(
                    num_cpus=1, max_restarts=0, max_task_retries=0, max_concurrency=1,
                    runtime_env=_runtime_env(self.engine.driver_python),
                ).remote(asdict(self.method)))
            self.model_actor = model.options(
                num_cpus=1, num_gpus=1, max_restarts=0, max_task_retries=0, max_concurrency=1,
                runtime_env=_runtime_env(self.engine.model_python),
            ).remote(asdict(self.engine), asdict(self.generation), self.resources.model_batch_rows)
            self.model_ready = self.wait_ready(
                self.model_actor.ready.remote(deadline), deadline, cancelled,
            )
            return self.model_ready
        except BaseException:
            self._start_failed = True
            raise

    def close(self) -> dict[str, Any]:
        """Retry API closure; actor kill requests never prove physical GPU exit."""

        self._closing = True
        errors = []
        if self.model_actor is not None and not (
            self._model_close_report or {}
        ).get("engine_api_shutdown_confirmed"):
            try:
                if self._model_close_ref is None:
                    self._model_close_ref = self.model_actor.close.remote(10.0)
                deadline = time.monotonic() + min(10.0, self.resources.startup_timeout_s)
                report = self.wait_ready(self._model_close_ref, deadline)
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
        actors = list(self.prepare_actors)
        if (self._model_close_report or {}).get("engine_api_shutdown_confirmed"):
            actors.append(self.model_actor)
        for actor in actors:
            if id(actor) in self._killed:
                continue
            try:
                self.ray.kill(actor, no_restart=True)
                self._killed.add(id(actor))
            except BaseException as exc:
                errors.append("actor_kill_request: " + type(exc).__name__)
        empty = self.model_actor is None
        gpu_exit = True if empty else (self._model_close_report or {}).get("gpu_exit_confirmed")
        owned_actors = list(self.prepare_actors)
        if self.model_actor is not None:
            owned_actors.append(self.model_actor)
        actors_requested = all(id(actor) in self._killed for actor in owned_actors)
        settled = not errors and actors_requested and (empty or bool(
            (self._model_close_report or {}).get("engine_api_shutdown_confirmed")
        )) and gpu_exit is True
        self.last_close_report = {
            "status": "closed" if settled else "unknown",
            "cleanup_scope": "ray_actor_and_engine_api",
            "model_ready": self.model_ready,
            "engine_close_report": self._model_close_report,
            "gpu_exit_confirmed": gpu_exit,
            "actor_kill_requests_confirmed": actors_requested,
            "model_actor_handle_retained": self.model_actor is not None,
            "cleanup_errors": errors,
        }
        return self.last_close_report
