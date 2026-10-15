"""Sequential LOTUS queries with group-owned reusable runtimes."""

from contextlib import contextmanager, ExitStack
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import threading
import time
import uuid

from src.baselines.common.private_artifacts import (
    new_private_directory,
    open_private_text,
    write_private_json,
)
from src.baselines.common.redact import redact_json_values
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.experiments.buffered_events import BufferedEvents
from src.experiments.execution_timing import is_timing_error, timed_call, timing_span
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from src.scheduling.core.session_contract import SessionTimeouts
from .cell_evidence import CellErrors
from .native_adapter_query import _runtime, resolve_adapter_limits
from .query_deadline import validate_owner_timeouts
from .supplier_adapter_query import (
    SUPPLIER_ARMS,
    NATIVE_SUPPLIER_BACKENDS,
    _drain_execution,
    prepare_lotus_lm,
    run_supplier_query,
    validate_lotus_observation_mode,
)


class _ArmOwner:
    def __init__(self, group, arm):
        self.group, self.arm = group, arm
        self.identity = uuid.uuid4().hex
        self.root = group.root / "owners" / arm
        new_private_directory(self.root)
        self.stack = ExitStack()
        self._close_errors = CellErrors()
        self._closed = False
        self.queries = 0
        self.binding = None
        self.execution = self.lm = None
        self.physical = group.physical if arm == "lotus-method-semloom" else None
        try:
            self.events = self._enter_component(
                BufferedEvents(self.root / "owner-events.jsonl"), "owner_events_close"
            )
            self.gateway = self._enter_component(
                ObservationGateway(
                    routes=(GatewayRoute(self.identity, "model", group.model.endpoint_url),),
                    trace_path=self.root / "http-trace.jsonl",
                    request_timeout_s=(
                        group.model.timeout_ms / 1000
                        if group.query_timeout_s is None
                        else min(group.query_timeout_s, group.model.timeout_ms / 1000)
                    ),
                    preserve_http=True,
                    before_forward=self.before,
                    after_forward=self.after,
                    query_identity=lambda: self.binding["unit_id"] if self.binding else None,
                ),
                "owner_gateway_close",
            )
            token = group.model.bearer_token or "local-fixture"
            self.model = replace(
                group.model,
                endpoint_url=self.gateway.endpoint_url(self.identity, "model"),
                bearer_token=token,
            )
            if arm == "lotus-method-semloom":
                self.execution = build_native_execution(
                    self.model,
                    physical=self.physical,
                    max_tasks=group.max_held_tasks,
                    max_active_requests=group.options.concurrency,
                    observer=self.observe,
                    timeouts=SessionTimeouts(
                        backend_s=120
                        if group.query_timeout_s is None
                        else max(45, group.query_timeout_s)
                    ),
                )
                self.stack.callback(
                    self._close_errors.attempt,
                    "owner_execution_close",
                    lambda: _drain_execution(self.execution),
                )
            self.lotus_lm()
        except BaseException as failure:
            self._close_errors.attempt("owner_components_close", self.stack.close)
            if self._close_errors.first is not None:
                if is_timing_error(failure) and not is_timing_error(self._close_errors.first):
                    self._close_errors.first.add_note(
                        "Detailed timing also failed: " + type(failure).__name__
                    )
                    raise self._close_errors.first
                failure.add_note(
                    "Persistent owner initialization cleanup also failed: "
                    + type(self._close_errors.first).__name__
                )
            raise
        self.started_ns = time.monotonic_ns()

    def _capture_component_close(self, phase):
        def collect(_error_type, error, _traceback):
            if error is not None:
                self._close_errors.record(phase, error)
                return True
            return False

        self.stack.push(collect)

    def _enter_component(self, component, phase):
        # Each exit is collected before another component can replace its error.
        self._capture_component_close(phase)
        return self.stack.enter_context(component)

    def before(self, route, body):
        if self.binding is None:
            raise RuntimeError("persistent owner has no active query")
        return self.binding["before"](route, body)

    def after(self, route, body, response, status):
        if self.binding is None:
            raise RuntimeError("persistent owner lost its response consumer")
        return self.binding["after"](route, body, response, status)

    def observe(self, event):
        value = dict(event, monotonic_ns=time.monotonic_ns())
        value["query_id"] = self.binding["unit_id"] if self.binding else None
        self.events.record(value)
        if self.binding is not None:
            self.binding["record"](event)

    def lifecycle(self):
        backend = NATIVE_SUPPLIER_BACKENDS.get(self.arm)
        return dict(
            owner_id=self.identity,
            ray_session_id=self.group.ray_session_id,
            execution_id=None if self.execution is None else str(id(self.execution)),
            lm_id=None if self.lm is None else str(id(self.lm)),
            completed_queries=self.queries,
            result_cache=False,
            core_usage=None
            if self.execution is None
            else asdict(self.execution.engine.capacity.usage()),
            core_jobs=None if self.execution is None else len(self.execution.engine.jobs.jobs),
            native_ray_actor_scope=(
                "original Ray Data graph creates its own actors"
                if backend == "ray-data"
                else "Daft Native owns per-query batch function graph"
                if backend == "daft"
                else "group-owned SemLoom workers"
                if self.execution is not None
                else "supplier SDK/client owns execution"
            ),
            native_http_client_scope=(
                "native graph HTTP kernels; LOTUS SDK parser receives full responses"
                if backend is not None
                else "supplier owns its SDK/client lifecycle"
            ),
        )

    @contextmanager
    def query(self, unit_id, root, before, after, record):
        if self.binding is not None:
            raise RuntimeError("one persistent arm cannot overlap query consumers")
        self.binding = dict(unit_id=unit_id, before=before, after=after, record=record)
        errors = CellErrors()
        primary = None
        try:
            yield self
        except BaseException as error:
            primary = error
            raise
        finally:
            if self.execution is not None:
                errors.attempt(
                    "query_core_drain", lambda: _drain_execution(self.execution, close=False)
                )
            traces = errors.attempt(
                "query_http_drain",
                lambda: self.gateway.snapshot(
                    self.group.model.timeout_ms / 1000 + 1, query_id=unit_id
                ),
            )
            if traces is not None:

                def save_trace():
                    with open_private_text(Path(root) / "http-trace.jsonl") as stream:
                        for row in traces:
                            stream.write(json.dumps(redact_json_values(row), sort_keys=True) + "\n")
                    return True

                if errors.attempt("query_trace_write", save_trace):
                    errors.attempt(
                        "query_trace_archive", lambda: self.gateway.archive_query(unit_id, traces)
                    )
            self.binding = None
            if errors.first is not None:
                self.group.poisoned = True
                if primary is not None:
                    if is_timing_error(primary) and not is_timing_error(errors.first):
                        errors.first.add_note(
                            "Detailed timing also failed: " + type(primary).__name__
                        )
                        errors.raise_if_failed()
                    primary.add_note(
                        "Persistent query cleanup also failed: " + type(errors.first).__name__
                    )
                else:
                    errors.raise_if_failed()

    def lotus_lm(self):
        if self.lm is None:
            if getattr(self.group, "sembench_dataset", None) is None:
                self.lm = prepare_lotus_lm(
                    self.group.plan, self.model, self.group.options, self.group.tokenizer_path
                )
            else:
                self.lm = prepare_lotus_lm(
                    self.group.plan,
                    self.model,
                    self.group.options,
                    self.group.tokenizer_path,
                    sembench=True,
                )
        return self.lm

    @timed_call("owner.arm_close")
    def close(self):
        if self._closed:
            return
        errors = self._close_errors
        errors.attempt("owner_components_close", self.stack.close)
        lifecycle = errors.attempt("owner_lifecycle", self.lifecycle)
        errors.attempt(
            "owner_summary",
            lambda: write_private_json(
                self.root / "owner-summary.json",
                redact_json_values(
                    dict(
                        lifecycle=lifecycle,
                        cleanup_errors=errors.details,
                        closed_ns=time.monotonic_ns(),
                    )
                ),
            ),
        )
        errors.raise_if_failed()
        self._closed = True


class PersistentAdapterGroup:
    """At most three comparison arms share one driver; each arm owns its backend."""

    def __init__(
        self,
        arms,
        *,
        plan,
        model,
        ledger,
        root,
        options=NativeGraphOptions(),
        physical=None,
        ray_temp_root=None,
        ray_address=None,
        query_timeout_s=120,
        owner_timeout_s=1800,
        tokenizer_path=None,
        max_held_tasks=None,
        sembench_dataset=None,
        lotus_observation_mode="qualification",
    ):
        self.arms = tuple(arms)
        if (
            not 1 <= len(self.arms) <= 3
            or len(set(self.arms)) != len(self.arms)
            or any(arm not in SUPPLIER_ARMS for arm in self.arms)
        ):
            raise ValueError("persistent measurement requires one to three distinct LOTUS arms")
        self.lotus_observation_mode = validate_lotus_observation_mode(lotus_observation_mode)
        self.plan, self.model, self.ledger = plan, model, ledger
        self.sembench_dataset = sembench_dataset
        if sembench_dataset is not None:
            from .sembench_queries import MovieDataset

            if type(sembench_dataset) is not MovieDataset or plan.max_tokens != 8:
                raise ValueError(
                    "original Movie owners require the selected dataset and Filter generation"
                )
        validate_owner_timeouts(
            query_timeout_s, owner_timeout_s, completion_allowed=sembench_dataset is not None
        )
        self.root, self.options, self.physical = Path(root), options, physical
        self.max_held_tasks = resolve_adapter_limits(options, max_held_tasks)
        for arm in self.arms:
            if NATIVE_SUPPLIER_BACKENDS.get(arm) == "ray-data":
                options.validate_backend("ray-data")
        self.ray_temp_root, self.ray_address = ray_temp_root, ray_address
        self.query_timeout_s, self.owner_timeout_s = query_timeout_s, owner_timeout_s
        self.tokenizer_path = tokenizer_path
        self.stack = ExitStack()
        self._close_errors = CellErrors()
        self.owners, self.used_units = {}, set()
        self.poisoned = False
        self.ray_session_id = None
        self.runtime = dict(owner="persistent group", source="finite raw input")
        self.daft_runtime = None
        self._lock = threading.Lock()

    def __enter__(self):
        if self.sembench_dataset is not None:
            self.sembench_preflight = self.sembench_dataset.preflight()
        self.started_ns = time.monotonic_ns()
        self.deadline = (
            None if self.owner_timeout_s is None else time.monotonic() + self.owner_timeout_s
        )
        new_private_directory(self.root)
        try:
            with timing_span("owner.startup"):
                if any(NATIVE_SUPPLIER_BACKENDS.get(arm) == "daft" for arm in self.arms):
                    _, self.daft_runtime = _runtime(
                        self.stack, "fixed-map-native-daft", self.options, None, None, None
                    )
                core = "lotus-method-semloom" in self.arms
                if core or any(
                    NATIVE_SUPPLIER_BACKENDS.get(arm) == "ray-data" for arm in self.arms
                ):
                    if core and self.physical is None:
                        raise ValueError(
                            "persistent SemLoom requires the existing physical configuration"
                        )
                    self.physical, self.runtime = _runtime(
                        self.stack,
                        "fixed-map-semloom" if core else "fixed-map-native-ray",
                        self.options,
                        self.physical,
                        self.ray_temp_root,
                        self.ray_address,
                    )
                    self.ray_session_id = uuid.uuid4().hex
                for arm in self.arms:
                    with timing_span("owner.arm_prepare", arm=arm):
                        owner = _ArmOwner(self, arm)
                        self.owners[arm] = owner
                        self.stack.callback(
                            self._close_errors.attempt, "group_owner_close." + arm, owner.close
                        )
                self.ready_ns = time.monotonic_ns()
                write_private_json(
                    self.root / "startup.json",
                    redact_json_values(
                        dict(
                            schema="semloom.persistent_adapter_group.v1",
                            arms=self.arms,
                            started_ns=self.started_ns,
                            ready_ns=self.ready_ns,
                            startup_seconds=(self.ready_ns - self.started_ns) / 1e9,
                            runtime=self.runtime,
                            ray_session_id=self.ray_session_id,
                            owner_timeout_s=self.owner_timeout_s,
                            cleanup_reserve_s=120,
                            max_held_tasks=self.max_held_tasks,
                            lotus_observation_mode=self.lotus_observation_mode,
                            hard_timeout_owner="calling process supervisor",
                            result_cache=False,
                        )
                    ),
                )
                return self
        except BaseException as failure:
            self._close_errors.attempt("group_close", self.stack.close)
            if self._close_errors.first is not None:
                if is_timing_error(failure) and not is_timing_error(self._close_errors.first):
                    self._close_errors.first.add_note(
                        "Detailed timing also failed: " + type(failure).__name__
                    )
                    raise self._close_errors.first
                failure.add_note(
                    "Persistent group initialization cleanup also failed: "
                    + type(self._close_errors.first).__name__
                )
            raise

    @timed_call("owner.query_call")
    def run(
        self,
        arm,
        *,
        unit_id,
        root,
        load_source=None,
        phase,
        reference_outputs=None,
        allowed_outputs=None,
        sembench_query=None,
    ):
        if phase not in ("qualification", "warmup", "measurement"):
            raise ValueError("query phase must be explicitly declared")
        if self.sembench_dataset is not None and sembench_query is None:
            raise ValueError("original Movie owner requires an explicit original query")
        with self._lock:
            if self.poisoned or arm not in self.owners or unit_id in self.used_units:
                raise RuntimeError("persistent group stopped or query identity already used")
            if (
                self.deadline is not None
                and time.monotonic() + self.query_timeout_s + 120 >= self.deadline
            ):
                raise TimeoutError("persistent group cannot fit another query and final cleanup")
            self.used_units.add(unit_id)
            owner = self.owners[arm]
            resources_before = self.ray_resources()
            arguments = dict(
                load_source=load_source,
                plan=self.plan,
                model=self.model,
                ledger=self.ledger,
                unit_id=unit_id,
                root=root,
                options=self.options,
                physical=owner.physical,
                max_held_tasks=self.max_held_tasks,
                query_timeout_s=self.query_timeout_s,
                reference_outputs=reference_outputs,
                allowed_outputs=allowed_outputs,
                owner=owner,
            )
            try:
                result = run_supplier_query(
                    arm,
                    **arguments,
                    tokenizer_path=self.tokenizer_path,
                    lotus_observation_mode=self.lotus_observation_mode,
                    sembench_query=sembench_query,
                )
                owner.queries += 1
                result["persistent_lifecycle"] = owner.lifecycle()
                result["measurement_phase"] = phase
                write_private_json(
                    Path(root) / "persistent-query.json",
                    redact_json_values(
                        dict(
                            schema="semloom.persistent_adapter_query.v1",
                            unit_id=unit_id,
                            arm=arm,
                            measurement_phase=phase,
                            persistent_lifecycle=owner.lifecycle(),
                            query_summary_sha256=hashlib.sha256(
                                (Path(root) / "summary.json").read_bytes()
                            ).hexdigest(),
                            t_release_ns=result["query_preparation_started_ns"],
                            t_submit_ns=result["execution"]["t_submit_ns"],
                            t_eof_ns=result["execution"]["t_query_terminal_ns"],
                            release_query_seconds=result["full_query_seconds"],
                            release_scope="raw input reading, query preparation, formatter, graph, materialization and consumer EOF",
                            submit_scope=result["execution"]["ready_query_timing_scope"],
                            lotus_observation_mode=result.get("lotus_observation_mode"),
                            actual_posts=result["actual_posts"],
                            rows=result["rows"],
                        )
                    ),
                )
                write_private_json(
                    Path(root) / "runtime-resources.json",
                    dict(
                        before_query=resources_before,
                        after_query=self.ray_resources(),
                        scope="same group Ray cluster; other persistent arm actors retain their declared CPUs",
                        resource_matching=(
                            "dedicated arm Ray cluster"
                            if len(self.arms) == 1
                            else "shared deployment recorded; not a dedicated per-arm cluster"
                        ),
                    ),
                )
                return result
            except BaseException as failure:
                self.poisoned = True
                summary_path = Path(root) / "summary.json"
                sidecar = Path(root) / "persistent-query.json"
                if summary_path.is_file() and not sidecar.exists():
                    try:
                        write_private_json(
                            sidecar,
                            dict(
                                schema="semloom.persistent_adapter_query.v1",
                                status="failed",
                                unit_id=unit_id,
                                arm=arm,
                                measurement_phase=phase,
                                lotus_observation_mode=self.lotus_observation_mode
                                if arm.startswith("lotus-")
                                else None,
                                persistent_lifecycle=owner.lifecycle(),
                                query_summary_sha256=hashlib.sha256(
                                    summary_path.read_bytes()
                                ).hexdigest(),
                            ),
                        )
                    except BaseException as observation:
                        failure.add_note(
                            "Persistent query metadata also failed: " + type(observation).__name__
                        )
                raise

    def ray_resources(self):
        if self.ray_session_id is None:
            return dict(status="unavailable", reason="this group uses no Ray runtime")
        import ray

        keys = ("CPU", "GPU", "object_store_memory")
        total, available = ray.cluster_resources(), ray.available_resources()
        return dict(
            cluster={key: total.get(key, 0) for key in keys},
            available={key: available.get(key, 0) for key in keys},
            semloom_actor_pools=[
                dict(arm=arm, workers=owner.physical.workers, declared_cpus=owner.physical.workers)
                for arm, owner in self.owners.items()
                if owner.physical is not None and owner.execution is not None
            ],
        )

    @timed_call("owner.close", finalize=True)
    def __exit__(self, error_type, error, traceback):
        errors = self._close_errors
        errors.attempt("group_close", self.stack.close)
        owners = {
            arm: errors.attempt("group_lifecycle." + arm, owner.lifecycle)
            for arm, owner in self.owners.items()
        }
        errors.attempt(
            "group_summary",
            lambda: write_private_json(
                self.root / "group-summary.json",
                redact_json_values(
                    dict(
                        status="failed"
                        if error is not None or self.poisoned or errors.first is not None
                        else "passed",
                        queries=len(self.used_units),
                        poisoned=self.poisoned,
                        ended_ns=time.monotonic_ns(),
                        cleanup_errors=errors.details,
                        owners=owners,
                    )
                ),
            ),
        )
        if errors.first is not None:
            if error is not None:
                if is_timing_error(error) and not is_timing_error(errors.first):
                    errors.first.add_note("Detailed timing also failed: " + type(error).__name__)
                    errors.raise_if_failed()
                error.add_note(
                    "Persistent group cleanup also failed: " + type(errors.first).__name__
                )
            else:
                errors.raise_if_failed()
