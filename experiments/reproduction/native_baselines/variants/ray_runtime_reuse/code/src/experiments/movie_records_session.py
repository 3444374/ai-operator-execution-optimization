"""Consecutive complete Movie queries; existing backends retain scheduling.

This worker is called inside the existing owner's lease/resource supervisor.
It creates no lease, budget, retries, detached workers, or performance claims.
Native, Core and opt-in IMLane CPython sessions expose reusable model owners.
Ray can reuse its runtime while each native Dataset still owns a new model.
The explicit ``isolated`` mode retains per-query driver startup and shutdown.
"""

from dataclasses import replace
from pathlib import Path
import time
import traceback
import uuid

from src.baselines.common.private_artifacts import new_private_directory, write_private_json, publish_private_json
from src.baselines.common.redact import redact_json_values


RESIDENT = frozenset({"lotus-native", "semloom-core", "imlane"})
BACKENDS = RESIDENT | {"ray-data", "daft", "imlane"}
LIFECYCLES = {"resident", "isolated", "runtime_reuse_only"}
# This lists implemented adapter paths, not every capability of each vendor.


def _failure(error):
    return redact_json_values(dict(error_type=type(error).__name__, error=str(error),
        traceback="".join(traceback.format_exception(type(error), error, error.__traceback__)),
        notes=list(getattr(error, "__notes__", ()))))


def _lifecycle_timing(report):
    """Compare recorded phases without treating driver start as model loading."""
    def total(values):
        return None if any(v is None for v in values) else sum(values)
    owners=[]
    for owner in report["owners"]:
        first=next((q for q in report["queries"] if q["query_id"]==owner["first_query_id"]),None)
        ready=owner.get("ready")
        ready=ready if isinstance(ready,dict) else {}
        runtime_only=ready.get("no_session_model") is True
        begin=None if first is None else min(owner["owner_start_ns"],first["query_start_ns"])
        def cold(name):
            end=None if first is None else first.get(name)
            return None if begin is None or end is None else (end-begin)/1e9
        overlap=None
        if first is not None and first.get("query_api_return_ns") is not None and owner.get("owner_ready_ns") is not None:
            overlap=max(0,min(owner["owner_ready_ns"],first["query_api_return_ns"])
                        -max(owner["owner_start_ns"],first["query_start_ns"]))/1e9
        owners.append(dict(owner_id=owner["owner_id"],first_query_id=owner["first_query_id"],
            owner_startup_scope=(None if not ready else "driver_runtime_only" if runtime_only else "backend_start"),
            model_initialization_scope=ready.get("model_startup_scope",None if not ready else "backend_start"),
            startup_seconds=owner.get("startup_seconds"),shutdown_seconds=owner.get("shutdown_seconds"),
            startup_already_in_query_api_seconds=overlap,
            cold_first_result_seconds=cold("query_result_ready_ns"),
            cold_first_api_return_seconds=cold("query_api_return_ns")))
    native_starts=[]
    for query in report["queries"]:
        backend=query.get("backend_report")
        close=backend.get("backend_close") if isinstance(backend,dict) else None
        records=close.get("engine_reports") if isinstance(close,dict) else None
        measured=[]
        for record in records if isinstance(records,list) else ():
            if not isinstance(record,dict):continue
            ready=record.get("engine_ready")
            uid=ready.get("engine_uid") if isinstance(ready,dict) else None
            seconds=record.get("startup_seconds")
            measured.append(dict(engine_uid=uid,startup_seconds=(seconds
                if uid and type(seconds) in (int,float) and seconds>=0 else None)))
        native_starts.append(dict(query_id=query["query_id"],reports=measured,
            source="backend_close.engine_reports; private process startup includes readiness IPC",
            missing_reason=None if measured else "backend did not expose a native constructor span"))
    return dict(owners=owners,native_query_model_startup=native_starts,
        owner_startup_seconds_sum=total([o["startup_seconds"] for o in owners]),
        owner_shutdown_seconds_sum=total([o["shutdown_seconds"] for o in owners]),
        owner_startup_already_in_query_api_seconds=total([o["startup_already_in_query_api_seconds"] for o in owners]),
        query_api_return_seconds_sum=total([q["query_api_return_seconds"] for q in report["queries"]]),
        query_api_timing_count=sum(q["query_api_return_ns"] is not None for q in report["queries"]),
        query_api_scope="query_start to immediate driver.query normal return; checkpoint and owner close separate",
        cold_first_scope="earlier owner/query start to first result or driver return; preflight before owner start separate",
        overlap_rule="owner startup may already be in query API time; components are not blindly additive",
        model_load_count_observed=report["model_load_count_observed"])


def run_sequence(queries, factory, output, *, backend, lifecycle="resident", observer=None, clock=None,
                 setup=None, checkpoint=None):
    """Run one ready owner or consecutive isolated owners, stopping on any failure.

    ``factory(directory, first_query)`` allocates no resources in its constructor. Its
    driver supplies start(), query(spec, emit), and close(). query must emit
    result_ready once after the complete result, before quality evaluation.
    Only the driver closes resources it created. Caller-owned leases and
    supervisors are never closed here. All durable evidence is redacted.
    An optional synchronous lifecycle checkpoint runs after a complete query is
    durably published, before advancing or closing its owner. Its failure stops
    the session; diagnostic observer failures remain noncritical.
    """
    if backend not in BACKENDS or lifecycle not in LIFECYCLES:
        raise ValueError("unsupported backend or lifecycle")
    if lifecycle == "runtime_reuse_only" and backend != "ray-data":
        raise ValueError("runtime_reuse_only is implemented only for ray-data")
    if lifecycle == "resident" and backend not in RESIDENT:
        raise ValueError(backend + " has no cross-query model reuse implemented by this adapter; select isolated")
    queries = tuple(dict(query) for query in queries)
    ids = [query["query_id"] for query in queries]
    if (not queries or len(ids) != len(set(ids)) or any(type(q) is not str or not q for q in ids)
            or any(query.get("phase") not in {"warmup", "measured"} for query in queries)):
        raise ValueError("declare unique nonempty query IDs and explicit warmup/measured phases")
    now = time.monotonic_ns if clock is None else clock
    output = Path(output)
    new_private_directory(output)
    session_id = uuid.uuid4().hex
    report = dict(schema="semloom.movie_records_session.v1", session_id=session_id,
        backend=backend, lifecycle=lifecycle, status="failed", performance_claim=False,
        cross_query_model_reuse_requested=lifecycle == "resident",
        model_load_count_observed=None, model_load_count_reason="requires model/runtime observation",
        automatic_retries=0, session_start_ns=now(), events=[], observer_errors=[], owners=[], queries=[],
        setup=setup, application_scope="sequence after preflight through final lifecycle event; final session.json publication and caller return separate",
        skipped_queries=[], clock="time.monotonic_ns, worker process", warmup_implicit=False,
        query_scope="fresh source creation/read/preflight, native prepare/operator, complete projection/aggregation",
        post_result_scope="quality evaluation and query iterator/client cleanup; final owner shutdown separate")
    write_private_json(output / "session-start.json", redact_json_values(dict(
        schema=report["schema"], session_id=session_id, backend=backend, lifecycle=lifecycle,
        queries=queries, session_start_ns=report["session_start_ns"])))
    primary = driver = owner = None

    def emit(name, *, query=None):
        owner_id = (query.get("owner_id") if query is not None else
                    None if owner is None else owner["owner_id"])
        event = dict(event=name, monotonic_ns=now(), session_id=session_id,
                     owner_id=owner_id,
                     query_id=None if query is None else query["query_id"])
        report["events"].append(event)
        if query is not None:
            query["events"].append(event)
            if name == "result_ready":
                if query.get("query_result_ready_ns") is not None:
                    raise ValueError("backend emitted duplicate complete-result events")
                query["query_result_ready_ns"] = event["monotonic_ns"]
        if observer is not None:
            try:
                observer(dict(event))
            except Exception as error:
                report["observer_errors"].append(_failure(error))
        return event["monotonic_ns"]

    def open_owner(spec):
        nonlocal driver, owner
        owner = dict(owner_id=uuid.uuid4().hex, status="starting", start_invocations=1,
                     first_query_id=spec["query_id"])
        report["owners"].append(owner)
        # Constructor precedes start but remains inside the reported startup.
        owner["owner_start_ns"] = emit("session_owner_start")
        directory = output / ("owner-" + str(len(report["owners"]) - 1))
        try:
            driver = factory(directory, spec)
            owner["ready"] = driver.start()
        except BaseException as error:
            owner.update(status="failed", failure=_failure(error))
            raise
        owner["owner_ready_ns"] = emit("session_owner_ready")
        owner.update(status="ready", startup_seconds=(owner["owner_ready_ns"] - owner["owner_start_ns"]) / 1e9)
        # Publish the original readiness receipt before any query, for the
        # independent parent monitor. This is evidence, not an ownership bypass.
        publish_private_json(output / ("owner-ready-" + str(len(report["owners"]) - 1) + ".json"),
                           redact_json_values(dict(session_id=session_id, **owner)))

    def close_owner():
        nonlocal driver
        if driver is None:
            return
        owner["owner_close_start_ns"] = emit("session_owner_close_start")
        try:
            owner["close"] = driver.close()
            if (not isinstance(owner["close"], dict) or owner["close"].get("status") != "closed"
                    or owner["close"].get("cleanup_errors")):
                raise RuntimeError("session owner cleanup remains unconfirmed")
            owner["status"] = "closed"
        except BaseException as error:
            owner.update(status="unknown", failure=_failure(error))
            raise
        finally:
            terminal = emit("session_owner_closed" if owner["status"] == "closed"
                            else "session_owner_close_unconfirmed")
            owner["owner_close_terminal_ns"] = terminal
            if owner["status"] == "closed":
                owner["owner_closed_ns"] = terminal
            owner["shutdown_seconds"] = (terminal - owner["owner_close_start_ns"]) / 1e9
            driver = None

    try:
        emit("session_start")
        if lifecycle != "isolated":
            open_owner(queries[0])
            emit("session_ready")
        for index, spec in enumerate(queries):
            query = dict(query_id=spec["query_id"], task=spec.get("task"), phase=spec["phase"],
                         status="failed", events=[], query_start_ns=now(), query_result_ready_ns=None,
                         query_api_return_ns=None,
                         owner_id=owner["owner_id"] if lifecycle != "isolated" else None)
            report["queries"].append(query)
            query_failure = None
            try:
                emit("query_start", query=query)
                if lifecycle == "isolated":
                    open_owner(spec)
                query["owner_id"] = owner["owner_id"]
                query["backend_report"] = driver.query(spec, lambda event: emit(event, query=query))
                query["query_api_return_ns"] = now()
                if query["backend_report"].get("status") != "completed":
                    raise RuntimeError("backend did not complete the query")
                if query["query_result_ready_ns"] is None:
                    raise ValueError("backend omitted the complete-result boundary")
                query["status"] = "completed"
            except BaseException as error:
                query_failure = error
                query["failure"] = _failure(error)
                query["backend_report"] = getattr(error, "query_report", query.get("backend_report"))
                raise
            finally:
                query["query_terminal_ns"] = emit("query_end", query=query)
                terminal = query["query_result_ready_ns"] or query["query_terminal_ns"]
                query["full_query_seconds"] = (terminal - query["query_start_ns"]) / 1e9
                query["complete_result_observed"] = query["query_result_ready_ns"] is not None
                query["post_result_seconds"] = (query["query_terminal_ns"] - terminal) / 1e9
                query["query_api_return_seconds"] = (None if query["query_api_return_ns"] is None else
                    (query["query_api_return_ns"]-query["query_start_ns"])/1e9)
                # Preserve primary failure evidence before final owner cleanup.
                try:
                    publish_private_json(output / ("query-" + str(index) + ".json"), redact_json_values(query))
                except BaseException as recording:
                    if query_failure is None:
                        raise
                    query_failure.add_note("query recording also failed: " + type(recording).__name__)
            if checkpoint is not None:
                emit("query_checkpoint_start", query=query)
                try:
                    query["owner_checkpoint"] = checkpoint(dict(session_id=session_id,
                        query_id=query["query_id"], owner_id=query["owner_id"], query_index=index))
                except BaseException as error:
                    query["checkpoint_failure"] = _failure(error)
                    emit("query_checkpoint_failed", query=query)
                    raise
                emit("query_checkpoint_end", query=query)
            if lifecycle == "isolated":
                close_owner()
        report["status"] = "completed"
    except BaseException as error:
        primary = error
        report["failure"] = _failure(error)
    finally:
        try:
            close_owner()
        except BaseException as cleanup:
            report["cleanup_failure"] = _failure(cleanup)
            if primary is None:
                primary = cleanup
                report["failure"] = _failure(cleanup)
            else:
                primary.add_note("session cleanup also failed: " + type(cleanup).__name__)
        report["skipped_queries"] = ids[len(report["queries"]):]
        report["owner_start_count"] = len(report["owners"])
        if primary is not None:
            report["status"] = "failed"
        report["session_end_ns"] = emit("session_closed" if primary is None else "session_failed")
        report["application_e2e_s"] = (report["session_end_ns"] - report["session_start_ns"]) / 1e9
        report["lifecycle_timing"] = _lifecycle_timing(report)
        if backend == "imlane" and lifecycle == "resident" and report["owners"]:
            first_owner = report["owners"][0]
            first_query = report["queries"][0] if report["queries"] else {}
            result_ns = first_query.get("query_result_ready_ns")
            report["resident_timing"] = dict(
                startup_seconds=first_owner.get("startup_seconds"),
                cold_first_result_seconds=(None if result_ns is None else
                    (result_ns - first_owner["owner_start_ns"]) / 1e9),
                first_ready_model_query_seconds=first_query.get("full_query_seconds"),
                steady_queries=[dict(query_id=query["query_id"], phase=query["phase"],
                    full_query_seconds=query.get("full_query_seconds")) for query in report["queries"][1:]],
                close_seconds=first_owner.get("shutdown_seconds"),
                cold_scope="owner start through first complete result; includes startup and readiness publication",
                steady_scope="complete later queries; owner startup and final close excluded")
        if setup is not None and setup.get("worker_entry_ns") is not None:
            report["worker_e2e_s"] = (report["session_end_ns"] - setup["worker_entry_ns"]) / 1e9
        try:
            write_private_json(output / "session.json", redact_json_values(report))
        except BaseException as recording:
            if primary is None:
                primary = recording
                report["status"] = "failed"
            else:
                primary.add_note("session recording also failed: " + type(recording).__name__)
        if primary is not None:
            primary.session_report = report
    if primary is not None:
        raise primary
    return report


def check_plan(plan):
    """Read-only CPU validation; never imports execution frameworks or a model."""
    from src.baselines.text.movie_records import MovieRecordSource
    from src.baselines.text.movie_tasks import MovieTask
    from src.semantic_methods.lotus.records import RecordsResources
    from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

    required = {"schema", "backend", "lifecycle", "model_name", "generation", "engine",
                "resources", "profile", "queries"}
    if (type(plan) is not dict or set(plan) != required
            or plan["schema"] != "semloom.movie_session_plan.v1"
            or plan["backend"] not in BACKENDS or plan["lifecycle"] not in LIFECYCLES):
        raise ValueError("an explicit Movie session plan is required")
    if plan["lifecycle"] == "runtime_reuse_only" and plan["backend"] != "ray-data":
        raise ValueError("runtime_reuse_only is implemented only for ray-data")
    if plan["lifecycle"] == "resident" and plan["backend"] not in RESIDENT:
        raise ValueError(plan["backend"] + " has no reusable cross-query model owner implemented by this adapter")
    engine, generation = EngineSpec.from_dict(plan["engine"]), GenerationSpec.from_dict(plan["generation"])
    resources = RecordsResources(**plan["resources"])
    if generation.max_tokens > engine.max_model_len:
        raise ValueError("generation exceeds model context limit")
    if plan["backend"] == "semloom-core" and (resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError("Core retains its native single-row preparation/model contract")
    core_profile = None
    if plan["backend"] == "semloom-core":
        from src.execution_provider.adapters.lotus_records_core import resolve_core_profile
        core_profile = resolve_core_profile(engine, resources, plan["profile"])
    if type(plan["queries"]) is not list or not plan["queries"]:
        raise ValueError("declare every complete warmup and measured query")
    checked, ids = [], set()
    for query in plan["queries"]:
        if (type(query) is not dict or set(query) != {"query_id", "task", "manifest", "phase"}
                or type(query["query_id"]) is not str or not query["query_id"]
                or query["query_id"] in ids or query["phase"] not in {"warmup", "measured"}):
            raise ValueError("each query needs a unique ID, task, manifest and explicit phase")
        ids.add(query["query_id"])
        task = MovieTask.from_config(query["task"])
        method = task.method(plan["model_name"])
        source = MovieRecordSource.from_manifest(query["manifest"])
        if source.task != task:
            raise ValueError("query task differs from source manifest")
        inputs = source.preflight(method)
        checked.append(dict(query, expected_calls=len(source.expected(query["query_id"])),
                            manifest_sha256=inputs["manifest_sha256"]))
    if plan["backend"] == "imlane":
        from dataclasses import asdict
        if (plan["profile"] != asdict(resources) or resources.cpu_workers != 1
                or resources.prepare_batch_rows > 16 or resources.model_batch_rows > 16
                or resources.max_active_batches > 2):
            raise ValueError("IMLane retains its one author worker and existing batch/backpressure capacities")
        if plan["lifecycle"] == "resident" and (len(checked) > 62
                or any(not 0 < query["expected_calls"] <= 4094 for query in checked)
                or engine.max_model_calls is None or engine.max_model_calls > 4096):
            raise ValueError("IMLane resident control records and whole-session calls exceed finite bridge capacity")
    total = sum(query["expected_calls"] for query in checked)
    if engine.max_model_calls is None or engine.max_model_calls < total:
        raise ValueError("max_model_calls must explicitly cover the whole session, including warmup")
    if plan["backend"] == "lotus-native":
        profile = plan["profile"]
        if (not plan["model_name"].startswith("openai/") or not plan["model_name"][7:]
                or type(profile) is not dict or set(profile) != {"native_lm_max_batch_size"}
                or type(profile["native_lm_max_batch_size"]) is not int or profile["native_lm_max_batch_size"] < 1):
            raise ValueError("Native retains the explicit OpenAI identity and original LM batch profile")
    return dict(schema="semloom.movie_session_check.v1", status="checked_cpu_only",
                inference_calls=0, expected_calls=total, queries=checked,
                model_cache_scope=("model owner shared by resident session; actual KV hits unavailable"
                    if plan["lifecycle"] == "resident" else
                    "fresh model ownership in each existing query adapter; actual KV hits unavailable"),
                lm_cache_scope="fresh per query, enable_cache=False",
                supports_resident=plan["backend"] in RESIDENT,
                supports_runtime_reuse_only=plan["backend"] == "ray-data",
                resident_support_scope="implemented adapter paths; not a general vendor capability claim",
                core_execution=core_profile)


class _MovieDriver:
    """Lifetime glue only. All query execution stays in the existing adapters."""

    def __init__(self, plan, context, directory, first_query):
        self.plan, self.context, self.directory = plan, context, Path(directory)
        self.first_query = first_query
        self.owner = self.service = self.ray = None
        self.created_ray = False
        self.ray_identity = None
        self.last_query_report = self.last_query_failure = self.last_query_directory = None
        self.last_expected = None
        self.core_execution = None
        self.imlane_guard = None
        self.imlane_handlers = {}

    def start(self):
        from src.baselines.text.movie_tasks import MovieTask
        from src.semantic_methods.lotus.records import RecordsResources
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        new_private_directory(self.directory)
        self.engine = EngineSpec.from_dict(self.plan["engine"])
        self.generation = GenerationSpec.from_dict(self.plan["generation"])
        self.resources = RecordsResources(**self.plan["resources"])
        backend = self.plan["backend"]
        method = MovieTask.from_config(self.first_query["task"]).method(self.plan["model_name"])
        if backend == "lotus-native" and self.plan["lifecycle"] == "resident":
            from src.baselines.text.frameworks.lotus_native_local_model import LocalService
            self.service = LocalService(self.engine, self.plan["model_name"][7:], self.resources,
                                        self.directory, self.context["runtime_binding"]["cuda_visible_devices"])
            return self.service.start()
        if backend == "imlane" and self.plan["lifecycle"] == "resident":
            import signal
            import threading
            from types import SimpleNamespace
            from integrations.imlane_records import owner as author_owner
            from integrations.imlane_records.chain_runtime import preflight_owner, AuthorRunGuard
            from integrations.imlane_records.model_lifecycle import MODEL_CLOSE_GRACE_S
            from integrations.imlane_records.resident import IMLaneResidentSession
            runtime, outer = self.context["runtime_binding"], self.context["owner_context"]
            if Path(outer["artifact_root"]).resolve() != Path(runtime["imlane_artifact_root"]).resolve():
                raise ValueError("IMLane session must retain its normal externally owned host lease root")
            value = dict(outer, author_root=runtime["imlane_author_root"],
                method_python=runtime["imlane_method_python"], bridge_binary=runtime["imlane_bridge_binary"],
                preflight_report=runtime["runtime_preflight_report"],
                model_environment=dict(CUDA_VISIBLE_DEVICES=runtime["cuda_visible_devices"]))
            preflight_owner(value, self.engine, chain=False)
            args = SimpleNamespace(output=self.directory, scopes=[],
                stop_budget=author_owner.StopBudget(threading.Event()),
                owner_resource_limits=outer["owner_resource_limits"],
                **{key: Path(value[key]) for key in ("author_root", "method_python", "bridge_binary", "artifact_root")})
            self.imlane_guard = AuthorRunGuard(value, args)
            self.imlane_handlers = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
            for number in self.imlane_handlers:
                signal.signal(number, args.stop_budget.signal)
            deadline = (time.monotonic() + self.resources.startup_timeout_s
                        + len(self.plan["queries"]) * self.resources.query_timeout_s + 2 * MODEL_CLOSE_GRACE_S)
            self.owner = IMLaneResidentSession(self.engine, self.generation, self.resources,
                author_args=args, model_environment=value["model_environment"], deadline_monotonic=deadline)
            return self.owner.start()
        if backend in {"semloom-core", "ray-data"}:
            import ray
            if ray.is_initialized():
                raise RuntimeError("session refuses an existing Ray runtime; use its private supervised worker")
            self.ray, self.created_ray = ray, True
            ray.init(address="local", num_cpus=self.resources.cpu_workers + self.resources.gpu_workers + 1,
                     num_gpus=1, include_dashboard=False,
                     runtime_env={"env_vars": {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                         "LITELLM_LOCAL_MODEL_COST_MAP": "True", "VLLM_WORKER_MULTIPROC_METHOD": "spawn"}})
            if backend == "ray-data":
                import os
                self.ray_identity = dict(driver_pid=os.getpid(), node_id=ray.get_runtime_context().get_node_id())
                return dict(no_session_model=True, model_startup_scope="inside each native Dataset",
                            runtime_identity=self.ray_identity)
        if backend == "semloom-core":
            from src.execution_provider.adapters.lotus_records_core import create_records_core, resolve_core_profile
            self.core_execution = resolve_core_profile(self.engine, self.resources, self.plan["profile"])
            self.owner = create_records_core(method, self.engine, self.generation, self.resources,
                                             profile=self.plan["profile"])
            ready = self.owner.start()
            if self.plan["lifecycle"] != "resident":
                return ready
            # Preserve the original Core receipt and observe its real process
            # birth while that model owner is held. The parent independently
            # checks the same birth, ancestry and engine before continuation.
            import psutil
            pid = ready.get("pid")
            if type(pid) is not int or pid <= 0:
                raise RuntimeError("Core model readiness lacks its actual process ID")
            process = psutil.Process(pid)
            birth = process.create_time()
            if process.pid != pid or psutil.Process(pid).create_time() != birth:
                raise RuntimeError("Core model birth changed during readiness observation")
            return dict(ready, readiness_observation=dict(pid=pid, create_time=birth,
                boundary="Core.start returned original model readiness"))
        return dict(no_session_model=True, model_startup_scope="inside existing per-query adapter")

    def query(self, spec, emit):
        from src.baselines.text.movie_records import MovieRecordSource
        from src.baselines.text.movie_tasks import MovieTask
        from src.experiments.lotus_records_movie import run_movie_query
        from integrations.imlane_records.chain_runtime import check_external_lease
        check_external_lease(self.context["owner_context"])
        # Every query reopens and verifies the original/materialized CSV input.
        source = MovieRecordSource.from_manifest(spec["manifest"])
        method = MovieTask.from_config(spec["task"]).method(self.plan["model_name"])
        binding = self.context["checked_queries"][spec["query_id"]]
        if (source.task.name != spec["task"]
                or source.preflight(method)["manifest_sha256"] != binding["manifest_sha256"]
                or len(source.expected(spec["query_id"])) != binding["expected_calls"]):
            raise ValueError("query source changed after session preflight")
        context = dict(self.context, claimed_budget=self.context["claims"][spec["query_id"]])
        if "budget" in context:
            context["budget"] = dict(context["budget"], unit_id=spec["query_id"])
        index = next(i for i, query in enumerate(self.plan["queries"]) if query["query_id"] == spec["query_id"])
        directory = self.directory / ("query-" + str(index))
        new_private_directory(directory)
        context["output_dir"] = directory
        checkout = self.context["sembench_checkout"]
        backend = self.plan["backend"]
        if backend == "ray-data" and (not self.ray.is_initialized()
                or self.ray.get_runtime_context().get_node_id() != self.ray_identity["node_id"]):
            raise RuntimeError("owned Ray runtime changed before query execution")
        if backend == "lotus-native":
            from src.baselines.text.frameworks.lotus_native_local_model import run
            return run(source, checkout, method, self.generation, self.engine, self.resources,
                query_id=spec["query_id"], profile=self.plan["profile"], context=context,
                observer=emit, shared_service=self.service)
        if backend == "imlane" and self.plan["lifecycle"] == "isolated":
            from src.experiments.imlane_movie_formal import run
            engine = replace(self.engine, max_model_calls=binding["expected_calls"])
            return run(source, checkout, method, self.generation, engine, self.resources,
                query_id=spec["query_id"], profile=self.plan["profile"], context=context, observer=emit)
        if backend == "imlane":
            self.imlane_guard()
            execute = lambda: self.owner.evaluate(source, method, query_id=spec["query_id"])
        elif backend == "semloom-core":
            execute = lambda: self.owner.evaluate(source, query_id=spec["query_id"], method=method)
        else:
            if backend == "ray-data":
                from src.baselines.text.frameworks.ray_data_lotus_records import evaluate
            else:
                from src.baselines.text.frameworks.daft_lotus_records import evaluate
            execute = lambda: evaluate(source, method, self.engine, self.generation, self.resources,
                                       query_id=spec["query_id"])
        self.last_expected, self.last_query_directory = source.expected(spec["query_id"]), directory
        self.last_query_failure = None
        self.last_query_report = None
        try:
            self.last_query_report = run_movie_query(source, checkout, method, self.generation, execute,
                                   query_id=spec["query_id"], observer=emit)
            if backend == "semloom-core":
                from src.experiments.movie_matrix_support import record_core_selection
                record_core_selection(self.last_query_report,self.context)
            return self.last_query_report
        except BaseException as error:
            if backend == "imlane" and self.owner is not None:
                self.owner.broken = True
            self.last_query_failure = error
            self.last_query_report = getattr(error, "query_report", self.last_query_report)
            raise

    def close(self):
        receipts, errors = {}, []
        for name, resource in (("model", self.owner), ("http_service", self.service)):
            if resource is not None:
                try:
                    receipts[name] = (resource.close(timeout=20)
                        if name == "model" and self.core_execution is not None
                        and self.core_execution["mode"] == "async" else resource.close())
                    if receipts[name].get("status") != "closed" or receipts[name].get("cleanup_errors"):
                        errors.append(name + " cleanup unconfirmed")
                except BaseException as error:
                    receipts[name] = _failure(error)
                    errors.append(name + " close failed")
        if self.owner is not None and self.last_query_report is not None:
            from src.experiments.lotus_records_movie import merge_unconsumed_call_results
            try:
                late = getattr(self.owner, "last_query_report", None)
                if late is not None:
                    merge_unconsumed_call_results(self.last_query_report, self.last_expected, late,
                                                  primary=self.last_query_failure)
                    receipts["final_query_accounting"] = self.last_query_report
                    write_private_json(self.last_query_directory / "query-accounting-final.json",
                                       redact_json_values(self.last_query_report))
            except BaseException as error:
                receipts["accounting_failure"] = _failure(error)
                errors.append("post-close query accounting failed")
        # This driver only initializes a fresh local runtime in its private worker.
        if self.created_ray and self.ray.is_initialized():
            try:
                if (self.plan["backend"] == "ray-data" and self.ray_identity is not None
                        and self.ray.get_runtime_context().get_node_id() != self.ray_identity["node_id"]):
                    raise RuntimeError("Ray runtime identity changed; replacement runtime left untouched")
                self.ray.shutdown()
                if self.plan["backend"] == "ray-data":
                    stopped = not self.ray.is_initialized()
                    receipts["ray"] = dict(status="closed" if stopped else "unknown",
                        runtime_shutdown_confirmed=stopped, runtime_identity=self.ray_identity,
                        scope="driver runtime only; native model lifetime remains per query",
                        independent_gpu_exit_confirmed=None)
                    if not stopped:
                        errors.append("owned Ray runtime still initialized after shutdown")
            except BaseException as error:
                receipts["ray"] = _failure(error)
                errors.append("owned Ray shutdown failed")
        if self.imlane_guard is not None:
            try:
                receipts["imlane_physical_exit"] = self.imlane_guard.finish()
                if (receipts["imlane_physical_exit"].get("gpu_exit_confirmed") is not True
                        or receipts["imlane_physical_exit"].get("external_lease_valid") is not True):
                    errors.append("IMLane independent GPU exit or external lease unconfirmed")
            except BaseException as error:
                receipts["imlane_physical_exit"] = _failure(error)
                errors.append("IMLane independent physical exit check failed")
        if self.imlane_handlers:
            import signal
            for number, handler in self.imlane_handlers.items():
                signal.signal(number, handler)
            self.imlane_handlers = {}
        return dict(status="closed" if not errors else "unknown", cleanup_errors=errors,
                    receipts=receipts, independent_gpu_exit_confirmed=None)


def run_movie_session(plan, context, output, *, observer=None, checkpoint=None):
    """Owner-facing model worker API, after reservations and external supervision.

    context carries already-claimed, non-transferable units in this process.
    A CLI/controller must claim existing reserved units before calling this API;
    it must preserve its normal host supervisor, limits and independent GPU exit.
    """
    preflight_started = time.monotonic_ns()
    from src.experiments.cell_budget import ClaimedUnit
    from src.baselines.text.movie_records import MovieRecordSource
    from src.baselines.text.movie_tasks import MovieTask
    from src.experiments.lotus_records_movie import model_preflight
    from integrations.imlane_records.chain_runtime import check_external_lease
    from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
    checked = check_plan(plan)
    check_external_lease(context["owner_context"])
    engine = EngineSpec.from_dict(plan["engine"])
    engine.validate_driver_environment()
    runtime = context["runtime_binding"]
    if any(runtime[name] != getattr(engine, name) for name in ("driver_python", "model_python", "model_path")):
        raise ValueError("session engine differs from the owner's runtime binding")
    if runtime.get("tokenizer_path", engine.model_path) != (engine.tokenizer_path or engine.model_path):
        raise ValueError("session tokenizer differs from the owner's runtime binding")
    for query in checked["queries"]:
        claim = context["claims"].get(query["query_id"])
        if (not isinstance(claim, ClaimedUnit) or claim.reservation.unit_id != query["query_id"]
                or claim.attempts or claim.remaining != query["expected_calls"]):
            raise ValueError("each query needs its own fresh, already-charged complete unit")
        model_preflight(MovieRecordSource.from_manifest(query["manifest"]), context["sembench_checkout"],
            MovieTask.from_config(query["task"]).method(plan["model_name"]),
            GenerationSpec.from_dict(plan["generation"]), engine)
    # Whole units were permanently charged by the controller already. This
    # process-local single-use binding also covers embedded engines that do not
    # call the HTTP per-POST reserve hook, without inventing model attempts.
    for query in checked["queries"]:
        context["claims"][query["query_id"]].bind_execution()
    context = dict(context, checked_queries={query["query_id"]: query for query in checked["queries"]})
    # Explicitly bind the assigned device before a local Ray model actor starts.
    import os
    import re
    device = runtime["cuda_visible_devices"]
    if type(device) is not str or re.fullmatch(r"(?:[0-9]+|GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})", device) is None:
        raise ValueError("session requires one explicit physical CUDA device")
    before = {key: os.environ.get(key) for key in ("CUDA_VISIBLE_DEVICES", "CUDA_DEVICE_ORDER")}
    os.environ.update(CUDA_VISIBLE_DEVICES=device, CUDA_DEVICE_ORDER="PCI_BUS_ID")
    try:
        setup = dict(preflight_start_ns=preflight_started, preflight_end_ns=time.monotonic_ns(),
            worker_entry_ns=context.get("worker_entry_ns"), check=checked,
            cache_policy=dict(lm="fresh per-query LM and enable_cache=False",
                worker=("shared model owner" if plan["lifecycle"] == "resident" else
                    "shared Ray runtime; fresh native model worker per query" if plan["lifecycle"] == "runtime_reuse_only" else
                    "fresh driver and model ownership per query"),
                query_state="fresh source, expected calls, ledger, results and evaluation for each query",
                model_internal_cache="native backend owns KV/prefix policy; cache hits unavailable"))
        setup["preflight_seconds"] = (setup["preflight_end_ns"] - preflight_started) / 1e9
        report = run_sequence(plan["queries"], lambda directory, spec: _MovieDriver(plan, context, directory, spec),
            output, backend=plan["backend"], lifecycle=plan["lifecycle"], observer=observer,
            setup=setup, checkpoint=checkpoint)
        return report
    finally:
        for key, value in before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
