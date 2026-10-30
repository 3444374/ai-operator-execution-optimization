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


# Compatibility exports: existing callers retain their module API.
from src.experiments.native_query_lifecycle import (
    RESIDENT, BACKENDS, LIFECYCLES, DAFT_SESSION_PROFILE,
    _failure, _lifecycle_timing, run_sequence,
)


def check_plan(plan):
    """Read-only CPU validation; never imports execution frameworks or a model."""
    from src.baselines.text.movie_records import MovieRecordSource
    from src.baselines.text.movie_tasks import MovieTask
    from src.semantic_methods.lotus.records import RecordsResources
    from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

    required = {"schema", "backend", "lifecycle", "model_name", "generation", "engine",
                "resources", "profile", "queries"}
    optional = {'admission_cache', 'prepared_batching', 'batch_concurrency', 'batch_feedback'}
    if (type(plan) is not dict or not required <= set(plan) or set(plan)-required-optional
            or plan["schema"] != "semloom.movie_session_plan.v1"
            or plan["backend"] not in BACKENDS or plan["lifecycle"] not in LIFECYCLES):
        raise ValueError("an explicit Movie session plan is required")
    if plan["lifecycle"] == "runtime_reuse_only" and plan["backend"] != "ray-data":
        raise ValueError("runtime_reuse_only is implemented only for ray-data")
    if plan["lifecycle"] == "resident" and plan["backend"] not in RESIDENT:
        raise ValueError(plan["backend"] + " has no reusable cross-query model owner implemented by this adapter")
    if plan["lifecycle"] == "native_single_thread_session" and (plan["backend"] != "daft"
            or plan["profile"] != DAFT_SESSION_PROFILE):
        raise ValueError("native_single_thread_session requires daft and its explicit different native profile")
    engine, generation = EngineSpec.from_dict(plan["engine"]), GenerationSpec.from_dict(plan["generation"])
    resources = RecordsResources(**plan["resources"])
    if generation.max_tokens > engine.max_model_len:
        raise ValueError("generation exceeds model context limit")
    if plan["backend"] == "semloom-core" and (resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError("Core retains its native single-row preparation/model contract")
    core_profile = None
    batching = None
    if plan['backend'] != 'semloom-core' and (set(plan) & {'prepared_batching', 'batch_concurrency', 'batch_feedback'}):
        raise ValueError('record batching options require semloom-core')
    if plan["backend"] == "semloom-static-reference" and plan["profile"] != {"mode": "sync"}:
        raise ValueError("static reference uses its explicit sync profile and existing resource batch sizes")
    if plan["backend"] == "semloom-core":
        from src.execution_provider.adapters.lotus_records_core import resolve_core_profile, resolve_record_batching
        from src.experiments.lotus_records_movie import _record_batching_options
        core_profile = resolve_core_profile(engine, resources, plan["profile"])
        batching = resolve_record_batching(core_profile, **_record_batching_options(plan))
        if batching is not None and (plan['lifecycle'] != 'isolated' or plan.get('admission_cache') is not None):
            raise ValueError('record token batches require isolated fresh owners without admission_cache')
    if plan.get("admission_cache") is not None:
        from src.experiments.movie_matrix_support import validate_record_admission_cache
        validate_record_admission_cache(plan["admission_cache"], core_profile)
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
    if plan["backend"] == "semloom-static-reference":
        methods = [MovieTask.from_config(query["task"]).method(plan["model_name"]) for query in checked]
        if any(method != methods[0] for method in methods):
            raise ValueError("a static reference session retains one method instruction")
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
                    if plan["lifecycle"] in {"resident", "native_single_thread_session"} else
                    "fresh model ownership in each existing query adapter; actual KV hits unavailable"),
                lm_cache_scope="fresh per query, enable_cache=False",
                supports_resident=plan["backend"] in RESIDENT,
                native_single_thread_session=plan["lifecycle"] == "native_single_thread_session",
                supports_runtime_reuse_only=plan["backend"] == "ray-data",
                resident_support_scope="implemented adapter paths; not a general vendor capability claim",
                core_execution=core_profile, **({} if batching is None else dict(record_batching=batching)))


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
        if backend == "daft" and self.plan["lifecycle"] == "native_single_thread_session":
            from src.baselines.text.frameworks.daft_lotus_session import DaftNativeSession
            deadline = (time.monotonic() + self.resources.startup_timeout_s
                        + len(self.plan["queries"]) * self.resources.query_timeout_s)
            self.owner = DaftNativeSession(self.engine, self.generation, self.resources,
                control_dir=self.directory / "model", deadline_monotonic=deadline)
            return self.owner.start()
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
        if backend in {"semloom-core", "semloom-static-reference", "ray-data"}:
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
        if backend in {"semloom-core", "semloom-static-reference"}:
            if backend == "semloom-core":
                from src.execution_provider.adapters.lotus_records_core import create_records_core, resolve_core_profile
                from src.experiments.lotus_records_movie import _record_batching_options
                self.core_execution = resolve_core_profile(self.engine, self.resources, self.plan["profile"])
                self.owner = create_records_core(method, self.engine, self.generation, self.resources,
                                                 profile=self.plan["profile"], **_record_batching_options(self.plan))
            else:
                from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
                self.owner = SemLoomRecordsStaticReference(method, self.engine, self.generation, self.resources)
            ready = self.owner.start()
            if self.plan["lifecycle"] != "resident":
                return ready
            # Preserve the original Core receipt and observe its real process
            # birth while that model owner is held. The parent independently
            # checks the same birth, ancestry and engine before continuation.
            import psutil
            pid = ready.get("pid")
            if type(pid) is not int or pid <= 0:
                raise RuntimeError(("Core" if backend == "semloom-core" else "static reference")
                                   + " model readiness lacks its actual process ID")
            process = psutil.Process(pid)
            birth = process.create_time()
            if process.pid != pid or psutil.Process(pid).create_time() != birth:
                raise RuntimeError(("Core" if backend == "semloom-core" else "static reference")
                                   + " model birth changed during readiness observation")
            return dict(ready, readiness_observation=dict(pid=pid, create_time=birth,
                boundary=("Core.start returned original model readiness" if backend == "semloom-core"
                          else "static reference start returned original model readiness")))
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
        query_options = {}
        if backend == "imlane":
            self.imlane_guard()
            execute = lambda: self.owner.evaluate(source, method, query_id=spec["query_id"])
        elif backend == "semloom-core":
            execute = lambda: self.owner.evaluate(source, query_id=spec["query_id"], method=method, **query_options)
        elif backend == "semloom-static-reference":
            if method != self.owner.method:
                raise ValueError("a static reference session retains one method instruction")
            execute = lambda: self.owner.evaluate(source, query_id=spec["query_id"])
        elif backend == "daft" and self.owner is not None:
            execute = lambda: self.owner.evaluate(source, method, query_id=spec["query_id"])
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
            if self.plan.get("admission_cache") is not None:
                from src.experiments.movie_matrix_support import run_record_admission
                def execute_query(window, *, allow_exploration, search_hint=None):
                    query_options.update(max_inflight=window, allow_exploration=allow_exploration,
                                         search_hint=search_hint)
                    return run_movie_query(source, checkout, method, self.generation, execute,
                                          query_id=spec["query_id"], observer=emit)
                self.last_query_report = run_record_admission(self.plan["admission_cache"],
                    self.core_execution, source, method, self.generation, self.engine, self.resources,
                    execute_query, warmup=spec["phase"] == "warmup")
            else:
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
                worker=("same Daft wrapper, single native thread, lazy model until session close"
                    if plan["lifecycle"] == "native_single_thread_session" else
                    "shared model owner" if plan["lifecycle"] == "resident" else
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
