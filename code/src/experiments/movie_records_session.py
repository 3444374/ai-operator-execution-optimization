"""Consecutive complete Movie queries; existing backends retain scheduling.

This worker is called inside the existing owner's lease/resource supervisor.
It creates no lease, budget, retries, detached workers, or performance claims.
Core, native LOTUS and opt-in IMLane CPython sessions expose reusable model owners.
Ray can reuse its runtime while each native Dataset still owns a new model.
The explicit ``isolated`` mode retains per-query driver startup and shutdown.
"""

from dataclasses import asdict, replace
from pathlib import Path
import time
import traceback
import uuid

from src.infrastructure.private_artifacts import new_private_directory, write_private_json, publish_private_json
from src.infrastructure.redact import redact_json_values


# Compatibility exports: existing callers retain their module API.
from src.experiments.native_query_lifecycle import (
    RESIDENT, BACKENDS, LIFECYCLES, DAFT_SESSION_PROFILE,
    _failure, _lifecycle_timing, run_sequence,
)


def check_plan(plan):
    """Read-only CPU validation; never imports execution frameworks or a model."""
    from src.baselines.text.movie_records import MovieRecordSource
    from src.baselines.text.movie_tasks import MovieTask
    from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec

    required = {"schema", "backend", "lifecycle", "model_name", "generation", "engine",
                "resources", "profile", "queries"}
    optional = {'prepared_batching', 'batch_concurrency', 'batch_feedback', 'rolling_refill',
                'experimental_skip_token_packing', 'core_capacity', 'async_limits', 'preparation_mode',
                'service_payload_bytes'}
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
    from src.experiments.movie_records_config import records_resources
    resources = records_resources(plan)
    context_limit = engine.max_model_len
    if context_limit is not None and generation.max_tokens > context_limit:
        raise ValueError("generation exceeds model context limit")
    if plan["backend"] == "semloom-core" and (resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError("Core retains its native single-row preparation/model contract")
    if plan["backend"] == "lotus-native" and (
            type(plan["profile"]) is not dict or set(plan["profile"]) != {"native_lm_max_batch_size"}
            or type(plan["profile"]["native_lm_max_batch_size"]) is not int
            or plan["profile"]["native_lm_max_batch_size"] < 1):
        raise ValueError("native LOTUS requires an explicit native_lm_max_batch_size")
    core_profile = None
    capacity = async_limits = None
    batching = None
    if plan['backend'] != 'semloom-core' and 'service_payload_bytes' in plan:
        raise ValueError('service_payload_bytes requires semloom-core')
    if plan['backend'] != 'semloom-core' and set(plan) & {'core_capacity', 'async_limits'}:
        raise ValueError('core_capacity and async_limits require semloom-core')
    if plan['backend'] != 'semloom-core' and (set(plan) & {'prepared_batching', 'batch_concurrency', 'batch_feedback', 'rolling_refill',
                                                       'experimental_skip_token_packing', 'preparation_mode'}):
        raise ValueError('record batching options require semloom-core')
    if plan["backend"] == "semloom-static-reference":
        resources.request_capacity()
        if plan["profile"] != {"mode": "sync"}:
            raise ValueError("static reference uses its explicit sync profile and existing resource batch sizes")
    if plan["backend"] == "semloom-core":
        from src.execution_provider.adapters.records_profile import resolve_record_batching
        from src.experiments.movie_records_config import (
            record_batching_options as _record_batching_options,
            records_runtime as _records_runtime,
        )
        configured = _records_runtime(plan, resources=resources, engine=engine)
        capacity = (configured.capacity if 'core_capacity' in plan or configured.capacity.budget is not None
                    else None)
        core_profile = configured.profile
        async_limits = (configured.async_limits if 'async_limits' in plan or configured.capacity.budget is not None
                        else None)
        batching = resolve_record_batching(core_profile, **_record_batching_options(plan))
        if batching is not None and (plan['lifecycle'] != 'isolated' and not (
                    plan['lifecycle'] == 'resident' and batching.get('rolling_refill'))):
            raise ValueError('record token batches require isolated or rolling resident owners')
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
        if (plan["profile"] != asdict(resources) or resources.cpu_workers != 1
                or resources.prepare_batch_rows > 16 or resources.model_batch_rows > 16
                or resources.request_capacity() > 2):
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
                core_execution=core_profile, **({} if batching is None else dict(record_batching=batching)),
                **({} if capacity is None else dict(core_capacity=asdict(capacity))),
                **({} if async_limits is None else dict(async_limits=asdict(async_limits))))


class _MovieDriver:
    """Lifetime glue only. All query execution stays in the existing adapters."""

    def __init__(self, plan, context, directory, first_query):
        self.plan, self.context, self.directory = plan, context, Path(directory)
        self.first_query = first_query
        self.owner = self.ray = None
        self.created_ray = False
        self.ray_identity = None
        self.last_query_report = self.last_query_failure = self.last_query_directory = None
        self.last_expected = None
        self.core_execution = None
        self.imlane_guard = None
        self.imlane_handlers = {}
        self._model_ready = self._owner_ready = None
        self.payload_pool = None
        self.service_runtime = None
        self._close_attempted = False

    def _check_core_model_ready(self, ready):
        from src.experiments.calibration.core import core_model_ready
        binding = self.context['plan_binding']
        expected = self.context.get('core_model_context_ready', binding['observed'].get('model_ready'))
        if expected is None and binding.get('resolve_model_context') is not True:
            raise RuntimeError('calibrated execution requires its observed model context')
        current = core_model_ready(self.plan, ready, expected=expected)
        if self._model_ready is not None and current['engine_uid'] != self._model_ready['engine_uid']:
            raise ValueError('calibrated query model owner changed after readiness')
        self._model_ready = current
        self.context['core_model_context_ready'] = current
        if self._owner_ready.get('no_session_model') is True:
            self._owner_ready['query_model_ready'] = current
        return current

    def start(self):
        from src.baselines.text.movie_tasks import MovieTask
        from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
        from src.experiments.movie_records_config import records_resources, records_service_pool
        self.engine = EngineSpec.from_dict(self.plan["engine"])
        self.generation = GenerationSpec.from_dict(self.plan["generation"])
        self.resources = records_resources(self.plan)
        backend = self.plan["backend"]
        if backend != 'semloom-core' and 'service_payload_bytes' in self.plan:
            raise ValueError('service_payload_bytes requires semloom-core')
        if backend != 'semloom-core' and 'preparation_mode' in self.plan:
            raise ValueError('preparation_mode requires semloom-core')
        if backend in ('semloom-static-reference', 'imlane'):
            self.resources.request_capacity()
        elif backend == 'semloom-core':
            from src.experiments.movie_records_config import (
                records_runtime as _records_runtime,
                record_batching_options as _record_batching_options,
            )
            from src.execution_provider.adapters.records_profile import resolve_record_batching
            configured = _records_runtime(self.plan, resources=self.resources, engine=self.engine)
            self.core_execution = configured.profile
            options = _record_batching_options(self.plan)
            resolve_record_batching(self.core_execution, **options)
        method = None
        if 'service_payload_bytes' in self.plan:
            from src.execution_provider.adapters.lotus_records_core import create_records_core
            from src.experiments.movie_records_config import core_capacity
            self.payload_pool = records_service_pool(self.plan, self.context)
            method = MovieTask.from_config(self.first_query['task']).method(self.plan['model_name'])
            self.owner = create_records_core(method, self.engine, self.generation, self.resources,
                profile=self.plan['profile'], capacity=core_capacity(self.plan),
                service_budget=self.payload_pool, **options)
            from .movie_records_service import verify_service_owner
            verify_service_owner(self.owner, configured)
            self.core_execution = self.owner.execution_profile
            allocation_id = self.owner._service_allocation.allocation_id
            self.context.setdefault('_records_payload_drivers', {})[allocation_id] = self
        new_private_directory(self.directory)
        if method is None:
            method = MovieTask.from_config(self.first_query["task"]).method(self.plan["model_name"])
        if backend == "lotus-native" and self.plan["lifecycle"] == "resident":
            from src.baselines.text.frameworks.lotus_native_local import LocalNativeModel
            self.owner = LocalNativeModel(self.engine, self.generation, self.resources,
                self.plan["profile"]["native_lm_max_batch_size"])
            return self.owner.start()
        if backend == "daft" and self.plan["lifecycle"] == "native_single_thread_session":
            from src.baselines.text.frameworks.daft_lotus_session import DaftNativeSession
            deadline = (time.monotonic() + self.resources.startup_timeout_s
                        + len(self.plan["queries"]) * self.resources.query_timeout_s)
            self.owner = DaftNativeSession(self.engine, self.generation, self.resources,
                control_dir=self.directory / "model", deadline_monotonic=deadline)
            return self.owner.start()
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
            slots = self.resources.cpu_workers + self.resources.gpu_workers + 1
            if backend != "ray-data":
                from src.infrastructure.cpu_allocation import checked_local_cpu_slots
                slots = checked_local_cpu_slots(slots)
            self.ray, self.created_ray = ray, True
            ray.init(address="local", num_cpus=slots,
                     num_gpus=1, include_dashboard=False,
                     runtime_env={"env_vars": {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                         "LITELLM_LOCAL_MODEL_COST_MAP": "True", "VLLM_WORKER_MULTIPROC_METHOD": "spawn"}})
            if self.payload_pool is not None:
                from .movie_records_service import service_runtime_identity
                self.service_runtime = service_runtime_identity(ray)
                self.owner.bind_model_runtime(self.service_runtime)
            if backend == "ray-data":
                import os
                self.ray_identity = dict(driver_pid=os.getpid(), node_id=ray.get_runtime_context().get_node_id())
                return dict(no_session_model=True, model_startup_scope="inside each native Dataset",
                            runtime_identity=self.ray_identity)
        if backend in {"semloom-core", "semloom-static-reference"}:
            if backend == "semloom-core":
                from src.execution_provider.adapters.lotus_records_core import create_records_core
                from src.experiments.movie_records_config import (
                    core_capacity as _core_capacity,
                )
                if options.get('rolling_refill') and self.plan['lifecycle'] == 'resident':
                    options['resident'] = True
                if self.owner is None:
                    self.owner = create_records_core(method, self.engine, self.generation, self.resources,
                        profile=self.plan["profile"], capacity=_core_capacity(self.plan),
                        async_limits=configured.async_limits, **options)
            else:
                from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
                self.owner = SemLoomRecordsStaticReference(method, self.engine, self.generation, self.resources)
            ready = self.owner.start()
            self._owner_ready = ready
            if backend == 'semloom-core' and self.engine.max_model_len is None and 'plan_binding' in self.context:
                if ready.get('no_session_model') is not True:
                    self._check_core_model_ready(ready)
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
        if backend == "lotus-native":
            from src.baselines.text.frameworks.lotus_native_local import run
            self.last_expected, self.last_query_directory = source.expected(spec["query_id"]), directory
            self.last_query_failure = self.last_query_report = None
            try:
                self.last_query_report = run(source, checkout, method, self.generation, self.engine,
                    self.resources, query_id=spec["query_id"], profile=self.plan["profile"],
                    observer=emit, context=context, shared_model=self.owner)
                return self.last_query_report
            except BaseException as error:
                self.last_query_failure = error
                self.last_query_report = getattr(error, "query_report", None)
                raise
        from src.experiments.lotus_records_movie import run_movie_query
        if backend == "ray-data" and (not self.ray.is_initialized()
                or self.ray.get_runtime_context().get_node_id() != self.ray_identity["node_id"]):
            raise RuntimeError("owned Ray runtime changed before query execution")
        if backend == "imlane" and self.plan["lifecycle"] == "isolated":
            from src.experiments.imlane_movie_formal import run
            engine = replace(self.engine, max_model_calls=binding["expected_calls"])
            return run(source, checkout, method, self.generation, engine, self.resources,
                query_id=spec["query_id"], profile=self.plan["profile"], context=context, observer=emit)
        if backend == "imlane":
            self.imlane_guard()
            execute = lambda: self.owner.evaluate(source, method, query_id=spec["query_id"])
        elif backend == "semloom-core":
            observed_context = self.engine.max_model_len is None and 'plan_binding' in self.context
            if observed_context and (self.owner.model_ready is not None or self._model_ready is not None):
                self._check_core_model_ready(self.owner.model_ready)
            def execute():
                stream = self.owner.evaluate(source, query_id=spec['query_id'], method=method)
                if observed_context:
                    current = self.owner.model_ready
                    if current is not None or source.expected(spec['query_id']):
                        self._check_core_model_ready(current)
                return stream
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
        repeated = self._close_attempted
        self._close_attempted = True
        resource = self.owner
        if resource is not None:
            try:
                if self.payload_pool is not None and repeated:
                    grant = resource._service_allocation
                    if not resource.cleanup_confirmed() and grant.describe()['status'] != 'released':
                        receipts["model"] = self.payload_pool.retry_cleanup(grant.allocation_id, timeout=20)
                    else:
                        from .movie_records_service import close_service_owner
                        receipts["model"] = close_service_owner(resource, self.ray, self.service_runtime, timeout=20)
                elif self.payload_pool is not None:
                    from .movie_records_service import close_service_owner
                    receipts["model"] = close_service_owner(resource, self.ray, self.service_runtime, timeout=20)
                else:
                    receipts["model"] = (resource.close(timeout=20)
                        if self.core_execution is not None
                        and self.core_execution["mode"] == "async" else resource.close())
                if receipts["model"].get("status") != "closed" or receipts["model"].get("cleanup_errors"):
                    errors.append("model cleanup unconfirmed")
            except BaseException as error:
                receipts["model"] = _failure(error)
                errors.append("model close failed")
        if self.owner is not None and self.last_query_report is not None:
            try:
                if self.plan['backend'] == 'lotus-native':
                    final = receipts.get('model', {}).get('native_final_accounting')
                    if final is not None:
                        if final['query_id'] != self.last_query_report['query_id']:
                            raise ValueError('native closing accounting belongs to another query')
                        # Source occurrences that the original SDK did not return
                        # stay unknown. Native late usage is separate evidence.
                        self.last_query_report['native_final_accounting'] = final
                        del receipts['model']['native_final_accounting']
                    if 'native_final_accounting' in self.last_query_report:
                        receipts['final_query_accounting'] = self.last_query_report
                        publish_private_json(self.last_query_directory / 'query-accounting-final.json',
                                             redact_json_values(self.last_query_report))
                elif (late := getattr(self.owner, "last_query_report", None)) is not None:
                    from src.experiments.lotus_records_movie import merge_unconsumed_call_results
                    merge_unconsumed_call_results(self.last_query_report, self.last_expected, late,
                                                  primary=self.last_query_failure)
                    receipts["final_query_accounting"] = self.last_query_report
                    if self.payload_pool is None:
                        write_private_json(self.last_query_directory / "query-accounting-final.json",
                                           redact_json_values(self.last_query_report))
            except BaseException as error:
                receipts["accounting_failure"] = _failure(error)
                errors.append("post-close query accounting failed")
        if self.payload_pool is not None and self.owner is not None:
            try:
                if self.last_query_report is not None and not errors:
                    from .movie_records_service import accept_service_results
                    transfer = accept_service_results(self.owner, self.last_query_report, self.last_expected,
                        self.last_query_directory, primary=self.last_query_failure)
                    if transfer is not None:
                        receipts['service_result_transfer'] = transfer
                        receipts['model'] = self.owner.last_close_report
                receipts['service_payload_pool'] = self.payload_pool.snapshot()
            except BaseException as error:
                receipts['service_acceptance_failure'] = _failure(error)
                errors.append('service result acceptance failed')
        # This driver only initializes a fresh local runtime in its private worker.
        if self.created_ray and (self.payload_pool is not None or self.ray.is_initialized()):
            try:
                if self.payload_pool is not None:
                    from .movie_records_service import finish_service_runtime
                    receipts['ray'] = finish_service_runtime(self.ray, self.service_runtime, self.owner)
                    if receipts['ray']['status'] != 'closed':
                        errors.append('original Ray retained for pending owner cleanup')
                elif (self.plan["backend"] == "ray-data" and self.ray_identity is not None
                        and self.ray.get_runtime_context().get_node_id() != self.ray_identity["node_id"]):
                    raise RuntimeError("Ray runtime identity changed; replacement runtime left untouched")
                else:
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
        if (self.payload_pool is not None and self.owner is not None and not errors
                and self.owner._service_allocation.describe()['status'] == 'released'):
            self.context.get('_records_payload_drivers', {}).pop(self.owner._service_allocation.allocation_id, None)
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
    from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
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
    pool_owned = 'service_payload_bytes' in plan and context.get('_records_payload_pool') is None
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
        try:
            report = run_sequence(plan["queries"], lambda directory, spec: _MovieDriver(plan, context, directory, spec),
                output, backend=plan["backend"], lifecycle=plan["lifecycle"], observer=observer,
                setup=setup, checkpoint=checkpoint)
        except BaseException as error:
            pool = context.get('_records_payload_pool')
            if pool is not None:
                error.records_payload_pool = pool
                error.records_payload_drivers = dict(context.get('_records_payload_drivers', {}))
                try:
                    if getattr(error, 'session_report', None) is not None:
                        from .movie_records_service import finish_service_pool
                        snapshot, artifact = finish_service_pool(pool, output, owned=pool_owned)
                        error.session_report.update(service_payload_pool=snapshot, service_payload_pool_artifact=artifact)
                except BaseException as recording:
                    error.add_note('service pool evidence: '+type(recording).__name__)
            raise
        pool = context.get('_records_payload_pool')
        if pool is not None:
            from .movie_records_service import finish_service_pool
            try:
                snapshot, artifact = finish_service_pool(pool, output, owned=pool_owned)
                report.update(service_payload_pool=snapshot, service_payload_pool_artifact=artifact)
            except BaseException as error:
                report.update(status='failed', service_pool_evidence_error=_failure(error),
                    service_payload_pool=pool.close() if pool_owned else pool.snapshot())
                error.session_report = report
                error.records_payload_pool = pool
                error.records_payload_drivers = dict(context.get('_records_payload_drivers', {}))
                raise
        return report
    finally:
        for key, value in before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
