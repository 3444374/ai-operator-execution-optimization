"""Construct the selected Movie model runtime and finish its original owner."""
from dataclasses import asdict
import json
import time

from src.infrastructure.private_artifacts import write_private_json
from src.infrastructure.redact import redact_text
from . import movie_records_query as query
from .movie_records_config import (
    ASYNC_CORE_BACKEND as _ASYNC_BACKEND,
    daft_native_profile as _daft_native_profile,
    core_capacity as _core_capacity,
    core_profile as _core_profile,
    record_batching_options as _record_batching_options,
    records_runtime as _records_runtime,
    records_service_pool as _records_service_pool,
)


def run_model_worker(config, source, checkout, method, generation, resources, engine, root, *, observer=None):
    if config.get("task", "movie-q3") != source.task.name:
        raise ValueError("execution configuration task differs from its source binding")
    if config['backend'] == 'semloom-local-model-static-reference':
        resources.request_capacity()
    emit = observer if observer is not None else lambda _event: None
    ray = None
    owner = None
    async_limits = None
    is_core = config["backend"] in ("semloom-local-model-core", _ASYNC_BACKEND)
    profile = _core_profile(config) if is_core else None
    is_async = False
    service_pool = None
    service_runtime = None
    service_runtime_event = 'runtime_not_started'
    failure = None
    report = {}
    runtime_started = time.monotonic()
    emit("runtime_setup_start")
    def create_core_owner():
        from src.execution_provider.adapters.lotus_records_core import create_records_core
        options = dict(profile=profile, capacity=_core_capacity(config), **_record_batching_options(config))
        if service_pool is not None:
            options['service_budget'] = service_pool
        else:
            options['async_limits'] = async_limits
        return create_records_core(method, engine, generation, resources, **options)
    try:
        if is_core:
            configured = _records_runtime(config, resources=resources, engine=engine)
            is_async = configured.profile['mode'] == 'async'
        if is_async:
            async_limits = configured.async_limits
        if 'service_payload_bytes' in config:
            service_pool = _records_service_pool(config)
            owner = create_core_owner()
            from .movie_records_service import verify_service_owner
            verify_service_owner(owner, configured)
        if config["backend"] != "daft-native-local-model":
            import ray

            if service_pool is not None and ray.is_initialized():
                raise RuntimeError('service worker refuses an existing Ray runtime')
            slots = resources.cpu_workers + resources.gpu_workers + 1
            if is_core or config["backend"] == "semloom-local-model-static-reference":
                from src.infrastructure.cpu_allocation import checked_local_cpu_slots
                slots = checked_local_cpu_slots(slots)
            ray.init(address="local", num_cpus=slots,
                     num_gpus=1, include_dashboard=False,
                     runtime_env={"env_vars": {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                                                 "LITELLM_LOCAL_MODEL_COST_MAP": "True",
                                                 "VLLM_WORKER_MULTIPROC_METHOD": "spawn"}})
            if service_pool is not None:
                from .movie_records_service import service_runtime_identity
                service_runtime = service_runtime_identity(ray)
                owner.bind_model_runtime(service_runtime)
        runtime_seconds = time.monotonic() - runtime_started
        emit("runtime_setup_end")
        def execute():
            nonlocal owner
            if config["backend"] == "daft-native-local-model":
                from src.baselines.text.frameworks.daft_lotus_records import evaluate
                return evaluate(source, method, engine, generation, resources, query_id=config["query_id"],
                                **_daft_native_profile(config.get('profile', {})))
            if config["backend"] == "ray-data-local-model":
                from src.baselines.text.frameworks.ray_data_lotus_records import evaluate
                return evaluate(source, method, engine, generation, resources, query_id=config["query_id"])
            if is_core:
                if owner is None:
                    owner = create_core_owner()
                return owner.evaluate(source, query_id=config["query_id"])
            from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
            owner = SemLoomRecordsStaticReference(method, engine, generation, resources)
            return owner.evaluate(source, query_id=config["query_id"])
        report = query.run_movie_query(source, checkout, method, generation, execute,
                                 query_id=config["query_id"], observer=observer)
        report.update(backend=config["backend"], runtime_setup_seconds=runtime_seconds,
                      execution_kind="synchronous local-model qualification", performance_claim=False)
    except BaseException as error:
        failure = error
        report = getattr(error, "query_report", dict(status="failed", error_type=type(error).__name__,
                                                     error=redact_text(str(error))))
    finally:
        try:
            if owner is not None:
                if service_pool is not None:
                    from .movie_records_service import close_service_owner
                    report['owner_close'] = close_service_owner(owner, ray, service_runtime, timeout=20)
                else:
                    report["owner_close"] = owner.close(timeout=20) if is_async else owner.close()
                if (report["owner_close"].get("cleanup_errors")
                        or is_async and report["owner_close"].get("status") != "closed"):
                    raise RuntimeError(("async Core" if is_async else "static") + " owner cleanup remains unconfirmed")
        except BaseException as cleanup:
            report.update(status="failed", close_error_type=type(cleanup).__name__)
            if failure is None:
                failure = cleanup
            else:
                failure.add_note("static owner close also failed: " + type(cleanup).__name__)
        finally:
            report.setdefault("production_core_used", False)
            try:
                report["production_core_used"] = report["production_core_used"] is True or (
                    getattr(owner, "production_core_connected", False) is True)
                if (owner is not None and type(report.get("calls")) is dict
                        and report["calls"].get("schema") == "semloom.lotus_call_ledger.v1"):
                    late_close = getattr(owner, "last_query_report", None)
                    if late_close is None:
                        late_close = report.get("owner_close", {}).get("last_query_report")
                    if late_close is not None:
                        report["backend_close"] = late_close
                        query.merge_unconsumed_call_results(report, source.expected(config["query_id"]),
                                                      late_close, primary=failure)
            except BaseException as cleanup:
                report.update(status="failed", close_receipt_error_type=type(cleanup).__name__)
                if failure is None:
                    failure = cleanup
                    report.update(error_type=type(cleanup).__name__, error=redact_text(str(cleanup)))
                else:
                    failure.add_note("owner close accounting also failed: " + type(cleanup).__name__)
        if is_async:
            report.update(backend=config["backend"], execution_kind="optional async local-model qualification",
                          performance_claim=False, generation=asdict(generation))
            if async_limits is not None:
                report["requested_async_limits"] = asdict(async_limits)
            elif "async_limits" not in config:
                report["requested_async_limits"] = None
            try:
                if owner is not None:
                    metadata = owner.execution_metadata()
                    if type(metadata) is not dict:
                        raise TypeError("async execution metadata must be an object")
                    report["execution_metadata"] = json.loads(json.dumps(metadata, allow_nan=False))
            except BaseException as cleanup:
                report.update(status="failed", execution_metadata_error_type=type(cleanup).__name__)
                if failure is None:
                    failure = cleanup
                else:
                    failure.add_note("async execution metadata also failed: " + type(cleanup).__name__)
        if service_pool is not None:
            try:
                if owner is not None and type(report.get('calls')) is dict:
                    from .movie_records_service import accept_service_results
                    transfer = accept_service_results(owner, report, source.expected(config['query_id']), root,
                                                       primary=failure)
                    if transfer is not None:
                        report['service_result_transfer'] = transfer
                        report['owner_close'] = owner.last_close_report
                        report['execution_metadata'] = owner.execution_metadata()
                report['service_payload_pool'] = service_pool.close()
            except BaseException as cleanup:
                report['status'] = 'failed'
                report['service_result_transfer_error_type'] = type(cleanup).__name__
                report['service_payload_pool'] = service_pool.close()
                if failure is None:
                    failure = cleanup
                else:
                    failure.add_note('service result acceptance failed: ' + type(cleanup).__name__)
        try:
            if service_pool is not None and ray is not None:
                from .movie_records_service import finish_service_runtime
                report['service_runtime_close'] = finish_service_runtime(ray, service_runtime, owner)
                if report['service_runtime_close']['status'] == 'closed':
                    service_runtime_event = 'runtime_closed'
                else:
                    service_runtime_event = 'runtime_retained'
                    raise RuntimeError('original Ray retained for pending owner cleanup')
            elif ray is not None and ray.is_initialized():
                ray.shutdown()
        except BaseException as cleanup:
            if service_pool is not None and service_runtime_event != 'runtime_retained':
                service_runtime_event = 'runtime_close_unconfirmed'
            report.update(status="failed", ray_close_error_type=type(cleanup).__name__)
            if failure is None:
                failure = cleanup
            else:
                failure.add_note("Ray close also failed: " + type(cleanup).__name__)
        report["independent_gpu_process_exit_check"] = False
        try:
            emit(service_runtime_event if service_pool is not None else 'runtime_closed')
        except BaseException as observation:
            report.setdefault("observer_errors", []).append(redact_text(str(observation)))
            report["status"] = "failed"
            if failure is None:
                failure = observation
            else:
                failure.add_note("runtime close observation also failed: " + type(observation).__name__)
        try:
            write_private_json(root / "query.json", report)
        except BaseException as recording:
            if failure is None:
                failure = recording
            else:
                failure.add_note("query report also failed: " + type(recording).__name__)
    if failure is not None:
        failure.query_report = report
        if service_pool is not None:
            failure.records_payload_pool = service_pool
            failure.records_owner = owner
            failure.records_runtime = service_runtime
            from functools import partial
            from .movie_records_service import finish_service_runtime
            failure.records_finish_runtime = (None if ray is None else
                partial(finish_service_runtime, ray, service_runtime, owner))
        raise failure
