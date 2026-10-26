"""Movie Q3 evaluation and explicit finite ordinary-record chain entry points."""

import argparse
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys
import time

from src.baselines.common.private_artifacts import json_metric, new_private_directory, write_private_json
from src.baselines.common.redact import redact_text
from src.baselines.text.movie_records import (
    MovieRecordSource, RECORD_COLUMNS, prepare_movie_source, validate_movie_method,
)
from src.baselines.text.sembench_movie import (
    classification_audit, evaluate_original, run_original_lotus_query, verify_checkout,
)
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources


BACKENDS = ("ray-data-local-model", "semloom-local-model-static-reference", "daft-native-local-model",
            "semloom-local-model-core", "semloom-local-model-core-async")
_ASYNC_BACKEND = "semloom-local-model-core-async"
CASCADE_BACKENDS = ("ray-data-local-model-cascade", "daft-native-local-model-cascade",
                    "semloom-local-model-core-cascade")


def _fixed_method(method, task="movie-q3"):
    validate_movie_method(method, task)


def _finish(iterator, primary):
    try:
        close = getattr(iterator, "close", None)
        if close is not None:
            close()
    except BaseException as error:
        if primary is None:
            raise
        primary.add_note("records result close also failed: " + type(error).__name__)


def merge_unconsumed_call_results(report, expected, close_report, *, primary=None):
    """Merge terminal close receipts into accounting, without interpreting them.

    Only IDs marked here as runner-created missing-call placeholders may later
    receive a real terminal result. A backend's own unknown remains authoritative.
    Bad receipts do not discard valid receipts on either side of them.
    """
    previous = CallLedger(expected, max_calls=max(1, len(expected)))
    for row in report["calls"]["records"]:
        previous.record(row)
    rows = previous.report()["records"]
    declared = {row["call_id"]: row for row in expected}
    placeholder_ids = set(report.get("runner_unconfirmed_call_ids", []))
    placeholders = {row["call_id"]: row for row in rows
                    if row["call_id"] in placeholder_ids and row["status"] == "unknown"
                    and row["submission_confirmed"] is False
                    and row["prompt_tokens"] is None and row["completion_tokens"] is None}
    ledger = CallLedger(expected, max_calls=max(1, len(expected)))
    known = {}
    for row in rows:
        if row["call_id"] not in placeholders:
            ledger.record(row)
            known[row["call_id"]] = row
    errors = []
    receipts = [] if close_report is None else close_report.get("unconsumed_call_results", [])
    if type(receipts) is not list:
        errors.append(ValueError("unconsumed call results must be a list"))
        receipts = []
    for receipt in receipts:
        try:
            call_id = receipt.get("call_id") if type(receipt) is dict else None
            if type(call_id) is not str or call_id not in declared:
                raise ValueError("close receipt belongs to an undeclared call identity")
            checked = CallLedger([declared[call_id]], max_calls=1)
            checked.record(receipt)
            row = checked.report()["records"][0]
            if call_id in known:
                if row != known[call_id]:
                    raise ValueError("close receipt conflicts with an existing call result")
                continue
            ledger.record(row)
            known[call_id] = row
            placeholders.pop(call_id, None)
        except BaseException as error:
            errors.append(error)
    for row in placeholders.values():
        ledger.record(row)
    missing = ledger.report()["missing_call_ids"]
    failure = primary if primary is not None else (errors[0] if errors else None)
    if failure is not None:
        ledger.mark_unconfirmed(type(failure).__name__)
        placeholder_ids = set(placeholders) | set(missing)
        report["status"] = "failed"
    else:
        placeholder_ids = set(placeholders)
    report["calls"] = ledger.report()
    report["runner_unconfirmed_call_ids"] = [row["call_id"] for row in expected
                                             if row["call_id"] in placeholder_ids]
    for error in errors:
        detail = dict(error_type=type(error).__name__, error=redact_text(str(error)))
        recorded = report.setdefault("close_receipt_errors", [])
        if detail not in recorded:
            recorded.append(detail)
        if failure is not None:
            note = "records close receipt rejected: " + detail["error"]
            if note not in getattr(failure, "__notes__", []):
                failure.add_note(note)
    if errors and primary is None:
        raise errors[0]


def run_movie_query(source, checkout, method, generation, execute, *, query_id, observer=None):
    """Execute once; only compact final engine results may collect at the driver.

    execute is a lazy backend factory. Preflight and LM policy validation happen
    before calling it. A failure carries query_report, including every known call.
    """
    import lotus
    from src.semantic_methods.lotus.engine import (
        interpret_results, lm_report, make_lm, validate_filter_source, validate_lm,
    )
    emit = observer if observer is not None else lambda _event: None
    emit("query_source_start")
    task = source.task
    _fixed_method(method, task.name)
    inputs = source.preflight(method)
    upstream = verify_checkout(checkout)
    method_source = validate_filter_source()
    expected = source.expected(query_id)
    emit("query_source_ready")
    ledger = CallLedger(expected, max_calls=max(1, len(expected)))
    lm = make_lm(method, generation)
    report = dict(schema="semloom.movie_records_query.v1", query_id=query_id,
                  status="failed", source=inputs, task=task.describe(), method=asdict(method),
                  generation=asdict(generation), method_source=method_source,
                  production_core_used=False, model_calls_are_not_output_rows=True,
                  query_timing_scope="backend construction, scan, prepare, model, native interpretation and task projection/aggregation",
                  evaluation_in_query_seconds=False, upstream_accessor_used_in_records=False)
    iterator = None
    primary = None
    started = time.monotonic()
    try:
        with lotus.settings.context(lm=lm, enable_cache=False):
            validate_lm(lm, method, generation)
            iterator = iter(execute())
            report["production_core_used"] = getattr(iterator, "production_core_connected", False) is True
            if report["production_core_used"]:
                profile = getattr(iterator, "execution_profile", None)
                if profile is not None:
                    report["core_execution"] = profile
            raw = []
            for value in iterator:
                ledger.record(value)
                raw.append(value)
            missing = ledger.report()["counts"]["missing"]
            if missing:
                raise ValueError("records execution omitted declared calls")
            by_call = {row["call_id"]: row for row in raw}
            ordered = [by_call[row["call_id"]] for row in expected]
            decisions = interpret_results(ordered, lm, method)
            result = task.result_frame(source.rows(), decisions)
            report.update(result=task.encode_result(result), decisions=decisions,
                          query_seconds=time.monotonic() - started)
            emit("result_ready")
            evaluated_at = time.monotonic()
            emit("evaluation_start")
            labeled = source.labeled_frame()
            quality = evaluate_original(upstream, task.query, result, labeled)
            rows = source.rows()
            labels = dict(zip((row["row_id"] for row in rows), labeled["scoreSentiment"], strict=True))
            quality["classification_audit"] = classification_audit(
                {row["row_id"]: labels[row["row_id"]] for row in expected},
                [(row["row_id"], "POSITIVE" if row["decision"] else "NEGATIVE") for row in decisions],
            )
            report.update(quality=json_metric(quality),
                          evaluation_seconds=time.monotonic() - evaluated_at,
                          status="completed")
            emit("evaluation_end")
    except BaseException as error:
        primary = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
    finally:
        try:
            if iterator is not None:
                _finish(iterator, primary)
        except BaseException as error:
            primary = error
            report.update(status="failed", error_type=type(error).__name__,
                          error=redact_text(str(error)))
        report["calls"] = ledger.report()
        try:
            report["backend_close"] = getattr(iterator, "last_close_report", None)
            merge_unconsumed_call_results(report, expected, report["backend_close"], primary=primary)
        except BaseException as error:
            if primary is None:
                primary = error
                report.update(error_type=type(error).__name__, error=redact_text(str(error)))
            else:
                primary.add_note("records close accounting also failed: " + type(error).__name__)
            merge_unconsumed_call_results(report, expected, None, primary=primary)
        report["lm"] = lm_report(lm)
        report["elapsed_seconds"] = time.monotonic() - started
        try:
            emit("query_cleanup_end")
        except BaseException as observation:
            report.setdefault("observer_errors", []).append(redact_text(str(observation)))
            if primary is None:
                primary = observation
            else:
                primary.add_note("cleanup observation also failed: " + type(observation).__name__)
        if primary is not None:
            report["status"] = "failed"
            primary.query_report = report
    if primary is not None:
        raise primary
    return report


def fixed_response_qualification(source, checkout, method, generation, responses, *, query_id):
    """Compare a small software fixture with unchanged native LOTUS Q3, without I/O calls."""
    import lotus
    import pandas as pd
    from src.baselines.text.frameworks.lotus_pg import observe_filter_rows
    from src.semantic_methods.lotus.engine import (
        LotusFilterMethod, bind_filter_method, engine_response, lm_report, make_lm, validate_filter_source,
    )

    task = source.task
    _fixed_method(method, task.name)
    source.preflight(method)
    validate_filter_source()
    expected = source.expected(query_id)
    if len(responses) != len(expected):
        raise ValueError("fixed responses must cover every eligible occurrence")
    by_row = {}
    for row in responses:
        if row["row_id"] in by_row:
            raise ValueError("duplicate fixed-response occurrence")
        by_row[row["row_id"]] = row
    if set(by_row) != {row["row_id"] for row in expected}:
        raise ValueError("fixed responses have foreign or missing occurrences")
    raw = []
    for identity in expected:
        value = dict(by_row[identity["row_id"]])
        raw.append(dict(value, **identity, status="succeeded", submission_confirmed=True,
                        engine_uid="software-fixture", engine_request_id=identity["call_id"],
                        finish_reason="stop", logprobs_json="null",
                        total_tokens=value["prompt_tokens"] + value["completion_tokens"],
                        usage_source="configured software fixture; no inference"))
    source_rows = {row["row_id"]: row for row in source.rows()}
    preparer = LotusFilterMethod(bind_filter_method(method))
    prepared = preparer.prepare_rows([dict(source_rows[row["row_id"]], **row) for row in expected])
    report = run_movie_query(source, checkout, method, generation, lambda: iter(raw), query_id=query_id)

    try:
        reference = make_lm(method, generation)
        captured, native_decisions, native_prompts = [], [], []
        def fixed(uncached, kwargs, *_):
            captured.append(dict(messages=[message for message, _ in uncached], kwargs=kwargs))
            if len(uncached) != len(raw):
                raise ValueError("native Q3 requested an unexpected fixture call shape")
            return [engine_response(row, reference) for row in raw]
        reference._process_uncached_messages = fixed
        frame = pd.DataFrame(source.rows(), columns=RECORD_COLUMNS).rename(
            columns={"row_id": "_execution_row_id"})
        with lotus.settings.context(lm=reference, enable_cache=False):
            with observe_filter_rows(native_decisions.extend, native_prompts.extend):
                result = run_original_lotus_query(checkout, task.query, lambda: frame)
        native_messages = [message for call in captured for message in call["messages"]]
        if (len(captured) != 1
                or native_messages != [json.loads(row["messages_json"]) for row in prepared]):
            raise ValueError("records preparation differs from original LOTUS messages")
        observed = [(row["row_id"], row["decision"]) for row in report["decisions"]]
        if native_decisions != observed or task.encode_result(result) != report["result"]:
            raise ValueError("records Filter decisions or task projection differ from original LOTUS")
        if lm_report(reference)["stats"] != report["lm"]["stats"]:
            raise ValueError("records physical/virtual usage differs from original LM accounting")
        report.update(execution_kind="fixed-response software qualification", inference_calls=0,
                      native_reference=dict(messages_equal=True, decisions_equal=True, result_equal=True,
                                            count_equal=True if task.query == 3 else None,
                                            lm_stats_equal=True, effective_kwargs=captured[0]["kwargs"]))
    except BaseException as error:
        report.update(status="failed", error_type=type(error).__name__, error=redact_text(str(error)),
                      inference_calls=0, execution_kind="fixed-response software qualification")
        error.query_report = report
        raise
    return report


def _async_limits(config):
    from src.serving.backends.vllm_async_engine import AsyncEngineLimits

    if "async_limits" not in config:
        return None  # Let the actor owner bind defaults to the Core request limit.
    value = config["async_limits"]
    if type(value) is not dict:
        raise ValueError("async_limits must be an object")
    return AsyncEngineLimits(**value)


def _core_profile(config):
    profile = config.get("core_profile")
    if config["backend"] == _ASYNC_BACKEND:
        if profile is not None and (type(profile) is not dict or profile.get("mode", "async") != "async"):
            raise ValueError("the explicit async backend requires async Core mode")
        profile = dict(profile or {}, mode="async")
    return profile


def read_config(path):
    from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

    value = json.loads(Path(path).read_text())
    allowed = {"schema", "backend", "query_id", "task", "method", "generation", "engine", "resources", "async_limits", "core_profile"}
    if (type(value) is not dict or set(value) - allowed
            or value.get("schema") != "semloom.movie_records_run.v1"
            or value.get("backend") not in BACKENDS):
        raise ValueError("unsupported ordinary Movie records configuration")
    if "core_profile" in value and value["backend"] not in ("semloom-local-model-core", _ASYNC_BACKEND):
        raise ValueError("core_profile requires a Core backend")
    method = FilterMethod.from_dict(value["method"])
    _fixed_method(method, value.get("task", "movie-q3"))
    generation = GenerationSpec.from_dict(value.get("generation", {}))
    resources = RecordsResources(**value.get("resources", {}))
    if value["backend"] in ("semloom-local-model-core", _ASYNC_BACKEND) and (
            resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError("Core records currently requires prepare_batch_rows=model_batch_rows=1")
    is_core = value["backend"] in ("semloom-local-model-core", _ASYNC_BACKEND)
    limits = None
    if is_core:
        profile = _core_profile(value)
        limits = _async_limits(value)
        if limits is not None:
            if type(profile) is not dict or profile.get("mode", "sync") != "async":
                raise ValueError("async_limits requires the explicit async backend or async Core profile")
            if (limits.max_inflight > resources.max_active_batches
                    or limits.result_slots < limits.max_inflight):
                raise ValueError("async_limits exceed the Core request or result capacity")
    elif "async_limits" in value:
        raise ValueError("async_limits requires the explicit async Core backend")
    engine = EngineSpec.from_dict(value["engine"])
    if is_core:
        from src.execution_provider.adapters.lotus_records_core import resolve_core_profile
        resolved = resolve_core_profile(engine, resources, profile)
        if limits is not None and (limits.max_inflight != resolved["max_inflight"]
                                   or limits.result_slots < resolved["max_inflight"]):
            raise ValueError("async_limits must cover and match the Core request limit")
    query_id = value["query_id"]
    # Validate query identity even for empty source relations.
    from src.semantic_methods.lotus.records import identity
    identity(query_id, "preflight", 0)
    return value, method, generation, resources, engine


def model_preflight(source, checkout, method, generation, engine):
    import lotus
    from src.semantic_methods.lotus.engine import make_lm, validate_filter_source, validate_lm

    _fixed_method(method, source.task.name)
    inputs = source.preflight(method)
    verify_checkout(checkout)
    method_source = validate_filter_source()
    lm = make_lm(method, generation)
    with lotus.settings.context(lm=lm, enable_cache=False):
        validate_lm(lm, method, generation)
    environment = engine.validate_driver_environment()
    return dict(status="preflight_only", inference_calls=0, source=inputs,
                method_source=method_source, engine_environment=environment)


def read_cascade_config(path):
    """An explicit global helper-LM profile, separate from ordinary Filter runs."""
    from lotus.types import CascadeArgs
    from src.semantic_methods.lotus.cascade import cascade_identity
    from src.semantic_methods.lotus.engine import validate_generation
    from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec

    value = json.loads(Path(path).read_text())
    required = {"schema", "backend", "query_id", "method", "helper_model_name", "engines",
                "generations", "cascade_args", "resources", "max_calls"}
    if (type(value) is not dict or set(value) != required
            or value["schema"] != "semloom.movie_records_cascade_run.v1"
            or value["backend"] not in CASCADE_BACKENDS):
        raise ValueError("unsupported Movie helper-cascade configuration")
    method = FilterMethod.from_dict(value["method"])
    _fixed_method(method)
    helper = FilterMethod(value["helper_model_name"])
    helper.describe()
    for name in ("engines", "generations"):
        if type(value[name]) is not dict or set(value[name]) != {"main", "helper"}:
            raise ValueError(name + " must declare exactly main and helper")
    generations = {role: GenerationSpec.from_dict(raw) for role, raw in value["generations"].items()}
    if (generations["main"].logprobs is not None or generations["helper"].logprobs is None
            or not 1 <= generations["helper"].logprobs <= 20):
        raise ValueError("main logprobs must be None and helper top logprobs must be 1..20")
    for role, binding in (("main", method), ("helper", helper)):
        validate_generation(binding, replace(generations[role], logprobs=None))
    raw_args = value["cascade_args"]
    if type(raw_args) is not dict or set(raw_args) - set(CascadeArgs.model_fields):
        raise ValueError("cascade_args must contain only original CascadeArgs fields")
    args = CascadeArgs.model_validate_json(json.dumps(raw_args), strict=True)
    if type(value["max_calls"]) is not int or value["max_calls"] < 1:
        raise ValueError("max_calls must be a positive integer")
    cascade_identity(value["query_id"], "preflight", 0, 0, model_role="helper", stage="helper_score")
    resources = RecordsResources(**value["resources"])
    if value["backend"] == "semloom-local-model-core-cascade" and (
            resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError("cascade Core requires prepare_batch_rows=model_batch_rows=1")
    engines = {role: EngineSpec.from_dict(raw) for role, raw in value["engines"].items()}
    if any(spec.max_model_calls is None for spec in engines.values()):
        raise ValueError("both cascade engines require an explicit max_model_calls lifetime cap")
    if Path(engines["main"].model_path).resolve() == Path(engines["helper"].model_path).resolve():
        raise ValueError("cascade requires distinct explicit main and helper model paths")
    for role in engines:
        if generations[role].max_tokens > engines[role].max_model_len:
            raise ValueError("cascade max_tokens exceeds max_model_len")
    return value, method, generations, resources, engines, args


def _cascade_lms(config, method, generations, engines):
    from tokenizers import Tokenizer
    from src.semantic_methods.lotus.engine import make_lm

    models = {}
    for role, binding in (("main", method), ("helper", FilterMethod(config["helper_model_name"]))):
        generation, engine = generations[role], engines[role]
        lm = make_lm(binding, replace(generation, logprobs=None))
        lm.tokenizer = Tokenizer.from_file(str(Path(engine.tokenizer_path or engine.model_path) / "tokenizer.json"))
        for label in ("True", "False"):
            ids = lm.tokenizer.encode(label, add_special_tokens=False).ids
            if len(ids) != 1 or lm.encode_text(label) != ids:
                raise ValueError("original LM and deployed tokenizer label IDs differ")
        if role == "helper":
            lm.kwargs["top_logprobs"] = generation.logprobs
        models[role] = lm
    return models


def cascade_model_preflight(config, source, checkout, method, generations, engines, cascade_args):
    """Validate original cascade semantics and both local deployments; allocate no model."""
    import lotus
    import pandas as pd
    from src.semantic_methods.lotus.cascade import validate_cascade, validate_cascade_source

    _fixed_method(method)
    inputs = source.preflight(method)
    verify_checkout(checkout)
    environment = {role: spec.validate_driver_environment() for role, spec in engines.items()}
    models = _cascade_lms(config, method, generations, engines)
    frame = pd.DataFrame([dict(row_id=row["row_id"], source_position=row["source_position"],
                               reviewText=row["reviewText"])
                          for row in source.rows() if row["id"] == "taken_3"],
                         columns=["row_id", "source_position", "reviewText"])
    with lotus.settings.context(enable_cache=False):
        args, upper, effective = validate_cascade(frame, models["main"], models["helper"], cascade_args,
            instruction=method.instruction, max_rows=max(1, inputs["rows"]), max_calls=config["max_calls"])
    n = len(frame)
    sample = int(n * args.sampling_percentage) if args.filter_pos_cascade_threshold is None else 0
    stage_limits = dict(helper=n, main=max(n, sample))
    if any(engines[role].max_model_calls < cap for role, cap in stage_limits.items()):
        raise ValueError("engine lifetime cap cannot cover every possible cascade stage")
    return dict(status="preflight_only", inference_calls=0, source=inputs,
                method_source=validate_cascade_source(), engine_environment=environment,
                method=method.describe(), helper_model_name=config["helper_model_name"],
                cascade_args=args.model_dump(mode="json"),
                generations={role: asdict(profile) for role, profile in generations.items()},
                effective_generation=effective, max_calls=config["max_calls"], max_possible_calls=upper,
                max_stage_calls=stage_limits, engine_lifetime_caps={role: spec.max_model_calls
                    for role, spec in engines.items()}, budget_scope="query max_calls and separate per-stage engine lifetimes")


def _cascade_backend(config, source, engines, resources):
    if config["backend"] == "ray-data-local-model-cascade":
        from src.baselines.text.frameworks.ray_data_lotus_cascade import RayCascadeBackend as Backend
    elif config["backend"] == "daft-native-local-model-cascade":
        from src.baselines.text.frameworks.daft_lotus_cascade import DaftCascadeBackend as Backend
    elif config["backend"] == "semloom-local-model-core-cascade":
        from src.execution_provider.adapters.lotus_cascade_core import CoreCascadeBackend as Backend
    else:
        raise ValueError("unsupported cascade backend")
    return Backend(source, engines=engines, resources=resources)


def _cascade_model_worker(config, source, checkout, method, generations, resources, engines, cascade_args, root):
    import lotus
    from .lotus_cascade_movie import run_movie_cascade_query

    ray = backend = failure = None
    report = dict(status="failed", query_id=config["query_id"])
    started = time.monotonic()
    try:
        # Repeat the public gates in a directly invoked worker, before runtime allocation.
        cascade_model_preflight(config, source, checkout, method, generations, engines, cascade_args)
        models = _cascade_lms(config, method, generations, engines)
        if config["backend"] != "daft-native-local-model-cascade":
            import ray
            ray.init(address="local", num_cpus=resources.cpu_workers + resources.gpu_workers + 1,
                     num_gpus=1, include_dashboard=False,
                     runtime_env={"env_vars": {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                         "LITELLM_LOCAL_MODEL_COST_MAP": "True", "VLLM_WORKER_MULTIPROC_METHOD": "spawn"}})
        runtime_seconds = time.monotonic() - started
        backend = _cascade_backend(config, source, engines, resources)
        with lotus.settings.context(enable_cache=False):
            report = run_movie_cascade_query(source, checkout, models["main"], models["helper"],
                cascade_args, backend, query_id=config["query_id"], max_calls=config["max_calls"])
        report["runtime_setup_seconds"] = runtime_seconds
    except BaseException as error:
        failure = error
        report = getattr(error, "query_report", report)
        report.update(status="failed", error_type=type(error).__name__, error=redact_text(str(error)))
    finally:
        try:
            close = getattr(backend, "close", None)
            if close is not None:
                report["owner_close"] = close()
                if report["owner_close"] is not None and (
                        report["owner_close"].get("status") != "closed"
                        or report["owner_close"].get("cleanup_errors")):
                    raise RuntimeError("cascade owner cleanup remains unconfirmed")
            reports = getattr(backend, "close_reports", [])
            report["backend_close"] = reports
            if any(row.get("stage_process_exit_confirmed") is not True for row in reports):
                raise RuntimeError("cascade stage cleanup remains unconfirmed")
        except BaseException as cleanup:
            report.update(status="failed", close_error_type=type(cleanup).__name__)
            if failure is None:
                failure = cleanup
            else:
                failure.add_note("cascade owner close also failed: " + type(cleanup).__name__)
        finally:
            try:
                report["backend_close"] = getattr(backend, "close_reports", report.get("backend_close", []))
                report["production_core_used"] = report.get("production_core_used") is True or (
                    getattr(backend, "production_core_connected", False) is True)
            except BaseException as cleanup:
                report.update(status="failed", close_metadata_error_type=type(cleanup).__name__)
                if failure is None:
                    failure = cleanup
                else:
                    failure.add_note("cascade close metadata also failed: " + type(cleanup).__name__)
        try:
            if ray is not None and ray.is_initialized():
                ray.shutdown()
        except BaseException as cleanup:
            report.update(status="failed", ray_close_error_type=type(cleanup).__name__)
            if failure is None:
                failure = cleanup
            else:
                failure.add_note("Ray close also failed: " + type(cleanup).__name__)
        report.update(backend=config["backend"], execution_kind="original global helper-LM cascade",
            method=method.describe(), helper_model_name=config["helper_model_name"],
            cascade_args=cascade_args.model_dump(mode="json"),
            generations={role: asdict(profile) for role, profile in generations.items()},
            engine_lifetime_caps={role: spec.max_model_calls for role, spec in engines.items()},
            max_calls=config["max_calls"], performance_claim=False, independent_gpu_process_exit_check=False)
        try:
            write_private_json(root / "query.json", report)
        except BaseException as recording:
            report.update(status="failed", report_error_type=type(recording).__name__)
            if failure is None:
                failure = recording
            else:
                failure.add_note("cascade query report also failed: " + type(recording).__name__)
    if failure is not None:
        failure.query_report = report
        raise failure


def _model_worker(config, source, checkout, method, generation, resources, engine, root, *, observer=None):
    if config.get("task", "movie-q3") != source.task.name:
        raise ValueError("execution configuration task differs from its source binding")
    emit = observer if observer is not None else lambda _event: None
    ray = None
    owner = None
    async_limits = None
    is_core = config["backend"] in ("semloom-local-model-core", _ASYNC_BACKEND)
    profile = _core_profile(config) if is_core else None
    is_async = False
    failure = None
    report = {}
    runtime_started = time.monotonic()
    emit("runtime_setup_start")
    try:
        if is_core:
            from src.execution_provider.adapters.lotus_records_core import resolve_core_profile
            is_async = resolve_core_profile(engine, resources, profile)["mode"] == "async"
        if is_async:
            async_limits = _async_limits(config)
        if config["backend"] != "daft-native-local-model":
            import ray

            ray.init(address="local", num_cpus=resources.cpu_workers + resources.gpu_workers + 1,
                     num_gpus=1, include_dashboard=False,
                     runtime_env={"env_vars": {"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                                                 "LITELLM_LOCAL_MODEL_COST_MAP": "True",
                                                 "VLLM_WORKER_MULTIPROC_METHOD": "spawn"}})
        runtime_seconds = time.monotonic() - runtime_started
        emit("runtime_setup_end")
        def execute():
            nonlocal owner
            if config["backend"] == "daft-native-local-model":
                from src.baselines.text.frameworks.daft_lotus_records import evaluate
                return evaluate(source, method, engine, generation, resources, query_id=config["query_id"])
            if config["backend"] == "ray-data-local-model":
                from src.baselines.text.frameworks.ray_data_lotus_records import evaluate
                return evaluate(source, method, engine, generation, resources, query_id=config["query_id"])
            if is_core:
                from src.execution_provider.adapters.lotus_records_core import create_records_core
                owner = create_records_core(method, engine, generation, resources,
                                             profile=profile, async_limits=async_limits)
                return owner.evaluate(source, query_id=config["query_id"])
            from src.execution_provider.adapters.lotus_records_static import SemLoomRecordsStaticReference
            owner = SemLoomRecordsStaticReference(method, engine, generation, resources)
            return owner.evaluate(source, query_id=config["query_id"])
        report = run_movie_query(source, checkout, method, generation, execute,
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
                        merge_unconsumed_call_results(report, source.expected(config["query_id"]),
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
        try:
            if ray is not None and ray.is_initialized():
                ray.shutdown()
        except BaseException as cleanup:
            report.update(status="failed", ray_close_error_type=type(cleanup).__name__)
            if failure is None:
                failure = cleanup
            else:
                failure.add_note("Ray close also failed: " + type(cleanup).__name__)
        report["independent_gpu_process_exit_check"] = False
        try:
            emit("runtime_closed")
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
        raise failure


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else list(argv)
    if arguments and arguments[0] in ("chain-check", "chain-run"):
        from .lotus_records_chain import main as chain_main
        return chain_main(arguments)
    if arguments and arguments[0] in ("record-cascade-check", "record-cascade-run"):
        from .lotus_records_cascade import main as record_cascade_main
        return record_cascade_main(arguments)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    commands.add_parser("chain-check", help="check an ordinary record chain and its explicit IMLane owner")
    commands.add_parser("chain-run", help="run an ordinary chain through the native IMLane author backend")
    commands.add_parser("record-cascade-check", help="check a generic IMLane helper cascade and its owner")
    commands.add_parser("record-cascade-run", help="run a generic helper cascade through the IMLane author backend")
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--input-csv", required=True)
    prepare.add_argument("--input-sha256", required=True)
    prepare.add_argument("--max-rows", type=int, required=True)
    prepare.add_argument("--role", choices=("software_fixture", "sembench_movie_subset", "sembench_movie_official"), required=True)
    prepare.add_argument("--task", choices=("movie-q1", "movie-q2", "movie-q3"), default=None)
    prepare.add_argument("--output-dir", required=True)
    for name in ("check", "run", "fixture", "cascade-check", "cascade-run"):
        command = commands.add_parser(name)
        command.add_argument("--source-manifest", required=True)
        command.add_argument("--sembench-checkout", required=True)
        command.add_argument("--config", required=name != "fixture")
        if name not in ("check", "cascade-check"):
            command.add_argument("--output-dir", required=True)
        if name in ("run", "cascade-run"):
            command.add_argument("--execute-model", action="store_true")
            command.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
        if name == "fixture":
            command.add_argument("--responses", required=True)
            command.add_argument("--model-name", default="openai/local-fixture")
    args = parser.parse_args(argv)
    if args.action == "prepare":
        print(prepare_movie_source(args.input_csv, args.output_dir,
                                   expected_csv_sha256=args.input_sha256,
                                   max_rows=args.max_rows, role=args.role, task=args.task))
        return 0
    source = MovieRecordSource.from_manifest(args.source_manifest)
    if args.action == "fixture":
        from src.serving.backends.vllm_engine import GenerationSpec
        method, generation = source.task.method(args.model_name), GenerationSpec()
        root = Path(args.output_dir).resolve()
        new_private_directory(root)
        try:
            report = fixed_response_qualification(source, args.sembench_checkout, method, generation,
                                                 json.loads(Path(args.responses).read_text()), query_id=source.task.name + "-fixture")
        except BaseException as error:
            report = getattr(error, "query_report", dict(status="failed", error_type=type(error).__name__,
                                                         error=redact_text(str(error)), inference_calls=0))
            try:
                write_private_json(root / "query.json", report)
            except BaseException as recording:
                error.add_note("fixture report also failed: " + type(recording).__name__)
            raise
        write_private_json(root / "query.json", report)
        print(json.dumps(dict(status=report["status"], inference_calls=0, result=report["result"])))
        return 0
    if args.action in ("cascade-check", "cascade-run"):
        config, method, generations, resources, engines, cascade_args = read_cascade_config(args.config)
        preflight = cascade_model_preflight(config, source, args.sembench_checkout, method,
                                           generations, engines, cascade_args)
        if args.action == "cascade-check" or not args.execute_model:
            print(json.dumps(preflight, ensure_ascii=False))
            return 0
        root = Path(args.output_dir).resolve()
        if args.worker:
            _cascade_model_worker(config, source, args.sembench_checkout, method, generations,
                                  resources, engines, cascade_args, root)
        else:
            from .lotus_records_supervisor import supervise
            new_private_directory(root)
            write_private_json(root / "preflight.json", preflight)
            command = [sys.executable, str(Path(__file__).parents[2] / "scripts/experiments/run_lotus_records_movie.py"),
                "cascade-run", "--source-manifest", str(source.manifest_path),
                "--sembench-checkout", str(Path(args.sembench_checkout).resolve()),
                "--config", str(Path(args.config).resolve()), "--output-dir", str(root), "--execute-model", "--worker"]
            supervise(command, root, timeout_s=resources.startup_timeout_s + resources.query_timeout_s + 30)
        return 0
    config, method, generation, resources, engine = read_config(args.config)
    if config.get("task", "movie-q3") != source.task.name:
        raise ValueError("execution configuration task differs from its source binding")
    preflight = model_preflight(source, args.sembench_checkout, method, generation, engine)
    if config["backend"] in ("semloom-local-model-core", _ASYNC_BACKEND):
        from src.execution_provider.adapters.lotus_records_core import resolve_core_profile
        preflight["core_execution"] = resolve_core_profile(engine, resources, _core_profile(config))
        if preflight["core_execution"]["mode"] == "async":
            limits = _async_limits(config)
            preflight["requested_async_limits"] = None if limits is None else asdict(limits)
    if args.action == "check" or not args.execute_model:
        print(json.dumps(preflight, ensure_ascii=False))
        return 0
    root = Path(args.output_dir).resolve()
    if args.worker:
        _model_worker(config, source, args.sembench_checkout, method, generation, resources, engine, root)
    else:
        from .lotus_records_supervisor import supervise
        new_private_directory(root)
        write_private_json(root / "preflight.json", preflight)
        command = [sys.executable, str(Path(__file__).parents[2] / "scripts/experiments/run_lotus_records_movie.py"),
                   "run", "--source-manifest", str(source.manifest_path),
                   "--sembench-checkout", str(Path(args.sembench_checkout).resolve()),
                   "--config", str(Path(args.config).resolve()), "--output-dir", str(root),
                   "--execute-model", "--worker"]
        supervise(command, root, timeout_s=resources.startup_timeout_s + resources.query_timeout_s + 30)
    return 0
