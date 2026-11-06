"""Movie Q3 evaluation and explicit finite ordinary-record chain entry points."""

import argparse
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys
import time

from src.infrastructure.private_artifacts import new_private_directory, write_private_json
from src.infrastructure.redact import redact_text
from src.baselines.text.movie_records import (
    MovieRecordSource, prepare_movie_source, validate_movie_method,
)
from src.baselines.text.sembench_movie import verify_checkout
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from .movie_records_query import fixed_response_qualification
from .movie_records_worker import run_model_worker

# Shared configuration for this runner and session execution.
from src.experiments.movie_records_config import (
    ASYNC_CORE_BACKEND as _ASYNC_BACKEND,
    daft_native_profile as _daft_native_profile,
    record_batching_options as _record_batching_options,
    records_runtime as _records_runtime,
    records_resources as _records_resources,
)


BACKENDS = ("ray-data-local-model", "semloom-local-model-static-reference", "daft-native-local-model",
            "semloom-local-model-core", "semloom-local-model-core-async")
CASCADE_BACKENDS = ("ray-data-local-model-cascade", "daft-native-local-model-cascade",
                    "semloom-local-model-core-cascade")


def read_config(path):
    from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec

    value = json.loads(Path(path).read_text())
    allowed = {"schema", "backend", "query_id", "task", "method", "generation", "engine", "resources", "async_limits", "core_capacity", "core_profile", "prepared_batching", "batch_concurrency", "batch_feedback", "rolling_refill", "experimental_skip_token_packing", "preparation_mode", "service_payload_bytes", "profile"}
    if (type(value) is not dict or set(value) - allowed
            or value.get("schema") != "semloom.movie_records_run.v1"
            or value.get("backend") not in BACKENDS):
        raise ValueError("unsupported ordinary Movie records configuration")
    if "core_profile" in value and value["backend"] not in ("semloom-local-model-core", _ASYNC_BACKEND):
        raise ValueError("core_profile requires a Core backend")
    if value['backend'] == 'daft-native-local-model':
        _daft_native_profile(value.get('profile', {}))
    elif 'profile' in value:
        raise ValueError('profile requires the Daft native backend')
    method = FilterMethod.from_dict(value["method"])
    validate_movie_method(method, value.get("task", "movie-q3"))
    generation = GenerationSpec.from_dict(value.get("generation", {}))
    resources = _records_resources(value)
    if value['backend'] == 'semloom-local-model-static-reference':
        resources.request_capacity()
    if value["backend"] in ("semloom-local-model-core", _ASYNC_BACKEND) and (
            resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError("Core records currently requires prepare_batch_rows=model_batch_rows=1")
    is_core = value["backend"] in ("semloom-local-model-core", _ASYNC_BACKEND)
    if not is_core and (set(value) & {'prepared_batching', 'batch_concurrency', 'batch_feedback', 'rolling_refill',
                                     'experimental_skip_token_packing', 'preparation_mode'}):
        raise ValueError('record batching options require a Core backend')
    if not is_core and 'service_payload_bytes' in value:
        raise ValueError('service_payload_bytes requires an async Core backend')
    if not is_core and set(value) & {"async_limits", "core_capacity"}:
        raise ValueError("async_limits and core_capacity require a Core backend")
    engine = EngineSpec.from_dict(value["engine"])
    if is_core:
        from src.execution_provider.adapters.records_profile import resolve_record_batching
        configured = _records_runtime(value, resources=resources, engine=engine)
        resolved = configured.profile
        resolve_record_batching(resolved, **_record_batching_options(value))
    query_id = value["query_id"]
    # Validate query identity even for empty source relations.
    from src.semantic_methods.lotus.records import identity
    identity(query_id, "preflight", 0)
    return value, method, generation, resources, engine


def model_preflight(source, checkout, method, generation, engine):
    import lotus
    from src.semantic_methods.lotus.engine import make_lm, validate_filter_source, validate_lm

    validate_movie_method(method, source.task.name)
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
    from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec

    value = json.loads(Path(path).read_text())
    required = {"schema", "backend", "query_id", "method", "helper_model_name", "engines",
                "generations", "cascade_args", "resources", "max_calls"}
    if (type(value) is not dict or not required <= set(value) or set(value) - required - {'profile'}
            or value["schema"] != "semloom.movie_records_cascade_run.v1"
            or value["backend"] not in CASCADE_BACKENDS):
        raise ValueError("unsupported Movie helper-cascade configuration")
    if value['backend'] == 'daft-native-local-model-cascade':
        _daft_native_profile(value.get('profile', {}))
    elif 'profile' in value:
        raise ValueError('profile requires the Daft native cascade backend')
    method = FilterMethod.from_dict(value["method"])
    validate_movie_method(method)
    helper = replace(method, model_name=value["helper_model_name"])
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
    if value['backend'] == 'semloom-local-model-core-cascade':
        resources.request_capacity()
    if value["backend"] == "semloom-local-model-core-cascade" and (
            resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1):
        raise ValueError("cascade Core requires prepare_batch_rows=model_batch_rows=1")
    engines = {role: EngineSpec.from_dict(raw) for role, raw in value["engines"].items()}
    if any(spec.max_model_calls is None for spec in engines.values()):
        raise ValueError("both cascade engines require an explicit max_model_calls lifetime cap")
    if Path(engines["main"].model_path).resolve() == Path(engines["helper"].model_path).resolve():
        raise ValueError("cascade requires distinct explicit main and helper model paths")
    for role in engines:
        limit = engines[role].max_model_len
        if limit is not None and generations[role].max_tokens > limit:
            raise ValueError("cascade max_tokens exceeds max_model_len")
    return value, method, generations, resources, engines, args


def _cascade_lms(config, method, generations, engines):
    from tokenizers import Tokenizer
    from src.semantic_methods.lotus.engine import make_lm

    models = {}
    for role, binding in (("main", method), ("helper", replace(method, model_name=config["helper_model_name"]))):
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

    validate_movie_method(method)
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
    options = {}
    if config["backend"] == "ray-data-local-model-cascade":
        from src.baselines.text.frameworks.ray_data_lotus_cascade import RayCascadeBackend as Backend
    elif config["backend"] == "daft-native-local-model-cascade":
        from src.baselines.text.frameworks.daft_lotus_cascade import DaftCascadeBackend as Backend
        options = _daft_native_profile(config.get('profile', {}))
    elif config["backend"] == "semloom-local-model-core-cascade":
        from src.execution_provider.adapters.lotus_cascade_core import CoreCascadeBackend as Backend
    else:
        raise ValueError("unsupported cascade backend")
    return Backend(source, engines=engines, resources=resources, **options)


def _cascade_model_worker(config, source, checkout, method, generations, resources, engines, cascade_args, root):
    if config['backend'] == 'semloom-local-model-core-cascade':
        resources.request_capacity()
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
            slots = resources.cpu_workers + resources.gpu_workers + 1
            if config["backend"] == "semloom-local-model-core-cascade":
                from src.infrastructure.cpu_allocation import checked_local_cpu_slots
                slots = checked_local_cpu_slots(slots)
            ray.init(address="local", num_cpus=slots,
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
    prepare.add_argument("--task", choices=("movie-q1", "movie-q2", "movie-q3", "movie-q4"), default=None)
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
        from src.serving.backends.vllm_protocol import GenerationSpec
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
            supervise(command, root, timeout_s=None if resources.query_timeout_s is None else resources.startup_timeout_s + resources.query_timeout_s + 30)
        return 0
    config, method, generation, resources, engine = read_config(args.config)
    if config.get("task", "movie-q3") != source.task.name:
        raise ValueError("execution configuration task differs from its source binding")
    preflight = model_preflight(source, args.sembench_checkout, method, generation, engine)
    if config["backend"] in ("semloom-local-model-core", _ASYNC_BACKEND):
        from src.execution_provider.adapters.records_profile import resolve_record_batching
        configured = _records_runtime(config, resources=resources, engine=engine)
        preflight['core_execution'] = configured.profile
        if 'core_capacity' in config or configured.capacity.budget is not None:
            preflight['core_capacity'] = asdict(configured.capacity)
        batching = resolve_record_batching(preflight['core_execution'], **_record_batching_options(config))
        if batching is not None:
            preflight['record_batching'] = batching
        if preflight["core_execution"]["mode"] == "async":
            limits = configured.async_limits if 'async_limits' in config else None
            preflight["requested_async_limits"] = None if limits is None else asdict(limits)
            preflight['resolved_async_limits_before_feedback'] = asdict(configured.async_limits)
    if args.action == "check" or not args.execute_model:
        print(json.dumps(preflight, ensure_ascii=False))
        return 0
    root = Path(args.output_dir).resolve()
    if args.worker:
        run_model_worker(config, source, args.sembench_checkout, method, generation, resources, engine, root)
    else:
        from .lotus_records_supervisor import supervise
        new_private_directory(root)
        write_private_json(root / "preflight.json", preflight)
        command = [sys.executable, str(Path(__file__).parents[2] / "scripts/experiments/run_lotus_records_movie.py"),
                   "run", "--source-manifest", str(source.manifest_path),
                   "--sembench-checkout", str(Path(args.sembench_checkout).resolve()),
                   "--config", str(Path(args.config).resolve()), "--output-dir", str(root),
                   "--execute-model", "--worker"]
        supervise(command, root, timeout_s=None if resources.query_timeout_s is None else resources.startup_timeout_s + resources.query_timeout_s + 30)
    return 0
