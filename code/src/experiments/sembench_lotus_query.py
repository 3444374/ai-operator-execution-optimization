"""Original LOTUS SemBench queries on an independently supervised service.

One query runner retains the original operators, complete sink, prepaid POST
accounting and failure evidence. Visual and hierarchical text configurations
keep their established schemas, preflight requirements and legacy clocks.
"""

from contextlib import nullcontext
from dataclasses import asdict
import json
import math
from pathlib import Path
import time
from urllib.parse import urlsplit

from src.baselines.sembench_lotus import OriginalLotusQuery
from src.baselines.text import sembench_mmqa_aggregate as mmqa
from src.infrastructure.private_artifacts import (
    json_metric, new_private_directory, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text
from src.semantic_methods.lotus.source import source_report
from .sembench_image_query import check as check_inputs
from .native_service_query import (
    NativeServiceCalls, make_lm, native_query_settings, run_prepaid_query,
)


VISUAL_SCHEMA = 'semloom.sembench_lotus_image_run.v1'
AGGREGATE_SCHEMA = 'semloom.sembench_lotus_mmqa_aggregate_run.v1'


def configuration(path):
    from src.serving.backends.vllm_protocol import GenerationSpec

    config = read_json(path)
    aggregate = isinstance(config, dict) and config.get('schema') == AGGREGATE_SCHEMA
    required = {'schema', 'query_id', 'model_name', 'api_base', 'api_key_env', 'generation',
                'native_lm_max_batch_size', 'query_timeout_s', 'service_profile'}
    required |= ({'native_lm_max_ctx_len', 'token_counter'} if aggregate else {'scenario', 'query'})
    optional = {'budget'} if aggregate else {'budget', 'scale_factor'}
    if (type(config) is not dict or not required <= config.keys()
            or config.keys() - required - optional
            or config['schema'] not in (VISUAL_SCHEMA, AGGREGATE_SCHEMA)):
        raise ValueError('an explicit native LOTUS MMQA Q5 service configuration is required' if aggregate
            else 'an explicit native LOTUS visual-service configuration is required')
    for name in ('query_id', 'api_key_env'):
        if type(config[name]) is not str or not config[name].strip():
            raise ValueError(name + ' must be a nonempty string')
    if (type(config['model_name']) is not str or not config['model_name'].startswith('openai/')
            or not config['model_name'].removeprefix('openai/')):
        raise ValueError('native service model_name must be openai/<served-model>' if aggregate
            else 'native visual service model_name must be openai/<served-model>')
    address = urlsplit(config['api_base'])
    if (address.scheme not in ('http', 'https') or not address.hostname or address.username
            or address.password or address.query or address.fragment):
        raise ValueError('api_base must identify the declared service without credentials' if aggregate
            else 'api_base must identify the declared OpenAI-compatible service without credentials')
    names = ('native_lm_max_batch_size', 'native_lm_max_ctx_len') if aggregate else ('native_lm_max_batch_size',)
    for name in names:
        if type(config[name]) is not int or config[name] < 1:
            raise ValueError(name + ' must be positive')
    timeout = config['query_timeout_s']
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('query_timeout_s must be positive')
    profile = config['service_profile']
    if type(profile) is not dict:
        raise ValueError('native_lm_max_ctx_len must match the declared service_profile.max_model_len' if aggregate
            else 'service_profile must describe the external model/processor/service configuration')
    if aggregate:
        counter = config['token_counter']
        if (type(profile.get('max_model_len')) is not int
                or profile['max_model_len'] != config['native_lm_max_ctx_len']):
            raise ValueError('native_lm_max_ctx_len must match the declared service_profile.max_model_len')
        fields = {'kind', 'path', 'sha256', 'tokenizer_id', 'tokenizer_revision'}
        if (type(counter) is not dict or set(counter) != fields or counter['kind'] != 'huggingface_tokenizer_file'
                or any(type(counter[name]) is not str or not counter[name].strip() for name in fields)
                or any(profile.get(name) != counter[name] for name in ('tokenizer_id', 'tokenizer_revision'))):
            raise ValueError('bind a local tokenizer JSON identity to the declared service tokenizer and revision')
    # The original runner uses 8192 without a newline stop; short Filter
    # generation is an explicit experiment override, never an implicit default.
    generation = GenerationSpec.from_dict({'max_tokens': 8192, 'stop': [], **config['generation']})
    if generation.repetition_penalty != 1 or generation.logprobs is not None or generation.response_format is not None:
        raise ValueError('Q5 uses native standard generation and the original unstructured aggregation parser' if aggregate
            else 'use native standard generation; the original Extract owns its JSON response format')
    if aggregate and generation.max_tokens >= config['native_lm_max_ctx_len']:
        raise ValueError('the service context must leave input space after max_tokens')
    return dict(config, generation=json_metric(asdict(generation))), generation


def _make_lm(config, *, preflight=False):
    if config['schema'] != AGGREGATE_SCHEMA:
        return make_lm(config)
    tokenizer = mmqa.load_token_counter(config['token_counter'])
    return make_lm(config, max_ctx_len=config['native_lm_max_ctx_len'], tokenizer=tokenizer,
                   api_key='no-model-preflight' if preflight else None)


def _check_aggregate(config, checkout, *, data_dir, _lm=None):
    """Use native token counting and batching without inference, HTTP or budget calls."""
    source = mmqa.input_identity(data_dir)
    lm = _make_lm(config, preflight=True) if _lm is None else _lm
    plan = mmqa.leaf_plan(checkout, data_dir, lm)
    if mmqa.input_identity(data_dir) != source:
        raise ValueError("MMQA input changed while preparing the native aggregation")
    provenance = source_report("sembench-mmqa-q5-native-aggregate", (
        "models/lm.py", "sem_ops/sem_agg.py", "long_context_strategy.py", "templates/task_instructions.py",
        "nl_expression.py", "cache.py", "settings.py", "types.py"), provider_sources=True)
    work = mmqa.describe_work(plan)
    return dict(schema="semloom.sembench_lotus_mmqa_aggregate_preflight.v1", status="checked_cpu_only",
        scenario="mmqa", query="Q5", sembench_commit=mmqa.SEMBENCH_COMMIT,
        lotus_version=provenance["versions"]["lotus-ai"], native_sources=provenance,
        source=source, native_plan=plan, work=work, generation=config["generation"],
        model_calls=0, requests_claimed=0, execution_enabled=False, execution_available=True,
        endpoint=config["api_base"].rstrip("/") + "/chat/completions",
        native_lm_max_batch_size=config["native_lm_max_batch_size"],
        native_lm_max_ctx_len=lm.max_ctx_len, native_input_token_budget=lm.max_ctx_len - lm.max_tokens,
        token_counter=dict(config["token_counter"], implementation="LOTUS LM.count_tokens -> LiteLLM token_counter with local custom tokenizer",
            model_tokenizer_verified=False, scope="declared service tokenizer JSON; native SDK counts include its own message overhead; no server tokenization checked"),
        service_profile_declared=config["service_profile"], service_startup_checked=False,
        lifecycle="attached external text service; model startup and physical service exit remain externally owned")


def _check_visual(config, checkout, *, data_dir=None, manifest=None, manifest_digest=None):
    inputs = check_inputs(checkout, config["scenario"], config["query"], data_dir=data_dir,
        manifest=manifest, manifest_digest=manifest_digest, scale_factor=config.get('scale_factor'))["inputs"]
    return dict(schema="semloom.sembench_lotus_image_preflight.v1", status="checked_cpu_only",
        inputs=inputs, model_calls=0, execution_enabled=False, execution_available=True,
        endpoint=config["api_base"].rstrip("/") + "/chat/completions",
        native_lm_max_batch_size=config["native_lm_max_batch_size"],
        service_profile_declared=config["service_profile"], service_startup_checked=False,
        lifecycle="existing external VLM service; model startup and physical service exit supervised separately")


def check(config, checkout, *, data_dir=None, manifest=None, manifest_digest=None, _lm=None):
    if config['schema'] == AGGREGATE_SCHEMA:
        if data_dir is None or manifest is not None or manifest_digest is not None:
            raise ValueError('MMQA aggregation requires data_dir without Animals manifest arguments')
        return _check_aggregate(config, checkout, data_dir=data_dir, _lm=_lm)
    return _check_visual(config, checkout, data_dir=data_dir, manifest=manifest, manifest_digest=manifest_digest)


def run_query(config, checkout, directory, *, budget, data_dir=None, manifest=None, manifest_digest=None):
    """Execute one complete original query; the service stays externally owned."""
    aggregate = config['schema'] == AGGREGATE_SCHEMA
    from src.baselines.text.frameworks.lotus_native_join import observe_native_join_inputs, describe_native_join_inputs
    from src.observability.buffered_events import BufferedEvents
    from src.experiments.native_http_observer import observe_native_httpx
    from src.semantic_methods.lotus.engine import lm_report

    report = dict(schema="semloom.sembench_lotus_image_result.v1", status="failed",
        query_id=config["query_id"], scenario="mmqa" if aggregate else config["scenario"], query="Q5" if aggregate else str(config["query"]),
        backend="lotus-native", execution_enabled=True, production_core_used=False,
        original_operators_used=True, scheduler_owner="original LOTUS LM / LiteLLM batch_completion",
        generation=config["generation"], service_profile_declared=config["service_profile"],
        lifecycle="attached external VLM service; readiness and startup are externally owned and unobserved",
        physical_gpu_exit_confirmed=None, model_process_owned=False, result_cache_enabled=False,
        driver_initialization_in_query_seconds=False, service_startup_in_query_seconds=None,
        model_ready_before_source=None,
        service_readiness_observation="no remote READY observation; any lazy startup/request wait remains in actual HTTP latency",
        complete_cli_timing="supervisor.elapsed_s includes driver imports, preflight, evaluation and audit flush",
        warmup_calls=0, retries=0, performance_claim=False,
        query_timing_scope="legacy source/query interval including post-result stage and response accounting",
        source_started_ns=None, sink_ready_ns=None, source_to_sink_seconds=None,
        query_clock_version="lotus_service.source_to_sink.v1",
        source_to_sink_timing_scope="actual image/source decode and CSV reads, native PNG formatting, "
            "complete original operators and memory sink; mandatory HTTP attempt accounting, request fingerprints/response observation, "
            "inline-image audit hashing and event encoding/queueing included; background event writes may overlap; "
            "all actual HTTP wait included; remote startup attribution unobserved; "
            "input preflight, LM/client setup, post-result audit, result serialization, "
            "evaluation and final audit drain excluded; independent service scope, not shared v3",
        evaluation_in_query_seconds=False)
    if aggregate:
        report.update(schema='semloom.sembench_lotus_mmqa_aggregate_result.v1',
            scheduler_owner='original LOTUS sem_agg hierarchy / LM / LiteLLM batch_completion',
            lifecycle='attached external text service; readiness and startup are externally owned and unobserved',
            query_timing_scope='legacy source/query interval including post-result input identity and response accounting',
            source_to_sink_timing_scope='actual source read and complete unchanged Q5 hierarchy/projection in memory; '
                'mandatory HTTP attempt accounting, request fingerprints/response observation and event encoding/queueing included; '
                'background event writes may overlap; '
                'all actual HTTP wait included; remote startup attribution unobserved; '
                'LM/client/tokenizer setup, input/leaf preflight, post-result audit, '
                'result serialization, evaluation and final audit drain excluded; independent service scope, not shared v3',
            complete_cli_timing='supervisor.elapsed_s includes imports, tokenizer/LM setup, preflight, evaluation and audit flush')
    started = time.monotonic()
    report["worker_entry_ns"] = time.monotonic_ns()
    writer = audit = lm = result = None
    join_inputs = []
    failure = None

    def preserve_failure(error, phase):
        nonlocal failure
        if failure is None:
            failure = error
        else:
            failure.add_note(phase + " failed: " + type(error).__name__)

    def serialize_result():
        import pandas as pd

        report["result_serialization_started_ns"] = time.monotonic_ns()
        try:
            relation = result.to_frame() if isinstance(result, pd.Series) else result
            report["result"] = json_metric(relation.to_dict(orient="split"))
        finally:
            report["result_serialization_finished_ns"] = time.monotonic_ns()

    try:
        query_started = started
        if aggregate:
            lm = _make_lm(config)
            report['driver_initialization_seconds'] = time.monotonic() - started
            query_started = time.monotonic()
            checked = report['preflight'] = check(config, checkout, data_dir=data_dir, _lm=lm)
            if budget.remaining < checked['work']['minimum_model_calls']:
                raise ValueError("the already-reserved request unit cannot cover Q5's native minimum request count")
            report['attempt_limit_remaining_at_query_start'] = budget.remaining
            task_inputs = checked
        else:
            checked = check(config, checkout, data_dir=data_dir, manifest=manifest, manifest_digest=manifest_digest)
            inputs = report['inputs'] = checked['inputs']
            maximum = inputs.get('expected_model_calls', inputs.get('maximum_model_calls'))
            if maximum is None:
                maximum = inputs['minimum_model_calls']
            if budget.remaining < maximum:
                raise ValueError("the reserved request unit is below this query's preflight requirement")
            task_inputs = inputs
        writer = BufferedEvents(Path(directory) / 'native-http.jsonl')
        audit = NativeServiceCalls(writer, config['model_name'])
        if not aggregate:
            lm = _make_lm(config)
        query = OriginalLotusQuery(checkout, report['scenario'], report['query'], inputs=task_inputs,
            data_dir=data_dir, manifest=manifest, manifest_digest=manifest_digest)
        report["preflight_finished_ns"] = time.monotonic_ns()
        with native_query_settings(lm), (nullcontext() if aggregate else observe_native_join_inputs(join_inputs)), \
                audit.observe_lm(lm, kind='aggregate' if aggregate else 'native_query' if config['scenario'] == 'ecomm' else None):
            with observe_native_httpx(budget, audit.record, checked["endpoint"], timeout_s=config["query_timeout_s"]):
                report["source_started_ns"] = time.monotonic_ns()
                result = query.execute()
                # The original relation is ready before observer teardown or audit.
                report["sink_ready_ns"] = time.monotonic_ns()
                report["source_to_sink_seconds"] = (report["sink_ready_ns"] - report["source_started_ns"]) / 1e9
        report["post_sink_audit_started_ns"] = time.monotonic_ns()
        try:
            query.verify_inputs()
            stages = audit.lm_calls
            query.validate_batch_sizes([stage['input_occurrences'] for stage in stages], result)
            if any((stage['post_attempts'] != stage['input_occurrences']) if aggregate else
                    (stage['post_attempts'] is not None and stage['post_attempts'] != stage['input_occurrences'])
                    for stage in stages):
                raise RuntimeError('native aggregation requests differ from the complete uncached SDK hierarchy' if aggregate
                    else 'native visual calls differ from the complete uncached original query')
            accounting = audit.snapshot()
            if (accounting["succeeded"] != sum(stage['input_occurrences'] for stage in audit.lm_calls)
                    or accounting["failed"] or accounting["unknown"] or accounting["unfinished"]):
                raise RuntimeError("native aggregate POSTs lack complete successful response/model evidence" if aggregate
                    else "native visual POSTs lack complete successful response/model evidence")
        finally:
            report["post_sink_audit_finished_ns"] = time.monotonic_ns()
        report["query_seconds"] = time.monotonic() - query_started
        serialize_result()
        evaluated = time.monotonic()
        report["evaluation_started_ns"] = time.monotonic_ns()
        try:
            quality = query.evaluate(result)
            report["quality"] = json_metric(quality)
        finally:
            report["evaluation_finished_ns"] = time.monotonic_ns()
            report["evaluation_seconds"] = time.monotonic() - evaluated
        report["status"] = "completed"
    except BaseException as error:
        failure = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
    finally:
        # Audit failure must not discard a complete original relation. Serialize
        # it once outside the legacy query interval; retain the object on failure.
        if result is not None and "result_serialization_started_ns" not in report:
            try:
                serialize_result()
            except BaseException as error:
                preserve_failure(error, "result serialization")
        report["audit_finalization_started_ns"] = time.monotonic_ns()
        if not aggregate:
            report['native_join_inputs'] = describe_native_join_inputs(join_inputs)
        if audit is not None:
            report.update(native_lm_calls=audit.lm_calls, model_calls=len(audit.requests))
            try:
                report["calls"] = audit.snapshot()
            except BaseException as error:
                preserve_failure(error, "native audit snapshot")
            try:
                report['native_sdk_response_audit'] = audit.record_sdk_responses()
            except BaseException as error:
                preserve_failure(error, 'native SDK response observation')
        else:
            report["model_calls"] = 0
        if lm is not None:
            try:
                report["lm"] = lm_report(lm)
            except BaseException as error:
                preserve_failure(error, "native LM report")
        report["audit_finalization_finished_ns"] = time.monotonic_ns()
        if writer is not None:
            report["audit_flush_started_ns"] = time.monotonic_ns()
            try:
                writer.close()
            except BaseException as error:
                if aggregate:
                    report["audit_cleanup_error"] = dict(error_type=type(error).__name__, error=redact_text(str(error)))
                preserve_failure(error, "native audit close")
            finally:
                report["audit_flush_finished_ns"] = time.monotonic_ns()
            try:
                report["event_writer"] = writer.snapshot()
            except BaseException as error:
                preserve_failure(error, "native audit writer snapshot")
        report["elapsed_seconds"] = time.monotonic() - started
    if failure is not None:
        report["status"] = "failed"
        report.setdefault("error_type", type(failure).__name__)
        report.setdefault("error", redact_text(str(failure)))
        failure.query_report = report
        if result is not None:
            failure.query_result = result
        raise failure
    return report


def run_worker(config, checkout, directory, **inputs):
    role = 'aggregate' if config['schema'] == AGGREGATE_SCHEMA else 'visual'
    return run_prepaid_query(config, checkout, directory, run_query, query_role=role, **inputs)


def main(argv=None):
    import argparse
    import sys

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    for action in ('check', 'run'):
        command = commands.add_parser(action)
        command.add_argument('--config', required=True)
        command.add_argument('--sembench-checkout', required=True)
        command.add_argument('--data-dir')
        command.add_argument('--manifest')
        command.add_argument('--manifest-digest')
        if action == 'run':
            command.add_argument('--output-dir', required=True)
            command.add_argument('--execute-model', action='store_true')
            command.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    config, _generation = configuration(args.config)
    inputs = dict(data_dir=args.data_dir, manifest=args.manifest, manifest_digest=args.manifest_digest)
    if config['schema'] == AGGREGATE_SCHEMA and (args.data_dir is None or args.manifest is not None or args.manifest_digest is not None):
        parser.error('MMQA aggregation requires --data-dir without Animals manifest arguments')
    if args.action == 'run' and args.execute_model and args.worker:
        run_worker(config, args.sembench_checkout, Path(args.output_dir).resolve(), **inputs)
        return 0
    checked = check(config, args.sembench_checkout, **inputs)
    if args.action == 'check' or not args.execute_model:
        print(json.dumps(checked, ensure_ascii=False))
        return 0
    from .lotus_records_supervisor import supervise
    root = Path(args.output_dir).resolve()
    new_private_directory(root)
    write_private_json(root / 'preflight.json', checked)
    command = [sys.executable, str(Path(__file__).parents[2] / 'scripts/experiments/run_sembench_lotus_query.py'),
        'run', '--config', str(Path(args.config).resolve()),
        '--sembench-checkout', str(Path(args.sembench_checkout).resolve()),
        '--output-dir', str(root), '--execute-model', '--worker']
    for key, value in inputs.items():
        if value is not None:
            command += ['--' + key.replace('_', '-'), str(Path(value).resolve()) if key != 'manifest_digest' else value]
    supervise(command, root, timeout_s=config['query_timeout_s'])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
