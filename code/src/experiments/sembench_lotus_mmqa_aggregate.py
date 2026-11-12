"""Original MMQA Q5 hierarchical LOTUS aggregation on an external text service.

Check runs the unchanged SDK planner only as far as its first LM boundary. Run
uses the original LM and full query, with caller-prepaid per-POST accounting.
"""

from dataclasses import asdict
import hashlib
from importlib import import_module
import json
import math
from pathlib import Path
import time
from urllib.parse import urlsplit

from src.baselines.text import sembench_mmqa_aggregate as mmqa
from src.infrastructure.private_artifacts import (
    file_identity, json_metric, new_private_directory, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text
from src.semantic_methods.lotus.source import source_report
from .native_service_query import NativeServiceCalls, make_lm, run_prepaid_query


SCHEMA = "semloom.sembench_lotus_mmqa_aggregate_run.v1"


def configuration(path):
    from src.serving.backends.vllm_protocol import GenerationSpec

    config = read_json(path)
    required = {"schema", "query_id", "model_name", "api_base", "api_key_env", "generation",
        "native_lm_max_batch_size", "native_lm_max_ctx_len", "token_counter", "query_timeout_s", "service_profile"}
    if (type(config) is not dict or not required <= config.keys() or config.keys() - required - {"budget"}
            or config["schema"] != SCHEMA):
        raise ValueError("an explicit native LOTUS MMQA Q5 service configuration is required")
    for name in ("query_id", "api_key_env"):
        if type(config[name]) is not str or not config[name].strip():
            raise ValueError(name + " must be a nonempty string")
    if (type(config["model_name"]) is not str or not config["model_name"].startswith("openai/")
            or not config["model_name"].removeprefix("openai/")):
        raise ValueError("native service model_name must be openai/<served-model>")
    address = urlsplit(config["api_base"])
    if (address.scheme not in ("http", "https") or not address.hostname or address.username
            or address.password or address.query or address.fragment):
        raise ValueError("api_base must identify the declared service without credentials")
    for name in ("native_lm_max_batch_size", "native_lm_max_ctx_len"):
        if type(config[name]) is not int or config[name] < 1:
            raise ValueError(name + " must be positive")
    timeout = config["query_timeout_s"]
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("query_timeout_s must be positive")
    profile, counter = config["service_profile"], config["token_counter"]
    if (type(profile) is not dict or type(profile.get("max_model_len")) is not int
            or profile["max_model_len"] != config["native_lm_max_ctx_len"]):
        raise ValueError("native_lm_max_ctx_len must match the declared service_profile.max_model_len")
    fields = {"kind", "path", "sha256", "tokenizer_id", "tokenizer_revision"}
    if (type(counter) is not dict or set(counter) != fields or counter["kind"] != "huggingface_tokenizer_file"
            or any(type(counter[name]) is not str or not counter[name].strip() for name in fields)
            or any(profile.get(name) != counter[name] for name in ("tokenizer_id", "tokenizer_revision"))):
        raise ValueError("bind a local tokenizer JSON identity to the declared service tokenizer and revision")
    # The original SemBench LM uses 8192 without a newline stop. Aggregation
    # must not inherit the independent short Filter configuration.
    generation = GenerationSpec.from_dict({"max_tokens": 8192, "stop": [], **config["generation"]})
    if generation.repetition_penalty != 1 or generation.logprobs is not None or generation.response_format is not None:
        raise ValueError("Q5 uses native standard generation and the original unstructured aggregation parser")
    if generation.max_tokens >= config["native_lm_max_ctx_len"]:
        raise ValueError("the service context must leave input space after max_tokens")
    return dict(config, generation=json_metric(asdict(generation))), generation


def _make_lm(config, *, preflight=False):
    from tokenizers import Tokenizer

    counter = config["token_counter"]
    identity = file_identity(Path(counter["path"]).resolve())
    if identity["sha256"] != counter["sha256"]:
        raise ValueError("the local token counter does not match its declared SHA256")
    tokenizer = Tokenizer.from_file(str(Path(counter["path"]).resolve()))
    return make_lm(config, max_ctx_len=config["native_lm_max_ctx_len"], tokenizer=tokenizer,
                   api_key="no-model-preflight" if preflight else None)


class _LeafPrepared(BaseException):
    """Private non-model stop at the unchanged SDK's first LM invocation."""


def _leaf_plan(checkout, data_dir, lm):
    import lotus

    module = import_module("lotus.sem_ops.sem_agg")
    accessor_type, lm_type = module.SemAggDataframe, type(lm)
    original_agg, original_call, original_count = accessor_type.__call__, lm_type.__call__, lm.count_tokens
    original_documents = module.sem_agg
    prepared = dict(token_count_calls=[])

    def count_tokens(value):
        count = original_count(value)
        payload = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)
        prepared["token_count_calls"].append(dict(sha256=hashlib.sha256(payload.encode()).hexdigest(),
                                                 bytes=len(payload.encode()), tokens=count))
        return count

    def observe_source(frame):
        prepared["source_rows"] = len(frame)
        prepared["source_columns"] = list(frame.columns)

    def observe_input(accessor, instruction, *args, **kwargs):
        frame = accessor._obj
        prepared["selection"] = dict(rows=len(frame), source_indices=json_metric(frame.index.tolist()),
            titles=frame["title"].tolist(), text_characters=sum(len(value) for value in frame["text"]),
            text_utf8_bytes=sum(len(value.encode()) for value in frame["text"]),
            title_utf8_bytes=sum(len(value.encode()) for value in frame["title"]),
            instruction=instruction, operator_args=json_metric(args), operator_kwargs=json_metric(kwargs))
        return original_agg(accessor, instruction, *args, **kwargs)

    def observe_documents(docs, *args, **kwargs):
        prepared["native_documents"] = len(docs)
        prepared["long_context_strategy"] = docs.strategy.name.lower() if isinstance(docs, module.ChunkedDocument) else None
        return original_documents(docs, *args, **kwargs)

    def capture(current, messages, *args, **kwargs):
        if current is not lm:
            raise RuntimeError("Q5 preflight encountered another LM")
        prepared.update(leaf_messages=messages, leaf_message_tokens=[original_count(message) for message in messages],
                        lm_args=json_metric(args), lm_kwargs=json_metric(kwargs))
        raise _LeafPrepared()

    accessor_type.__call__, lm_type.__call__, lm.count_tokens = observe_input, capture, count_tokens
    module.sem_agg = observe_documents
    try:
        with lotus.settings.context(lm=lm, enable_cache=False):
            try:
                mmqa.run_original_query(checkout, data_dir, input_observer=observe_source)
            except _LeafPrepared:
                return prepared
        raise RuntimeError("the original Q5 query did not reach native aggregation")
    finally:
        accessor_type.__call__, lm_type.__call__, lm.count_tokens = original_agg, original_call, original_count
        module.sem_agg = original_documents


def check(config, checkout, *, data_dir, _lm=None):
    """Use native token counting and batching without inference, HTTP or budget calls."""
    source = mmqa.input_identity(data_dir)
    lm = _make_lm(config, preflight=True) if _lm is None else _lm
    plan = _leaf_plan(checkout, data_dir, lm)
    if mmqa.input_identity(data_dir) != source:
        raise ValueError("MMQA input changed while preparing the native aggregation")
    provenance = source_report("sembench-mmqa-q5-native-aggregate", (
        "models/lm.py", "sem_ops/sem_agg.py", "long_context_strategy.py", "templates/task_instructions.py",
        "nl_expression.py", "cache.py", "settings.py", "types.py"), provider_sources=True)
    batches = len(plan["leaf_messages"])
    work = dict(leaf_requests=batches, expected_model_calls=1 if batches == 1 else None,
        minimum_model_calls=batches if batches == 1 else batches + 1,
        maximum_model_calls=1 if batches == 1 else None,
        total_request_rule="exactly one leaf request" if batches == 1 else
            "leaf requests plus native reduction requests determined by actual summary token lengths; caller budget bounds attempts")
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


def run_query(config, checkout, directory, *, budget, data_dir):
    """Execute the complete original hierarchy within an already reserved unit."""
    import lotus
    from src.observability.buffered_events import BufferedEvents
    from src.experiments.native_http_observer import observe_native_httpx
    from src.semantic_methods.lotus.engine import lm_report

    report = dict(schema="semloom.sembench_lotus_mmqa_aggregate_result.v1", status="failed",
        query_id=config["query_id"], scenario="mmqa", query="Q5", backend="lotus-native",
        execution_enabled=True, original_operators_used=True, production_core_used=False,
        scheduler_owner="original LOTUS sem_agg hierarchy / LM / LiteLLM batch_completion",
        generation=config["generation"], service_profile_declared=config["service_profile"],
        lifecycle="attached-ready external text service; not cold model startup", model_process_owned=False,
        physical_gpu_exit_confirmed=None, service_startup_in_query_seconds=False, result_cache_enabled=False,
        driver_initialization_in_query_seconds=False,
        warmup_calls=0, retries=0, performance_claim=False, evaluation_in_query_seconds=False,
        query_timing_scope="legacy source/query interval including post-result input identity and response accounting",
        source_started_ns=None, sink_ready_ns=None, source_to_sink_seconds=None,
        query_clock_version="sembench.source_to_sink.v2",
        source_to_sink_timing_scope="actual source read and complete unchanged Q5 hierarchy/projection in memory; "
            "LM/client/tokenizer setup, input/leaf preflight, external service startup, post-result audit, "
            "serialization, evaluation and audit flush excluded",
        complete_cli_timing="supervisor.elapsed_s includes imports, tokenizer/LM setup, preflight, evaluation and audit flush")
    started = time.monotonic()
    report["worker_entry_ns"] = time.monotonic_ns()
    writer = audit = lm = result = failure = None

    def preserve_failure(error, phase):
        nonlocal failure
        if failure is None:
            failure = error
        else:
            failure.add_note(phase + " failed: " + type(error).__name__)

    def serialize_result():
        report["result_serialization_started_ns"] = time.monotonic_ns()
        try:
            report["result"] = json_metric(result.to_dict(orient="split"))
        finally:
            report["result_serialization_finished_ns"] = time.monotonic_ns()

    try:
        lm = _make_lm(config)
        report["driver_initialization_seconds"] = time.monotonic() - started
        query_started = time.monotonic()
        checked = report["preflight"] = check(config, checkout, data_dir=data_dir, _lm=lm)
        if budget.remaining < checked["work"]["minimum_model_calls"]:
            raise ValueError("the already-reserved request unit cannot cover Q5's native minimum request count")
        report["attempt_limit_remaining_at_query_start"] = budget.remaining
        writer = BufferedEvents(Path(directory) / "native-http.jsonl")
        audit = NativeServiceCalls(writer, config["model_name"])
        report["preflight_finished_ns"] = time.monotonic_ns()
        with lotus.settings.context(lm=lm, enable_cache=False), audit.observe_lm(lm, kind="aggregate"):
            with observe_native_httpx(budget, audit.record, checked["endpoint"], timeout_s=config["query_timeout_s"]):
                report["source_started_ns"] = time.monotonic_ns()
                result = mmqa.run_original_query(checkout, data_dir)
                report["sink_ready_ns"] = time.monotonic_ns()
                report["source_to_sink_seconds"] = (report["sink_ready_ns"] - report["source_started_ns"]) / 1e9
        report["post_sink_audit_started_ns"] = time.monotonic_ns()
        try:
            if mmqa.input_identity(data_dir) != checked["source"]:
                raise ValueError("MMQA input changed while executing the original aggregation")
            stages = audit.lm_calls
            if (not stages or stages[0]["input_occurrences"] != checked["work"]["leaf_requests"]
                    or stages[-1]["input_occurrences"] != 1
                    or any(stage["post_attempts"] != stage["input_occurrences"] for stage in stages)):
                raise RuntimeError("native aggregation requests differ from the complete uncached SDK hierarchy")
            accounting = audit.snapshot()
            if (accounting["succeeded"] != sum(stage["input_occurrences"] for stage in stages)
                    or accounting["failed"] or accounting["unknown"] or accounting["unfinished"]):
                raise RuntimeError("native aggregate POSTs lack complete successful response/model evidence")
        finally:
            report["post_sink_audit_finished_ns"] = time.monotonic_ns()
        report["query_seconds"] = time.monotonic() - query_started
        serialize_result()
        evaluated = time.monotonic()
        report["evaluation_started_ns"] = time.monotonic_ns()
        try:
            report["quality"] = json_metric(mmqa.evaluate_original(checkout, result))
        finally:
            report["evaluation_finished_ns"] = time.monotonic_ns()
            report["evaluation_seconds"] = time.monotonic() - evaluated
        report["status"] = "completed"
    except BaseException as error:
        failure = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
    finally:
        if result is not None and "result_serialization_started_ns" not in report:
            try:
                serialize_result()
            except BaseException as error:
                preserve_failure(error, "result serialization")
        report["audit_finalization_started_ns"] = time.monotonic_ns()
        if audit is not None:
            report.update(native_lm_calls=audit.lm_calls, model_calls=len(audit.requests))
            try:
                report["calls"] = audit.snapshot()
            except BaseException as error:
                preserve_failure(error, "native audit snapshot")
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
    return run_prepaid_query(config, checkout, directory, run_query, query_role="aggregate", **inputs)


def main(argv=None):
    import argparse
    import sys

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for action in ("check", "run"):
        command = commands.add_parser(action)
        command.add_argument("--config", required=True)
        command.add_argument("--sembench-checkout", required=True)
        command.add_argument("--data-dir", required=True)
        if action == "run":
            command.add_argument("--output-dir", required=True)
            command.add_argument("--execute-model", action="store_true")
            command.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    config, _generation = configuration(args.config)
    if args.action == "run" and args.execute_model and args.worker:
        run_worker(config, args.sembench_checkout, Path(args.output_dir).resolve(), data_dir=args.data_dir)
        return 0
    checked = check(config, args.sembench_checkout, data_dir=args.data_dir)
    if args.action == "check" or not args.execute_model:
        print(json.dumps(checked, ensure_ascii=False))
        return 0
    from .lotus_records_supervisor import supervise
    root = Path(args.output_dir).resolve()
    new_private_directory(root)
    write_private_json(root / "preflight.json", checked)
    command = [sys.executable, str(Path(__file__).parents[2] / "scripts/experiments/run_sembench_lotus_mmqa_aggregate.py"),
        "run", "--config", str(Path(args.config).resolve()),
        "--sembench-checkout", str(Path(args.sembench_checkout).resolve()),
        "--data-dir", str(Path(args.data_dir).resolve()), "--output-dir", str(root), "--execute-model", "--worker"]
    supervise(command, root, timeout_s=config["query_timeout_s"])
    return 0
