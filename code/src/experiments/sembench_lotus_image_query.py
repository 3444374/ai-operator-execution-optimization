"""Original LOTUS visual queries against an independently supervised VLM service.

The native SDK owns messages, batching and parsers. This entry owns finite
source/evaluator binding and request audit; it never starts or stops the service.
"""

from contextlib import contextmanager
from dataclasses import asdict
import hashlib
import json
import math
import os
from pathlib import Path
import threading
import time
from urllib.parse import urlsplit

from src.infrastructure.private_artifacts import (
    json_metric, new_private_directory, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text
from .sembench_image_query import check as check_inputs


def configuration(path):
    from src.serving.backends.vllm_protocol import GenerationSpec

    config = read_json(path)
    required = {"schema", "scenario", "query", "query_id", "model_name", "api_base", "api_key_env",
        "generation", "native_lm_max_batch_size", "query_timeout_s", "service_profile"}
    if (type(config) is not dict or not required <= config.keys()
            or config.keys() - required - {"budget"}
            or config["schema"] != "semloom.sembench_lotus_image_run.v1"):
        raise ValueError("an explicit native LOTUS visual-service configuration is required")
    for name in ("query_id", "api_key_env"):
        if type(config[name]) is not str or not config[name].strip():
            raise ValueError(name + " must be a nonempty string")
    if (type(config["model_name"]) is not str or not config["model_name"].startswith("openai/")
            or not config["model_name"].removeprefix("openai/")):
        raise ValueError("native visual service model_name must be openai/<served-model>")
    address = urlsplit(config["api_base"])
    if (address.scheme not in ("http", "https") or not address.hostname or address.username
            or address.password or address.query or address.fragment):
        raise ValueError("api_base must identify the declared OpenAI-compatible service without credentials")
    if type(config["native_lm_max_batch_size"]) is not int or config["native_lm_max_batch_size"] < 1:
        raise ValueError("native_lm_max_batch_size must be positive")
    timeout = config["query_timeout_s"]
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("query_timeout_s must be positive")
    if type(config["service_profile"]) is not dict:
        raise ValueError("service_profile must describe the external model/processor/service configuration")
    # SemBench's native runner selects 8192 tokens without a newline stop.
    # The shared short-text Filter defaults (8 / newline) do not belong here.
    generation = GenerationSpec.from_dict({"max_tokens": 8192, "stop": [], **config["generation"]})
    if generation.repetition_penalty != 1 or generation.logprobs is not None or generation.response_format is not None:
        raise ValueError("use native standard generation; the original Extract owns its JSON response format")
    return dict(config, generation=json_metric(asdict(generation))), generation


def check(config, checkout, *, data_dir=None, manifest=None, manifest_digest=None):
    inputs = check_inputs(checkout, config["scenario"], config["query"], data_dir=data_dir,
        manifest=manifest, manifest_digest=manifest_digest)["inputs"]
    return dict(schema="semloom.sembench_lotus_image_preflight.v1", status="checked_cpu_only",
        inputs=inputs, model_calls=0, execution_enabled=False, execution_available=True,
        endpoint=config["api_base"].rstrip("/") + "/chat/completions",
        native_lm_max_batch_size=config["native_lm_max_batch_size"],
        service_profile_declared=config["service_profile"], service_startup_checked=False,
        lifecycle="existing external VLM service; model startup and physical service exit supervised separately")


def _make_lm(config):
    from lotus.models import LM
    from src.serving.backends.vllm_protocol import GenerationSpec

    generation = GenerationSpec.from_dict(config["generation"])
    options = dict(top_p=generation.top_p, stop=list(generation.stop),
        presence_penalty=generation.presence_penalty, frequency_penalty=generation.frequency_penalty,
        api_base=config["api_base"].rstrip("/"), api_key=os.environ[config["api_key_env"]],
        timeout=config["query_timeout_s"], num_retries=0, max_retries=0)
    if generation.seed is not None:
        options["seed"] = generation.seed
    return LM(config["model_name"], temperature=generation.temperature, max_tokens=generation.max_tokens,
        max_batch_size=config["native_lm_max_batch_size"], **options)


def _audit_value(value):
    """Hash inline image bytes in evidence, leaving the actual SDK POST untouched."""
    if isinstance(value, dict):
        if value.get("type") == "image_url":
            image = value["image_url"]
            url = image["url"]
            return dict(type="image_url", image_url={k: v for k, v in image.items() if k != "url"},
                url_sha256=hashlib.sha256(url.encode()).hexdigest(), url_bytes=len(url.encode()),
                representation="data:image/png;base64" if url.startswith("data:image/png;base64,") else "other")
        return {key: _audit_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_audit_value(item) for item in value]
    return value


class _NativeCalls:
    def __init__(self, writer, model_name):
        self.writer, self.model = writer, model_name.removeprefix("openai/")
        self.lock = threading.Lock()
        self.requests, self.succeeded, self.failed, self.finished = set(), set(), set(), set()
        self.lm_calls = []

    def record(self, event):
        attempt, kind = event.get("attempt"), event["event"]
        with self.lock:
            if kind == "request":
                self.requests.add(attempt)
            elif kind == "http_response":
                response = event["response"]
                if (event["status"] == 200 and type(response) is dict
                        and response.get("model") == self.model and response.get("choices")):
                    self.succeeded.add(attempt)
                else:
                    self.failed.add(attempt)
            elif kind == "http_finished":
                self.finished.add(attempt)
                if event.get("error_type"):
                    self.failed.add(attempt)
        self.writer.record(_audit_value(event))

    @contextmanager
    def observe_lm(self, lm):
        lm_type = type(lm)
        original = lm_type.__call__

        def observed(current, messages, *args, **kwargs):
            if current is not lm:
                raise RuntimeError("the original visual query invoked another model")
            before = len(self.requests)
            stage = dict(input_occurrences=len(messages),
                kind="extract" if kwargs.get("response_format") == {"type": "json_object"} else "filter_or_join")
            self.lm_calls.append(stage)
            try:
                return original(current, messages, *args, **kwargs)
            finally:
                stage["post_attempts"] = len(self.requests) - before

        lm_type.__call__ = observed
        try:
            yield
        finally:
            lm_type.__call__ = original

    def snapshot(self):
        with self.lock:
            return dict(attempts=len(self.requests), succeeded=len(self.succeeded), failed=len(self.failed),
                unknown=len(self.requests - self.succeeded - self.failed),
                unfinished=len(self.requests - self.finished))


def run_query(config, checkout, directory, *, budget, data_dir=None, manifest=None, manifest_digest=None):
    """Execute one complete native query; the VLM service remains externally owned."""
    import lotus
    from src.observability.buffered_events import BufferedEvents
    from src.experiments.native_http_observer import observe_native_httpx
    from src.semantic_methods.lotus.engine import lm_report

    report = dict(schema="semloom.sembench_lotus_image_result.v1", status="failed",
        query_id=config["query_id"], scenario=config["scenario"], query=str(config["query"]),
        backend="lotus-native", execution_enabled=True, production_core_used=False,
        original_operators_used=True, scheduler_owner="original LOTUS LM / LiteLLM batch_completion",
        generation=config["generation"], service_profile_declared=config["service_profile"],
        lifecycle="existing independently supervised VLM service; not cold model startup",
        physical_gpu_exit_confirmed=None, model_process_owned=False, result_cache_enabled=False,
        driver_initialization_in_query_seconds=False, service_startup_in_query_seconds=False,
        complete_cli_timing="supervisor.elapsed_s includes driver imports, preflight, evaluation and audit flush",
        warmup_calls=0, retries=0, performance_claim=False,
        query_timing_scope="source identity/decode and CSV reads, native PNG formatting, complete original operators and sink",
        evaluation_in_query_seconds=False)
    started = time.monotonic()
    writer = audit = lm = None
    failure = None
    try:
        checked = check(config, checkout, data_dir=data_dir, manifest=manifest, manifest_digest=manifest_digest)
        inputs = report["inputs"] = checked["inputs"]
        maximum = inputs.get("expected_model_calls", inputs.get("maximum_model_calls"))
        if budget.remaining < maximum:
            raise ValueError("the already-reserved request unit cannot cover this complete visual query")
        writer = BufferedEvents(Path(directory) / "native-http.jsonl")
        audit = _NativeCalls(writer, config["model_name"])
        lm = _make_lm(config)
        with lotus.settings.context(lm=lm, enable_cache=False), audit.observe_lm(lm):
            with observe_native_httpx(budget, audit.record, checked["endpoint"], timeout_s=config["query_timeout_s"]):
                if config["scenario"] == "animals":
                    from src.baselines.image.sembench_animals import AnimalsDataset, run_original_query, evaluate_original
                    source = AnimalsDataset(Path(checkout), Path(manifest), manifest_digest)
                    result = run_original_query(checkout, source.frame, inputs["query_id"])
                    expected_stages = [inputs["rows"]] * inputs["native_filter_passes"]
                else:
                    from src.baselines.image.sembench_mmqa import run_original_query, evaluate_original
                    result = run_original_query(checkout, data_dir, config["query"])
                    expected_stages = [inputs["exact_join_candidates"]]
                    if inputs["query"] == "Q2b":
                        expected_stages.append(len(result))
        if ([stage["input_occurrences"] for stage in audit.lm_calls] != expected_stages
                or any(stage["post_attempts"] != stage["input_occurrences"] for stage in audit.lm_calls)):
            raise RuntimeError("native visual calls differ from the complete uncached original query")
        accounting = audit.snapshot()
        if accounting["succeeded"] != sum(expected_stages) or accounting["failed"] or accounting["unknown"] or accounting["unfinished"]:
            raise RuntimeError("native visual POSTs lack complete successful response/model evidence")
        report["query_seconds"] = time.monotonic() - started
        report["result"] = json_metric(result.to_dict(orient="split"))
        evaluated = time.monotonic()
        report["quality"] = json_metric(evaluate_original(checkout, result, source.frame(references=True), inputs["query_id"])
            if config["scenario"] == "animals" else evaluate_original(checkout, config["query"], result))
        report["evaluation_seconds"] = time.monotonic() - evaluated
        report["status"] = "completed"
    except BaseException as error:
        failure = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
    finally:
        if audit is not None:
            report.update(calls=audit.snapshot(), native_lm_calls=audit.lm_calls, model_calls=len(audit.requests))
        else:
            report["model_calls"] = 0
        if lm is not None:
            report["lm"] = lm_report(lm)
        if writer is not None:
            try:
                writer.close()
            except BaseException as error:
                if failure is None:
                    failure = error
                else:
                    failure.add_note("native audit close failed: " + type(error).__name__)
            report["event_writer"] = writer.snapshot()
        report["elapsed_seconds"] = time.monotonic() - started
    if failure is not None:
        report["status"] = "failed"
        report.setdefault("error_type", type(failure).__name__)
        report.setdefault("error", redact_text(str(failure)))
        failure.query_report = report
        raise failure
    return report


def run_worker(config, checkout, directory, **inputs):
    from .attempt_ledger import AttemptBudget
    from .cell_budget import CellBudgetLedger

    if not os.environ.get(config["api_key_env"]):
        raise ValueError("set the declared api_key_env before claiming a visual query unit")
    value = config["budget"]
    ledger = CellBudgetLedger(Path(value["path"]), AttemptBudget(value["id"], value["limit"]))
    try:
        claim = ledger.claim_unit(value["unit_id"])
        claim.bind_execution()
        report = run_query(config, checkout, directory, budget=claim, **inputs)
    except BaseException as error:
        report = getattr(error, "query_report", dict(status="failed", error_type=type(error).__name__,
            error=redact_text(str(error)), model_calls=0))
        report["permanent_budget"] = ledger.snapshot()
        write_private_json(Path(directory) / "query.json", report)
        raise
    report["permanent_budget"] = ledger.snapshot()
    write_private_json(Path(directory) / "query.json", report)
    return report


def main(argv=None):
    import argparse
    import sys

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    for action in ("check", "run"):
        command = commands.add_parser(action)
        command.add_argument("--config", required=True)
        command.add_argument("--sembench-checkout", required=True)
        command.add_argument("--data-dir")
        command.add_argument("--manifest")
        command.add_argument("--manifest-digest")
        if action == "run":
            command.add_argument("--output-dir", required=True)
            command.add_argument("--execute-model", action="store_true")
            command.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    config, _generation = configuration(args.config)
    inputs = dict(data_dir=args.data_dir, manifest=args.manifest, manifest_digest=args.manifest_digest)
    checked = check(config, args.sembench_checkout, **inputs)
    if args.action == "check" or not args.execute_model:
        print(json.dumps(checked, ensure_ascii=False))
        return 0
    root = Path(args.output_dir).resolve()
    if args.worker:
        run_worker(config, args.sembench_checkout, root, **inputs)
    else:
        from .lotus_records_supervisor import supervise
        new_private_directory(root)
        write_private_json(root / "preflight.json", checked)
        command = [sys.executable, str(Path(__file__).parents[2] / "scripts/experiments/run_sembench_lotus_image_query.py"),
            "run", "--config", str(Path(args.config).resolve()),
            "--sembench-checkout", str(Path(args.sembench_checkout).resolve()),
            "--output-dir", str(root), "--execute-model", "--worker"]
        for key, value in inputs.items():
            if value is not None:
                command += ["--" + key.replace("_", "-"), str(Path(value).resolve()) if key != "manifest_digest" else value]
        supervise(command, root, timeout_s=config["query_timeout_s"])
    return 0
