"""Bounded Q2b: explicit native CoT strategy compatibility, then JSON Extract."""

import argparse
import base64
from dataclasses import asdict, replace
import hashlib
import importlib
import json
from pathlib import Path
import subprocess
import sys
import time

from src.baselines.common.private_artifacts import (file_identity, new_private_directory, open_private_text,
    publish_private_json, write_private_json)
from src.baselines.common.redact import redact_text
from src.baselines.image.sembench_mmqa import (MmqaDataset, evaluate_original, prepare_dataset, run_original_query)
from src.baselines.image.sembench_mmqa_compat import strategy_profile
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.mmqa_q2b_observation import Q2bObservation
from src.experiments.postgresql.query_supervisor import supervise
from src.experiments.sembench_animals import configure_lm, observed_http_peak
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.infrastructure.runner_lease import acquire_runner_lease, HOST_SCOPE_FINGERPRINT
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from src.semantic_methods.lotus.sdk import IMAGE_TEXT_PROFILE, validate_source


NATIVE_SOURCES = {
    "sem_ops/sem_join.py": "9d1c1cc9af03ef4446c31cf607ed26dacef572c89d72746b0a351d8421edbd01",
    "sem_ops/sem_extract.py": "0f58eda86d645b85b8659591d61366a86809a4d189dbec6c491fe5b4628c163a",
}


def validate_native_source():
    validate_source(message_profile=IMAGE_TEXT_PROFILE)
    root = Path(importlib.import_module("lotus").__file__).parent
    for name, expected in NATIVE_SOURCES.items():
        if file_identity(root / name)["sha256"] != expected:
            raise ValueError("Q2b native LOTUS source differs at " + name)
    return NATIVE_SOURCES


def validate_profile(profile, model, tokenizer, max_tokens):
    if (not isinstance(profile, dict) or profile.get("model", {}).get("id") != model.model_id
            or not profile["model"].get("revision") or not profile["model"].get("dtype")
            or profile.get("generation") != dict(temperature=0, top_p=1, max_tokens=max_tokens, retries=0)
            or not profile.get("service_preprocess", {}).get("processor_revision")
            or not profile["service_preprocess"].get("parameters")
            or profile.get("tokenizer_sha256") != file_identity(tokenizer)["sha256"]):
        raise ValueError("Q2b requires the exact model/generation/processor/tokenizer profile")
    return json.loads(json.dumps(profile, allow_nan=False))


def run_query(dataset, model, *, output, unit, tokenizer, profile, concurrency, max_tokens,
              max_request_bytes, max_prepared_bytes, max_result_bytes):
    """Only this isolated worker installs scoped observers; native scheduling stays intact."""
    if (type(concurrency) is not int or not 1 <= concurrency <= 16
            or type(max_tokens) is not int or not 1 <= max_tokens <= 8192
            or any(type(v) is not int or v < 1 for v in (max_request_bytes, max_prepared_bytes, max_result_bytes))):
        raise ValueError("Q2b requires finite concurrency, generation and byte maxima")
    preflight = dataset.preflight()
    sources = validate_native_source()
    profile = validate_profile(profile, model, tokenizer, max_tokens)
    if unit.budget.limit != preflight["budget"]["total_maximum"]:
        raise ValueError("Q2b allocation must equal the complete conservative native call cap")
    output = Path(output)
    new_private_directory(output)
    summary = dict(schema="semloom.mmqa_q2b_observation.v1", status="failed", preflight=preflight,
        arm="lotus-native", scheduler_owner="lotus_native_lm_and_vllm", profile=profile,
        strategy_compatibility=strategy_profile(),
        native_sources=sources, concurrency=concurrency, max_tokens=max_tokens,
        max_request_bytes=max_request_bytes, max_prepared_bytes=max_prepared_bytes,
        max_result_bytes=max_result_bytes, warmup_model_calls=0, retries=0, query_count=1,
        model_calls=0, remote_compute_end_known=False,
        scope="original Q2b on finite external inputs; no method comparison or PG claim",
        unavailable=dict(model_queue="requires service evidence", GPU_kernel="not collected",
            HBM="requires independent sampler", physical_transfer="not instrumented",
            exclusive_memory_release="pandas return does not establish physical release"))
    started, failure, gateway, observation = time.monotonic_ns(), None, None, None
    try:
        with open_private_text(output / "events.jsonl") as events, open_private_text(output / "requests.jsonl") as requests, open_private_text(output / "responses.jsonl") as responses:
            def event(value):
                events.write(json.dumps(value, ensure_ascii=False) + "\n")

            def before(_route, body):
                if len(body) > max_request_bytes:
                    raise ValueError("Q2b complete request exceeds the declared byte maximum")
                value = json.loads(body)
                if value.get("model") != model.model_id:
                    raise ValueError("Q2b request model identity differs")
                member = observation.reserve_request(value)
                attempt = unit.reserve(hashlib.sha256(body).hexdigest())
                requests.write(json.dumps(dict(member, attempt=attempt, body_sha256=hashlib.sha256(body).hexdigest(),
                    body=value, observed_monotonic_ns=time.monotonic_ns()), ensure_ascii=False) + "\n")

            def after(_route, body, response, status):
                if len(response) > max_result_bytes:
                    raise ValueError("Q2b response exceeds the declared result byte maximum")
                responses.write(json.dumps(dict(request_sha256=hashlib.sha256(body).hexdigest(), status=status,
                    body_base64=base64.b64encode(response).decode(), observed_monotonic_ns=time.monotonic_ns())) + "\n")

            with ObservationGateway(routes=(GatewayRoute("mmqa-q2b", "model", model.endpoint_url),),
                    trace_path=output / "http-trace.jsonl", request_timeout_s=model.timeout_ms / 1000,
                    before_forward=before, after_forward=after, preserve_http=True) as gateway:
                routed = replace(model, endpoint_url=gateway.endpoint_url("mmqa-q2b", "model"))
                lm = configure_lm(routed, concurrency, max_tokens, tokenizer)
                if unit.attempts:
                    raise ValueError("Q2b preparation unexpectedly submitted model requests")
                observation = Q2bObservation(dataset, lm, record=event, max_prepared_bytes=max_prepared_bytes)
                query_started = time.monotonic_ns()
                publish_private_json(output / "started.json", dict(started_ns=query_started))
                try:
                    with observation.observe():
                        result = run_original_query(dataset)
                finally:
                    summary["full_query_seconds"] = (time.monotonic_ns() - query_started) / 1e9
                publish_private_json(output / "execution.json", dict(completed_ns=time.monotonic_ns()))
                if (observation.active is not None or [s["stage_id"] for s in observation.stages] != ["join", "extract"]
                        or unit.attempts != preflight["budget"]["join"] + len(observation.matched)):
                    raise ValueError("Q2b full query is incomplete or has unexpected model calls")
                if list(result.columns) != ["ID", "image_id", "color"] or len(result) != len(observation.matched):
                    raise ValueError("Q2b final projection differs from the original stage result")
                raw_result = json.loads(result.to_json(orient="split"))
                if len(json.dumps(raw_result).encode()) > max_result_bytes:
                    raise ValueError("Q2b final result exceeds the declared byte maximum")
                write_private_json(output / "original-result.json", raw_result)
                event(dict(event="result_consumed", monotonic_ns=time.monotonic_ns(), rows=len(result)))
                evaluation_started = time.monotonic_ns()
                summary["quality"] = evaluate_original(dataset.checkout, result, output)
                summary["evaluation_seconds"] = (time.monotonic_ns() - evaluation_started) / 1e9
                summary["lm_stats"] = asdict(lm.stats)
                trace = gateway.snapshot()
                if len(trace) != unit.attempts or any(row["status"] != "completed" for row in trace):
                    raise ValueError("Q2b HTTP evidence has failed or unknown outcomes")
                summary["http_peak"] = observed_http_peak(trace)
                if summary["http_peak"] > concurrency:
                    raise ValueError("native Q2b HTTP concurrency exceeded its declared maximum")
                summary["status"] = "completed"
            event(dict(event="observation_footer", monotonic_ns=time.monotonic_ns(), status="completed",
                       model_calls=unit.attempts))
    except BaseException as error:
        failure = error
    finally:
        summary["application_seconds"] = (time.monotonic_ns() - started) / 1e9
        summary["stages"] = observation.stages if observation else []
        try:
            summary["model_calls"] = unit.attempts
            # The gateway has already stopped and flushed its trace here.
            trace_path = output / "http-trace.jsonl"
            trace = [json.loads(line) for line in trace_path.read_text().splitlines()] if trace_path.exists() else []
            summary["http_requests"] = len(trace)
            summary["known_http_completed"] = sum(row["status"] == "completed" for row in trace)
            summary["unknown_or_failed_attempts"] = summary["model_calls"] - summary["known_http_completed"]
        except BaseException as accounting:
            summary["accounting_error_type"] = type(accounting).__name__
            failure = failure or accounting
        if failure is not None:
            summary.update(status="failed", error_type=type(failure).__name__, error=redact_text(str(failure)))
        try:
            write_private_json(output / "summary.json", summary)
            write_private_json(output / "observation-footer.json", dict(status=summary["status"],
                model_calls=summary["model_calls"], files={name: file_identity(output / name)
                    for name in ("events.jsonl", "requests.jsonl", "responses.jsonl", "summary.json")
                    if (output / name).is_file()}))
        except BaseException as recording:
            if failure is None:
                raise
            failure.add_note("Q2b evidence recording also failed: " + type(recording).__name__)
            raise failure from recording
    if failure is not None:
        raise failure
    return summary


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--checkout", type=Path, required=True)
    mode = result.add_mutually_exclusive_group(required=True)
    for name in ("prepare", "preflight", "execute", "worker"):
        mode.add_argument("--" + name, action="store_true", help=argparse.SUPPRESS if name == "worker" else None)
    for name in ("manifest", "output", "model", "tokenizer", "model-profile", "lease-root", "source-data", "source-archive"):
        result.add_argument("--" + name, type=Path)
    result.add_argument("--manifest-digest")
    result.add_argument("--repository-commit")
    for name in ("scale-factor", "max-attempts", "concurrency", "max-tokens", "max-request-bytes", "max-prepared-bytes", "max-result-bytes", "deadline-seconds"):
        result.add_argument("--" + name, type=int)
    return result


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    if args.prepare:
        if any(v is None for v in (args.source_data, args.source_archive, args.output, args.scale_factor)):
            cli.error("prepare requires source-data/source-archive/output/scale-factor")
        value = prepare_dataset(args.checkout, args.source_data, args.output,
                                scale_factor=args.scale_factor, source_archive=args.source_archive)
        print(json.dumps(dict(manifest_digest=value["sha256"], images=len(value["images"]))))
        return 0
    if args.manifest is None or args.manifest_digest is None:
        cli.error("preflight/execute require manifest and manifest-digest")
    dataset = MmqaDataset(args.checkout, args.manifest, args.manifest_digest)
    preflight = dataset.preflight()
    if args.preflight:
        print(json.dumps(preflight, indent=2))
        return 0
    required = (args.output, args.model, args.tokenizer, args.model_profile, args.lease_root,
        args.repository_commit, args.max_attempts, args.concurrency, args.max_tokens,
        args.max_request_bytes, args.max_prepared_bytes, args.max_result_bytes, args.deadline_seconds)
    if any(v is None for v in required):
        cli.error("execute requires model/profile/tokenizer, exact commit, lease, attempt/concurrency/token/byte/deadline maxima")
    if args.max_attempts != preflight["budget"]["total_maximum"] or not 1 <= args.deadline_seconds <= 7200:
        cli.error("attempt cap must equal the complete derived Q2b cap, deadline must be 1..7200s")
    model = load_fixed_model_config(args.model)
    profile = json.loads(args.model_profile.read_text())
    validate_native_source()
    validate_profile(profile, model, args.tokenizer, args.max_tokens)
    budget = AttemptBudget("mmqa-q2b", args.max_attempts)
    if args.worker:
        ledger = CellBudgetLedger(args.output / "budget.sqlite", budget)
        unit = ledger.claim_shared_unit("q2b")
        try:
            publish_private_json(args.output / "unit/unit-reserved.json", dict(unit_id="q2b"))
            run_query(dataset, model, output=args.output / "unit/q0", unit=unit, tokenizer=args.tokenizer,
                profile=profile, concurrency=args.concurrency, max_tokens=args.max_tokens,
                max_request_bytes=args.max_request_bytes, max_prepared_bytes=args.max_prepared_bytes,
                max_result_bytes=args.max_result_bytes)
        finally:
            ledger.close_shared_unit("q2b")
        return 0
    repo = Path(__file__).resolve().parents[3]
    actual_commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(["git", "-C", str(repo), "status", "--porcelain", "--untracked-files=all", "--", "code"], text=True)
    if actual_commit != args.repository_commit or dirty:
        raise ValueError("Q2b execution requires the exact clean reviewed Git code identity")
    with acquire_runner_lease(args.lease_root, config_fingerprint=HOST_SCOPE_FINGERPRINT,
            repository_commit=args.repository_commit, recover_stale=False):
        new_private_directory(args.output)
        (args.output / "unit").mkdir()
        ledger = CellBudgetLedger.create(args.output / "budget.sqlite", budget,
                                        deadline_utc=time.time() + args.deadline_seconds)
        ledger.reserve_unit("q2b", args.max_attempts)
        resolved_argv = list(sys.argv[1:] if argv is None else argv)
        command = [sys.executable, "-B", str(Path(__file__).resolve().parents[2] / "scripts/experiments/run_sembench_mmqa.py"),
                   *("--worker" if v == "--execute" else v for v in resolved_argv)]
        write_private_json(args.output / "run-config.json", dict(repository_commit=actual_commit,
            preflight=preflight, profile=profile, max_attempts=args.max_attempts,
            concurrency=args.concurrency, max_tokens=args.max_tokens, deadline_seconds=args.deadline_seconds,
            max_request_bytes=args.max_request_bytes, max_prepared_bytes=args.max_prepared_bytes,
            max_result_bytes=args.max_result_bytes))
        try:
            supervise(command, args.output, lambda: ledger.close_shared_unit("q2b"),
                      query_timeout_s=args.deadline_seconds, max_duration_s=args.deadline_seconds)
        finally:
            ledger.close_shared_unit("q2b")
    print((args.output / "unit/q0/summary.json").read_text())
    return 0
