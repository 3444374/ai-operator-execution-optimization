"""One bounded Animals Q1 qualification using the existing LOTUS executors."""

import argparse
import base64
from collections import Counter
from contextlib import ExitStack
from dataclasses import asdict, replace
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import threading
import time

from src.baselines.common.private_artifacts import (
    content_digest,
    new_private_directory,
    open_private_text,
    write_private_json,
)
from src.baselines.common.redact import redact_text
from src.baselines.image.sembench_animals import (
    AnimalsDataset,
    classification_audit,
    evaluate_original,
    run_original_query,
)
from src.baselines.text.frameworks.lotus_pg import observe_filter_rows
from src.baselines.text.frameworks.native_calls import NativeCallExecutor
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.experiments.attempt_ledger import AttemptBudget, AttemptLedger
from src.infrastructure.runner_lease import acquire_runner_lease, HOST_SCOPE_FINGERPRINT
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from src.semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor
from src.semantic_methods.lotus.limits import LotusCallLimits
from src.semantic_methods.lotus.native import LotusNativeBatchExecutor
from src.semantic_methods.lotus.sdk import IMAGE_TEXT_PROFILE, validate_source


def configure_lm(model, concurrency, max_tokens, tokenizer_path):
    import lotus
    from lotus.models import LM
    from tokenizers import Tokenizer

    if not model.endpoint_url.endswith("/chat/completions") or model.timeout_ms > 120000:
        raise ValueError("Animals requires a chat endpoint and finite request timeout <=120s")
    lm = LM(
        "openai/" + model.model_id,
        api_base=model.endpoint_url[: -len("/chat/completions")],
        api_key=model.bearer_token or "local-fixture",
        max_batch_size=concurrency,
        max_tokens=max_tokens,
        temperature=0,
        top_p=1,
        tokenizer=Tokenizer.from_file(str(tokenizer_path)),
        timeout=model.timeout_ms / 1000,
        num_retries=0,
        max_retries=0,
    )
    lotus.settings.configure(lm=lm, enable_cache=False)
    return lm


def observed_http_peak(trace):
    """One gateway clock: upstream dispatch until its complete response body."""
    events = []
    for row in trace:
        start = row["upstream_dispatch_started_monotonic_ns"]
        end = row["upstream_response_body_read_completed_monotonic_ns"]
        if type(start) is not int or type(end) is not int or end < start:
            raise ValueError("Animals HTTP interval is incomplete")
        events.extend(((start, 1), (end, -1)))
    active = peak = 0
    for _, delta in sorted(events):
        active += delta
        peak = max(peak, active)
    return peak


def close_core_execution(execution, summary, primary_error=None):
    """Record unresolved ownership without replacing an earlier query failure."""
    close_error = None
    try:
        closed = execution.close()
    except BaseException as error:
        closed = False
        close_error = error
    summary["core_cleanup"] = dict(
        closed=closed,
        usage=asdict(execution.engine.capacity.usage()),
        open_jobs=len(execution.engine.jobs.jobs),
    )
    if close_error is not None:
        summary["core_cleanup"]["error_type"] = type(close_error).__name__
    if not closed:
        error = close_error or RuntimeError("Animals Core retained unconfirmed ownership at close")
        if primary_error is None:
            raise error
        primary_error.add_note("Animals Core cleanup: " + type(error).__name__ + ": " + str(error))
    return False


def run_query(
    dataset,
    model,
    *,
    arm,
    output,
    tokenizer_path,
    concurrency=4,
    max_tokens=8192,
    observation_mode,
    ray_temp_root=None,
    core_request_bytes=None,
    core_input_bytes=None,
    core_ray_window_bytes=None,
    core_ray_object_bytes=None,
):
    """No constructor warmup; actual POST/response bodies, status and original decisions persist."""
    if arm not in ("lotus-native", "daft", "ray-data", "semloom"):
        raise ValueError("Animals requires an explicitly supported full-request executor")
    if observation_mode not in ("performance", "qualification"):
        raise ValueError("Animals requires an explicit observation mode")
    if type(concurrency) is not int or not 2 <= concurrency <= 16:
        raise ValueError("Animals request allowance must be between 2 and 16")
    if type(max_tokens) is not int or not 1 <= max_tokens <= 8192:
        raise ValueError("Animals generation allowance must be between 1 and 8192")
    if arm == "semloom":
        if concurrency > 8:
            raise ValueError("Animals Core request allowance exceeds its eight Ray CPUs")
        values = (
            core_request_bytes,
            core_input_bytes,
            core_ray_window_bytes,
            core_ray_object_bytes,
        )
        if (
            any(type(v) is not int or v < 1 for v in values)
            or core_request_bytes > 8 * 1024 * 1024
            or core_input_bytes < core_request_bytes
            or core_ray_window_bytes < core_request_bytes + 24
            or core_ray_object_bytes < core_ray_window_bytes + 8
        ):
            raise ValueError(
                "Animals Core requires explicit compatible item/input and Ray window/object bytes"
            )
    preflight = dataset.preflight()
    validate_source(message_profile=IMAGE_TEXT_PROFILE)
    root = Path(output)
    new_private_directory(root)
    write_private_json(root / "preflight.json", preflight)
    rows = dataset.read()["rows"]
    ledger = AttemptLedger.create(root / "attempts.jsonl", AttemptBudget("animals-q1", len(rows)))
    expected, actual = Counter(), Counter()
    decisions, prompts = [], []
    lock = threading.Lock()
    started = time.monotonic_ns()
    summary = dict(
        schema="semloom.sembench_animals_query.v1",
        status="failed",
        arm=arm,
        manifest_digest=dataset.expected_digest,
        rows=len(rows),
        model_id=model.model_id,
        observation_mode=observation_mode,
        cache=False,
        retries=0,
        max_tokens=max_tokens,
        concurrency=concurrency,
        warmup_model_calls=0,
        query_count=1,
        upstream_lotus_version="1.1.3",
        selected_lotus_version="1.2.4",
        execution_scope="original Q1 on finite external ImageArray; no PostgreSQL claim",
        query_tail_quantiles="unavailable: one complete query",
        observation_scope="matched proxy, request/response body and status persistence; message identity checks",
        full_query_scope="source verification/read, original query/import, native graph and complete result",
        constructor_warmup="not invoked; unbound original Q1 method with caller-configured LM",
    )
    failure = None
    try:
        with ExitStack() as stack:
            requests = stack.enter_context(open_private_text(root / "requests.jsonl"))
            responses = stack.enter_context(open_private_text(root / "responses.jsonl"))

            def before(_route, body):
                value = json.loads(body)
                digest = content_digest(value.get("messages"))
                with lock:
                    if value.get("model") != model.model_id or actual[digest] >= expected[digest]:
                        raise ValueError(
                            "Animals outgoing request differs from its original LM message batch"
                        )
                    attempt = ledger.reserve(hashlib.sha256(body).hexdigest())
                    actual[digest] += 1
                    requests.write(
                        json.dumps(
                            dict(
                                attempt=attempt,
                                body=value,
                                bytes=len(body),
                                body_sha256=hashlib.sha256(body).hexdigest(),
                            ),
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

            def after(_route, body, response, status):
                with lock:
                    responses.write(
                        json.dumps(
                            dict(
                                request_sha256=hashlib.sha256(body).hexdigest(),
                                status=status,
                                body_base64=base64.b64encode(response).decode("ascii"),
                            )
                        )
                        + "\n"
                    )

            gateway = stack.enter_context(
                ObservationGateway(
                    routes=(GatewayRoute("animals-q1", "model", model.endpoint_url),),
                    trace_path=root / "http-trace.jsonl",
                    request_timeout_s=model.timeout_ms / 1000,
                    before_forward=before,
                    after_forward=after,
                    preserve_http=True,
                )
            )
            routed = replace(
                model,
                endpoint_url=gateway.endpoint_url("animals-q1", "model"),
                bearer_token=model.bearer_token or "local-fixture",
            )
            lm = configure_lm(routed, concurrency, max_tokens, tokenizer_path)
            summary["preparation_model_calls"] = ledger.attempts
            if ledger.attempts:
                raise ValueError("unexpected model call during preparation")
            executor = None
            if arm in ("daft", "ray-data"):
                ray_actors = 2 if concurrency % 2 == 0 else 1
                options = NativeGraphOptions(
                    concurrency=concurrency,
                    num_threads=8,
                    ray_actors=ray_actors,
                    ray_async_batches_per_actor=concurrency // ray_actors,
                    ray_batch_rows=1,
                    source_blocks=4,
                )
                summary["native_options"] = asdict(options)
                summary["configured_http_upper_bound"] = (
                    options.concurrency if arm == "daft" else options.ray_active_http_upper_bound
                )
                if arm == "daft":
                    from src.infrastructure.daft_runtime import configure_daft_runner

                    configure_daft_runner("native", num_threads=options.num_threads)
                else:
                    os.environ["RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY"] = str(
                        options.ray_async_batches_per_actor
                    )
                    import ray
                    from src.experiments.postgresql.query_config import NativeRayRuntimeConfig
                    from src.experiments.postgresql.query_execution import native_ray_runtime

                    summary["ray_runtime"] = stack.enter_context(
                        native_ray_runtime(
                            ray,
                            NativeRayRuntimeConfig(
                                8,
                                options.ray_async_batches_per_actor,
                                ray_object_store_bytes=512 * 1024 * 1024,
                            ),
                            ray_temp_root,
                        )
                    )
                native = NativeCallExecutor(
                    arm, options, max_batch_rows=64, max_batch_bytes=256 * 1024 * 1024
                )
                limits = LotusCallLimits(
                    routed.timeout_ms / 1000,
                    max_request_bytes=8 * 1024 * 1024,
                    max_batch_rows=64,
                    max_batch_input_bytes=256 * 1024 * 1024,
                    batch_timeout_s=None,
                )
                executor = LotusNativeBatchExecutor(
                    native,
                    routed,
                    query_id="animals-q1",
                    operator_id="filter",
                    call_limits=limits,
                    message_profile=IMAGE_TEXT_PROFILE,
                    retain_full_responses=False,
                )
            elif arm == "semloom":
                import ray
                from src.execution_provider.adapters.native_tasks import build_native_execution
                from src.execution_provider.adapters.ray_map_transport import RayMapConfig
                from src.experiments.postgresql.query_config import NativeRayRuntimeConfig
                from src.experiments.postgresql.query_execution import native_ray_runtime

                summary["ray_runtime"] = stack.enter_context(
                    native_ray_runtime(
                        ray,
                        NativeRayRuntimeConfig(8, 2, ray_object_store_bytes=512 * 1024 * 1024),
                        ray_temp_root,
                    )
                )
                physical = RayMapConfig(
                    ray.get_runtime_context().gcs_address,
                    concurrency,
                    1,
                    core_ray_window_bytes,
                    core_ray_object_bytes,
                    payload_backend="arrow",
                )
                summary["core_capacity"] = dict(
                    item_input_bytes=core_request_bytes,
                    input_bytes=core_input_bytes,
                    held_tasks=concurrency,
                    active_requests=concurrency,
                    ray_workers=concurrency,
                    ray_window_bytes=core_ray_window_bytes,
                    ray_object_bytes=core_ray_object_bytes,
                    preparation=None,
                    lotus_prepared_batch_input_bytes=128 * 1024 * 1024,
                    scope="Core logical input; SDK/proxy/Arrow and unaccepted producer suffix are separate",
                )
                summary["configured_http_upper_bound"] = concurrency
                execution = build_native_execution(
                    routed,
                    physical=physical,
                    max_request_bytes=core_request_bytes,
                    input_bytes=core_input_bytes,
                    max_tasks=concurrency,
                    max_active_requests=concurrency,
                )
                stack.push(
                    lambda _type, error, _trace: close_core_execution(execution, summary, error)
                )
                limits = LotusCallLimits(
                    routed.timeout_ms / 1000,
                    max_request_bytes=core_request_bytes,
                    max_batch_rows=64,
                    max_batch_input_bytes=128 * 1024 * 1024,
                    batch_timeout_s=None,
                )
                executor = LotusBatchExecutor(
                    execution,
                    routed,
                    query_id="animals-q1",
                    operator_id="filter",
                    call_limits=limits,
                    message_profile=IMAGE_TEXT_PROFILE,
                    retain_full_responses=False,
                )

            def record_prompts(values):
                if prompts or [p["row_id"] for p in values] != [r["row_id"] for r in rows]:
                    raise ValueError(
                        "Animals requires one original LM message per source occurrence"
                    )
                prompts.extend(values)
                expected.update(content_digest(p["messages"]) for p in values)
                if observation_mode == "qualification":
                    # Optional extra source/pixel verification; performance skips this second read.
                    dataset.read()

            query_started = time.monotonic_ns()
            try:
                with (
                    lotus_executor(lm, executor),
                    observe_filter_rows(decisions.extend, record_prompts),
                ):
                    result = run_original_query(dataset.checkout, dataset.frame)
            finally:
                summary["full_query_seconds"] = (time.monotonic_ns() - query_started) / 1e9
            if actual != expected or sum(actual.values()) != len(rows):
                raise ValueError(
                    "Animals full-query model requests do not cover every original input"
                )
            if (
                list(result.columns) != ["count(*)"]
                or len(result) != 1
                or int(result.iloc[0, 0]) != sum(v for _, v in decisions)
            ):
                raise ValueError("original Animals COUNT differs from its Filter decisions")
            summary["classification_audit"] = classification_audit(rows, decisions)
            summary["quality"] = evaluate_original(
                dataset.checkout, result, dataset.frame(references=True)
            )
            summary["model_calls"] = ledger.attempts
            summary["lm_stats"] = asdict(lm.stats)
            trace = gateway.snapshot()
            summary["http_requests"] = len(trace)
            write_private_json(
                root / "original-result.json", json.loads(result.to_json(orient="split"))
            )
            write_private_json(root / "filter-decisions.json", decisions)
            if executor is not None:
                summary["executor_close_report"] = (
                    dict(
                        lotus_cleanup_errors=[
                            dict(phase=phase, error_type=type(error).__name__)
                            for phase, error in executor.last_cleanup_errors
                        ]
                    )
                    if arm == "semloom"
                    else executor.last_close_report
                )
            peak = observed_http_peak(trace)
            summary["resource_gate"] = dict(
                request_allowance=concurrency,
                observed_upstream_peak=peak,
                passed=peak <= concurrency,
                interval="upstream dispatch to complete response body, gateway monotonic clock",
            )
            if peak > concurrency:
                raise ValueError("Animals observed HTTP concurrency exceeds its declared allowance")
            summary["status"] = "completed"
    except BaseException as error:
        failure = error
    finally:
        try:
            summary["model_calls"] = ledger.attempts
        except BaseException as error:
            summary["model_calls"] = None
            summary["model_call_accounting"] = dict(
                status="unavailable", error_type=type(error).__name__
            )
            if failure is None:
                failure = error
            else:
                failure.add_note("Animals attempt accounting cleanup: " + type(error).__name__)
        summary["application_seconds"] = (time.monotonic_ns() - started) / 1e9
        summary["versions"] = {}
        for name in ("lotus-ai", "litellm", "openai", "pandas", "duckdb"):
            try:
                summary["versions"][name] = importlib.metadata.version(name)
            except importlib.metadata.PackageNotFoundError:
                summary["versions"][name] = dict(
                    status="unavailable", reason="distribution is not installed"
                )
        if failure is not None:
            summary["status"] = "failed"
            summary["error_type"] = type(failure).__name__
            summary["error"] = redact_text(str(failure))
            summary["error_notes"] = [
                redact_text(str(note)) for note in getattr(failure, "__notes__", ())
            ]
        write_private_json(root / "summary.json", summary)
    if failure is not None:
        raise failure
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--manifest-digest", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--model", type=Path)
    parser.add_argument("--tokenizer", type=Path)
    parser.add_argument(
        "--arm", choices=("lotus-native", "daft", "ray-data", "semloom"), default="lotus-native"
    )
    parser.add_argument("--observation-mode", choices=("qualification", "performance"))
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--max-tokens", type=int, default=8192)
    parser.add_argument("--ray-temp-root", type=Path)
    parser.add_argument("--core-request-bytes", type=int)
    parser.add_argument("--core-input-bytes", type=int)
    parser.add_argument("--core-ray-window-bytes", type=int)
    parser.add_argument("--core-ray-object-bytes", type=int)
    parser.add_argument("--lease-root", type=Path)
    parser.add_argument("--repository-commit")
    args = parser.parse_args(argv)
    dataset = AnimalsDataset(args.checkout, args.manifest, args.manifest_digest)
    if args.preflight:
        print(json.dumps(dataset.preflight(), indent=2))
        return 0
    if any(
        v is None
        for v in (
            args.output,
            args.model,
            args.tokenizer,
            args.observation_mode,
            args.lease_root,
            args.repository_commit,
        )
    ):
        parser.error(
            "execute requires output/model/tokenizer/observation-mode/lease-root/repository-commit"
        )
    with acquire_runner_lease(
        args.lease_root,
        config_fingerprint=HOST_SCOPE_FINGERPRINT,
        repository_commit=args.repository_commit,
        recover_stale=False,
    ):
        summary = run_query(
            dataset,
            load_fixed_model_config(args.model),
            arm=args.arm,
            output=args.output,
            tokenizer_path=args.tokenizer,
            concurrency=args.concurrency,
            max_tokens=args.max_tokens,
            observation_mode=args.observation_mode,
            ray_temp_root=args.ray_temp_root,
            core_request_bytes=args.core_request_bytes,
            core_input_bytes=args.core_input_bytes,
            core_ray_window_bytes=args.core_ray_window_bytes,
            core_ray_object_bytes=args.core_ray_object_bytes,
        )
    print(
        json.dumps(
            {
                k: summary[k]
                for k in ("status", "arm", "rows", "model_calls", "full_query_seconds", "quality")
            },
            indent=2,
        )
    )
    return 0
