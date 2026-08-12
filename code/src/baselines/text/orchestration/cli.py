"""Thin command-line dispatcher for same-condition baseline adapters."""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import math
import os
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from ..ceilings import (
    VllmBenchConfig,
    build_vllm_bench_command,
    extract_vllm_bench_latency_distribution,
    extract_vllm_bench_request_timings,
    summarize_vllm_bench_latency_distribution,
    write_vllm_custom_dataset,
)
from ..controls import (
    BatchedCompletionsConfig,
    BoundedHttpConfig,
    TimedHttpJob,
    run_batched_completions,
    run_bounded_http,
    run_bounded_http_jobs,
)
from src.baselines.common.contracts import BaselineRequestResult, ChatRequest
from src.baselines.common.gate import validate_gate
from src.baselines.common.manifests import (
    PARTITION_POLICIES,
    assign_endpoints,
    partition_summary,
    read_manifest,
    write_manifest,
    write_manifest_metadata,
)
from ..products import (
    DuckDBAiConfig,
    OceanBaseConfig,
    inspect_duckdb_ai_runtime,
    run_duckdb_ai_complete,
    run_oceanbase_ai_complete,
)
from .postgres_manifest import load_postgres_requests
from src.baselines.common.provenance import adapter_provenance, registered_adapters
from src.baselines.common.results import summarize_results
from ..frameworks import (
    DaftPromptConfig,
    RayDataHttpConfig,
    run_daft_prompt,
    run_ray_data_http,
)


ADAPTERS = registered_adapters()
_OBSERVABILITY_BY_ADAPTER = {
    "bounded_http": ("request", "server_usage"),
    "bounded_completions": ("http_batch", "service_counter"),
    "vllm_bench": ("request", "official_benchmark"),
    "daft_native": ("shard_barrier", "manifest_prompt_only"),
    "daft_ray": ("shard_barrier", "manifest_prompt_only"),
    "ray_data_http": ("shard_barrier", "server_usage"),
    "oceanbase": ("query_barrier", "unavailable"),
    "duckdb_ai": ("query_barrier", "unavailable"),
}


def _observability_fields(adapter: str) -> dict[str, str]:
    timing_granularity, token_accounting = _OBSERVABILITY_BY_ADAPTER[
        adapter
    ]
    return {
        "timing_granularity": timing_granularity,
        "token_accounting": token_accounting,
        **adapter_provenance(adapter).summary_fields(),
    }


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _write_results(
    path: Path,
    results: tuple[BaselineRequestResult, ...],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=list(
                asdict(results[0]).keys()
                if results
                else BaselineRequestResult.__dataclass_fields__.keys()
            ),
        )
        writer.writeheader()
        writer.writerows(asdict(result) for result in results)
    temporary.replace(path)


def _service_fingerprint(
    args: argparse.Namespace,
    completion_protocol: str,
) -> str:
    payload = json.dumps(
        {
            "model": args.model,
            "protocol": completion_protocol,
            "temperature": 0.0,
            "ignore_eos": bool(getattr(args, "ignore_eos", False)),
            "service_prefix_caching": args.service_prefix_caching,
            "service_max_num_seqs": args.service_max_num_seqs,
            "service_max_num_batched_tokens": args.service_max_num_batched_tokens,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _chat_base_url(endpoint_url: str) -> str:
    suffix = "/chat/completions"
    if not endpoint_url.endswith(suffix):
        raise ValueError("endpoint URL must end with /v1/chat/completions")
    return endpoint_url[: -len(suffix)]


def _completion_protocol(adapter: str) -> str:
    return (
        "completions"
        if adapter == "bounded_completions"
        else "chat_completions"
    )


def _validate_endpoint_url(
    endpoint_url: str,
    completion_protocol: str,
) -> None:
    suffix = (
        "/v1/completions"
        if completion_protocol == "completions"
        else "/v1/chat/completions"
    )
    if not endpoint_url.endswith(suffix):
        raise ValueError(f"endpoint URL must end with {suffix}")


def _run_adapter(
    requests: tuple[ChatRequest, ...],
    args: argparse.Namespace,
) -> tuple[BaselineRequestResult, ...]:
    if args.adapter == "bounded_http":
        return asyncio.run(
            run_bounded_http(
                requests,
                BoundedHttpConfig(
                    endpoint_urls=(args.endpoint_url,),
                    model=args.model,
                    concurrency_per_endpoint=args.concurrency,
                    timeout_s=args.timeout_s,
                    api_key=args.api_key,
                    endpoint_index_offset=args.endpoint_index,
                    replay_arrivals=not args.disable_arrival_replay,
                    arrival_time_scale=args.arrival_time_scale,
                    ignore_eos=args.ignore_eos,
                ),
            )
        )
    if args.adapter == "bounded_completions":
        return asyncio.run(
            run_batched_completions(
                requests,
                BatchedCompletionsConfig(
                    endpoint_urls=(args.endpoint_url,),
                    model=args.model,
                    batch_rows=args.batch_size,
                    concurrency_per_endpoint=args.concurrency,
                    timeout_s=args.timeout_s,
                    api_key=args.api_key,
                    endpoint_index_offset=args.endpoint_index,
                ),
            )
        )
    if args.adapter in {"daft_native", "daft_ray"}:
        return run_daft_prompt(
            requests,
            DaftPromptConfig(
                runner=("native" if args.adapter == "daft_native" else "ray"),
                base_url=_chat_base_url(args.endpoint_url),
                api_key=args.api_key,
                model=args.model,
                max_tokens=requests[0].max_output_tokens,
                ray_address=args.ray_address,
            ),
        )
    if args.adapter == "ray_data_http":
        return run_ray_data_http(
            requests,
            RayDataHttpConfig(
                endpoint_url=args.endpoint_url,
                api_key=args.api_key,
                model=args.model,
                max_tokens=requests[0].max_output_tokens,
                batch_size=args.batch_size,
                concurrency=args.concurrency,
                ray_address=args.ray_address,
            ),
        )
    if args.adapter == "oceanbase":
        required = {
            "--oceanbase-user": args.oceanbase_user,
            "--oceanbase-database": args.oceanbase_database,
            "--oceanbase-model-key": args.oceanbase_model_key,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError("missing OceanBase options: " + ", ".join(missing))
        return run_oceanbase_ai_complete(
            requests,
            OceanBaseConfig(
                host=args.oceanbase_host,
                port=args.oceanbase_port,
                user=args.oceanbase_user,
                password=args.oceanbase_password,
                database=args.oceanbase_database,
                model_key=args.oceanbase_model_key,
                model_name=args.model,
                endpoint_url=args.endpoint_url,
                access_key=args.api_key or "not-needed",
                parallel_degree=args.oceanbase_parallel_degree,
                source_table=(f"baseline_requests_ep{args.endpoint_index}"),
                result_table=(f"baseline_results_ep{args.endpoint_index}"),
                register_model=args.oceanbase_register_model,
            ),
        )
    if args.adapter == "duckdb_ai":
        return run_duckdb_ai_complete(
            requests,
            DuckDBAiConfig(
                endpoint_base_url=_chat_base_url(args.endpoint_url),
                model=args.model,
                api_key=args.api_key or "EMPTY",
                max_tokens=requests[0].max_output_tokens,
                database_path=args.duckdb_database,
                max_concurrent_requests=args.duckdb_max_concurrent_requests,
                response_cache=args.duckdb_response_cache,
                provider_prompt_cache_hints=args.duckdb_prompt_cache_hints,
                retry_count=args.duckdb_retry_count,
                retry_backoff_ms=args.duckdb_retry_backoff_ms,
                min_request_interval_ms=args.duckdb_min_request_interval_ms,
                timeout_seconds=args.duckdb_timeout_seconds,
            ),
        )
    raise ValueError("vllm_bench is prepared and executed by its dedicated branch")


def _run_shard(args: argparse.Namespace) -> dict[str, object]:
    manifest = read_manifest(args.manifest)
    requests = tuple(
        request for request in manifest if request.endpoint_index == args.endpoint_index
    )
    if not requests:
        raise ValueError("selected endpoint shard is empty")
    completion_protocol = _completion_protocol(args.adapter)
    _validate_endpoint_url(args.endpoint_url, completion_protocol)
    if args.adapter in {"daft_ray", "ray_data_http"} and not args.ray_address:
        raise ValueError(f"{args.adapter} requires an explicit --ray-address")
    if args.adapter == "vllm_bench":
        if not args.tokenizer:
            raise ValueError("vllm_bench requires an explicit --tokenizer local directory")
        if not Path(args.tokenizer).is_dir():
            raise ValueError("vllm_bench --tokenizer must be an existing local directory")
    base_summary: dict[str, object] = {
        "adapter": args.adapter,
        **_observability_fields(args.adapter),
        "status": "dry_run" if args.dry_run else "running",
        "request_count": len(requests),
        "endpoint_index": args.endpoint_index,
        "endpoint_url": args.endpoint_url,
        "predicted_work": sum(request.estimated_work for request in requests),
        "model_name": args.model,
        "completion_protocol": completion_protocol,
        "ignore_eos": args.ignore_eos,
        "service_prefix_caching": args.service_prefix_caching,
        "service_max_num_seqs": args.service_max_num_seqs,
        "service_max_num_batched_tokens": args.service_max_num_batched_tokens,
        "http_batch_rows": (
            args.batch_size
            if args.adapter == "bounded_completions"
            else 1
        ),
        "service_config_sha256": _service_fingerprint(
            args,
            completion_protocol,
        ),
    }
    if args.adapter == "duckdb_ai":
        base_summary.update(
            {
                "duckdb_database": args.duckdb_database,
                "duckdb_ai_max_concurrent_requests": (
                    args.duckdb_max_concurrent_requests
                ),
                "duckdb_ai_response_cache": args.duckdb_response_cache,
                "duckdb_ai_prompt_cache_hints": args.duckdb_prompt_cache_hints,
                "duckdb_ai_retry_count": args.duckdb_retry_count,
                "duckdb_ai_retry_backoff_ms": args.duckdb_retry_backoff_ms,
                "duckdb_ai_min_request_interval_ms": (
                    args.duckdb_min_request_interval_ms
                ),
                "duckdb_ai_timeout_seconds": args.duckdb_timeout_seconds,
            }
        )
    if args.dry_run:
        return base_summary

    output_dir = Path(args.output_dir)
    if output_dir.exists():
        raise FileExistsError(f"output directory already exists: {output_dir}")
    output_dir.mkdir(parents=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir()
    manifest_bytes = Path(args.manifest).read_bytes()
    _atomic_json(
        output_dir / "manifest_metadata.json",
        {
            "row_count": len(manifest),
            "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        },
    )

    if args.adapter == "vllm_bench":
        dataset_path = raw_dir / "dataset.jsonl"
        write_vllm_custom_dataset(dataset_path, requests)
        command = build_vllm_bench_command(
            VllmBenchConfig(
                python_executable=args.python_executable,
                base_url=_chat_base_url(args.endpoint_url).removesuffix("/v1"),
                model=args.model,
                tokenizer=args.tokenizer,
                dataset_path=dataset_path,
                result_dir=raw_dir,
                result_filename="vllm_bench.json",
                num_prompts=len(requests),
                max_concurrency=args.concurrency,
            )
        )
        _atomic_json(raw_dir / "command.json", command)
        subprocess.run(command, check=True)
        prepared = {
            **base_summary,
            "status": "raw_complete",
            "normalization_required": True,
        }
        _atomic_json(output_dir / "summary.json", prepared)
        return prepared

    try:
        if args.adapter == "duckdb_ai":
            base_summary.update(
                inspect_duckdb_ai_runtime(
                    DuckDBAiConfig(
                        endpoint_base_url=_chat_base_url(args.endpoint_url),
                        model=args.model,
                        api_key=args.api_key or "EMPTY",
                        max_tokens=requests[0].max_output_tokens,
                        database_path=args.duckdb_database,
                    )
                )
            )
        results = _run_adapter(requests, args)
    except Exception as exc:
        _atomic_json(
            output_dir / "summary.json",
            {
                **base_summary,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "worker_failures": 1,
            },
        )
        raise
    _write_results(output_dir / "requests.csv", results)
    try:
        result_summary = summarize_results(requests, results)
    except Exception as exc:
        _atomic_json(
            output_dir / "summary.json",
            {
                **base_summary,
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "observed_result_count": len(results),
                "failed_result_count": sum(
                    result.status != "completed" or bool(result.error) for result in results
                ),
                "worker_failures": 0,
            },
        )
        raise
    normalized = {
        **base_summary,
        **result_summary,
        "status": "completed",
        "vllm_num_requests_running_final": (args.vllm_running_final),
        "vllm_num_requests_waiting_final": (args.vllm_waiting_final),
        "worker_failures": 0,
    }
    _atomic_json(output_dir / "summary.json", normalized)
    return normalized


def _export_manifest(args: argparse.Namespace) -> dict[str, object]:
    rows = [json.loads(line) for line in Path(args.input).read_text(encoding="utf-8").splitlines()]
    requests = tuple(
        ChatRequest(
            **{
                **row,
                "endpoint_index": int(row.get("endpoint_index", -1)),
            }
        )
        for row in rows
    )
    assigned = assign_endpoints(
        requests,
        args.endpoint_count,
        policy=args.partition_policy,
        seed=args.partition_seed,
    )
    metadata = write_manifest(args.output, assigned)
    summary = partition_summary(assigned, args.endpoint_count)
    write_manifest_metadata(
        args.output,
        partition_policy=args.partition_policy,
        partition_seed=args.partition_seed,
        row_count=metadata.row_count,
        manifest_sha256=metadata.sha256,
        partition_summary_dict=summary,
    )
    return {
        "status": "completed",
        "row_count": metadata.row_count,
        "sha256": metadata.sha256,
        "partition_policy": args.partition_policy,
        "partition_seed": args.partition_seed,
        **summary,
    }


def _connect_postgres(database_url: str):
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("PostgreSQL manifest export requires psycopg") from exc
    return psycopg.connect(database_url)


def _export_postgres_manifest(
    args: argparse.Namespace,
) -> dict[str, object]:
    with _connect_postgres(args.database_url) as connection:
        requests = load_postgres_requests(
            connection,
            workload_name=args.workload_name,
            row_count=args.row_count,
            row_offset=args.row_offset,
            max_output_tokens=args.max_output_tokens,
            estimated_output_mode=args.estimated_output_mode,
        )
    assigned = assign_endpoints(
        requests,
        args.endpoint_count,
        policy=args.partition_policy,
        seed=args.partition_seed,
    )
    metadata = write_manifest(args.output, assigned)
    summary = partition_summary(assigned, args.endpoint_count)
    write_manifest_metadata(
        args.output,
        partition_policy=args.partition_policy,
        partition_seed=args.partition_seed,
        row_count=metadata.row_count,
        manifest_sha256=metadata.sha256,
        partition_summary_dict=summary,
    )
    return {
        "status": "completed",
        "row_count": metadata.row_count,
        "sha256": metadata.sha256,
        "workload_name": args.workload_name,
        "row_offset": args.row_offset,
        "max_output_tokens": args.max_output_tokens,
        "estimated_output_mode": args.estimated_output_mode,
        "partition_policy": args.partition_policy,
        "partition_seed": args.partition_seed,
        **summary,
    }


def _read_result_csv(path: str | Path) -> tuple[BaselineRequestResult, ...]:
    with Path(path).open(encoding="utf-8", newline="") as stream:
        rows = tuple(csv.DictReader(stream))
    return tuple(
        BaselineRequestResult(
            doc_id=int(row["doc_id"]),
            endpoint_index=int(row["endpoint_index"]),
            status=row["status"],
            error=row["error"] or None,
            submitted_at_s=float(row["submitted_at_s"]),
            started_at_s=float(row["started_at_s"]),
            completed_at_s=float(row["completed_at_s"]),
            input_tokens=int(row["input_tokens"]),
            output_tokens=int(row["output_tokens"]),
            output_text=row["output_text"] or None,
            finish_reason=row["finish_reason"] or None,
        )
        for row in rows
    )


def _parse_timed_job(value: str) -> tuple[str, Path, float]:
    fields = value.split("=", 2)
    if len(fields) != 3 or not fields[0].strip() or not fields[1].strip():
        raise argparse.ArgumentTypeError(
            "timed job must use JOB_ID=/path/to/manifest.jsonl=OFFSET_S"
        )
    try:
        offset_s = float(fields[2])
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "timed job OFFSET_S must be numeric"
        ) from exc
    if not math.isfinite(offset_s) or offset_s < 0:
        raise argparse.ArgumentTypeError(
            "timed job OFFSET_S must be finite and non-negative"
        )
    return fields[0].strip(), Path(fields[1]), offset_s


def _run_jobs_control(
    args: argparse.Namespace,
) -> dict[str, object]:
    """Run one merged-arrival, endpoint-bounded, no-Job-policy control."""

    from .gate_runner import (
        sample_vllm_token_counters,
        wait_for_idle,
    )

    if len(args.endpoint_url) != len(args.metrics_url):
        raise ValueError("run-jobs-control requires one metrics URL per endpoint")
    if len(set(args.endpoint_url)) != len(args.endpoint_url):
        raise ValueError("run-jobs-control endpoint URLs must be unique")
    raw_jobs = tuple(args.job)
    job_ids = [job_id for job_id, _path, _offset in raw_jobs]
    if len(set(job_ids)) != len(job_ids):
        raise ValueError("run-jobs-control job IDs must be unique")
    timed_jobs = []
    metadata = {}
    for job_id, path, offset_s in raw_jobs:
        if not path.is_file():
            raise ValueError(f"timed job manifest does not exist: {path}")
        requests = read_manifest(path)
        timed_jobs.append(TimedHttpJob(job_id, requests, offset_s))
        payload = path.read_bytes()
        metadata[job_id] = {
            "path": str(path.resolve()),
            "rows": len(requests),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "arrival_offset_s": offset_s,
        }
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        raise FileExistsError(f"output directory already exists: {output_dir}")
    output_dir.mkdir(parents=True)
    raw_dir = output_dir / "raw"
    raw_dir.mkdir()
    wait_for_idle(tuple(args.metrics_url), args.idle_timeout_s)
    counters_before = sample_vllm_token_counters(tuple(args.metrics_url))
    grouped = asyncio.run(
        run_bounded_http_jobs(
            timed_jobs,
            BoundedHttpConfig(
                endpoint_urls=tuple(args.endpoint_url),
                model=args.model,
                concurrency_per_endpoint=args.concurrency,
                timeout_s=args.timeout_s,
                api_key=args.api_key,
                replay_arrivals=True,
                arrival_time_scale=args.arrival_time_scale,
                ignore_eos=args.ignore_eos,
            ),
        )
    )
    final_queues = wait_for_idle(tuple(args.metrics_url), args.idle_timeout_s)
    counters_after = sample_vllm_token_counters(tuple(args.metrics_url))
    job_summaries = {}
    combined_results = []
    request_by_job = {job.job_id: job.requests for job in timed_jobs}
    for job_id in job_ids:
        results = grouped[job_id]
        combined_results.extend(results)
        _write_results(raw_dir / f"{job_id}.requests.csv", results)
        job_summaries[job_id] = summarize_results(
            request_by_job[job_id],
            results,
        )
    combined = tuple(combined_results)
    if not combined:
        raise RuntimeError("run-jobs-control produced no results")
    service_prompt_tokens = sum(
        counters_after[index]["prompt_tokens"]
        - counters_before[index]["prompt_tokens"]
        for index in counters_before
    )
    service_generation_tokens = sum(
        counters_after[index]["generation_tokens"]
        - counters_before[index]["generation_tokens"]
        for index in counters_before
    )
    group_wall_s = (
        max(item.completed_at_s for item in combined)
        - min(item.submitted_at_s for item in combined)
    )
    job_windows = {
        job_id: (
            min(item.submitted_at_s for item in grouped[job_id]),
            max(item.completed_at_s for item in grouped[job_id]),
        )
        for job_id in job_ids
    }
    ordered_jobs = sorted(job_ids, key=lambda job_id: job_windows[job_id][0])
    active_set_status = "unavailable:requires_staggered_two_jobs"
    active_set_overlap_s = 0.0
    if len(ordered_jobs) == 2:
        bulk_id, foreground_id = ordered_jobs
        bulk_start, bulk_end = job_windows[bulk_id]
        foreground_start, foreground_end = job_windows[foreground_id]
        active_set_overlap_s = max(
            0.0,
            min(bulk_end, foreground_end) - foreground_start,
        )
        active_set_status = (
            "ok:observed_bulk_then_foreground_then_bulk_drain"
            if bulk_start < foreground_start
            and active_set_overlap_s > 0
            and foreground_end < bulk_end
            else "active_set_contract_not_observed"
        )
    slo_target_s = (
        args.request_slo_ms / 1000.0 if args.request_slo_ms > 0 else None
    )
    slo_metrics = {}
    for job_id in job_ids:
        latencies = [item.latency_s for item in grouped[job_id]]
        slo_metrics[job_id] = {
            "target_ms": args.request_slo_ms,
            "violation_ratio": (
                sum(latency > slo_target_s for latency in latencies)
                / len(latencies)
                if slo_target_s is not None
                else None
            ),
        }
    summary = {
        "schema_version": 1,
        "status": "completed",
        "experiment_id": args.experiment_id,
        "phase": args.phase,
        "repeat_index": args.repeat_index,
        "control": "direct_merged_arrival_no_job_scheduler",
        "scheduler_owner": "endpoint_http_bound_then_vllm_fcfs",
        "project_job_scheduler": False,
        "comparison_role": "direct_client_control",
        "implementation_provenance": "project_bounded_async_client",
        "formal_baseline_eligible": False,
        "formal_control_eligible": True,
        "qualification_gate": (
            "same_manifest_model_protocol_endpoints_and_idle_boundaries"
        ),
        "job_count": len(timed_jobs),
        "concurrency_per_endpoint": args.concurrency,
        "endpoint_count": len(args.endpoint_url),
        "model_name": args.model,
        "arrival_time_scale": args.arrival_time_scale,
        "manifests": metadata,
        "job_metrics": job_summaries,
        "job_slo_metrics": slo_metrics,
        "active_set_contract_status": active_set_status,
        "active_set_overlap_s": active_set_overlap_s,
        "group_wall_s": group_wall_s,
        "service_prompt_tokens": service_prompt_tokens,
        "service_generation_tokens": service_generation_tokens,
        "service_total_tokens_per_s": (
            (service_prompt_tokens + service_generation_tokens) / group_wall_s
            if group_wall_s > 0
            else 0.0
        ),
        "final_queues": final_queues,
    }
    _write_results(raw_dir / "all.requests.csv", combined)
    _atomic_json(output_dir / "summary.json", summary)
    return summary


def _normalize_vllm_bench(
    args: argparse.Namespace,
) -> dict[str, object]:
    manifest = read_manifest(args.manifest)
    requests = tuple(
        request for request in manifest if request.endpoint_index == args.endpoint_index
    )
    raw = json.loads(Path(args.input).read_text(encoding="utf-8"))
    input_lens = raw.get("input_lens") or raw.get("input_lengths")
    output_lens = raw.get("output_lens") or raw.get("output_lengths")
    if not all(isinstance(values, list) for values in (input_lens, output_lens)):
        raise ValueError(
            "unsupported vLLM detailed result: expected input_lens, output_lens arrays"
        )
    if not (len(requests) == len(input_lens) == len(output_lens)):
        raise ValueError("vLLM detailed result length does not match manifest shard")
    submitted_at, latencies = extract_vllm_bench_request_timings(
        raw,
        len(requests),
    )
    ttfts, itls = extract_vllm_bench_latency_distribution(raw, len(requests))
    latency_distribution = summarize_vllm_bench_latency_distribution(ttfts, itls)
    generated_texts = raw.get("generated_texts")
    if not isinstance(generated_texts, list) or len(generated_texts) != len(requests):
        generated_texts = [None] * len(requests)
    results = tuple(
        BaselineRequestResult(
            doc_id=request.doc_id,
            endpoint_index=request.endpoint_index,
            status="completed",
            error=None,
            submitted_at_s=float(submitted),
            started_at_s=float(submitted),
            completed_at_s=float(submitted + latency),
            input_tokens=int(input_length),
            output_tokens=int(output_length),
            output_text=(str(generated_text) if generated_text is not None else None),
            finish_reason=None,
        )
        for request, input_length, output_length, submitted, latency, generated_text in zip(
            requests,
            input_lens,
            output_lens,
            submitted_at,
            latencies,
            generated_texts,
        )
    )
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        raise FileExistsError(f"output directory already exists: {output_dir}")
    output_dir.mkdir(parents=True)
    summary = {
        **summarize_results(requests, results),
        **latency_distribution,
        "adapter": "vllm_bench",
        **_observability_fields("vllm_bench"),
        "status": "completed",
        "endpoint_index": args.endpoint_index,
        "endpoint_url": args.endpoint_url,
        "predicted_work": sum(request.estimated_work for request in requests),
        "model_name": args.model,
        "completion_protocol": "chat_completions",
        "service_prefix_caching": args.service_prefix_caching,
        "service_max_num_seqs": args.service_max_num_seqs,
        "service_max_num_batched_tokens": args.service_max_num_batched_tokens,
        "service_config_sha256": _service_fingerprint(
            args,
            "chat_completions",
        ),
        "vllm_num_requests_running_final": args.vllm_running_final,
        "vllm_num_requests_waiting_final": args.vllm_waiting_final,
        "worker_failures": 0,
    }
    _write_results(output_dir / "requests.csv", results)
    _atomic_json(output_dir / "summary.json", summary)
    return summary


def _validate_gate_command(
    args: argparse.Namespace,
) -> dict[str, object]:
    report = validate_gate(
        manifest=read_manifest(args.manifest),
        summaries=tuple(
            json.loads(Path(path).read_text(encoding="utf-8")) for path in args.summary
        ),
        request_results=tuple(
            result for path in args.request_results for result in _read_result_csv(path)
        ),
    )
    payload = {
        "status": "passed" if report.passed else "failed",
        "passed": report.passed,
        "incidents": list(report.incidents),
        "metrics": report.metrics,
    }
    if args.output:
        _atomic_json(Path(args.output), payload)
    return payload


def _add_partition_policy_args(subparser: argparse.ArgumentParser) -> None:
    """Add the manifest partition-policy flags to an export subparser."""

    subparser.add_argument(
        "--partition-policy",
        choices=PARTITION_POLICIES,
        default="preexecution_token_work_balanced",
        help=(
            "equal_rows = stable SHA256 sort + round-robin (strict 128:128 at 256/2);"
            " preexecution_token_work_balanced = largest-work-first on pre-submission"
            " estimated_work (prompt_tokens + estimated_output_tokens). NOT an oracle."
        ),
    )
    subparser.add_argument(
        "--partition-seed",
        type=int,
        default=0,
        help="fixed seed for the equal_rows SHA256 sort (ignored by work-balanced)",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run same-condition text service ceilings, controls, and "
            "vendor-native AI operator baselines."
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)

    export = commands.add_parser("export-manifest")
    export.add_argument("--input", required=True)
    export.add_argument("--output", required=True)
    export.add_argument("--endpoint-count", type=int, required=True)
    _add_partition_policy_args(export)

    postgres_export = commands.add_parser("export-postgres-manifest")
    postgres_export.add_argument("--database-url", required=True)
    postgres_export.add_argument("--workload-name", required=True)
    postgres_export.add_argument("--row-count", type=int, required=True)
    postgres_export.add_argument("--row-offset", type=int, default=0)
    postgres_export.add_argument(
        "--max-output-tokens",
        type=int,
        required=True,
    )
    postgres_export.add_argument(
        "--estimated-output-mode",
        choices=("fixed_cap", "trace_target"),
        required=True,
    )
    postgres_export.add_argument(
        "--endpoint-count",
        type=int,
        required=True,
    )
    postgres_export.add_argument("--output", required=True)
    _add_partition_policy_args(postgres_export)

    run = commands.add_parser("run-shard")
    run.add_argument("--adapter", choices=ADAPTERS, required=True)
    run.add_argument("--manifest", required=True)
    run.add_argument("--endpoint-index", type=int, required=True)
    run.add_argument("--endpoint-url", required=True)
    run.add_argument("--model", required=True)
    run.add_argument("--tokenizer")
    run.add_argument("--concurrency", type=int, default=1)
    run.add_argument("--batch-size", type=int, default=1)
    run.add_argument("--timeout-s", type=float, default=120.0)
    run.add_argument("--api-key")
    run.add_argument("--ray-address")
    run.add_argument("--arrival-time-scale", type=float, default=1.0)
    run.add_argument("--disable-arrival-replay", action="store_true")
    run.add_argument(
        "--ignore-eos",
        action="store_true",
        help="Require vLLM-compatible Chat requests to consume max_tokens.",
    )
    run.add_argument("--python-executable", default="python")
    run.add_argument("--output-dir", required=True)
    run.add_argument("--vllm-running-final", type=int, default=-1)
    run.add_argument("--vllm-waiting-final", type=int, default=-1)
    run.add_argument(
        "--service-prefix-caching",
        choices=("enabled", "disabled", "unknown"),
        default="unknown",
    )
    run.add_argument("--service-max-num-seqs", type=int, default=-1)
    run.add_argument("--service-max-num-batched-tokens", type=int, default=-1)
    run.add_argument("--duckdb-database", default=":memory:")
    run.add_argument("--duckdb-max-concurrent-requests", type=int, default=32)
    run.add_argument("--duckdb-response-cache", action="store_true")
    run.add_argument("--duckdb-prompt-cache-hints", action="store_true")
    run.add_argument("--duckdb-retry-count", type=int, default=0)
    run.add_argument("--duckdb-retry-backoff-ms", type=int, default=0)
    run.add_argument("--duckdb-min-request-interval-ms", type=int, default=0)
    run.add_argument("--duckdb-timeout-seconds", type=int, default=120)
    run.add_argument("--oceanbase-host", default="127.0.0.1")
    run.add_argument("--oceanbase-port", type=int, default=2881)
    run.add_argument("--oceanbase-user")
    run.add_argument("--oceanbase-password", default="")
    run.add_argument("--oceanbase-database")
    run.add_argument("--oceanbase-model-key")
    run.add_argument(
        "--oceanbase-parallel-degree",
        type=int,
        default=1,
    )
    run.add_argument(
        "--oceanbase-register-model",
        action="store_true",
    )
    run.add_argument("--dry-run", action="store_true")

    jobs = commands.add_parser("run-jobs-control")
    jobs.add_argument("--experiment-id", required=True)
    jobs.add_argument(
        "--phase",
        choices=("smoke", "warmup", "formal"),
        required=True,
    )
    jobs.add_argument("--repeat-index", type=int, required=True)
    jobs.add_argument(
        "--job",
        action="append",
        type=_parse_timed_job,
        required=True,
        help="JOB_ID=/absolute/manifest.jsonl=OFFSET_S; repeat per Job",
    )
    jobs.add_argument("--endpoint-url", action="append", required=True)
    jobs.add_argument("--metrics-url", action="append", required=True)
    jobs.add_argument("--model", required=True)
    jobs.add_argument("--concurrency", type=int, required=True)
    jobs.add_argument("--timeout-s", type=float, default=300.0)
    jobs.add_argument("--idle-timeout-s", type=float, default=300.0)
    jobs.add_argument("--api-key")
    jobs.add_argument("--arrival-time-scale", type=float, default=1.0)
    jobs.add_argument("--request-slo-ms", type=float, default=0.0)
    jobs.add_argument("--ignore-eos", action="store_true")
    jobs.add_argument("--output-dir", required=True)

    normalize = commands.add_parser("normalize-vllm-bench")
    normalize.add_argument("--manifest", required=True)
    normalize.add_argument("--endpoint-index", type=int, required=True)
    normalize.add_argument("--endpoint-url", required=True)
    normalize.add_argument("--model", required=True)
    normalize.add_argument("--input", required=True)
    normalize.add_argument("--output-dir", required=True)
    normalize.add_argument("--vllm-running-final", type=int, required=True)
    normalize.add_argument("--vllm-waiting-final", type=int, required=True)
    normalize.add_argument(
        "--service-prefix-caching",
        choices=("enabled", "disabled", "unknown"),
        default="unknown",
    )
    normalize.add_argument("--service-max-num-seqs", type=int, default=-1)
    normalize.add_argument(
        "--service-max-num-batched-tokens",
        type=int,
        default=-1,
    )

    gate = commands.add_parser("validate-gate")
    gate.add_argument("--manifest", required=True)
    gate.add_argument("--summary", action="append", required=True)
    gate.add_argument(
        "--request-results",
        action="append",
        required=True,
    )
    gate.add_argument("--output")
    return parser


def run_cli(argv: Sequence[str]) -> dict[str, object]:
    args = _parser().parse_args(list(argv))
    if args.command == "export-manifest":
        return _export_manifest(args)
    if args.command == "export-postgres-manifest":
        return _export_postgres_manifest(args)
    if args.command == "run-shard":
        return _run_shard(args)
    if args.command == "run-jobs-control":
        return _run_jobs_control(args)
    if args.command == "normalize-vllm-bench":
        return _normalize_vllm_bench(args)
    if args.command == "validate-gate":
        return _validate_gate_command(args)
    raise AssertionError(f"unhandled command: {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = run_cli(sys.argv[1:] if argv is None else argv)
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("status") != "failed" else 2
