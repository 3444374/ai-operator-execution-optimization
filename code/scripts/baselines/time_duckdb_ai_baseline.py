"""Time the DuckDB-ai AI_COMPLETE baseline against the local Qwen vLLM endpoint.

Single verifiable goal: measure how long one DuckDB-ai ``ai_complete`` shard
takes at a chosen row count, with the same observation surface the project uses
for GPU-backed runs (vLLM Prometheus counters + nvidia-smi), so the formal
baseline duration can be estimated before committing GPU time.

This is a timing probe, not a formal result: one endpoint shard, no warmup
repeats, no gate validation. It reuses the immutable PostgreSQL workload
(``sharegpt_multiturn`` by default), the DuckDB-ai product adapter (native
extension scheduling — no project credit/router injected), and the shared
observability primitives from ``src.observability.metrics``.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.text.orchestration.postgres_manifest import (  # noqa: E402
    load_postgres_requests,
)
from src.baselines.text.products.duckdb_ai import (  # noqa: E402
    DuckDBAiConfig,
    run_duckdb_ai_complete,
)
from src.observability.metrics import (  # noqa: E402
    PeriodicSampler,
    gpu_metadata,
    resource_sample_stats,
    scrape_prometheus_metrics,
    vllm_metric_delta_stats,
)


def _endpoint_base_url(endpoint_url: str) -> str:
    suffix = "/chat/completions"
    if not endpoint_url.endswith(suffix):
        raise ValueError(
            "endpoint URL must end with /v1/chat/completions, got: " + endpoint_url
        )
    return endpoint_url[: -len(suffix)]


def _load_requests(args: argparse.Namespace) -> tuple[ChatRequest, ...]:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "timing script requires psycopg; run inside the driver venv"
        ) from exc
    with psycopg.connect(args.database_url) as connection:
        requests = load_postgres_requests(
            connection,
            workload_name=args.workload_name,
            row_count=args.row_count,
            row_offset=0,
            max_output_tokens=args.max_output_tokens,
            estimated_output_mode="fixed_cap",
        )
    # Single-shard timing: pin every row to endpoint 0.
    return tuple(
        dataclasses.replace(request, endpoint_index=0) for request in requests
    )


def _resource_snapshot(metrics_url: str, gpu_ids: tuple[str, ...]) -> dict:
    snapshot = {"sample_epoch_s": time.time()}
    snapshot.update(gpu_metadata(gpu_ids or None))
    snapshot.update(scrape_prometheus_metrics(metrics_url))
    return snapshot


def _run_timing(args: argparse.Namespace) -> dict:
    requests = _load_requests(args)
    config = DuckDBAiConfig(
        endpoint_base_url=_endpoint_base_url(args.endpoint_url),
        model=args.model,
        api_key=args.api_key,
        max_tokens=args.max_output_tokens,
        database_path=args.duckdb_database,
    )

    metrics_before = scrape_prometheus_metrics(args.metrics_url)
    sampler = PeriodicSampler(
        lambda: _resource_snapshot(args.metrics_url, tuple(args.gpu_ids)),
        interval_s=args.sample_interval_s,
    )
    run_started = time.time()
    try:
        results = run_duckdb_ai_complete(requests, config)
        run_error = None
    except Exception as exc:  # noqa: BLE001 - record and report, do not hide
        results = ()
        run_error = f"{type(exc).__name__}: {exc}"
    run_finished = time.time()
    sampler.close()
    metrics_after = scrape_prometheus_metrics(args.metrics_url)

    wall_s = run_finished - run_started
    completed = sum(1 for r in results if r.status == "completed" and not r.error)
    failed = len(results) - completed
    delta = vllm_metric_delta_stats(metrics_before, metrics_after)
    observed_tokens = (
        delta.get("vllm_prompt_tokens_delta", 0)
        + delta.get("vllm_generation_tokens_delta", 0)
    )
    resource_stats = resource_sample_stats(
        sampler.samples,
        observed_tokens=observed_tokens,
    )
    offered_rate = (len(requests) / wall_s) if wall_s > 0 else 0.0
    successful_rate = (completed / wall_s) if wall_s > 0 else 0.0
    summary = {
        "status": (
            "completed" if run_error is None and failed == 0 else "failed"
        ),
        "error": run_error,
        "adapter": "duckdb_ai",
        "workload_name": args.workload_name,
        "row_count": len(requests),
        "max_output_tokens": args.max_output_tokens,
        "endpoint_base_url": config.endpoint_base_url,
        "model": config.model,
        "wall_s": round(wall_s, 4),
        "offered_rows_per_s": round(offered_rate, 4),
        "successful_rows_per_s": round(successful_rate, 4),
        "completed_count": completed,
        "failed_count": failed,
        "exactly_once_ids_match": (
            {r.doc_id for r in results} == {req.doc_id for req in requests}
            if run_error is None
            else False
        ),
        "vllm_metrics_url": args.metrics_url,
        "vllm_metric_delta": delta,
        "resource_stats": resource_stats,
        "output_samples": [r.output_text for r in results[: args.show_samples]],
        "extrapolation": {
            "note": (
                "linear scale from one single-endpoint shard; not a measurement "
                "of the formal gate. Two endpoint shards run concurrently, so "
                "this estimate multiplies by repeat count, not endpoint count."
            ),
            "single_shard_wall_s": round(wall_s, 4),
            "estimated_two_endpoint_warmup_plus_3_formal_wall_s": round(
                wall_s * 4,
                4,
            ),
        },
    }
    return summary


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument(
        "--endpoint-url",
        required=True,
        help="vLLM OpenAI-compatible chat URL ending in /v1/chat/completions",
    )
    parser.add_argument("--metrics-url", required=True, help="vLLM /metrics URL")
    parser.add_argument("--model", required=True)
    parser.add_argument("--api-key", default="EMPTY")
    parser.add_argument("--workload-name", default="sharegpt_multiturn")
    parser.add_argument("--row-count", type=int, default=64)
    parser.add_argument("--max-output-tokens", type=int, default=256)
    parser.add_argument("--gpu-ids", nargs="*", default=[])
    parser.add_argument("--sample-interval-s", type=float, default=0.25)
    parser.add_argument(
        "--duckdb-database",
        default=":memory:",
        help="DuckDB connection target (default in-memory)",
    )
    parser.add_argument("--show-samples", type=int, default=3)
    parser.add_argument("--output", help="write JSON summary here")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse(sys.argv[1:] if argv is None else argv)
    summary = _run_timing(args)
    print(json.dumps(summary, indent=2, sort_keys=True))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return 0 if summary.get("status") == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
