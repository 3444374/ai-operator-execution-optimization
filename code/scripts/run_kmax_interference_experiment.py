#!/usr/bin/env python3
"""Run a shared-vLLM K_max interference experiment.

The goal is to test admission-control value, not single-job throughput. A large
background job starts first; a smaller foreground job starts shortly after and
measures how much foreground latency changes when the background job uses a
bounded versus unbounded in-flight submission window.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PROFILE_SCRIPT = ROOT / "code" / "scripts" / "postgres_ai_operator_profile.py"
RESULT_DIR = ROOT / "experiments" / "results" / "local_vllm_qwen15b_baseline"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run K_max shared-service interference experiment.")
    parser.add_argument("--database-url", default="postgresql://postgres:postgres@localhost:5432/ai_operator")
    parser.add_argument("--endpoint-url", default="http://localhost:8000/v1/completions")
    parser.add_argument("--metrics-url", default="http://localhost:8000/metrics")
    parser.add_argument("--model", default="qwen2.5-1.5b")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--ramp-up-s", type=float, default=1.5)
    parser.add_argument("--request-timeout-s", type=float, default=300.0)
    parser.add_argument("--foreground-rows", type=int, default=128)
    parser.add_argument("--background-rows", type=int, default=1024)
    parser.add_argument("--ray-batch-rows", type=int, default=16)
    parser.add_argument("--completion-max-tokens", type=int, default=64)
    parser.add_argument(
        "--background-static-kmax",
        default="8,16,unbounded",
        help="Comma-separated static background K_max values; use 'unbounded' for 100000.",
    )
    parser.add_argument("--include-adaptive", action="store_true")
    parser.add_argument("--adaptive-min-inflight", type=int, default=8)
    parser.add_argument("--adaptive-max-inflight", type=int, default=64)
    parser.add_argument("--adaptive-running-threshold", type=int, default=160)
    parser.add_argument("--adaptive-queue-threshold", type=int, default=0)
    parser.add_argument("--adaptive-kv-threshold", type=float, default=0.85)
    parser.add_argument("--small-output", default=str(RESULT_DIR / "sharegpt_burstgpt_kmax_interference_small_20260719.csv"))
    parser.add_argument("--bulk-output", default=str(RESULT_DIR / "sharegpt_burstgpt_kmax_interference_bulk_20260719.csv"))
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def profile_command(args: argparse.Namespace, *, experiment_id: str, total_rows: int, ray_batch_rows: int,
                    max_inflight: int, output: str, completion_max_tokens: int,
                    scheduling_policy: str = "static") -> list[str]:
    command = [
        sys.executable,
        str(PROFILE_SCRIPT),
        "--database-url",
        args.database_url,
        "--setup",
        "--total-rows",
        str(total_rows),
        "--db-fetch-rows",
        str(total_rows),
        "--ray-batch-rows",
        str(ray_batch_rows),
        "--batching-policy",
        "fixed_rows",
        "--operator",
        "ai_complete",
        "--executor",
        "ray_task",
        "--model-backend",
        "compatible_http",
        "--completion-endpoint-url",
        args.endpoint_url,
        "--completion-model",
        args.model,
        "--completion-max-tokens",
        str(completion_max_tokens),
        "--completion-request-timeout-s",
        str(args.request_timeout_s),
        "--model-metrics-url",
        args.metrics_url,
        "--data-source",
        "daft_postgres",
        "--source-workload-name",
        "sharegpt_burstgpt",
        "--source-order",
        "arrival_time",
        "--organizer",
        "daft",
        "--writeback-mode",
        "none",
        "--warmup-runs",
        "0",
        "--repeats",
        "1",
        "--max-inflight",
        str(max_inflight),
        "--scheduling-policy",
        scheduling_policy,
        "--experiment-id",
        experiment_id,
        "--output",
        output,
    ]
    if scheduling_policy == "queue_adaptive":
        command.extend(
            [
                "--adaptive-min-inflight",
                str(args.adaptive_min_inflight),
                "--adaptive-max-inflight",
                str(args.adaptive_max_inflight),
                "--adaptive-running-threshold",
                str(args.adaptive_running_threshold),
                "--adaptive-queue-threshold",
                str(args.adaptive_queue_threshold),
                "--adaptive-kv-threshold",
                str(args.adaptive_kv_threshold),
            ]
        )
    return command


def parse_kmax_values(text: str) -> list[tuple[int, str]]:
    values = []
    for item in text.split(","):
        cleaned = item.strip().lower()
        if not cleaned:
            continue
        if cleaned in {"inf", "infinite", "unbounded"}:
            values.append((100000, "bulk_unbounded"))
        else:
            value = int(cleaned)
            values.append((value, f"bulk_k{value}"))
    return values


def run_checked(cmd: list[str]) -> None:
    completed = subprocess.run(cmd, cwd=ROOT)
    if completed.returncode != 0:
        raise SystemExit(f"Command failed with exit code {completed.returncode}: {' '.join(cmd)}")


def main() -> None:
    args = parse_args()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    if args.overwrite:
        for path in [Path(args.small_output), Path(args.bulk_output)]:
            if path.exists():
                path.unlink()

    # Foreground-only baseline: tells us the small job's unloaded latency.
    for repeat in range(1, args.repeats + 1):
        run_checked(
            profile_command(
                args,
                experiment_id=f"interference_small_solo_r{repeat}",
                total_rows=args.foreground_rows,
                ray_batch_rows=args.ray_batch_rows,
                max_inflight=min(8, args.adaptive_max_inflight),
                output=args.small_output,
                completion_max_tokens=args.completion_max_tokens,
            )
        )

    scenarios = [(bulk_k, label, "static") for bulk_k, label in parse_kmax_values(args.background_static_kmax)]
    if args.include_adaptive:
        scenarios.append((args.adaptive_max_inflight, "bulk_adaptive", "queue_adaptive"))

    for bulk_k, label, scheduling_policy in scenarios:
        for repeat in range(1, args.repeats + 1):
            bulk_cmd = profile_command(
                args,
                experiment_id=f"interference_{label}_background_r{repeat}",
                total_rows=args.background_rows,
                ray_batch_rows=args.ray_batch_rows,
                max_inflight=bulk_k,
                output=args.bulk_output,
                completion_max_tokens=args.completion_max_tokens,
                scheduling_policy=scheduling_policy,
            )
            small_cmd = profile_command(
                args,
                experiment_id=f"interference_small_during_{label}_r{repeat}",
                total_rows=args.foreground_rows,
                ray_batch_rows=args.ray_batch_rows,
                max_inflight=min(8, args.adaptive_max_inflight),
                output=args.small_output,
                completion_max_tokens=args.completion_max_tokens,
            )
            bulk = subprocess.Popen(bulk_cmd, cwd=ROOT)
            time.sleep(args.ramp_up_s)
            try:
                run_checked(small_cmd)
                return_code = bulk.wait(timeout=args.request_timeout_s + 60)
            except Exception:
                bulk.terminate()
                try:
                    bulk.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    bulk.kill()
                raise
            if return_code != 0:
                raise SystemExit(f"Background job failed with exit code {return_code}: {' '.join(bulk_cmd)}")


if __name__ == "__main__":
    main()
