#!/usr/bin/env python3
"""Run a shared-vLLM K_max interference experiment.

The goal is to test admission-control value, not single-job throughput. A large
background job starts first; a smaller foreground job starts shortly after and
measures how much foreground latency changes when the background job uses a
bounded versus unbounded in-flight submission window.
"""

from __future__ import annotations

import argparse
import random
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
    parser.add_argument(
        "--endpoint-urls",
        default=None,
        help="Comma-separated completion endpoint URLs (multi-GPU). Overrides --endpoint-url when set.",
    )
    parser.add_argument("--metrics-url", default="http://localhost:8000/metrics")
    parser.add_argument(
        "--metrics-urls",
        default=None,
        help="Comma-separated metrics URLs (multi-GPU). Overrides --metrics-url when set.",
    )
    parser.add_argument("--endpoint-gpu-ids", default=None)
    parser.add_argument(
        "--endpoint-routing",
        choices=["round_robin", "least_queued"],
        default="least_queued",
    )
    parser.add_argument(
        "--admission-scope",
        choices=["global", "per_endpoint"],
        default="per_endpoint",
        help=(
            "Static K semantics. per_endpoint is required for fair multi-GPU "
            "capacity; adaptive controllers remain global-only."
        ),
    )
    parser.add_argument("--max-active-work-per-endpoint", type=int, default=0)
    parser.add_argument("--shared-credit-coordinator-name", default="")
    parser.add_argument("--shared-credit-request-limit", type=int, default=0)
    parser.add_argument("--shared-credit-work-limit", type=int, default=0)
    parser.add_argument("--shared-credit-quantum", type=int, default=0)
    parser.add_argument("--background-job-weight", type=int, default=1)
    parser.add_argument("--foreground-job-weight", type=int, default=1)
    parser.add_argument("--model", default="qwen2.5-1.5b")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--random-seed", type=int, default=20260804)
    parser.add_argument("--ramp-up-s", type=float, default=1.5)
    parser.add_argument("--request-timeout-s", type=float, default=300.0)
    parser.add_argument("--foreground-rows", type=int, default=128)
    parser.add_argument("--background-rows", type=int, default=1024)
    parser.add_argument("--ray-batch-rows", type=int, default=16)
    parser.add_argument("--completion-max-tokens", type=int, default=64)
    parser.add_argument("--source-max-prompt-tokens", type=int, default=1500)
    parser.add_argument(
        "--batching-policy",
        choices=["fixed_rows", "token_budget"],
        default="token_budget",
    )
    parser.add_argument("--token-budget", type=int, default=6144)
    parser.add_argument("--arrival-time-scale", type=float, default=0.0005)
    parser.add_argument(
        "--flush-policy",
        choices=["fixed_timeout", "queue_adaptive"],
        default="fixed_timeout",
    )
    parser.add_argument("--flush-timeout-ms", type=float, default=50.0)
    parser.add_argument("--flush-max-wait-ms", type=float, default=50.0)
    parser.add_argument("--request-slo-ms", type=float, default=180000.0)
    parser.add_argument("--gpu-peak-tflops", type=float, default=61.7)
    parser.add_argument("--model-workers", type=int, default=2)
    parser.add_argument("--ray-worker-num-cpus", type=float, default=0.25)
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
    parser.add_argument("--include-aimd", action="store_true")
    parser.add_argument("--include-aimd-hol", action="store_true")
    parser.add_argument("--controller-min-window", type=int, default=4)
    parser.add_argument("--controller-max-window", type=int, default=16)
    parser.add_argument("--controller-initial-window", type=int, default=8)
    parser.add_argument(
        "--hol-age-congestion-s",
        type=float,
        default=2.0,
        help="aimd_hol multiplicative-decrease threshold on Ray-side head-of-line age (s).",
    )
    parser.add_argument(
        "--hol-age-low-load-s",
        type=float,
        default=0.5,
        help="aimd_hol additive-increase threshold on Ray-side head-of-line age (s).",
    )
    parser.add_argument(
        "--trace-dir",
        default=None,
        help="Optional directory for per-process request, submission, resource, flush, and control traces.",
    )
    parser.add_argument("--small-output", default=str(RESULT_DIR / "sharegpt_burstgpt_kmax_interference_small_20260726.csv"))
    parser.add_argument("--bulk-output", default=str(RESULT_DIR / "sharegpt_burstgpt_kmax_interference_bulk_20260726.csv"))
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def profile_command(args: argparse.Namespace, *, experiment_id: str, total_rows: int, ray_batch_rows: int,
                    max_inflight: int, output: str, completion_max_tokens: int,
                    scheduling_policy: str = "static",
                    shared_credit_job_weight: int | None = None) -> list[str]:
    endpoint_url_arg = _split_urls(
        args.endpoint_urls or args.endpoint_url,
        "endpoint URLs",
    )
    metrics_url_arg = _split_urls(
        args.metrics_urls or args.metrics_url,
        "metrics URLs",
    )
    if len(metrics_url_arg) != len(endpoint_url_arg):
        raise ValueError(
            "metrics URL count must equal completion endpoint URL count"
        )
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
        args.batching_policy,
        "--operator",
        "ai_complete",
        "--executor",
        "ray_task",
        "--model-backend",
        "compatible_http",
        "--completion-endpoint-urls",
        ",".join(endpoint_url_arg),
        "--completion-model",
        args.model,
        "--completion-max-tokens",
        str(completion_max_tokens),
        "--completion-return-token-ids",
        "--completion-prompt-format",
        "chatml",
        "--completion-temperature",
        "0",
        "--completion-request-timeout-s",
        str(args.request_timeout_s),
        "--model-metrics-urls",
        ",".join(metrics_url_arg),
        "--source-max-prompt-tokens",
        str(args.source_max_prompt_tokens),
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
        "--admission-scope",
        args.admission_scope,
        "--max-active-work-per-endpoint",
        str(args.max_active_work_per_endpoint),
        "--model-workers",
        str(args.model_workers),
        "--ray-worker-num-cpus",
        str(args.ray_worker_num_cpus),
        "--arrival-replay",
        "--arrival-time-scale",
        str(args.arrival_time_scale),
        "--flush-policy",
        args.flush_policy,
        "--flush-timeout-ms",
        str(args.flush_timeout_ms),
        "--flush-max-wait-ms",
        str(args.flush_max_wait_ms),
        "--request-slo-ms",
        str(args.request_slo_ms),
        "--resource-sample-interval-s",
        "0.25",
        "--gpu-peak-tflops",
        str(args.gpu_peak_tflops),
        "--mfu-precision",
        "bf16_dense_fp32_accumulate",
        "--scheduling-policy",
        scheduling_policy,
        "--random-seed",
        str(args.random_seed),
        "--scenario-id",
        experiment_id,
        "--experiment-id",
        experiment_id,
        "--output",
        output,
    ]
    if len(endpoint_url_arg) > 1:
        command.extend(
            [
                "--endpoint-routing",
                args.endpoint_routing,
                "--endpoint-gpu-ids",
                args.endpoint_gpu_ids or ",".join(str(i) for i in range(len(endpoint_url_arg))),
            ]
        )
    if args.shared_credit_coordinator_name:
        command.extend(
            [
                "--shared-credit-coordinator-name",
                args.shared_credit_coordinator_name,
                "--shared-credit-request-limit",
                str(args.shared_credit_request_limit),
                "--shared-credit-work-limit",
                str(args.shared_credit_work_limit),
                "--shared-credit-quantum",
                str(args.shared_credit_quantum),
                "--shared-credit-job-weight",
                str(
                    shared_credit_job_weight
                    if shared_credit_job_weight is not None
                    else 1
                ),
            ]
        )
    if args.batching_policy == "token_budget":
        command.extend(
            [
                "--token-budget",
                str(args.token_budget),
                "--cost-model-id",
                args.model,
                "--cost-tokenizer-id",
                args.model,
                "--output-cost-mode",
                "fixed_output_cap",
            ]
        )
    if scheduling_policy == "queue_adaptive":
        if args.admission_scope != "global":
            raise ValueError(
                "queue_adaptive requires --admission-scope global; use static "
                "per-endpoint controls for the multi-GPU fairness experiment"
            )
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
    elif scheduling_policy in {"aimd", "aimd_hol"}:
        if args.admission_scope != "global":
            raise ValueError(
                f"{scheduling_policy} requires --admission-scope global"
            )
        command.extend(
            [
                "--controller-min-window",
                str(args.controller_min_window),
                "--controller-max-window",
                str(args.controller_max_window),
                "--controller-initial-window",
                str(args.controller_initial_window),
            ]
        )
        if scheduling_policy == "aimd_hol":
            command.extend(
                [
                    "--hol-age-congestion-s",
                    str(args.hol_age_congestion_s),
                    "--hol-age-low-load-s",
                    str(args.hol_age_low_load_s),
                ]
            )
    if args.trace_dir:
        trace_stem = Path(args.trace_dir) / experiment_id
        command.extend(
            [
                "--request-trace-output",
                str(trace_stem.with_suffix(".requests.csv")),
                "--submission-trace-output",
                str(trace_stem.with_suffix(".submissions.csv")),
                "--resource-trace-output",
                str(trace_stem.with_suffix(".resources.csv")),
                "--flush-trace-output",
                str(trace_stem.with_suffix(".flush.csv")),
            ]
        )
        if scheduling_policy in {"aimd", "aimd_hol"}:
            command.extend(
                [
                    "--control-trace-output",
                    str(trace_stem.with_suffix(".control.csv")),
                ]
            )
    return command


def _split_urls(text: str, label: str) -> list[str]:
    values = [value.strip() for value in text.split(",") if value.strip()]
    if not values:
        raise ValueError(f"{label} must contain at least one non-empty URL")
    return values


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
            if value <= 0:
                raise ValueError("K_max values must be positive")
            values.append((value, f"bulk_k{value}"))
    return values


def run_checked(cmd: list[str]) -> None:
    completed = subprocess.run(cmd, cwd=ROOT)
    if completed.returncode != 0:
        raise SystemExit(f"Command failed with exit code {completed.returncode}: {' '.join(cmd)}")


def build_scenarios(args: argparse.Namespace) -> list[tuple[int, str, str]]:
    scenarios = [(bulk_k, label, "static") for bulk_k, label in parse_kmax_values(args.background_static_kmax)]
    if args.include_adaptive:
        scenarios.append((args.adaptive_max_inflight, "bulk_adaptive", "queue_adaptive"))
    if args.include_aimd:
        scenarios.append((args.controller_max_window, "bulk_aimd", "aimd"))
    if args.include_aimd_hol:
        scenarios.append((args.controller_max_window, "bulk_aimd_hol", "aimd_hol"))
    if not scenarios:
        raise ValueError("at least one interference scenario is required")
    return scenarios


def scenarios_for_repeat(args: argparse.Namespace, repeat: int) -> list[tuple[int, str, str]]:
    scenarios = build_scenarios(args)
    random.Random(args.random_seed + repeat).shuffle(scenarios)
    return scenarios


def main() -> None:
    args = parse_args()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    Path(args.small_output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.bulk_output).parent.mkdir(parents=True, exist_ok=True)
    if args.trace_dir:
        Path(args.trace_dir).mkdir(parents=True, exist_ok=True)
    if args.overwrite:
        for path in [Path(args.small_output), Path(args.bulk_output)]:
            if path.exists():
                path.unlink()

    # Each repeat starts with its unloaded foreground baseline, then runs one
    # deterministically shuffled copy of every shared-service scenario.
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
                shared_credit_job_weight=args.foreground_job_weight,
            )
        )
        for bulk_k, label, scheduling_policy in scenarios_for_repeat(args, repeat):
            bulk_cmd = profile_command(
                args,
                experiment_id=f"interference_{label}_background_r{repeat}",
                total_rows=args.background_rows,
                ray_batch_rows=args.ray_batch_rows,
                max_inflight=bulk_k,
                output=args.bulk_output,
                completion_max_tokens=args.completion_max_tokens,
                scheduling_policy=scheduling_policy,
                shared_credit_job_weight=args.background_job_weight,
            )
            small_cmd = profile_command(
                args,
                experiment_id=f"interference_small_during_{label}_r{repeat}",
                total_rows=args.foreground_rows,
                ray_batch_rows=args.ray_batch_rows,
                max_inflight=min(8, args.adaptive_max_inflight),
                output=args.small_output,
                completion_max_tokens=args.completion_max_tokens,
                shared_credit_job_weight=args.foreground_job_weight,
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
