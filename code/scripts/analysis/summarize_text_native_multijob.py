#!/usr/bin/env python3
"""Fail-closed aggregation for the opening native staggered two-job matrix."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any


EXPECTED_ARMS = {"daft_native", "daft_ray", "ray_data_http"}
EXPECTED_JOBS = {"short", "long"}


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty CSV: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _finite(value: object, name: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    return parsed


def _service_tokens(matrix_root: Path, run: dict[str, Any]) -> tuple[int, int, int]:
    path = matrix_root / "runs" / str(run["run_id"]) / "service_counters.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    delta = payload.get("delta")
    if not isinstance(delta, dict) or not delta:
        raise ValueError(f"{run['run_id']} has no service counter delta")
    prompt = sum(int(item["prompt_tokens"]) for item in delta.values())
    generation = sum(int(item["generation_tokens"]) for item in delta.values())
    if prompt <= 0 or generation <= 0:
        raise ValueError(f"{run['run_id']} has non-positive service token delta")
    return prompt, generation, prompt + generation


def _job_map(run: dict[str, Any]) -> dict[str, dict[str, Any]]:
    jobs = run.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError(f"{run.get('run_id')} jobs must be a list")
    mapped = {str(job.get("job_id")): job for job in jobs}
    if set(mapped) != EXPECTED_JOBS:
        raise ValueError(f"{run.get('run_id')} job set is {sorted(mapped)}")
    return mapped


def _run_row(
    matrix_root: Path,
    run: dict[str, Any],
    *,
    gpu_peak_tflops: float,
) -> dict[str, Any]:
    if run.get("status") != "passed" or run.get("exactly_once") is not True:
        raise ValueError(f"{run.get('run_id')} failed status/exactly-once gate")
    if run.get("phase") == "formal" and run.get("comparison_eligible") is not True:
        raise ValueError(f"{run.get('run_id')} is not comparison eligible")
    duration = _finite(run["arm_barrier_jct_s"], "arm_barrier_jct_s")
    if duration <= 0:
        raise ValueError(f"{run.get('run_id')} duration must be positive")
    jobs = _job_map(run)
    short = jobs["short"]
    long = jobs["long"]
    for job in jobs.values():
        if job.get("status") != "passed" or job.get("exactly_once") is not True:
            raise ValueError(f"{run.get('run_id')}/{job.get('job_id')} failed")
        if int(job.get("completed_count", -1)) != 512:
            raise ValueError(f"{run.get('run_id')}/{job.get('job_id')} is not 512 rows")
    prompt, generation, total = _service_tokens(matrix_root, run)

    latency = run.get("vllm_latency_deltas")
    if not isinstance(latency, dict) or not latency:
        raise ValueError(f"{run.get('run_id')} lacks vLLM latency deltas")
    flops = sum(
        _finite(item["vllm_estimated_flops_per_gpu_delta"], "estimated flops")
        for item in latency.values()
    )
    successes = sum(int(item["vllm_request_success_delta"]) for item in latency.values())
    if successes != 1024:
        raise ValueError(f"{run.get('run_id')} service success delta is {successes}, not 1024")
    gpu_count = len(latency)
    mfu = flops / (duration * gpu_count * gpu_peak_tflops * 1e12)

    gauge = run.get("gauge_summary")
    gpu = run.get("gpu_summary")
    if not isinstance(gauge, dict) or not isinstance(gpu, dict):
        raise ValueError(f"{run.get('run_id')} lacks resource summaries")
    util_values = [
        _finite(value, key)
        for key, value in gpu.items()
        if key.startswith("gpu") and key.endswith("_util_mean")
    ]
    power_values = [
        _finite(value, key)
        for key, value in gpu.items()
        if key.startswith("gpu") and key.endswith("_power_mean")
    ]
    if len(util_values) != gpu_count or len(power_values) != gpu_count:
        raise ValueError(f"{run.get('run_id')} GPU summary count mismatch")

    short_start = _finite(short["actual_launch_epoch_s"], "short start")
    short_end = _finite(short["ended_epoch_s"], "short end")
    long_start = _finite(long["actual_launch_epoch_s"], "long start")
    long_end = _finite(long["ended_epoch_s"], "long end")
    overlap = max(0.0, min(short_end, long_end) - max(short_start, long_start))
    return {
        "run_id": run["run_id"],
        "phase": run["phase"],
        "repeat": int(run["repeat"]),
        "arm_id": run["arm_id"],
        "adapter": run["adapter"],
        "arm_barrier_jct_s": duration,
        "service_prompt_tokens": prompt,
        "service_generation_tokens": generation,
        "service_total_tokens": total,
        "service_tokens_per_s": total / duration,
        "service_token_source": "vllm_counter_delta",
        "short_jct_s": _finite(short["job_barrier_jct_s"], "short JCT"),
        "long_jct_s": _finite(long["job_barrier_jct_s"], "long JCT"),
        "short_end_relative_s": short_end - short_start,
        "long_start_relative_s": long_start - short_start,
        "long_end_relative_s": long_end - short_start,
        "job_overlap_s": overlap,
        "short_manifest_sha256": short["manifest_sha256"],
        "long_manifest_sha256": long["manifest_sha256"],
        "gpu_utilization_pct_mean": statistics.fmean(util_values),
        "gpu_energy_j": sum(power_values) * duration,
        "mfu_fraction": mfu,
        "vllm_running_mean": _finite(gauge["vllm_running_mean"], "running mean"),
        "vllm_running_max": _finite(gauge["vllm_running_max"], "running max"),
        "vllm_waiting_mean": _finite(gauge["vllm_waiting_mean"], "waiting mean"),
        "vllm_waiting_max": _finite(gauge["vllm_waiting_max"], "waiting max"),
        "vllm_kv_cache_usage_mean": _finite(
            gauge["vllm_kv_cache_usage_mean"], "KV mean"
        ),
        "vllm_kv_cache_usage_max": _finite(
            gauge["vllm_kv_cache_usage_max"], "KV max"
        ),
        "request_success_delta": successes,
    }


def _mean(values: list[float]) -> float:
    return statistics.fmean(values)


def _summary(arm: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    def values(key: str) -> list[float]:
        return [float(row[key]) for row in rows]

    tps = values("service_tokens_per_s")
    return {
        "arm_id": arm,
        "formal_repeats": len(rows),
        "service_tokens_per_s_mean": _mean(tps),
        "service_tokens_per_s_sd": statistics.stdev(tps),
        "service_tokens_per_s_cv": statistics.stdev(tps) / _mean(tps),
        "arm_barrier_jct_s_mean": _mean(values("arm_barrier_jct_s")),
        "short_jct_s_mean": _mean(values("short_jct_s")),
        "long_jct_s_mean": _mean(values("long_jct_s")),
        "job_overlap_s_mean": _mean(values("job_overlap_s")),
        "gpu_utilization_pct_mean": _mean(values("gpu_utilization_pct_mean")),
        "mfu_fraction_mean": _mean(values("mfu_fraction")),
        "gpu_energy_j_mean": _mean(values("gpu_energy_j")),
        "vllm_running_mean": _mean(values("vllm_running_mean")),
        "vllm_running_max": max(values("vllm_running_max")),
        "vllm_waiting_mean": _mean(values("vllm_waiting_mean")),
        "vllm_waiting_max": max(values("vllm_waiting_max")),
        "vllm_kv_cache_usage_mean": _mean(values("vllm_kv_cache_usage_mean")),
        "vllm_kv_cache_usage_max": max(values("vllm_kv_cache_usage_max")),
        "service_tokens_per_s_repeats": json.dumps(tps),
        "short_jct_s_repeats": json.dumps(values("short_jct_s")),
        "long_jct_s_repeats": json.dumps(values("long_jct_s")),
    }


def summarize(matrix_root: Path, output: Path, *, gpu_peak_tflops: float) -> bool:
    if not math.isfinite(gpu_peak_tflops) or gpu_peak_tflops <= 0:
        raise ValueError("gpu_peak_tflops must be finite and positive")
    index = json.loads((matrix_root / "matrix_index.json").read_text(encoding="utf-8"))
    errors: list[str] = []
    if index.get("status") != "passed":
        errors.append(f"matrix status is {index.get('status')!r}")
    if index.get("comparison_admission") != "admissible":
        errors.append(f"comparison admission is {index.get('comparison_admission')!r}")
    configured = {str(item.get("id")) for item in index.get("arms", [])}
    if configured != EXPECTED_ARMS:
        errors.append(f"arm set is {sorted(configured)}")

    rows: list[dict[str, Any]] = []
    for run in index.get("runs", []):
        try:
            rows.append(_run_row(matrix_root, run, gpu_peak_tflops=gpu_peak_tflops))
        except (KeyError, TypeError, ValueError, FileNotFoundError, json.JSONDecodeError) as exc:
            errors.append(f"{run.get('run_id', 'unknown')}: {exc}")
    formal = [row for row in rows if row["phase"] == "formal"]
    warmup = [row for row in rows if row["phase"] == "warmup"]
    for arm in EXPECTED_ARMS:
        if sum(row["arm_id"] == arm for row in warmup) != 1:
            errors.append(f"{arm} does not have exactly one warm-up")
        if sum(row["arm_id"] == arm for row in formal) != 3:
            errors.append(f"{arm} does not have exactly three formal repeats")
    manifest_pairs = {
        (str(row["short_manifest_sha256"]), str(row["long_manifest_sha256"]))
        for row in rows
    }
    if len(manifest_pairs) != 1:
        errors.append("short/long manifest identity changed across runs")

    output.mkdir(parents=True, exist_ok=True)
    audit = {
        "status": "passed" if not errors else "failed",
        "matrix_root": str(matrix_root.resolve()),
        "repository_commit": index.get("repository_commit"),
        "gpu_peak_tflops_per_gpu": gpu_peak_tflops,
        "mfu_formula": "sum(vllm_estimated_flops_per_gpu_delta)/(arm_barrier_jct_s*gpu_count*gpu_peak_tflops*1e12)",
        "throughput_source": "vLLM prompt+generation service-counter delta divided by arm barrier JCT",
        "excluded_metric": "matrix_index.group_barrier_tokens_per_s uses adapter request files whose output-token coverage differs; it is not rankable",
        "request_tail_latency": "not_collected_by_native_multijob_adapter; do not infer request P95/P99 from barrier JCT",
        "observed_runs": len(rows),
        "observed_formal_runs": len(formal),
        "errors": errors,
    }
    (output / "audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    if rows:
        _write_csv(output / "all_runs.csv", rows)
    if formal and not errors:
        summaries = [
            _summary(arm, [row for row in formal if row["arm_id"] == arm])
            for arm in sorted(EXPECTED_ARMS)
        ]
        _write_csv(output / "formal_summary.csv", summaries)
    return not errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--gpu-peak-tflops", type=float, default=165.0)
    args = parser.parse_args()
    try:
        passed = summarize(
            args.matrix_root, args.output, gpu_peak_tflops=args.gpu_peak_tflops
        )
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
