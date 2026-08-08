#!/usr/bin/env python3
"""Audit and summarize the frozen opening database-E2E text matrix.

Input is the matrix output root produced by ``opening_database_e2e_matrix.py``.
The script validates the 2 workloads × 3 arms × (1 warm-up + 3 formal)
contract, flattens every cell report, and writes formal-run and aggregate CSVs
plus a machine-readable audit. It never drops failed or semantically degraded
rows; such outcomes remain explicit columns in the output.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


EXPECTED_ARMS = (
    "direct_static_sharded",
    "duckdb_ai_static_sharded",
    "project_frozen_static",
)
EXPECTED_WORKLOADS = ("squad_uniform", "sharegpt_controlled_skew")
GPU_PEAK_TFLOPS_PER_4090_BF16 = 165.0


def _float(value: Any, default: float = math.nan) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _first_csv_row(path: Path) -> dict[str, str]:
    with path.open(newline="", encoding="utf-8") as handle:
        return next(csv.DictReader(handle))


def _skew(values: dict[str, Any]) -> float:
    nums = [_float(value) for value in values.values()]
    nums = [value for value in nums if math.isfinite(value)]
    if len(nums) < 2 or max(nums) <= 0:
        return 0.0
    return (max(nums) - min(nums)) / max(nums)


def _weighted_prefix_hit_rate(endpoint_deltas: dict[str, Any]) -> float:
    hits = 0.0
    queries = 0.0
    for endpoint in endpoint_deltas.values():
        hits += _float(endpoint.get("vllm_prefix_cache_hits_delta"), 0.0)
        queries += _float(endpoint.get("vllm_prefix_cache_queries_delta"), 0.0)
    return hits / queries if queries > 0 else 0.0


def _max_endpoint_metric(endpoint_deltas: dict[str, Any], name: str) -> float:
    values = [_float(endpoint.get(name)) for endpoint in endpoint_deltas.values()]
    values = [value for value in values if math.isfinite(value)]
    return max(values) if values else math.nan


def _mean_gpu_metric(gpu: dict[str, Any], suffix: str) -> float:
    values = [
        _float(value)
        for key, value in gpu.items()
        if key.endswith(suffix) and math.isfinite(_float(value))
    ]
    return statistics.fmean(values) if values else math.nan


def _sum_gpu_metric(gpu: dict[str, Any], suffix: str) -> float:
    values = [
        _float(value)
        for key, value in gpu.items()
        if key.endswith(suffix) and math.isfinite(_float(value))
    ]
    return sum(values) if values else math.nan


def _project_profiler_metrics(cell_dir: Path, relative_path: str | None) -> dict[str, Any]:
    if not relative_path:
        return {}
    path = cell_dir / relative_path
    if not path.exists():
        return {}
    row = _first_csv_row(path)
    estimated_flops = _float(row.get("vllm_estimated_flops_per_gpu_delta"))
    operator_wall_s = _float(row.get("operator_wall_s"))
    recovered_mfu = (
        estimated_flops / (operator_wall_s * GPU_PEAK_TFLOPS_PER_4090_BF16 * 1e12)
        if estimated_flops > 0 and operator_wall_s > 0
        else math.nan
    )
    return {
        "mfu_recovered_fraction": recovered_mfu,
        "mfu_recovery_formula": (
            "vllm_estimated_flops_per_gpu_delta / "
            "(operator_wall_s * 165e12)"
        ),
        "ttft_p95_s": _float(row.get("vllm_time_to_first_token_p95_s")),
        "itl_p95_s": _float(row.get("vllm_inter_token_latency_p95_s")),
        "operator_wall_s": operator_wall_s,
    }


def _flatten_record(root: Path, record: dict[str, Any]) -> dict[str, Any]:
    portable_cell_dir = root / "raw" / record["workload"] / Path(record["cell_dir"]).name
    cell_dir = portable_cell_dir if portable_cell_dir.exists() else Path(record["cell_dir"])
    report_path = cell_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    metrics = report["metrics"]
    resources = report.get("resources", {})
    quality = metrics.get("quality", {})
    endpoint_deltas = resources.get("endpoint_vllm_deltas", {})
    gpu = resources.get("gpu", {})
    gauges = resources.get("vllm_gauges", {})
    mfu = resources.get("mfu", {})
    project = _project_profiler_metrics(cell_dir, report.get("profiler_summary"))

    if endpoint_deltas:
        gpu_util_mean = _mean_gpu_metric(gpu, "_util_mean")
        gpu_power_mean = _sum_gpu_metric(gpu, "_power_mean")
        kv_mean = _float(gauges.get("vllm_kv_cache_usage_mean"))
        kv_max = _float(gauges.get("vllm_kv_cache_usage_max"))
        running_mean = _float(gauges.get("vllm_running_mean"))
        running_max = _float(gauges.get("vllm_running_max"))
        waiting_mean = _float(gauges.get("vllm_waiting_mean"))
        waiting_max = _float(gauges.get("vllm_waiting_max"))
        prefix_hit_rate = _weighted_prefix_hit_rate(endpoint_deltas)
        ttft_p95 = _max_endpoint_metric(endpoint_deltas, "vllm_time_to_first_token_p95_s")
        itl_p95 = _max_endpoint_metric(endpoint_deltas, "vllm_inter_token_latency_p95_s")
        mfu_recorded = _float(mfu.get("mfu_estimate"))
        mfu_status = mfu.get("mfu_status", "")
    else:
        gpu_util_mean = _float(resources.get("gpu_utilization_pct_mean"))
        gpu_power_mean = _float(resources.get("gpu_power_w_mean"))
        kv_mean = _float(resources.get("vllm_kv_cache_usage_mean"))
        kv_max = _float(resources.get("vllm_kv_cache_usage_max"))
        running_mean = _float(resources.get("vllm_running_mean"))
        running_max = _float(resources.get("vllm_running_max"))
        waiting_mean = _float(resources.get("vllm_waiting_mean"))
        waiting_max = _float(resources.get("vllm_waiting_max"))
        prefix_hit_rate = _float(resources.get("vllm_prefix_cache_hit_rate"))
        ttft_p95 = _float(project.get("ttft_p95_s"))
        itl_p95 = _float(project.get("itl_p95_s"))
        mfu_recorded = _float(resources.get("mfu_estimate"))
        mfu_status = resources.get("mfu_status", "")

    recovered_mfu = _float(project.get("mfu_recovered_fraction"))
    effective_mfu = mfu_recorded if math.isfinite(mfu_recorded) else recovered_mfu
    observed_tokens = _float(metrics.get("observed_tokens"), 0.0)
    if endpoint_deltas:
        service_prompt_tokens = sum(
            _float(endpoint.get("vllm_prompt_tokens_delta"), 0.0)
            for endpoint in endpoint_deltas.values()
        )
        service_generation_tokens = sum(
            _float(endpoint.get("vllm_generation_tokens_delta"), 0.0)
            for endpoint in endpoint_deltas.values()
        )
        service_observed_tokens = _float(
            resources.get("service_observed_tokens"),
            service_prompt_tokens + service_generation_tokens,
        )
    else:
        service_prompt_tokens = _float(resources.get("vllm_prompt_tokens_delta"), 0.0)
        service_generation_tokens = _float(resources.get("vllm_generation_tokens_delta"), 0.0)
        service_observed_tokens = service_prompt_tokens + service_generation_tokens
    database_e2e_s = _float(metrics.get("database_e2e_s"))

    flat: dict[str, Any] = {
        "workload": record["workload"],
        "phase": record["phase"],
        "repeat": record["repeat"],
        "order": record["order"],
        "arm": record["arm"],
        "status": report.get("status", record.get("status", "")),
        "scheduler_owner": report.get("scheduler_owner", ""),
        "database_e2e_boundary": report.get("database_e2e_boundary", ""),
        "manifest_sha256": report.get("manifest_sha256", ""),
        "server_version": report.get("identity", {}).get("server_version", ""),
        "pgvector_version": report.get("identity", {}).get("pgvector_version", ""),
        "row_count": metrics.get("row_count", 0),
        "correct_rows": metrics.get("correct_rows", 0),
        "database_e2e_s": database_e2e_s,
        "raw_rows_per_s": _float(metrics.get("raw_rows_per_s")),
        "correct_rows_per_s": _float(metrics.get("correct_rows_per_s")),
        "request_latency_s_p50": _float(metrics.get("request_latency_s_p50")),
        "request_latency_s_p95": _float(metrics.get("request_latency_s_p95")),
        "request_latency_s_p99": _float(metrics.get("request_latency_s_p99")),
        "observed_tokens": observed_tokens,
        "service_prompt_tokens": service_prompt_tokens,
        "service_generation_tokens": service_generation_tokens,
        "service_observed_tokens": service_observed_tokens,
        "service_tokens_per_s": service_observed_tokens / database_e2e_s if database_e2e_s > 0 else math.nan,
        "failure_count": metrics.get("failure_count", 0),
        "infrastructure_failure_count": metrics.get("infrastructure_failure_count", 0),
        "cap_semantic_failure_count": metrics.get("cap_semantic_failure_count", 0),
        "finish_reason_length_count": metrics.get("finish_reason_length_count", 0),
        "null_output_count": metrics.get("null_output_count", 0),
        "exactly_once": metrics.get("exactly_once", False),
        "sink_rows_written": report.get("sink", {}).get("rows_written", 0),
        "sink_readback_matched": report.get("sink", {}).get("readback", {}).get("matched", False),
        "endpoint_row_skew": _skew(metrics.get("endpoint_rows", {})),
        "endpoint_observed_work_skew": _skew(metrics.get("endpoint_observed_tokens", {})),
        "gpu_utilization_pct_mean": gpu_util_mean,
        "gpu_power_w_mean": gpu_power_mean,
        "gpu_energy_j": _float(resources.get("gpu_energy_j")),
        "energy_j_per_correct_row": _float(resources.get("energy_j_per_correct_row")),
        "mfu_fraction": effective_mfu,
        "mfu_source": "recorded" if math.isfinite(mfu_recorded) else "recovered_from_profiler",
        "mfu_status_original": mfu_status,
        "vllm_running_mean": running_mean,
        "vllm_running_max": running_max,
        "vllm_waiting_mean": waiting_mean,
        "vllm_waiting_max": waiting_max,
        "vllm_kv_cache_usage_mean": kv_mean,
        "vllm_kv_cache_usage_max": kv_max,
        "vllm_prefix_cache_hit_rate": prefix_hit_rate,
        "ttft_p95_s": ttft_p95,
        "itl_p95_s": itl_p95,
    }
    for key, value in quality.items():
        flat[f"quality_{key}"] = value
    return flat


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _sample_stats(values: Iterable[Any]) -> dict[str, float | int]:
    nums = [_float(value) for value in values]
    nums = [value for value in nums if math.isfinite(value)]
    if not nums:
        return {"n": 0, "mean": math.nan, "median": math.nan, "sd": math.nan, "cv_pct": math.nan}
    mean = statistics.fmean(nums)
    sd = statistics.stdev(nums) if len(nums) > 1 else 0.0
    return {
        "n": len(nums),
        "mean": mean,
        "median": statistics.median(nums),
        "sd": sd,
        "cv_pct": sd / mean * 100 if mean else 0.0,
    }


def _summaries(formal: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base_metrics = (
        "row_count",
        "correct_rows",
        "database_e2e_s",
        "raw_rows_per_s",
        "correct_rows_per_s",
        "request_latency_s_p50",
        "request_latency_s_p95",
        "request_latency_s_p99",
        "service_prompt_tokens",
        "service_generation_tokens",
        "service_observed_tokens",
        "service_tokens_per_s",
        "endpoint_row_skew",
        "endpoint_observed_work_skew",
        "gpu_utilization_pct_mean",
        "gpu_power_w_mean",
        "gpu_energy_j",
        "energy_j_per_correct_row",
        "mfu_fraction",
        "vllm_running_mean",
        "vllm_running_max",
        "vllm_waiting_mean",
        "vllm_waiting_max",
        "vllm_kv_cache_usage_mean",
        "vllm_kv_cache_usage_max",
        "vllm_prefix_cache_hit_rate",
        "ttft_p95_s",
        "itl_p95_s",
    )
    quality_metrics = sorted(
        {
            key
            for row in formal
            for key, value in row.items()
            if key.startswith("quality_") and math.isfinite(_float(value))
        }
    )
    metrics = base_metrics + tuple(quality_metrics)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in formal:
        grouped[(row["workload"], row["arm"])].append(row)
    output = []
    for (workload, arm), rows in sorted(grouped.items()):
        summary: dict[str, Any] = {
            "workload": workload,
            "arm": arm,
            "formal_repeats": len(rows),
            "all_status_passed": all(row["status"] == "passed" for row in rows),
            "all_exactly_once": all(row["exactly_once"] for row in rows),
            "all_sink_readback_matched": all(row["sink_readback_matched"] for row in rows),
            "infrastructure_failures_total": sum(int(row["infrastructure_failure_count"]) for row in rows),
            "cap_semantic_failures_total": sum(int(row["cap_semantic_failure_count"]) for row in rows),
            "null_outputs_total": sum(int(row["null_output_count"]) for row in rows),
        }
        for metric in metrics:
            stats = _sample_stats(row.get(metric) for row in rows)
            for suffix, value in stats.items():
                summary[f"{metric}_{suffix}"] = value
        output.append(summary)
    return output


def _audit(rows: list[dict[str, Any]], summaries: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter((row["workload"], row["arm"], row["phase"]) for row in rows)
    count_contract = {
        f"{workload}/{arm}/{phase}": counts[(workload, arm, phase)]
        for workload in EXPECTED_WORKLOADS
        for arm in EXPECTED_ARMS
        for phase in ("warmup", "formal")
    }
    expected_counts_ok = all(
        value == (1 if key.endswith("/warmup") else 3)
        for key, value in count_contract.items()
    )
    manifests: dict[str, set[str]] = defaultdict(set)
    identities: set[tuple[str, str]] = set()
    for row in rows:
        manifests[row["workload"]].add(row["manifest_sha256"])
        identities.add((row["server_version"], row["pgvector_version"]))
    direct_service_rate = {
        row["workload"]: _float(row["service_tokens_per_s_mean"])
        for row in summaries
        if row["arm"] == "direct_static_sharded"
    }
    summary_gates = []
    for row in summaries:
        reference_rate = direct_service_rate.get(row["workload"], math.nan)
        observed_rate = _float(row["service_tokens_per_s_mean"])
        feeding_ratio = (
            observed_rate / reference_rate
            if math.isfinite(reference_rate) and reference_rate > 0
            else math.nan
        )
        summary_gates.append(
            {
                "workload": row["workload"],
                "arm": row["arm"],
                "correct_rows_per_s_cv_pct": row["correct_rows_per_s_cv_pct"],
                "database_e2e_s_cv_pct": row["database_e2e_s_cv_pct"],
                "gpu_utilization_pct_mean": row["gpu_utilization_pct_mean_mean"],
                "feeding_gpu_util_gate_ge_80": row["gpu_utilization_pct_mean_mean"] >= 80.0,
                "feeding_service_tokens_ratio_vs_direct": feeding_ratio,
                "feeding_service_tokens_gate_ge_0_95": feeding_ratio >= 0.95,
            }
        )
    return {
        "schema_version": 1,
        "cell_count": len(rows),
        "formal_cell_count": sum(row["phase"] == "formal" for row in rows),
        "expected_count_contract": count_contract,
        "expected_counts_ok": expected_counts_ok,
        "all_status_passed": all(row["status"] == "passed" for row in rows),
        "all_exactly_once": all(row["exactly_once"] for row in rows),
        "all_sink_readback_matched": all(row["sink_readback_matched"] for row in rows),
        "infrastructure_failure_count": sum(int(row["infrastructure_failure_count"]) for row in rows),
        "cap_semantic_failure_count": sum(int(row["cap_semantic_failure_count"]) for row in rows),
        "manifest_sha256_by_workload": {key: sorted(value) for key, value in manifests.items()},
        "identity_pairs": [list(pair) for pair in sorted(identities)],
        "mfu_recovery_note": (
            "project_frozen_static recorded missing_gpu_peak_tflops; the audit recovers MFU from the "
            "profiler counter using estimated_flops_per_gpu_delta/(operator_wall_s*165e12). "
            "Baseline arms retain their runner-recorded aggregate-two-GPU MFU."
        ),
        "feeding_reference": (
            "Within each workload, formal mean service_tokens_per_s is divided by the "
            "direct_static_sharded formal mean. Values below 0.95 fail the pre-registered "
            "feeding-saturation gate and cannot support a strategy-performance claim."
        ),
        "all_feeding_service_token_gates_passed": all(
            gate["feeding_service_tokens_gate_ge_0_95"] for gate in summary_gates
        ),
        "summary_gates": summary_gates,
    }


def _headline_summary(summaries: list[dict[str, Any]], audit: dict[str, Any]) -> dict[str, Any]:
    gates = {
        (row["workload"], row["arm"]): row
        for row in audit["summary_gates"]
    }
    workloads: dict[str, dict[str, Any]] = defaultdict(dict)
    for row in summaries:
        gate = gates[(row["workload"], row["arm"])]
        workloads[row["workload"]][row["arm"]] = {
            "formal_repeats": row["formal_repeats"],
            "correct_rows_per_s_mean": row["correct_rows_per_s_mean"],
            "correct_rows_per_s_cv_pct": row["correct_rows_per_s_cv_pct"],
            "database_e2e_s_mean": row["database_e2e_s_mean"],
            "raw_rows_per_s_mean": row["raw_rows_per_s_mean"],
            "service_tokens_per_s_mean": row["service_tokens_per_s_mean"],
            "feeding_service_tokens_ratio_vs_direct": gate["feeding_service_tokens_ratio_vs_direct"],
            "feeding_service_tokens_gate_ge_0_95": gate["feeding_service_tokens_gate_ge_0_95"],
            "infrastructure_failures_total": row["infrastructure_failures_total"],
            "cap_semantic_failures_total": row["cap_semantic_failures_total"],
            "null_outputs_total": row["null_outputs_total"],
        }
    return {
        "schema_version": 1,
        "workloads": dict(workloads),
        "claim_rule": (
            "An arm below 0.95 of the workload-matched direct formal mean service tokens/s "
            "is not eligible to support a strategy-performance claim."
        ),
    }


def summarize(root: Path, output: Path) -> dict[str, Any]:
    status_path = root / "matrix_status.json"
    records = json.loads(status_path.read_text(encoding="utf-8"))
    rows = [_flatten_record(root, record) for record in records]
    formal = [row for row in rows if row["phase"] == "formal"]
    summaries = _summaries(formal)
    audit = _audit(rows, summaries)
    headline = _headline_summary(summaries, audit)

    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "all_runs.csv", rows)
    _write_csv(output / "formal_runs.csv", formal)
    _write_csv(output / "formal_summary.csv", summaries)
    (output / "audit.json").write_text(
        json.dumps(audit, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    (output / "headline_summary.json").write_text(
        json.dumps(headline, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    return audit


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    audit = summarize(args.matrix_root.resolve(), args.output.resolve())
    print(json.dumps(audit, indent=2, ensure_ascii=False))
    return 0 if audit["expected_counts_ok"] and audit["all_status_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
