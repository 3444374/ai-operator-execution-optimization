#!/usr/bin/env python3
"""Summarize repeated output-aware batching runs in plot-ready long form."""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_METRICS = (
    "tokens_per_s",
    "e2e_s",
    "request_e2e_s_p50",
    "request_e2e_s_p95",
    "request_e2e_s_p99",
    "batch_service_s_p99",
    "operator_invocations",
    "packing_budget_utilization_mean",
    "packing_budget_utilization_p95",
    "batch_estimated_cost_units_p95",
    "gpu_utilization_pct_mean",
    "gpu_utilization_pct_p50",
    "gpu_utilization_pct_p95",
    "gpu_utilization_pct_max",
    "gpu_utilization_below_10pct_ratio",
    "gpu_memory_used_mib_mean",
    "gpu_memory_used_mib_max",
    "gpu_memory_utilization_pct_mean",
    "gpu_memory_utilization_pct_max",
    "gpu_power_w_mean",
    "gpu_power_w_max",
    "gpu_energy_j",
    "energy_j_per_1k_observed_tokens",
    "vllm_running_mean",
    "vllm_running_p95",
    "vllm_running_max",
    "vllm_waiting_mean",
    "vllm_waiting_p95",
    "vllm_waiting_max",
    "vllm_kv_cache_usage_mean",
    "vllm_kv_cache_usage_p95",
    "vllm_kv_cache_usage_max",
    "mfu_estimate",
)
SUMMARY_COLUMNS = (
    "scenario_id",
    "metric",
    "n",
    "mean",
    "sample_std",
    "p50",
    "min",
    "max",
)


def _number(value: object) -> float | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(resolved):
        raise ValueError(f"metric value must be finite: {value}")
    return resolved


def _nearest_rank(values: Sequence[float], percentile: float) -> float:
    ordered = sorted(values)
    index = math.ceil(percentile / 100.0 * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


def summarize_rows(
    rows: Iterable[dict[str, object]],
    *,
    metric_names: Sequence[str] = DEFAULT_METRICS,
) -> list[dict[str, float | int | str]]:
    formal_rows = [
        row
        for row in rows
        if row.get("phase") == "formal" and row.get("status") == "ok"
    ]
    scenario_ids = sorted(
        {
            str(row.get("scenario_id", "")).strip()
            for row in formal_rows
            if str(row.get("scenario_id", "")).strip()
        }
    )
    output = []
    for scenario_id in scenario_ids:
        scenario_rows = [
            row
            for row in formal_rows
            if str(row.get("scenario_id", "")).strip() == scenario_id
        ]
        for metric in metric_names:
            values = [
                resolved
                for row in scenario_rows
                if (resolved := _number(row.get(metric))) is not None
            ]
            output.append(
                {
                    "scenario_id": scenario_id,
                    "metric": metric,
                    "n": len(values),
                    "mean": statistics.mean(values) if values else "",
                    "sample_std": (
                        statistics.stdev(values)
                        if len(values) > 1
                        else 0.0
                        if values
                        else ""
                    ),
                    "p50": _nearest_rank(values, 50) if values else "",
                    "min": min(values) if values else "",
                    "max": max(values) if values else "",
                }
            )
    return output


def _read_runs(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"status", "phase", "scenario_id", *DEFAULT_METRICS}
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise ValueError(
                "runs CSV is missing required columns: "
                + ", ".join(sorted(missing))
            )
        return list(reader)


def _write_summary(
    path: Path,
    rows: list[dict[str, float | int | str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize output-aware BFD formal repeats."
    )
    parser.add_argument("--runs", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows = _read_runs(args.runs)
    _write_summary(args.output, summarize_rows(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
