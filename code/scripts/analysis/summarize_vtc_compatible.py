#!/usr/bin/env python3
"""Fail-closed summary for VTC-compatible upstream multi-job matrices."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--suite",
        required=True,
        choices=("on_off_overload", "overload_multi"),
    )
    return parser.parse_args()


def _read(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _array(row: dict[str, str], key: str, count: int) -> list[float]:
    values = json.loads(row[key])
    if not isinstance(values, list) or len(values) != count:
        raise ValueError(f"{key} must contain {count} job values")
    resolved = [float(value) for value in values]
    if any(not math.isfinite(value) for value in resolved):
        raise ValueError(f"{key} contains non-finite values")
    return resolved


def _jain(values: list[float]) -> float:
    total = sum(values)
    return total * total / (len(values) * sum(value * value for value in values))


def _mean(row_group: list[dict[str, str]], field: str) -> float:
    return statistics.fmean(float(row[field]) for row in row_group)


def summarize(root: Path, output: Path, suite: str) -> None:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    rows = _read(root / "group_runs.csv")
    client_count = 2 if suite == "on_off_overload" else 8
    multi_prefix = "on_off" if suite == "on_off_overload" else "overload_multi"
    expected = {
        *(f"solo_client_{index}_full_pool" for index in range(client_count)),
        f"{multi_prefix}_static_partition",
        f"{multi_prefix}_shared_fcfs_control",
        f"{multi_prefix}_shared_work",
    }
    errors: list[str] = []
    if manifest.get("status") != "completed":
        errors.append(f"manifest status is {manifest.get('status')!r}")
    if manifest.get("incidents"):
        errors.append("manifest contains incidents")
    observed = {row.get("scenario_id", "") for row in rows}
    if observed != expected:
        errors.append(f"scenario set mismatch: {sorted(observed)}")
    formal_by_scenario: dict[str, list[dict[str, str]]] = {}
    for scenario in sorted(expected):
        scenario_rows = [row for row in rows if row.get("scenario_id") == scenario]
        warmup = [row for row in scenario_rows if row.get("phase") == "warmup"]
        formal = [row for row in scenario_rows if row.get("phase") == "formal"]
        if len(warmup) != 1 or len(formal) != 3:
            errors.append(
                f"{scenario} requires 1 warmup + 3 formal; "
                f"observed {len(warmup)} + {len(formal)}"
            )
        formal_by_scenario[scenario] = formal
    if errors:
        output.mkdir(parents=True, exist_ok=True)
        (output / "validation.json").write_text(
            json.dumps({"status": "failed", "errors": errors}, indent=2) + "\n",
            encoding="utf-8",
        )
        raise ValueError("; ".join(errors))

    solo_rates = []
    for index in range(client_count):
        solo_rows = formal_by_scenario[f"solo_client_{index}_full_pool"]
        rates = [
            _array(row, "job_actual_work", 1)[0]
            / _array(row, "job_jct_s", 1)[0]
            for row in solo_rows
        ]
        solo_rates.append(statistics.fmean(rates))

    summaries = []
    for policy_suffix in (
        "static_partition",
        "shared_fcfs_control",
        "shared_work",
    ):
        scenario = f"{multi_prefix}_{policy_suffix}"
        scenario_rows = formal_by_scenario[scenario]
        normalized_progress = []
        for row in scenario_rows:
            actual_work = _array(row, "job_actual_work", client_count)
            jct = _array(row, "job_jct_s", client_count)
            progress = [
                (work / duration) / solo_rate
                for work, duration, solo_rate in zip(actual_work, jct, solo_rates)
            ]
            normalized_progress.append(progress)
            if any(_array(row, "job_failed_rows", client_count)):
                raise ValueError(f"{scenario} contains failed requests")
            if _array(row, "job_arrived_rows", client_count) != _array(
                row, "job_completed_rows", client_count
            ):
                raise ValueError(f"{scenario} is not exactly-once")
        tokens = [float(row["tokens_per_s"]) for row in scenario_rows]
        summaries.append(
            {
                "scenario_id": scenario,
                "policy": scenario_rows[0]["policy"],
                "formal_repeats": len(scenario_rows),
                "tokens_per_s_mean": statistics.fmean(tokens),
                "tokens_per_s_cv": statistics.stdev(tokens) / statistics.fmean(tokens),
                "max_job_p99_s_mean": statistics.fmean(
                    max(_array(row, "job_p99_s", client_count))
                    for row in scenario_rows
                ),
                "vllm_ttft_p99_s_mean": _mean(
                    scenario_rows, "vllm_time_to_first_token_p99_s"
                ),
                "slo_violation_ratio_mean": statistics.fmean(
                    value
                    for row in scenario_rows
                    for value in _array(row, "job_slo_violation_ratio", client_count)
                ),
                "solo_normalized_progress_jain_mean": statistics.fmean(
                    _jain(values) for values in normalized_progress
                ),
                "solo_normalized_progress_min": min(
                    value for values in normalized_progress for value in values
                ),
                "max_backlogged_service_disparity_ratio_mean": _mean(
                    scenario_rows,
                    "max_overlap_normalized_service_disparity_ratio",
                ),
                "credit_endpoint_idle_sample_fraction_mean": _mean(
                    scenario_rows,
                    "credit_endpoint_idle_sample_fraction",
                ),
                "credit_borrowed_work_mean": _mean(
                    scenario_rows, "credit_borrowed_work_mean"
                ),
                "gpu_utilization_pct_mean": _mean(
                    scenario_rows, "gpu_utilization_pct_mean"
                ),
                "vllm_running_mean": _mean(scenario_rows, "vllm_running_mean"),
                "vllm_waiting_mean": _mean(scenario_rows, "vllm_waiting_mean"),
                "vllm_kv_usage_mean": _mean(scenario_rows, "vllm_kv_usage_mean"),
            }
        )
    output.mkdir(parents=True, exist_ok=True)
    with (output / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    (output / "validation.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "label": "VTC-compatible upstream evaluation; not official VTC reproduction",
                "suite": suite,
                "solo_service_rates": solo_rates,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    args = _args()
    summarize(args.matrix_root.resolve(), args.output_dir.resolve(), args.suite)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
