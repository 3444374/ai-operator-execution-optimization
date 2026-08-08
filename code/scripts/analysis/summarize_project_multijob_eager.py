#!/usr/bin/env python3
"""Audit and summarize the matched eager Project short/long diagnostic."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


EXPECTED_MAIN = {
    "single_short_full_pool_all_at_t0": 1,
    "staggered_static_partition_all_at_t0": 2,
    "staggered_shared_work_all_at_t0": 2,
}
HALF = "single_short_half_pool_all_at_t0"
PAIRS = (
    ("quota_only", "single_short_full_pool_all_at_t0", HALF),
    ("long_competition_static", HALF, "staggered_static_partition_all_at_t0"),
    (
        "long_competition_shared",
        "single_short_full_pool_all_at_t0",
        "staggered_shared_work_all_at_t0",
    ),
    (
        "shared_vs_static",
        "staggered_static_partition_all_at_t0",
        "staggered_shared_work_all_at_t0",
    ),
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty CSV: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(rows[0]),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def _array(value: object) -> list[float]:
    decoded = json.loads(value) if isinstance(value, str) else value
    if not isinstance(decoded, list):
        raise ValueError("expected JSON list")
    parsed = [float(item) for item in decoded]
    if any(not math.isfinite(item) for item in parsed):
        raise ValueError("non-finite array value")
    return parsed


def _float(value: object) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("non-finite numeric value")
    return parsed


def _summary(values: Iterable[float]) -> tuple[float, float, float]:
    materialized = list(values)
    mean = statistics.fmean(materialized)
    sd = statistics.stdev(materialized) if len(materialized) > 1 else 0.0
    return mean, sd, sd / mean if mean else 0.0


def _percentile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _delta(candidate: float, baseline: float) -> float:
    return (candidate / baseline - 1.0) * 100.0


def _load_formal(
    root: Path,
    expected: dict[str, int],
    errors: list[str],
) -> list[dict[str, Any]]:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "completed" or manifest.get("incidents"):
        errors.append(f"{root.name}: manifest is not cleanly completed")
    raw = _read_csv(root / "group_runs.csv")
    rows: list[dict[str, Any]] = []
    for scenario, job_count in expected.items():
        warmup = [r for r in raw if r["scenario_id"] == scenario and r["phase"] == "warmup"]
        formal = [r for r in raw if r["scenario_id"] == scenario and r["phase"] == "formal"]
        if len(warmup) != 1 or len(formal) != 3:
            errors.append(f"{scenario}: expected 1 warmup + 3 formal")
        for row in formal:
            jct, p99, work = (
                _array(row["job_jct_s"]),
                _array(row["job_p99_s"]),
                _array(row["job_actual_work"]),
            )
            if len(jct) != job_count or len(p99) != job_count or len(work) != job_count:
                errors.append(f"{scenario}/{row['repeat_index']}: job array length mismatch")
                continue
            for field in ("metrics_status", "resource_metrics_status", "mfu_status"):
                if row[field] != "ok":
                    errors.append(f"{scenario}/{row['repeat_index']}: {field}={row[field]}")
            if int(row["actor_worker_failures"]) or int(row["incidents"]):
                errors.append(f"{scenario}/{row['repeat_index']}: failure or incident")
            rows.append(
                {
                    "root": root,
                    "scenario_id": scenario,
                    "repeat_index": int(row["repeat_index"]),
                    "job_count": job_count,
                    "short_jct_s": jct[0],
                    "short_p99_s": p99[0],
                    "short_actual_work": work[0],
                    "short_work_per_s": work[0] / jct[0],
                    "long_jct_s": jct[1] if job_count == 2 else "",
                    "group_tokens_per_s": _float(row["tokens_per_s"]),
                    "gpu_utilization_pct_mean": _float(row["gpu_utilization_pct_mean"]),
                    "mfu_pct": _float(row["mfu_estimate"]) * 100.0,
                    "vllm_running_mean": _float(row["vllm_running_mean"]),
                    "vllm_waiting_mean": _float(row["vllm_waiting_mean"]),
                    "vllm_kv_usage_pct_mean": _float(row["vllm_kv_usage_mean"]) * 100.0,
                    "jain_fairness": _float(row["jain_fairness"]),
                }
            )
    observed = {r["scenario_id"] for r in raw}
    if observed != set(expected):
        errors.append(f"{root.name}: scenario set mismatch {sorted(observed)}")
    return rows


def _request_rows(row: dict[str, Any], errors: list[str]) -> list[dict[str, str]]:
    pattern = (
        f"*_formal_{row['repeat_index']}_{row['scenario_id']}_job0.requests.csv"
    )
    matches = list((row["root"] / "jobs").glob(pattern))
    if len(matches) != 1:
        raise ValueError(f"request trace mismatch: {pattern}")
    requests = _read_csv(matches[0])
    endpoint_counts: dict[str, int] = defaultdict(int)
    for request in requests:
        endpoint_counts[request["endpoint_id"]] += 1
    arrivals = [_float(request["arrival_epoch_s"]) for request in requests]
    if (
        len(requests) != 512
        or len({request["doc_id"] for request in requests}) != 512
        or endpoint_counts != {"endpoint-0": 256, "endpoint-1": 256}
        or max(arrivals) - min(arrivals) > 0.001
    ):
        errors.append(
            f"{row['scenario_id']}/{row['repeat_index']}: request identity/arrival gate failed"
        )
    return requests


def summarize(
    main_root: Path,
    half_root: Path,
    output: Path,
    native_comparisons: Path | None = None,
) -> bool:
    errors: list[str] = []
    rows = _load_formal(main_root, EXPECTED_MAIN, errors)
    rows += _load_formal(half_root, {HALF: 1}, errors)
    compact = [{key: value for key, value in row.items() if key != "root"} for row in rows]

    summaries: list[dict[str, Any]] = []
    stage_rows: list[dict[str, Any]] = []
    requests_by_run: dict[tuple[str, int], list[dict[str, str]]] = {}
    for row in rows:
        requests_by_run[(row["scenario_id"], row["repeat_index"])] = _request_rows(row, errors)
    for scenario in sorted(EXPECTED_MAIN | {HALF: 1}):
        selected = [row for row in rows if row["scenario_id"] == scenario]
        summary: dict[str, Any] = {"scenario_id": scenario, "formal_repeats": len(selected)}
        for field in (
            "short_jct_s",
            "short_p99_s",
            "short_work_per_s",
            "group_tokens_per_s",
            "gpu_utilization_pct_mean",
            "mfu_pct",
            "vllm_running_mean",
            "vllm_waiting_mean",
            "vllm_kv_usage_pct_mean",
            "jain_fairness",
        ):
            mean, sd, cv = _summary(_float(row[field]) for row in selected)
            summary[f"{field}_mean"] = mean
            summary[f"{field}_sd"] = sd
            summary[f"{field}_cv"] = cv
        long_values = [_float(row["long_jct_s"]) for row in selected if row["long_jct_s"] != ""]
        summary["long_jct_s_mean"] = statistics.fmean(long_values) if long_values else ""
        summaries.append(summary)

        requests = [
            request
            for row in selected
            for request in requests_by_run[(scenario, row["repeat_index"])]
        ]
        stage_fields = {
            "arrival_to_flush_s": [_float(r["buffer_s"]) for r in requests],
            "flush_to_submit_s": [
                _float(r["submit_epoch_s"]) - _float(r["flush_epoch_s"]) for r in requests
            ],
            "submit_to_service_s": [_float(r["submit_to_service_s"]) for r in requests],
            "service_s": [_float(r["service_s"]) for r in requests],
            "request_e2e_s": [_float(r["e2e_s"]) for r in requests],
        }
        for stage, values in stage_fields.items():
            stage_rows.append(
                {
                    "scenario_id": scenario,
                    "stage": stage,
                    "requests": len(values),
                    "mean_s": statistics.fmean(values),
                    "p99_s": _percentile(values, 0.99),
                }
            )

    by_id = {row["scenario_id"]: row for row in summaries}
    comparisons: list[dict[str, Any]] = []
    for comparison_id, baseline_id, candidate_id in PAIRS:
        baseline, candidate = by_id[baseline_id], by_id[candidate_id]
        comparisons.append(
            {
                "comparison_id": comparison_id,
                "baseline": baseline_id,
                "candidate": candidate_id,
                "short_jct_delta_pct": _delta(candidate["short_jct_s_mean"], baseline["short_jct_s_mean"]),
                "short_p99_delta_pct": _delta(candidate["short_p99_s_mean"], baseline["short_p99_s_mean"]),
                "short_work_rate_delta_pct": _delta(candidate["short_work_per_s_mean"], baseline["short_work_per_s_mean"]),
                "group_tokens_per_s_delta_pct": _delta(candidate["group_tokens_per_s_mean"], baseline["group_tokens_per_s_mean"]),
                "mfu_delta_pp": candidate["mfu_pct_mean"] - baseline["mfu_pct_mean"],
                "jain_fairness_delta": candidate["jain_fairness_mean"]
                - baseline["jain_fairness_mean"],
            }
        )

    stage_by_key = {
        (row["scenario_id"], row["stage"]): row for row in stage_rows
    }
    stage_comparisons: list[dict[str, Any]] = []
    for comparison_id, baseline_id, candidate_id in PAIRS:
        for stage in sorted({row["stage"] for row in stage_rows}):
            baseline = stage_by_key[(baseline_id, stage)]
            candidate = stage_by_key[(candidate_id, stage)]
            stage_comparisons.append(
                {
                    "comparison_id": comparison_id,
                    "stage": stage,
                    "baseline": baseline_id,
                    "candidate": candidate_id,
                    "mean_delta_pct": _delta(candidate["mean_s"], baseline["mean_s"]),
                    "p99_delta_pct": _delta(candidate["p99_s"], baseline["p99_s"]),
                }
            )

    phase_rows: list[dict[str, Any]] = []
    for row in rows:
        if row["job_count"] != 2:
            continue
        record_pattern = f"*_formal_{row['repeat_index']}_{row['scenario_id']}.json"
        record_path = next((row["root"] / "records").glob(record_pattern))
        record = json.loads(record_path.read_text(encoding="utf-8"))
        starts = _array(record["replay_configured_start_epoch_s"])
        jct = _array(record["job_jct_s"])
        bounds = (
            ("pre_long", starts[0], starts[1]),
            ("overlap", starts[1], min(starts[0] + jct[0], starts[1] + jct[1])),
            ("long_drain", starts[0] + jct[0], starts[1] + jct[1]),
        )
        resource_path = row["root"] / "traces" / f"{record_path.stem}.resources.csv"
        samples: dict[float, list[dict[str, str]]] = defaultdict(list)
        for sample in _read_csv(resource_path):
            samples[_float(sample["observed_epoch_s"])].append(sample)
        for phase, start, end in bounds:
            points = []
            for epoch, endpoint_rows in samples.items():
                if start <= epoch < end:
                    points.append(
                        (
                            sum(_float(item["running"]) for item in endpoint_rows),
                            sum(_float(item["waiting"]) for item in endpoint_rows),
                            statistics.fmean(_float(item["kv_usage"]) for item in endpoint_rows) * 100,
                            statistics.fmean(_float(item["gpu_utilization_pct"]) for item in endpoint_rows),
                        )
                    )
            if points:
                phase_rows.append(
                    {
                        "scenario_id": row["scenario_id"],
                        "repeat_index": row["repeat_index"],
                        "phase": phase,
                        "samples": len(points),
                        "running_sum_mean": statistics.fmean(point[0] for point in points),
                        "waiting_sum_mean": statistics.fmean(point[1] for point in points),
                        "kv_usage_pct_endpoint_mean": statistics.fmean(point[2] for point in points),
                        "gpu_utilization_pct_endpoint_mean": statistics.fmean(point[3] for point in points),
                    }
                )

    phase_summaries: list[dict[str, Any]] = []
    for scenario in sorted({row["scenario_id"] for row in phase_rows}):
        for phase in ("pre_long", "overlap", "long_drain"):
            selected = [
                row
                for row in phase_rows
                if row["scenario_id"] == scenario and row["phase"] == phase
            ]
            sample_count = sum(int(row["samples"]) for row in selected)
            phase_summaries.append(
                {
                    "scenario_id": scenario,
                    "phase": phase,
                    "formal_repeats": len(selected),
                    "samples": sample_count,
                    **{
                        field: sum(
                            _float(row[field]) * int(row["samples"]) for row in selected
                        )
                        / sample_count
                        for field in (
                            "running_sum_mean",
                            "waiting_sum_mean",
                            "kv_usage_pct_endpoint_mean",
                            "gpu_utilization_pct_endpoint_mean",
                        )
                    },
                }
            )

    cross_system: list[dict[str, Any]] = [
        {
            "system": "project_static",
            "baseline": HALF,
            "candidate": "staggered_static_partition_all_at_t0",
            "evidence_status": "causal:matched_half_quota_eager",
            "short_jct_delta_pct": comparisons[1]["short_jct_delta_pct"],
            "short_p99_delta_pct": comparisons[1]["short_p99_delta_pct"],
            "note": "within-Project paired delta; do not rank absolute cross-system JCT",
        },
        {
            "system": "project_shared",
            "baseline": "single_short_full_pool_all_at_t0",
            "candidate": "staggered_shared_work_all_at_t0",
            "evidence_status": "causal:matched_full_pool_eager",
            "short_jct_delta_pct": comparisons[2]["short_jct_delta_pct"],
            "short_p99_delta_pct": comparisons[2]["short_p99_delta_pct"],
            "note": "within-Project paired delta; do not rank absolute cross-system JCT",
        },
    ]
    if native_comparisons is not None:
        for row in _read_csv(native_comparisons):
            if row.get("causal_status") != "observational:overlap_present":
                continue
            system = row["comparison_id"].removesuffix("_native_two_job_observation")
            cross_system.append(
                {
                    "system": system,
                    "baseline": row["baseline"],
                    "candidate": row["candidate"],
                    "evidence_status": row["causal_status"],
                    "short_jct_delta_pct": row["short_jct_delta_pct"],
                    "short_p99_delta_pct": "",
                    "note": "within-native-arm observed delta; native adapter has no request P99",
                }
            )

    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "formal_runs.csv", compact)
    _write_csv(output / "scenario_summary.csv", summaries)
    _write_csv(output / "comparisons.csv", comparisons)
    _write_csv(output / "short_request_stage_summary.csv", stage_rows)
    _write_csv(output / "short_request_stage_comparisons.csv", stage_comparisons)
    _write_csv(output / "phase_state_runs.csv", phase_rows)
    _write_csv(output / "phase_state_summary.csv", phase_summaries)
    _write_csv(output / "cross_system_short_impact.csv", cross_system)
    audit = {
        "status": "passed" if not errors else "failed",
        "main_root_name": main_root.name,
        "half_root_name": half_root.name,
        "formal_runs": len(rows),
        "checks": {
            "one_warmup_three_formal_per_scenario": not errors,
            "metrics_resources_mfu_ok": not errors,
            "exactly_once_512_and_endpoint_balance": not errors,
            "arrival_span_below_1ms": not errors,
            "zero_worker_failure_and_incident": not errors,
        },
        "errors": errors,
    }
    (output / "audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return not errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-root", type=Path, required=True)
    parser.add_argument("--half-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--native-comparisons", type=Path)
    args = parser.parse_args()
    return (
        0
        if summarize(
            args.main_root,
            args.half_root,
            args.output,
            native_comparisons=args.native_comparisons,
        )
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
