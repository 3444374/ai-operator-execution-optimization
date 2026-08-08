#!/usr/bin/env python3
"""Fail-closed summary for the opening staggered two-job matrix."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path
from typing import Any


EXPECTED_SCENARIOS = {
    "staggered_static_partition",
    "staggered_shared_work",
}
PAIRS = {
    "staggered": (
        "staggered_static_partition",
        "staggered_shared_work",
    ),
}
ARRAY_FIELDS = (
    "job_jct_s",
    "job_p99_s",
    "job_slo_violation_ratio",
    "job_slo_goodput_per_s",
    "job_slo_token_goodput_per_s",
    "job_predicted_work",
    "job_actual_work",
    "job_normalized_service_rate",
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty CSV: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _float(row: dict[str, str], key: str) -> float:
    value = float(row[key])
    if not math.isfinite(value):
        raise ValueError(f"{key} is not finite")
    return value


def _array(row: dict[str, str], key: str) -> list[float]:
    values = json.loads(row[key])
    if not isinstance(values, list) or len(values) != 2:
        raise ValueError(f"{key} must contain exactly two jobs")
    parsed = [float(value) for value in values]
    if any(not math.isfinite(value) for value in parsed):
        raise ValueError(f"{key} contains a non-finite value")
    return parsed


def _json_list(row: dict[str, str], key: str) -> list[Any]:
    values = json.loads(row[key])
    if not isinstance(values, list):
        raise ValueError(f"{key} must contain a JSON list")
    return values


def _mean(values: list[float]) -> float:
    return statistics.fmean(values)


def _sd(values: list[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def _pct_change(candidate: float, baseline: float) -> float:
    if baseline == 0:
        raise ValueError("percentage baseline is zero")
    return (candidate / baseline - 1.0) * 100.0


def _scenario_summary(
    scenario_id: str, rows: list[dict[str, str]]
) -> dict[str, Any]:
    tokens = [_float(row, "tokens_per_s") for row in rows]
    duration = [_float(row, "duration_s") for row in rows]
    gpu = [_float(row, "gpu_utilization_pct_mean") for row in rows]
    mfu = [_float(row, "mfu_estimate") * 100.0 for row in rows]
    jain = [_float(row, "jain_fairness") for row in rows]
    arrays = {
        key: [_array(row, key) for row in rows] for key in ARRAY_FIELDS
    }
    manifest_sha = {
        tuple(str(value) for value in _json_list(row, "request_manifest_sha256"))
        for row in rows
    }
    if len(manifest_sha) != 1 or len(next(iter(manifest_sha))) != 2:
        raise ValueError(f"{scenario_id} manifest identity changed across repeats")
    return {
        "scenario_id": scenario_id,
        "policy": rows[0]["policy"],
        "repeats": len(rows),
        "tokens_per_s_mean": _mean(tokens),
        "tokens_per_s_sd": _sd(tokens),
        "tokens_per_s_cv": _sd(tokens) / _mean(tokens),
        "duration_s_mean": _mean(duration),
        "gpu_utilization_pct_mean": _mean(gpu),
        "mfu_pct_mean": _mean(mfu),
        "jain_fairness_median": statistics.median(jain),
        "jain_fairness_min": min(jain),
        "job0_jct_s_mean": _mean([value[0] for value in arrays["job_jct_s"]]),
        "job1_jct_s_mean": _mean([value[1] for value in arrays["job_jct_s"]]),
        "max_job_jct_s_mean": _mean([max(value) for value in arrays["job_jct_s"]]),
        "max_job_p99_s_mean": _mean([max(value) for value in arrays["job_p99_s"]]),
        "slo_violation_ratio_mean": _mean(
            [item for values in arrays["job_slo_violation_ratio"] for item in values]
        ),
        "slo_goodput_per_s_mean": _mean(
            [item for values in arrays["job_slo_goodput_per_s"] for item in values]
        ),
        "slo_token_goodput_per_s_mean": _mean(
            [item for values in arrays["job_slo_token_goodput_per_s"] for item in values]
        ),
        "min_normalized_service_rate": min(
            item for values in arrays["job_normalized_service_rate"] for item in values
        ),
        "job0_predicted_work_mean": _mean(
            [value[0] for value in arrays["job_predicted_work"]]
        ),
        "job1_predicted_work_mean": _mean(
            [value[1] for value in arrays["job_predicted_work"]]
        ),
        "job0_actual_work_mean": _mean(
            [value[0] for value in arrays["job_actual_work"]]
        ),
        "job1_actual_work_mean": _mean(
            [value[1] for value in arrays["job_actual_work"]]
        ),
        "request_manifest_sha256": json.dumps(next(iter(manifest_sha))),
    }


def summarize(matrix_root: Path, output: Path) -> bool:
    manifest_path = matrix_root / "manifest.json"
    runs_path = matrix_root / "group_runs.csv"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = _read_csv(runs_path)
    errors: list[str] = []
    if manifest.get("status") != "completed":
        errors.append(f"manifest status is {manifest.get('status')!r}")
    if manifest.get("incidents"):
        errors.append("manifest contains incidents")
    observed_scenarios = {row.get("scenario_id", "") for row in rows}
    if observed_scenarios != EXPECTED_SCENARIOS:
        errors.append(
            f"scenario set mismatch: {sorted(observed_scenarios)}"
        )

    by_scenario: dict[str, list[dict[str, str]]] = {}
    compact: list[dict[str, Any]] = []
    for scenario_id in sorted(EXPECTED_SCENARIOS):
        scenario_rows = [row for row in rows if row.get("scenario_id") == scenario_id]
        warmups = [row for row in scenario_rows if row.get("phase") == "warmup"]
        formal = [row for row in scenario_rows if row.get("phase") == "formal"]
        if len(warmups) != 1 or len(formal) != 3:
            errors.append(
                f"{scenario_id} requires 1 warmup + 3 formal; "
                f"observed {len(warmups)} + {len(formal)}"
            )
        by_scenario[scenario_id] = formal
        for row in scenario_rows:
            try:
                if row.get("metrics_status") != "ok":
                    raise ValueError("metrics_status is not ok")
                if row.get("resource_metrics_status") != "ok":
                    raise ValueError("resource_metrics_status is not ok")
                if row.get("mfu_status") != "ok":
                    raise ValueError("mfu_status is not ok")
                if int(row.get("actor_worker_failures", "-1")) != 0:
                    raise ValueError("actor_worker_failures is non-zero")
                if int(row.get("incidents", "-1")) != 0:
                    raise ValueError("incidents is non-zero")
                offsets = _json_list(row, "source_row_offsets")
                manifests = _json_list(row, "request_manifest_sha256")
                if len(offsets) != 2 or len(set(offsets)) != 2:
                    raise ValueError("jobs do not have two distinct source offsets")
                if len(manifests) != 2 or not all(manifests) or len(set(manifests)) != 2:
                    raise ValueError("jobs do not have two distinct validated manifests")
                parsed = {key: _array(row, key) for key in ARRAY_FIELDS}
                compact.append(
                    {
                        "scenario_id": scenario_id,
                        "phase": row["phase"],
                        "repeat_index": int(row["repeat_index"]),
                        "policy": row["policy"],
                        "tokens_per_s": _float(row, "tokens_per_s"),
                        "duration_s": _float(row, "duration_s"),
                        "gpu_utilization_pct_mean": _float(
                            row, "gpu_utilization_pct_mean"
                        ),
                        "mfu_pct": _float(row, "mfu_estimate") * 100.0,
                        "jain_fairness": _float(row, "jain_fairness"),
                        "job0_jct_s": parsed["job_jct_s"][0],
                        "job1_jct_s": parsed["job_jct_s"][1],
                        "job0_p99_s": parsed["job_p99_s"][0],
                        "job1_p99_s": parsed["job_p99_s"][1],
                        "job0_predicted_work": parsed["job_predicted_work"][0],
                        "job1_predicted_work": parsed["job_predicted_work"][1],
                        "job0_actual_work": parsed["job_actual_work"][0],
                        "job1_actual_work": parsed["job_actual_work"][1],
                    }
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                errors.append(
                    f"{scenario_id}/{row.get('phase')}/"
                    f"{row.get('repeat_index')}: {exc}"
                )

    summaries: list[dict[str, Any]] = []
    if not errors:
        for scenario_id in sorted(EXPECTED_SCENARIOS):
            summaries.append(_scenario_summary(scenario_id, by_scenario[scenario_id]))
    summary_by_id = {row["scenario_id"]: row for row in summaries}
    comparisons: list[dict[str, Any]] = []
    if not errors:
        for factor, (static_id, shared_id) in PAIRS.items():
            static = summary_by_id[static_id]
            shared = summary_by_id[shared_id]
            comparisons.append(
                {
                    "factor": factor,
                    "baseline": static_id,
                    "candidate": shared_id,
                    "tokens_per_s_delta_pct": _pct_change(
                        shared["tokens_per_s_mean"], static["tokens_per_s_mean"]
                    ),
                    "max_job_jct_delta_pct": _pct_change(
                        shared["max_job_jct_s_mean"], static["max_job_jct_s_mean"]
                    ),
                    "max_job_p99_delta_pct": _pct_change(
                        shared["max_job_p99_s_mean"], static["max_job_p99_s_mean"]
                    ),
                    "job0_jct_delta_pct": _pct_change(
                        shared["job0_jct_s_mean"], static["job0_jct_s_mean"]
                    ),
                    "job1_jct_delta_pct": _pct_change(
                        shared["job1_jct_s_mean"], static["job1_jct_s_mean"]
                    ),
                    "slo_token_goodput_delta_pct": _pct_change(
                        shared["slo_token_goodput_per_s_mean"],
                        static["slo_token_goodput_per_s_mean"],
                    ),
                    "shared_jain_fairness_median": shared[
                        "jain_fairness_median"
                    ],
                    "shared_min_normalized_service_rate": shared[
                        "min_normalized_service_rate"
                    ],
                }
            )

    output.mkdir(parents=True, exist_ok=True)
    audit = {
        "status": "passed" if not errors else "failed",
        "matrix_root": str(matrix_root.resolve()),
        "manifest_status": manifest.get("status"),
        "expected_scenarios": sorted(EXPECTED_SCENARIOS),
        "observed_group_runs": len(rows),
        "observed_formal_runs": sum(
            row.get("phase") == "formal" for row in rows
        ),
        "checks": {
            "one_warmup_three_formal": not errors,
            "metrics_resources_mfu_ok": not errors,
            "zero_worker_failure_and_incident": not errors,
            "distinct_short_long_manifest_sha": not errors,
            "runner_exactly_once_evidence_validation": not errors,
        },
        "errors": errors,
    }
    (output / "audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if compact:
        _write_csv(output / "formal_runs_compact.csv", compact)
    if summaries:
        _write_csv(output / "scenario_summary.csv", summaries)
        _write_csv(output / "pairwise_comparison.csv", comparisons)
    return not errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        passed = summarize(args.matrix_root, args.output)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
