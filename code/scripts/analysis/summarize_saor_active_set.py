#!/usr/bin/env python3
"""Fail-closed formal summary for fixed-envelope SAOR active-set evaluation."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path


ACTIVE_SCENARIOS = (
    "active_set_direct_no_job",
    "active_set_static_partition",
    "active_set_shared_fifo",
    "active_set_shared_drr",
    "active_set_external_vtc",
    "active_set_saor_release",
)
SOLO_PROJECT = ("solo_project_bulk", "solo_project_foreground")
SOLO_DIRECT = ("solo_direct_bulk", "solo_direct_foreground")
CREDIT_POLICIES = {"shared_fifo", "shared_drr", "external_vtc", "saor_release"}
TRACE_OBSERVATION_INTERVAL_S = 0.25


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--mechanism-only",
        action="store_true",
        help=(
            "replay only the credit-policy mechanism gate from compact "
            "group_runs.csv; does not validate the full formal matrix"
        ),
    )
    return parser.parse_args()


def _read(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _array(row: dict[str, str], key: str, count: int) -> list[float]:
    values = json.loads(row[key])
    if not isinstance(values, list) or len(values) != count:
        raise ValueError(f"{key} must contain {count} values")
    resolved = [float(value) for value in values]
    if any(not math.isfinite(value) for value in resolved):
        raise ValueError(f"{key} contains non-finite values")
    return resolved


def _jain(values: list[float]) -> float:
    if not values or any(value < 0 for value in values):
        raise ValueError("Jain inputs must be non-negative")
    squares = sum(value * value for value in values)
    return sum(values) ** 2 / (len(values) * squares) if squares else 1.0


def _mean(rows: list[dict[str, str]], field: str) -> float:
    return statistics.fmean(float(row[field]) for row in rows)


def _effective_mechanism_gate(
    row: dict[str, str],
    *,
    observation_interval_s: float = TRACE_OBSERVATION_INTERVAL_S,
) -> tuple[bool, str]:
    """Apply the frozen simultaneous-drain rule to legacy compact evidence."""

    if row.get("active_set_mechanism_passed", "").lower() == "true":
        return True, row.get("active_set_mechanism_status", "passed")
    # New runner records carry an explicit post-drain applicability decision.
    # Never let legacy compatibility override a failure from the new schema.
    if row.get("active_set_post_drain_applicable", ""):
        return False, row.get(
            "active_set_mechanism_status",
            "active_set_mechanism_not_observed",
        )
    try:
        offsets = _array(row, "arrival_offsets_s", 2)
        jcts = _array(row, "job_jct_s", 2)
        completion_gap_s = abs(
            (offsets[0] + jcts[0]) - (offsets[1] + jcts[1])
        )
        reclassifiable = bool(
            row.get("active_set_mechanism_applicable", "").lower() == "true"
            and row.get("active_set_lifecycle_passed", "").lower() == "true"
            and row.get("active_set_overlap_reclaim_observed", "").lower()
            == "true"
            and float(row.get("active_set_pre_bulk_dominant_share_max", "0"))
            > 0.5
            and int(row.get("active_set_bulk_only_post_samples", "-1")) == 0
            and int(row.get("active_set_post_fit_violation_samples", "-1")) == 0
            and completion_gap_s < observation_interval_s
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        reclassifiable = False
    if reclassifiable:
        return True, "reclassified:post_drain_below_trace_resolution"
    return False, row.get(
        "active_set_mechanism_status",
        "active_set_mechanism_not_observed",
    )


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def replay_compact_mechanism_gate(root: Path, output: Path) -> dict[str, object]:
    """Replay the mechanism rule without upgrading full-formal validity."""

    rows = [
        row
        for row in _read(root / "group_runs.csv")
        if row.get("phase") == "formal" and row.get("policy") in CREDIT_POLICIES
    ]
    expected = {
        (scenario, repeat)
        for scenario in ACTIVE_SCENARIOS
        for repeat in ("1", "2", "3")
        if scenario
        in {
            "active_set_shared_fifo",
            "active_set_shared_drr",
            "active_set_external_vtc",
            "active_set_saor_release",
        }
    }
    observed = {
        (row.get("scenario_id", ""), row.get("repeat_index", ""))
        for row in rows
    }
    errors = []
    if observed != expected:
        errors.append("compact evidence does not contain the 12 credit formal cells")
    results = []
    for row in sorted(
        rows,
        key=lambda item: (item["scenario_id"], int(item["repeat_index"])),
    ):
        passed, status = _effective_mechanism_gate(row)
        offsets = _array(row, "arrival_offsets_s", 2)
        jcts = _array(row, "job_jct_s", 2)
        results.append(
            {
                "scenario_id": row["scenario_id"],
                "policy": row["policy"],
                "repeat_index": int(row["repeat_index"]),
                "original_mechanism_passed": (
                    row.get("active_set_mechanism_passed", "").lower() == "true"
                ),
                "completion_gap_s": abs(
                    (offsets[0] + jcts[0]) - (offsets[1] + jcts[1])
                ),
                "effective_mechanism_passed": passed,
                "effective_mechanism_status": status,
            }
        )
        if not passed:
            errors.append(
                f"{row['scenario_id']} repeat {row['repeat_index']} still fails"
            )
    payload = {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "scope": "compact_mechanism_gate_only",
        "full_formal_validation_updated": False,
        "trace_observation_interval_s": TRACE_OBSERVATION_INTERVAL_S,
        "errors": errors,
        "results": results,
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "mechanism_gate_replay.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    if errors:
        raise ValueError("; ".join(errors))
    return payload


def summarize(root: Path, output: Path) -> None:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    rows = _read(root / "group_runs.csv")
    expected = set(ACTIVE_SCENARIOS + SOLO_PROJECT + SOLO_DIRECT)
    errors: list[str] = []
    if manifest.get("status") != "completed" or manifest.get("incidents"):
        errors.append("runner manifest is not cleanly completed")
    if {row.get("scenario_id", "") for row in rows} != expected:
        errors.append("scenario set does not match the frozen formal matrix")
    formal: dict[str, list[dict[str, str]]] = {}
    for scenario in sorted(expected):
        selected = [row for row in rows if row.get("scenario_id") == scenario]
        warmups = [row for row in selected if row.get("phase") == "warmup"]
        formals = [row for row in selected if row.get("phase") == "formal"]
        if len(warmups) != 1 or len(formals) != 3:
            errors.append(f"{scenario} does not contain 1 warmup + 3 formal")
        if any(int(row.get("incidents", "-1")) != 0 for row in formals):
            errors.append(f"{scenario} contains a formal incident")
        formal[scenario] = formals
    if errors:
        output.mkdir(parents=True, exist_ok=True)
        (output / "validation.json").write_text(
            json.dumps({"status": "failed", "errors": errors}, indent=2) + "\n",
            encoding="utf-8",
        )
        raise ValueError("; ".join(errors))

    def solo_references(names: tuple[str, str]) -> tuple[list[float], list[float]]:
        jcts = []
        rates = []
        for name in names:
            jct_values = [
                _array(row, "job_jct_s", 1)[0] for row in formal[name]
            ]
            values = [
                _array(row, "job_actual_work", 1)[0]
                / _array(row, "job_jct_s", 1)[0]
                for row in formal[name]
            ]
            jcts.append(statistics.fmean(jct_values))
            rates.append(statistics.fmean(values))
        return jcts, rates

    project_solo_jct, project_solo_rate = solo_references(SOLO_PROJECT)
    direct_solo_jct, direct_solo_rate = solo_references(SOLO_DIRECT)
    formal_gate_errors: list[str] = []
    mechanism_reclassifications: list[dict[str, str]] = []
    summaries = []
    per_job = []
    for scenario in ACTIVE_SCENARIOS:
        scenario_rows = formal[scenario]
        policy = scenario_rows[0]["policy"]
        reference_jct = (
            direct_solo_jct if policy == "direct_no_job" else project_solo_jct
        )
        reference_rate = (
            direct_solo_rate if policy == "direct_no_job" else project_solo_rate
        )
        run_slowdowns: list[list[float]] = []
        for row in scenario_rows:
            if row.get("metrics_status") != "ok":
                formal_gate_errors.append(
                    f"{scenario} repeat {row['repeat_index']} lacks model metrics"
                )
            if row.get("resource_metrics_status") != "ok":
                formal_gate_errors.append(
                    f"{scenario} repeat {row['repeat_index']} lacks resource metrics"
                )
            if int(row.get("actor_worker_failures", "-1")) != 0:
                formal_gate_errors.append(
                    f"{scenario} repeat {row['repeat_index']} has actor failures"
                )
            if row.get("active_set_lifecycle_passed", "").lower() != "true":
                formal_gate_errors.append(
                    f"{scenario} repeat {row['repeat_index']} failed lifecycle gate"
                )
            mechanism_applicable = (
                row.get("active_set_mechanism_applicable", "").lower() == "true"
            )
            mechanism_passed, mechanism_status = _effective_mechanism_gate(row)
            if mechanism_status.startswith("reclassified:"):
                mechanism_reclassifications.append(
                    {
                        "scenario_id": scenario,
                        "repeat_index": row["repeat_index"],
                        "status": mechanism_status,
                    }
                )
            if policy in CREDIT_POLICIES and not mechanism_passed:
                formal_gate_errors.append(
                    f"{scenario} repeat {row['repeat_index']} failed mechanism gate"
                )
            if policy not in CREDIT_POLICIES and mechanism_applicable:
                formal_gate_errors.append(
                    f"{scenario} repeat {row['repeat_index']} emitted credit trace"
                )
            actual_work = _array(row, "job_actual_work", 2)
            jct = _array(row, "job_jct_s", 2)
            progress = [work / duration for work, duration in zip(actual_work, jct)]
            normalized = [
                value / solo for value, solo in zip(progress, reference_rate)
            ]
            slowdowns = [
                duration / solo for duration, solo in zip(jct, reference_jct)
            ]
            jct_progress = [1.0 / value for value in slowdowns]
            run_slowdowns.append(slowdowns)
            for job_index, slowdown in enumerate(slowdowns):
                per_job.append(
                    {
                        "scenario_id": scenario,
                        "policy": policy,
                        "repeat_index": row["repeat_index"],
                        "job_index": job_index,
                        "solo_reference": (
                            "direct" if policy == "direct_no_job" else "project"
                        ),
                        "jct_s": jct[job_index],
                        "actual_work": actual_work[job_index],
                        "solo_normalized_work_rate": normalized[job_index],
                        "solo_normalized_jct_progress": jct_progress[job_index],
                        "jct_slowdown": slowdown,
                    }
                )
        tokens = [float(row["tokens_per_s"]) for row in scenario_rows]
        summaries.append(
            {
                "scenario_id": scenario,
                "policy": policy,
                "formal_repeats": 3,
                "tokens_per_s_mean": statistics.fmean(tokens),
                "tokens_per_s_cv": statistics.stdev(tokens) / statistics.fmean(tokens),
                "max_slowdown_mean": statistics.fmean(
                    max(values) for values in run_slowdowns
                ),
                "foreground_slowdown_mean": statistics.fmean(
                    values[1] for values in run_slowdowns
                ),
                "solo_normalized_jct_progress_jain_mean": statistics.fmean(
                    _jain([1.0 / value for value in values])
                    for values in run_slowdowns
                ),
                "request_p99_s_max_mean": statistics.fmean(
                    max(_array(row, "job_p99_s", 2)) for row in scenario_rows
                ),
                "slo_violation_ratio_mean": statistics.fmean(
                    value
                    for row in scenario_rows
                    for value in _array(row, "job_slo_violation_ratio", 2)
                ),
                "gpu_utilization_pct_mean": _mean(
                    scenario_rows, "gpu_utilization_pct_mean"
                ),
                "vllm_running_mean": _mean(scenario_rows, "vllm_running_mean"),
                "vllm_waiting_mean": _mean(scenario_rows, "vllm_waiting_mean"),
                "vllm_kv_usage_mean": _mean(scenario_rows, "vllm_kv_usage_mean"),
                "lifecycle_pass_rate": statistics.fmean(
                    row["active_set_lifecycle_passed"].lower() == "true"
                    for row in scenario_rows
                ),
                "mechanism_gate": (
                    "required_and_passed"
                    if policy in CREDIT_POLICIES
                    else "not_applicable"
                ),
            }
        )
    if formal_gate_errors:
        output.mkdir(parents=True, exist_ok=True)
        (output / "validation.json").write_text(
            json.dumps(
                {"status": "failed", "errors": formal_gate_errors},
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        raise ValueError("; ".join(formal_gate_errors))
    output.mkdir(parents=True, exist_ok=True)
    _write(output / "formal_summary.csv", summaries)
    _write(output / "per_job_slowdown.csv", per_job)
    (output / "validation.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "full_formal_validation_updated": True,
                "formal_matrix": "fixed-envelope SAOR active-set release",
                "mechanism_gate_evaluation": "resolution_aware_v2",
                "trace_observation_interval_s": TRACE_OBSERVATION_INTERVAL_S,
                "project_solo_jct_s": project_solo_jct,
                "direct_solo_jct_s": direct_solo_jct,
                "project_solo_service_rates": project_solo_rate,
                "direct_solo_service_rates": direct_solo_rate,
                "claim_boundary": (
                    "descriptive efficiency/fairness/SLO comparison; no theorem "
                    "or dynamic-capacity claim"
                ),
                "mechanism_reclassifications": mechanism_reclassifications,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    args = _args()
    if args.mechanism_only:
        replay_compact_mechanism_gate(
            args.matrix_root.resolve(),
            args.output_dir.resolve(),
        )
        return 0
    summarize(args.matrix_root.resolve(), args.output_dir.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
