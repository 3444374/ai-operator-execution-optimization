#!/usr/bin/env python3
"""Fail-closed summary for the two-Job release-only priority reachability gate."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from pathlib import Path


EXPECTED = {
    "active_set_static_partition": "static_partition",
    "active_set_saor_release": "saor_release",
    "active_set_foreground_strict_priority": "foreground_strict_priority",
}
CREDIT_POLICIES = {"saor_release", "foreground_strict_priority"}
FOREGROUND_P99_LIMIT_S = 30.7
FOREGROUND_SLO_VIOLATION_LIMIT = 0.01


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
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


def _integer_array(row: dict[str, str], key: str, count: int) -> list[int]:
    values = json.loads(row[key])
    if (
        not isinstance(values, list)
        or len(values) != count
        or any(
            not isinstance(value, int) or isinstance(value, bool)
            for value in values
        )
    ):
        raise ValueError(f"{key} must contain {count} integers")
    return values


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def summarize(root: Path, output: Path) -> dict[str, object]:
    """Validate the frozen matrix and decide release-only reachability."""

    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("matrix manifest.json is missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = _read(root / "group_runs.csv")
    errors: list[str] = []
    if manifest.get("status") != "completed" or manifest.get("incidents"):
        errors.append("runner manifest is not cleanly completed")
    observed = {
        row.get("scenario_id", ""): row.get("policy", "") for row in rows
    }
    if observed != EXPECTED:
        errors.append("scenario matrix does not match the frozen reachability gate")

    formal_by_scenario: dict[str, list[dict[str, str]]] = {}
    summary_rows: list[dict[str, object]] = []
    for scenario_id, policy in EXPECTED.items():
        scenario_rows = [
            row for row in rows if row.get("scenario_id") == scenario_id
        ]
        warmups = [row for row in scenario_rows if row.get("phase") == "warmup"]
        formals = [row for row in scenario_rows if row.get("phase") == "formal"]
        if len(warmups) != 1 or len(formals) != 3:
            errors.append(f"{scenario_id} does not contain 1 warmup + 3 formal")
        formal_by_scenario[scenario_id] = formals
        for row in formals:
            repeat = row.get("repeat_index", "")
            if row.get("policy") != policy:
                errors.append(f"{scenario_id} repeat {repeat} policy mismatch")
            if int(row.get("incidents", "-1")) != 0:
                errors.append(f"{scenario_id} repeat {repeat} contains an incident")
            if row.get("metrics_status") != "ok":
                errors.append(f"{scenario_id} repeat {repeat} lacks model metrics")
            if row.get("resource_metrics_status") != "ok":
                errors.append(f"{scenario_id} repeat {repeat} lacks resource metrics")
            if int(row.get("actor_worker_failures", "-1")) != 0:
                errors.append(f"{scenario_id} repeat {repeat} has actor failures")
            if row.get("active_set_lifecycle_passed", "").lower() != "true":
                errors.append(f"{scenario_id} repeat {repeat} failed lifecycle gate")
            try:
                arrived = _integer_array(row, "job_arrived_rows", 2)
                completed = _integer_array(row, "job_completed_rows", 2)
                failed = _integer_array(row, "job_failed_rows", 2)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                arrived, completed, failed = [], [], []
            if not arrived or arrived != completed or failed != [0, 0]:
                errors.append(
                    f"{scenario_id} repeat {repeat} failed exactly-once gate"
                )
            mechanism_applicable = (
                row.get("active_set_mechanism_applicable", "").lower() == "true"
            )
            mechanism_passed = (
                row.get("active_set_mechanism_passed", "").lower() == "true"
            )
            if policy in CREDIT_POLICIES and not (
                mechanism_applicable and mechanism_passed
            ):
                errors.append(f"{scenario_id} repeat {repeat} failed mechanism gate")
            if policy not in CREDIT_POLICIES and mechanism_applicable:
                errors.append(f"{scenario_id} repeat {repeat} emitted credit trace")
            try:
                priorities = _integer_array(row, "job_priorities", 2)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                priorities = []
            expected_priorities = (
                [0, 1] if policy == "foreground_strict_priority" else [0, 0]
            )
            if priorities != expected_priorities:
                errors.append(
                    f"{scenario_id} repeat {repeat} priority action is not auditable"
                )
        if len(formals) == 3:
            foreground_p99 = [
                _array(row, "job_p99_s", 2)[1] for row in formals
            ]
            foreground_slo = [
                _array(row, "job_slo_violation_ratio", 2)[1]
                for row in formals
            ]
            throughput = [float(row["tokens_per_s"]) for row in formals]
            summary_rows.append(
                {
                    "scenario_id": scenario_id,
                    "policy": policy,
                    "formal_repeats": 3,
                    "tokens_per_s_mean": statistics.fmean(throughput),
                    "tokens_per_s_cv": (
                        statistics.stdev(throughput)
                        / statistics.fmean(throughput)
                    ),
                    "foreground_p99_s_mean": statistics.fmean(foreground_p99),
                    "foreground_p99_s_max": max(foreground_p99),
                    "foreground_slo_violation_mean": statistics.fmean(
                        foreground_slo
                    ),
                    "foreground_slo_violation_max": max(foreground_slo),
                }
            )

    static_rows = formal_by_scenario.get("active_set_static_partition", [])
    saor_rows = formal_by_scenario.get("active_set_saor_release", [])
    priority_rows = formal_by_scenario.get(
        "active_set_foreground_strict_priority", []
    )
    observed_priority_action = (
        _integer_array(priority_rows[0], "job_priorities", 2)
        if priority_rows
        else []
    )

    def foreground_mean(
        selected: list[dict[str, str]],
        field: str,
    ) -> float:
        return (
            statistics.fmean(_array(row, field, 2)[1] for row in selected)
            if len(selected) == 3
            else math.nan
        )

    static_p99 = foreground_mean(static_rows, "job_p99_s")
    saor_p99 = foreground_mean(saor_rows, "job_p99_s")
    priority_p99 = foreground_mean(priority_rows, "job_p99_s")
    priority_slo = foreground_mean(
        priority_rows,
        "job_slo_violation_ratio",
    )
    if math.isfinite(priority_p99) and priority_p99 > FOREGROUND_P99_LIMIT_S:
        errors.append(
            "strict-priority foreground P99 does not reach the preregistered limit: "
            f"{priority_p99:.6f}s > {FOREGROUND_P99_LIMIT_S:.6f}s"
        )
    if (
        math.isfinite(priority_slo)
        and priority_slo > FOREGROUND_SLO_VIOLATION_LIMIT
    ):
        errors.append(
            "strict-priority foreground SLO violation does not reach the "
            "preregistered limit: "
            f"{priority_slo:.6f} > {FOREGROUND_SLO_VIOLATION_LIMIT:.6f}"
        )

    output.mkdir(parents=True, exist_ok=True)
    if summary_rows:
        _write_csv(output / "reachability_summary.csv", summary_rows)
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "scope": "foreground_strict_priority_release_only_upper_bound",
        "release_only_reachability": "passed" if not errors else "failed",
        "strict_priority_job_priorities": observed_priority_action,
        "foreground_p99_limit_s": FOREGROUND_P99_LIMIT_S,
        "foreground_slo_violation_limit": FOREGROUND_SLO_VIOLATION_LIMIT,
        "static_foreground_p99_s_mean": static_p99,
        "saor_foreground_p99_s_mean": saor_p99,
        "strict_priority_foreground_p99_s_mean": priority_p99,
        "strict_priority_foreground_slo_violation_mean": priority_slo,
        "throughput_is_context_not_a_reachability_gate": True,
        "claim_boundary": (
            "diagnoses the upper bound of non-preemptive release ordering; "
            "does not establish a SAOR or reservation-policy win"
        ),
        "errors": errors,
    }
    (output / "validation.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    if errors:
        raise ValueError("; ".join(errors))
    return payload


def main() -> int:
    args = _args()
    summarize(args.matrix_root.resolve(), args.output_dir.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
