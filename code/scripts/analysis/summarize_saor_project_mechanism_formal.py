#!/usr/bin/env python3
"""Validate and evaluate the frozen Project SAOR mechanism formal matrix."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.saor.project_mechanism_formal import (  # noqa: E402
    DRR,
    EXPECTED_SCENARIOS,
    FIFO,
    PROPOSED,
    STATIC,
    STRICT_PRIORITY,
    VTC,
    completion_fairness_from_raw,
    load_contract,
    read_csv,
    sha256_file,
)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-root", required=True, type=Path)
    parser.add_argument("--evaluation-contract", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def _truth(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true"}


def _array(row: dict[str, str], key: str) -> list[float]:
    values = json.loads(row[key])
    if not isinstance(values, list) or len(values) != 2:
        raise ValueError(f"{key} must contain exactly two values")
    resolved = [float(value) for value in values]
    if any(not math.isfinite(value) for value in resolved):
        raise ValueError(f"{key} contains non-finite values")
    return resolved


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _cell_metrics(
    root: Path,
    row: dict[str, str],
    *,
    expected_phase: str = "formal",
    expected_execution_mode: str = "configured_matrix",
) -> tuple[dict[str, object], list[str]]:
    scenario_id = row["scenario_id"]
    expected_policy, observation_contract = EXPECTED_SCENARIOS[scenario_id]
    bounded = observation_contract == "bounded_concrete_pre_registration"
    proposed = scenario_id == PROPOSED
    errors: list[str] = []
    arrived = _array(row, "job_arrived_rows")
    completed = _array(row, "job_completed_rows")
    failed = _array(row, "job_failed_rows")
    jct = _array(row, "job_jct_s")
    p99 = _array(row, "job_p99_s")
    slo = _array(row, "job_slo_violation_ratio")
    slo_goodput = _array(row, "job_slo_goodput_per_s")
    token_goodput = _array(row, "job_slo_token_goodput_per_s")
    correctness = bool(
        row.get("policy") == expected_policy
        and row.get("ready_observation_contract") == observation_contract
        and row.get("phase") == expected_phase
        and row.get("execution_mode") == expected_execution_mode
        and int(row.get("incidents", "-1")) == 0
        and int(row.get("actor_worker_failures", "-1")) == 0
        and row.get("metrics_status") == "ok"
        and row.get("resource_metrics_status") == "ok"
        and _truth(row.get("active_set_lifecycle_passed"))
        and arrived == completed
        and failed == [0.0, 0.0]
    )
    observation = bool(
        not bounded
        or (
            row.get("bounded_ready_event_status") == "ok:actor_event_join"
            and _truth(row.get("bounded_ready_lifecycle_complete"))
            and int(row.get("bounded_ready_jobs_with_intervals", "0")) == 2
            and int(row.get("bounded_ready_intervals", "0")) >= 2
            and int(row.get("bounded_ready_max_ready_requests_seen", "0")) >= 2
            and int(row.get("bounded_ready_max_ready_work_seen", "0")) > 0
            and int(
                row.get("bounded_ready_max_ready_payload_bytes_seen", "0")
            )
            > 0
        )
    )
    fairness = completion_fairness_from_raw(root, row)
    fairness_available = bool(
        fairness["completion_service_lag_status"]
        == "ok:registered_backlog_completion_accounted_empirical"
    )
    fairness_applicable = bounded
    fairness_gate = not fairness_applicable or fairness_available
    mechanism = bool(
        not proposed
        or (
            row.get("bounded_saor_event_status") == "ok:lossless_ledger"
            and _truth(row.get("bounded_saor_event_sequence_complete"))
            and int(row.get("bounded_saor_slo_priority_grants", "0")) >= 1
            and int(row.get("bounded_saor_debt_recovery_grants", "0")) >= 1
            and int(row.get("bounded_saor_recovery_completions", "0")) >= 1
            and int(row.get("bounded_saor_unmatched_recovery_grants", "-1"))
            == 0
            and int(row.get("bounded_saor_debt_repayment_episodes", "0")) >= 1
            and int(row.get("bounded_saor_debt_repayment_completed", "0")) >= 1
            and int(row.get("bounded_saor_debt_repayment_completed", "0"))
            + int(
                row.get(
                    "bounded_saor_debt_repayment_censored_no_demand",
                    "0",
                )
            )
            == int(row.get("bounded_saor_debt_repayment_episodes", "-1"))
            and int(row.get("bounded_saor_debt_repayment_unresolved", "-1"))
            == 0
            and row.get("bounded_saor_projection_status")
            == "ok:offline_recomputed"
            and int(row.get("bounded_saor_projection_checked_events", "0"))
            >= 1
            and int(row.get("bounded_saor_projection_checked_events", "-1"))
            == int(row.get("bounded_saor_projection_expected_events", "-2"))
            and int(row.get("bounded_saor_projection_violation_events", "-1"))
            == 0
            and int(
                row.get(
                    "bounded_saor_projected_overshoot_bound_violation_events",
                    "-1",
                )
            )
            == 0
            and int(
                row.get(
                    "bounded_saor_projection_estimation_overrun_events",
                    "-1",
                )
            )
            == 0
            and int(
                row.get(
                    "bounded_saor_recovery_estimation_overrun_events",
                    "-1",
                )
            )
            == 0
            and float(
                row.get("bounded_saor_recovery_inflight_work_max", "0")
            )
            > 0
            and int(row.get("bounded_saor_avoidable_idle_events", "-1")) == 0
            and int(
                row.get(
                    "bounded_saor_foreign_grant_over_debt_critical_events",
                    "-1",
                )
            )
            == 0
            and int(row.get("bounded_ready_foreign_fallback_events", "-1"))
            == 0
            and row.get("active_set_post_drain_status")
            in {
                "ok:observed_work_conserving_drain",
                "not_applicable:drain_below_trace_resolution",
            }
            and (
                not _truth(row.get("active_set_post_drain_applicable"))
                or _truth(row.get("active_set_post_work_conserving_passed"))
            )
        )
    )
    evidence_passed = correctness and observation and fairness_gate and mechanism
    if not evidence_passed:
        errors.append(
            f"repeat {row.get('repeat_index')} {scenario_id} failed formal "
            "correctness, observation, fairness, or mechanism evidence"
        )
    metrics: dict[str, object] = {
        "scenario_id": scenario_id,
        "policy": expected_policy,
        "phase": row["phase"],
        "repeat_index": int(row["repeat_index"]),
        "order_index": int(row["order_index"]),
        "correctness_passed": correctness,
        "observation_passed": observation,
        "fairness_evidence_applicable": fairness_applicable,
        "fairness_evidence_passed": fairness_available,
        "fairness_gate_passed": fairness_gate,
        "mechanism_passed": mechanism,
        "cell_evidence_passed": evidence_passed,
        "tokens_per_s": float(row["tokens_per_s"]),
        "group_jct_s": float(row["duration_s"]),
        "mfu_fraction": float(row["mfu_estimate"]),
        "bulk_jct_s": jct[0],
        "foreground_jct_s": jct[1],
        "bulk_p99_s": p99[0],
        "foreground_p99_s": p99[1],
        "bulk_slo_violation": slo[0],
        "foreground_slo_violation": slo[1],
        "bulk_slo_goodput_per_s": slo_goodput[0],
        "foreground_slo_goodput_per_s": slo_goodput[1],
        "bulk_slo_token_goodput_per_s": token_goodput[0],
        "foreground_slo_token_goodput_per_s": token_goodput[1],
        "jain_fairness": float(row.get("jain_fairness", "nan")),
        **fairness,
        "bounded_saor_recovery_completion_p95_s": float(
            row.get("bounded_saor_recovery_completion_p95_s", "0") or 0
        ),
        "bounded_saor_debt_repayment_p95_s": float(
            row.get("bounded_saor_debt_repayment_p95_s", "0") or 0
        ),
        "bounded_saor_debt_repayment_unresolved": int(
            row.get("bounded_saor_debt_repayment_unresolved", "0") or 0
        ),
        "bounded_saor_debt_repayment_completed": int(
            row.get("bounded_saor_debt_repayment_completed", "0") or 0
        ),
        "bounded_saor_debt_repayment_censored_no_demand": int(
            row.get(
                "bounded_saor_debt_repayment_censored_no_demand",
                "0",
            )
            or 0
        ),
        "bounded_saor_recovery_inflight_work_max": float(
            row.get("bounded_saor_recovery_inflight_work_max", "0") or 0
        ),
        "bounded_saor_recovery_inflight_work_at_repayment_max": float(
            row.get(
                "bounded_saor_recovery_inflight_work_at_repayment_max",
                "0",
            )
            or 0
        ),
        "bounded_saor_debt_repayment_overshoot_work_max": float(
            row.get(
                "bounded_saor_debt_repayment_overshoot_work_max",
                "0",
            )
            or 0
        ),
        "bounded_saor_projected_overshoot_work_max": float(
            row.get("bounded_saor_projected_overshoot_work_max", "0") or 0
        ),
        "bounded_saor_projected_overshoot_bound_max": float(
            row.get(
                "bounded_saor_projected_overshoot_bound_max",
                "0",
            )
            or 0
        ),
        "bounded_saor_projection_violation_events": int(
            row.get("bounded_saor_projection_violation_events", "0") or 0
        ),
        "bounded_saor_projected_overshoot_bound_violation_events": int(
            row.get(
                "bounded_saor_projected_overshoot_bound_violation_events",
                "0",
            )
            or 0
        ),
        "bounded_saor_recovery_estimation_overrun_events": int(
            row.get(
                "bounded_saor_recovery_estimation_overrun_events",
                "0",
            )
            or 0
        ),
        "bounded_saor_projection_estimation_overrun_events": int(
            row.get(
                "bounded_saor_projection_estimation_overrun_events",
                "0",
            )
            or 0
        ),
    }
    return metrics, errors


def evaluate_decision(
    metrics: list[dict[str, object]],
    decision: dict[str, object],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    by_scenario: dict[str, list[dict[str, object]]] = {
        scenario_id: sorted(
            (row for row in metrics if row["scenario_id"] == scenario_id),
            key=lambda row: int(row["repeat_index"]),
        )
        for scenario_id in EXPECTED_SCENARIOS
    }
    proposed = by_scenario[PROPOSED]
    reference = by_scenario[VTC]

    def ratio(numerator: object, denominator: object, name: str) -> float:
        resolved_denominator = float(denominator)
        if resolved_denominator <= 0:
            raise ValueError(f"{name} reference must be positive")
        return float(numerator) / resolved_denominator

    paired = []
    for proposed_row, reference_row in zip(proposed, reference):
        paired.append(
            {
                "repeat_index": proposed_row["repeat_index"],
                "foreground_p99_relative_improvement": 1.0
                - ratio(
                    proposed_row["foreground_p99_s"],
                    reference_row["foreground_p99_s"],
                    "foreground P99",
                ),
                "service_lag_p95_relative_improvement": 1.0
                - ratio(
                    proposed_row["completion_service_lag_p95_work"],
                    reference_row["completion_service_lag_p95_work"],
                    "completion service lag P95",
                ),
                "throughput_ratio": ratio(
                    proposed_row["tokens_per_s"],
                    reference_row["tokens_per_s"],
                    "throughput",
                ),
                "bulk_jct_ratio": ratio(
                    proposed_row["bulk_jct_s"],
                    reference_row["bulk_jct_s"],
                    "bulk JCT",
                ),
                "bulk_slo_violation_delta": (
                    float(proposed_row["bulk_slo_violation"])
                    - float(reference_row["bulk_slo_violation"])
                ),
                "longest_no_service_ratio": ratio(
                    proposed_row["completion_longest_no_service_s"],
                    reference_row["completion_longest_no_service_s"],
                    "longest no-service interval",
                ),
            }
        )

    def mean(key: str) -> float:
        return statistics.fmean(float(row[key]) for row in paired)

    minimum = float(decision["headline_relative_improvement_min"])
    p99_headline = bool(
        mean("foreground_p99_relative_improvement") >= minimum
        and all(
            float(row["foreground_p99_relative_improvement"]) >= 0
            for row in paired
        )
    )
    lag_headline = bool(
        mean("service_lag_p95_relative_improvement") >= minimum
        and all(
            float(row["service_lag_p95_relative_improvement"]) >= 0
            for row in paired
        )
    )
    proposed_longest = max(
        float(row["completion_longest_no_service_s"])
        for row in proposed
    )
    protected = {
        "throughput_noninferior": mean("throughput_ratio")
        >= float(decision["throughput_ratio_min"]),
        "bulk_jct_noninferior": mean("bulk_jct_ratio")
        <= float(decision["bulk_jct_ratio_max"]),
        "bulk_slo_noninferior": mean("bulk_slo_violation_delta")
        <= float(decision["bulk_slo_violation_delta_max"]),
        "foreground_slo_satisfied": max(
            float(row["foreground_slo_violation"]) for row in proposed
        )
        <= float(decision["foreground_slo_violation_max"]),
        "longest_no_service_noninferior": mean("longest_no_service_ratio")
        <= float(decision["longest_no_service_ratio_max"]),
        "longest_no_service_absolute": proposed_longest
        <= float(decision["longest_no_service_absolute_max_s"]),
        "debt_repayment_bounded_empirically": max(
            float(row["bounded_saor_debt_repayment_p95_s"])
            for row in proposed
        )
        <= float(decision["debt_repayment_p95_max_s"]),
        "debt_repayment_resolved": max(
            int(row["bounded_saor_debt_repayment_unresolved"])
            for row in proposed
        )
        <= int(decision["debt_repayment_unresolved_max"]),
        "debt_repayment_observed": min(
            int(row["bounded_saor_debt_repayment_completed"])
            for row in proposed
        )
        >= int(decision["debt_repayment_completed_min"]),
        "projection_recomputed_without_violation": max(
            int(row["bounded_saor_projection_violation_events"])
            for row in proposed
        )
        == 0,
        "discrete_overshoot_bound_satisfied": max(
            int(
                row[
                    "bounded_saor_projected_overshoot_bound_violation_events"
                ]
            )
            for row in proposed
        )
        == 0,
        "projection_estimate_upper_bound_satisfied": max(
            int(row["bounded_saor_projection_estimation_overrun_events"])
            for row in proposed
        )
        == 0,
    }

    mean_rows = {
        scenario_id: {
            key: statistics.fmean(float(row[key]) for row in rows)
            for key in (
                "tokens_per_s",
                "bulk_jct_s",
                "foreground_p99_s",
                "completion_service_lag_p95_work",
                "completion_longest_no_service_s",
            )
        }
        for scenario_id, rows in by_scenario.items()
        if scenario_id != STATIC
    }
    dominated_by = []
    proposed_mean = mean_rows[PROPOSED]
    for scenario_id in (FIFO, DRR, VTC):
        candidate = mean_rows[scenario_id]
        no_worse = bool(
            candidate["tokens_per_s"] >= proposed_mean["tokens_per_s"]
            and candidate["bulk_jct_s"] <= proposed_mean["bulk_jct_s"]
            and candidate["foreground_p99_s"]
            <= proposed_mean["foreground_p99_s"]
            and candidate["completion_service_lag_p95_work"]
            <= proposed_mean["completion_service_lag_p95_work"]
            and candidate["completion_longest_no_service_s"]
            <= proposed_mean["completion_longest_no_service_s"]
        )
        strictly_better = any(
            not math.isclose(candidate[key], proposed_mean[key])
            for key in candidate
        )
        if no_worse and strictly_better:
            dominated_by.append(scenario_id)

    headline = p99_headline or lag_headline
    result: dict[str, object] = {
        "reference_scenario_id": VTC,
        "headline_foreground_p99_passed": p99_headline,
        "headline_service_lag_passed": lag_headline,
        "headline_any_passed": headline,
        **protected,
        "all_protected_passed": all(protected.values()),
        "empirically_nondominated": not dominated_by,
        "dominated_by": dominated_by,
        "claim_gate_passed": (
            headline and all(protected.values()) and not dominated_by
        ),
        "paired_means": {
            key: mean(key) for key in paired[0] if key != "repeat_index"
        },
        "strict_priority_role": "boundary_control_not_fair_comparator",
    }
    return result, paired


def _arm_summary(metrics: list[dict[str, object]]) -> list[dict[str, object]]:
    keys = (
        "tokens_per_s",
        "group_jct_s",
        "mfu_fraction",
        "bulk_jct_s",
        "foreground_jct_s",
        "bulk_p99_s",
        "foreground_p99_s",
        "bulk_slo_violation",
        "foreground_slo_violation",
        "completion_service_lag_p95_work",
        "completion_longest_no_service_s",
    )
    rows = []
    for scenario_id in EXPECTED_SCENARIOS:
        selected = [
            row for row in metrics if row["scenario_id"] == scenario_id
        ]
        if not selected:
            continue
        summary: dict[str, object] = {
            "scenario_id": scenario_id,
            "policy": selected[0]["policy"],
            "formal_repeats": len(selected),
            "fairness_evidence_applicable": selected[0][
                "fairness_evidence_applicable"
            ],
        }
        for key in keys:
            summary[f"{key}_mean"] = statistics.fmean(
                float(row[key]) for row in selected
            )
        rows.append(summary)
    return rows


def rehearsal_safety(
    proposed: dict[str, object],
    decision: dict[str, object],
) -> dict[str, bool]:
    """Apply frozen absolute safety gates without ranking rehearsal arms."""

    return {
        "foreground_slo_satisfied": float(
            proposed["foreground_slo_violation"]
        )
        <= float(decision["foreground_slo_violation_max"]),
        "longest_no_service_absolute": float(
            proposed["completion_longest_no_service_s"]
        )
        <= float(decision["longest_no_service_absolute_max_s"]),
        "debt_repayment_bounded_empirically": float(
            proposed["bounded_saor_debt_repayment_p95_s"]
        )
        <= float(decision["debt_repayment_p95_max_s"]),
        "debt_repayment_resolved": int(
            proposed["bounded_saor_debt_repayment_unresolved"]
        )
        <= int(decision["debt_repayment_unresolved_max"]),
        "debt_repayment_observed": int(
            proposed["bounded_saor_debt_repayment_completed"]
        )
        >= int(decision["debt_repayment_completed_min"]),
        "projection_recomputed_without_violation": int(
            proposed["bounded_saor_projection_violation_events"]
        )
        == 0,
        "projection_estimate_upper_bound_satisfied": int(
            proposed["bounded_saor_projection_estimation_overrun_events"]
        )
        == 0,
    }


def validate_rehearsal_root(
    root: Path,
    contract_path: Path,
) -> dict[str, object]:
    """Validate one six-arm rehearsal without making a performance claim."""

    errors: list[str] = []
    contract = load_contract(contract_path)
    try:
        snapshot = json.loads(
            (root / "project_mechanism_contract.json").read_text(
                encoding="utf-8"
            )
        )
        manifest = json.loads(
            (root / "manifest.json").read_text(encoding="utf-8")
        )
        rows = read_csv(root / "group_runs.csv")
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"rehearsal evidence is unreadable: {exc}") from exc
    if snapshot.get("contract_sha256") != sha256_file(contract_path):
        errors.append("rehearsal contract snapshot does not match input contract")
    if snapshot.get("contract") != contract:
        errors.append("rehearsal contract payload drifted")
    if snapshot.get("readiness", {}).get("status") != "passed":
        errors.append("rehearsal lacks a passed readiness snapshot")
    if (
        manifest.get("status") != "completed"
        or manifest.get("execution_mode") != "rehearsal"
        or manifest.get("incidents")
    ):
        errors.append("rehearsal manifest is incomplete or incident-bearing")
    if len(rows) != len(EXPECTED_SCENARIOS):
        errors.append("rehearsal must contain exactly one cell per frozen arm")
    if {row.get("scenario_id") for row in rows} != set(EXPECTED_SCENARIOS):
        errors.append("rehearsal does not contain the frozen six-arm set")
    metrics: list[dict[str, object]] = []
    for row in rows:
        scenario_id = row.get("scenario_id", "")
        if scenario_id not in EXPECTED_SCENARIOS:
            continue
        cell, cell_errors = _cell_metrics(
            root,
            row,
            expected_phase="warmup",
            expected_execution_mode="rehearsal",
        )
        metrics.append(cell)
        errors.extend(cell_errors)
    proposed = next(
        (row for row in metrics if row["scenario_id"] == PROPOSED),
        None,
    )
    decision = contract.get("decision_contract")
    safety: dict[str, bool] = {}
    if proposed is None or not isinstance(decision, dict):
        errors.append("rehearsal lacks proposed metrics or decision contract")
    else:
        safety = rehearsal_safety(proposed, decision)
        if not all(safety.values()):
            errors.append("rehearsal proposed arm failed a frozen absolute gate")
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "formal_authorized": False,
        "performance_ranking_decided": False,
        "absolute_safety_gates": safety,
        "errors": errors,
    }
    (root / "rehearsal_validation.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    if errors:
        raise ValueError("; ".join(errors))
    return payload


def summarize(
    root: Path,
    contract_path: Path,
    output: Path,
) -> dict[str, object]:
    errors: list[str] = []
    contract = load_contract(contract_path)
    if contract.get("formal_authorized") is not True:
        errors.append("evaluation contract did not authorize the formal run")
    try:
        snapshot = json.loads(
            (root / "project_mechanism_contract.json").read_text(
                encoding="utf-8"
            )
        )
        manifest = json.loads(
            (root / "manifest.json").read_text(encoding="utf-8")
        )
        rows = read_csv(root / "group_runs.csv")
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"formal evidence is unreadable: {exc}") from exc
    if snapshot.get("contract_sha256") != sha256_file(contract_path):
        errors.append("formal root contract snapshot does not match input contract")
    if snapshot.get("contract") != contract:
        errors.append("formal root contract payload drifted")
    if snapshot.get("readiness", {}).get("status") != "passed":
        errors.append("formal root lacks a passed readiness snapshot")
    if (
        manifest.get("status") != "completed"
        or manifest.get("execution_mode") != "configured_matrix"
        or manifest.get("incidents")
    ):
        errors.append("formal manifest is incomplete or incident-bearing")

    warmups = [row for row in rows if row.get("phase") == "warmup"]
    formal = [row for row in rows if row.get("phase") == "formal"]
    if len(warmups) != len(EXPECTED_SCENARIOS):
        errors.append("formal root does not contain exactly six warm-ups")
    for repeat in range(1, 4):
        observed = {
            row.get("scenario_id")
            for row in formal
            if int(row.get("repeat_index", "-1")) == repeat
        }
        if observed != set(EXPECTED_SCENARIOS):
            errors.append(f"formal repeat {repeat} is not a complete six-arm block")
    if len(formal) != 3 * len(EXPECTED_SCENARIOS):
        errors.append("formal root does not contain exactly eighteen formal cells")

    metrics: list[dict[str, object]] = []
    for row in formal:
        scenario_id = row.get("scenario_id", "")
        if scenario_id not in EXPECTED_SCENARIOS:
            errors.append(f"unexpected formal scenario: {scenario_id}")
            continue
        cell, cell_errors = _cell_metrics(root, row)
        metrics.append(cell)
        errors.extend(cell_errors)

    decision_result: dict[str, object] = {"claim_gate_passed": False}
    paired: list[dict[str, object]] = []
    if not errors:
        decision = contract.get("decision_contract")
        if not isinstance(decision, dict):
            errors.append("evaluation contract lacks decision_contract")
        else:
            decision_result, paired = evaluate_decision(metrics, decision)

    output.mkdir(parents=True, exist_ok=True)
    _write(output / "formal_runs.csv", metrics)
    _write(output / "arm_summary.csv", _arm_summary(metrics))
    _write(output / "paired_vtc_comparison.csv", paired)
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "passed" if not errors else "failed",
        "evidence_valid": not errors,
        "experiment_layer": "project_internal_mechanism_formal",
        "native_baseline_count": 0,
        "fairness_scope": "single_tenant_multi_job_differentiated_service",
        "static_fairness_applicability": "not_applicable",
        "claim_language": (
            "baseline-relative empirical constrained Pareto support"
        ),
        **decision_result,
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
    summarize(
        args.matrix_root.resolve(),
        args.evaluation_contract.resolve(),
        args.output_dir.resolve(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
