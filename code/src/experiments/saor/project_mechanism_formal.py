"""Frozen contracts for the Project SAOR mechanism formal matrix.

This module deliberately separates evidence validity from a performance claim:
an experiment can be valid while the proposed selector fails its Pareto gate.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from pathlib import Path

from src.experiments.scenarios.core import build_scenario_schedule
from src.experiments.shared_vllm.config import SharedVllmConfig
from src.experiments.shared_vllm.metrics import (
    completion_accounted_service_fairness,
)


STATIC = "active_set_project_frozen_static"
FIFO = "active_set_project_bounded_ready_fifo"
DRR = "active_set_project_bounded_ready_drr"
VTC = "active_set_project_bounded_ready_vtc_style"
STRICT_PRIORITY = "active_set_project_bounded_ready_strict_priority"
PROPOSED = "active_set_project_bounded_ready_guarded_debt_0125we"

EXPECTED_SCENARIOS = {
    STATIC: ("static_partition", "single_head"),
    FIFO: ("shared_fifo", "bounded_concrete_pre_registration"),
    DRR: ("shared_drr", "bounded_concrete_pre_registration"),
    VTC: ("external_vtc", "bounded_concrete_pre_registration"),
    STRICT_PRIORITY: (
        "foreground_strict_priority",
        "bounded_concrete_pre_registration",
    ),
    PROPOSED: ("saor_bounded_ready", "bounded_concrete_pre_registration"),
}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_contract(path: Path) -> dict[str, object]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Project mechanism contract must be a JSON object")
    return payload


def validate_contract(
    payload: dict[str, object],
    config: SharedVllmConfig,
    *,
    formal_run: bool,
) -> list[str]:
    """Return every fail-closed contract violation."""

    errors: list[str] = []
    if payload.get("schema_version") != 1:
        errors.append("mechanism contract schema_version must be 1")
    if payload.get("experiment_id") != config.experiment_id:
        errors.append("mechanism contract experiment_id does not match config")
    if payload.get("readiness_profile") != "matched_ready_selector_ablation":
        errors.append("mechanism contract readiness profile is invalid")
    try:
        contract_warmups = int(payload.get("warmup_runs_per_scenario", -1))
        contract_repeats = int(payload.get("formal_repeats", -1))
    except (TypeError, ValueError):
        contract_warmups = -1
        contract_repeats = -1
    if contract_warmups != 1:
        errors.append("mechanism contract requires one warm-up per scenario")
    if contract_repeats != 3:
        errors.append("mechanism contract requires three formal repeats")
    if config.warmup_runs_per_scenario != 1 or config.formal_repeats != 3:
        errors.append("mechanism config must freeze exactly 1 warm-up + 3 formal")

    observed = {
        scenario.scenario_id: (
            scenario.policy,
            scenario.ready_observation_contract,
        )
        for scenario in config.scenarios
    }
    if observed != EXPECTED_SCENARIOS:
        errors.append("mechanism config does not match the frozen six-arm matrix")

    schedule = build_scenario_schedule(
        tuple(EXPECTED_SCENARIOS),
        config.warmup_runs_per_scenario,
        config.formal_repeats,
        config.seed,
    )
    positions: dict[str, list[int]] = {
        scenario_id: [] for scenario_id in EXPECTED_SCENARIOS
    }
    for scheduled in schedule:
        if scheduled.phase == "formal":
            positions[scheduled.scenario_id].append(
                scheduled.order_index % len(EXPECTED_SCENARIOS)
            )
    if any(len(values) != 3 or len(set(values)) != 3 for values in positions.values()):
        errors.append(
            "formal schedule is not position-balanced across the three repeats"
        )

    decision = payload.get("decision_contract")
    if not isinstance(decision, dict):
        errors.append("mechanism contract lacks decision_contract")
    else:
        if decision.get("reference_scenario_id") != VTC:
            errors.append("formal reference must be bounded-ready VTC-style")
        if decision.get("primary_rule") != "any_headline_and_all_protected":
            errors.append("mechanism primary decision rule is invalid")
        numeric_paths = (
            ("headline_relative_improvement_min", 0.05),
            ("throughput_ratio_min", 0.95),
            ("bulk_jct_ratio_max", 1.05),
            ("bulk_slo_violation_delta_max", 0.05),
            ("foreground_slo_violation_max", 0.01),
            ("longest_no_service_ratio_max", 1.05),
            ("longest_no_service_absolute_max_s", 30.0),
            ("debt_repayment_p95_max_s", 30.0),
        )
        for key, expected in numeric_paths:
            try:
                value = float(decision.get(key))
            except (TypeError, ValueError):
                errors.append(f"decision_contract.{key} must be numeric")
                continue
            if not math.isclose(value, expected, rel_tol=0.0, abs_tol=1e-12):
                errors.append(
                    f"decision_contract.{key} drifted from frozen {expected}"
                )
        if decision.get("require_each_repeat_headline_nonnegative") is not True:
            errors.append("headline direction must hold in every formal repeat")

    authorized = payload.get("formal_authorized") is True
    if authorized:
        if payload.get("status") != "formal_ready":
            errors.append("authorized contract status must be formal_ready")
        rehearsal = payload.get("rehearsal_validation")
        if not isinstance(rehearsal, dict) or not rehearsal.get("sha256"):
            errors.append("formal authorization requires frozen rehearsal evidence")
    if formal_run and not authorized:
        errors.append("formal run is not authorized by the frozen contract")
    return errors


def contract_snapshot(
    path: Path,
    payload: dict[str, object],
    readiness: dict[str, object],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "contract_path": str(path.resolve()),
        "contract_sha256": sha256_file(path.resolve()),
        "contract": payload,
        "readiness": readiness,
    }


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def completion_fairness_from_raw(
    root: Path,
    row: dict[str, str],
    *,
    job_count: int = 2,
    weights: tuple[int, ...] = (1, 1),
) -> dict[str, float | int | str]:
    """Replay completion-granularity fairness from joined raw traces."""

    unavailable = [
        {"ready_lifecycle_complete": False, "ready_lifecycle_rows": []}
        for _index in range(job_count)
    ]
    if not (root / "jobs").is_dir():
        return completion_accounted_service_fairness(unavailable, weights)
    evidence = []
    order = int(row["order_index"])
    phase = row["phase"]
    repeat = int(row["repeat_index"])
    scenario = row["scenario_id"]
    for job_index in range(job_count):
        stem = f"{order:03d}_{phase}_{repeat}_{scenario}_job{job_index}"
        request_path = root / "jobs" / f"{stem}.requests.csv"
        submission_path = root / "jobs" / f"{stem}.submissions.csv"
        if not request_path.is_file() or not submission_path.is_file():
            return completion_accounted_service_fairness(unavailable, weights)
        requests = read_csv(request_path)
        submissions = read_csv(submission_path)
        service_by_id: dict[str, tuple[float, int]] = {}
        for request in requests:
            submission_id = str(request.get("submission_id", "") or "")
            actual_output = request.get("actual_output_tokens", "")
            output_work = int(
                actual_output
                if actual_output not in (None, "")
                else request.get("client_estimated_output_tokens", "")
                or request["estimated_output_tokens"]
            )
            service_by_id[submission_id] = (
                float(request["completion_epoch_s"]),
                int(request["prompt_tokens"]) + output_work,
            )
        lifecycle = []
        for submission in submissions:
            ready = submission.get("ready_epoch_s", "")
            registered = submission.get("credit_registered_epoch_s", "")
            granted = submission.get("credit_granted_epoch_s", "")
            if not ready and not registered and not granted:
                continue
            submission_id = str(submission.get("submission_id", "") or "")
            if (
                not ready
                or not registered
                or not granted
                or submission_id not in service_by_id
            ):
                raise ValueError(
                    f"{stem} has an incomplete registered-ready service join"
                )
            completion, work = service_by_id[submission_id]
            lifecycle.append(
                {
                    "registered_epoch_s": float(registered),
                    "completion_epoch_s": completion,
                    "actual_work": work,
                }
            )
        evidence.append(
            {
                "ready_lifecycle_complete": (
                    bool(requests) and len(lifecycle) == len(requests)
                ),
                "ready_lifecycle_rows": lifecycle,
            }
        )
    return completion_accounted_service_fairness(evidence, weights)
