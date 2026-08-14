"""Matched bounded-direct feeding ceiling for the SAOR mechanism matrix."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from src.experiments.shared_vllm.config import SharedVllmConfig

from .project_mechanism_formal import PROPOSED


CEILING_SCENARIO = "active_set_project_direct_bounded_ceiling"


def validate_ceiling_config(
    reference: SharedVllmConfig,
    ceiling: SharedVllmConfig,
) -> list[str]:
    """Require the direct ceiling to differ only in scheduler ownership."""

    errors: list[str] = []
    exact_fields = (
        "endpoint_ids",
        "service_signature",
        "service_metadata",
        "request_limit_per_endpoint",
        "work_limit_per_endpoint",
        "credit_quantum",
        "gpu_peak_tflops",
        "mfu_precision",
        "common_args",
        "calibration_contract",
        "ready_payload_bytes_limit_per_job",
        "job_internal_arrival_contract",
    )
    for field in exact_fields:
        if getattr(reference, field) != getattr(ceiling, field):
            errors.append(f"feeding ceiling {field} drifted")
    if reference.completion_work_cost != ceiling.completion_work_cost:
        errors.append("feeding ceiling work-cost contract drifted")
    if (
        ceiling.warmup_runs_per_scenario != 1
        or ceiling.formal_repeats != 0
    ):
        errors.append("feeding ceiling must contain exactly one warm-up cell")
    if not ceiling.fail_closed_rehearsal:
        errors.append("feeding ceiling must fail closed")
    if len(ceiling.scenarios) != 1:
        errors.append("feeding ceiling must contain exactly one scenario")
        return errors
    direct = ceiling.scenarios[0]
    if direct.scenario_id != CEILING_SCENARIO:
        errors.append("feeding ceiling scenario identity drifted")
    if direct.policy != "direct_no_job":
        errors.append("feeding ceiling scheduler owner must be direct_no_job")
    try:
        proposed = next(
            scenario
            for scenario in reference.scenarios
            if scenario.scenario_id == PROPOSED
        )
    except StopIteration:
        errors.append("reference config lacks the frozen SAOR scenario")
        return errors
    comparable = (
        "job_count",
        "rows_per_job",
        "rows_per_jobs",
        "arrival_offsets_s",
        "source_row_offsets",
        "request_manifests",
    )
    for field in comparable:
        if getattr(proposed, field) != getattr(direct, field):
            errors.append(f"feeding ceiling scenario {field} drifted")
    if direct.ready_observation_contract != "single_head":
        errors.append("feeding ceiling must not use bounded-ready observation")
    return errors


def summarize_feeding_ceiling(
    project_root: Path,
    ceiling_root: Path,
    *,
    ratio_min: float = 0.95,
) -> dict[str, object]:
    """Validate one ceiling cell and compare it with the sealed SAOR cell."""

    project_rows = _read_csv(project_root / "group_runs.csv")
    ceiling_rows = _read_csv(ceiling_root / "group_runs.csv")
    errors: list[str] = []
    proposed = [row for row in project_rows if row.get("scenario_id") == PROPOSED]
    if len(proposed) != 1:
        errors.append("project root must contain exactly one SAOR rehearsal cell")
    if len(ceiling_rows) != 1:
        errors.append("ceiling root must contain exactly one result cell")
    if errors:
        return {
            "schema_version": 1,
            "status": "invalid_evidence",
            "evidence_valid": False,
            "feeding_gate_passed": False,
            "errors": errors,
        }
    project = proposed[0]
    ceiling = ceiling_rows[0]
    if ceiling.get("scenario_id") != CEILING_SCENARIO:
        errors.append("ceiling result scenario identity drifted")
    if ceiling.get("policy") != "direct_no_job":
        errors.append("ceiling result scheduler owner drifted")
    for field, expected in (
        ("phase", "warmup"),
        ("execution_mode", "rehearsal"),
        ("metrics_status", "ok"),
        ("resource_metrics_status", "ok"),
    ):
        if ceiling.get(field) != expected:
            errors.append(f"ceiling result {field} is invalid")
    for field in (
        "request_manifest_sha256",
        "arrival_offsets_s",
        "job_arrived_rows",
        "job_completed_rows",
        "job_failed_rows",
        "request_success_delta",
        "prompt_tokens_delta",
    ):
        if ceiling.get(field) != project.get(field):
            errors.append(f"ceiling result {field} does not match SAOR")
    if ceiling.get("job_exactly_once") != "[true, true]":
        errors.append("ceiling result is not exactly-once")
    try:
        project_tokens_per_s = float(project["tokens_per_s"])
        ceiling_tokens_per_s = float(ceiling["tokens_per_s"])
    except (KeyError, TypeError, ValueError):
        errors.append("feeding throughput is unavailable")
        project_tokens_per_s = 0.0
        ceiling_tokens_per_s = 0.0
    if project_tokens_per_s <= 0 or ceiling_tokens_per_s <= 0:
        errors.append("feeding throughput must be positive")
    evidence_valid = not errors
    ratio = (
        project_tokens_per_s / ceiling_tokens_per_s
        if evidence_valid
        else None
    )
    gate_passed = bool(ratio is not None and ratio >= ratio_min)
    return {
        "schema_version": 1,
        "status": (
            "passed"
            if gate_passed
            else "failed_feeding"
            if evidence_valid
            else "invalid_evidence"
        ),
        "evidence_valid": evidence_valid,
        "feeding_gate_passed": gate_passed,
        "ratio_min": ratio_min,
        "project_scenario_id": PROPOSED,
        "ceiling_scenario_id": CEILING_SCENARIO,
        "project_tokens_per_s": project_tokens_per_s,
        "ceiling_tokens_per_s": ceiling_tokens_per_s,
        "feeding_ratio": ratio,
        "errors": errors,
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_summary(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
