"""Matched bounded-direct feeding ceiling for the SAOR mechanism matrix."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from src.experiments.shared_vllm.config import SharedVllmConfig

from .project_mechanism_formal import (
    FROZEN_FEEDING_EVIDENCE,
    PROPOSED,
    REVIEWED_REHEARSAL_EVIDENCE,
)


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
    evidence_contract: dict[str, object] | None = None,
    project_archive: Path | None = None,
    ceiling_archive: Path | None = None,
    ratio_min: float = 0.95,
) -> dict[str, object]:
    """Compare only artifacts bound by the frozen evidence contract."""

    errors: list[str] = []
    rehearsal_identity = (
        evidence_contract.get("rehearsal_validation")
        if isinstance(evidence_contract, dict)
        else None
    )
    ceiling_identity = (
        evidence_contract.get("feeding_validation")
        if isinstance(evidence_contract, dict)
        else None
    )
    if not isinstance(rehearsal_identity, dict):
        errors.append("feeding summary lacks frozen rehearsal identity")
    elif rehearsal_identity != REVIEWED_REHEARSAL_EVIDENCE:
        errors.append("feeding summary rehearsal identity is not frozen")
    if not isinstance(ceiling_identity, dict):
        errors.append("feeding summary lacks frozen ceiling identity")
    elif ceiling_identity != FROZEN_FEEDING_EVIDENCE:
        errors.append("feeding summary ceiling identity is not frozen")
    if isinstance(ceiling_identity, dict):
        try:
            frozen_ratio_min = float(ceiling_identity["ratio_min"])
        except (KeyError, TypeError, ValueError):
            errors.append("frozen feeding ratio threshold is invalid")
        else:
            if frozen_ratio_min != ratio_min:
                errors.append("feeding ratio threshold drifted")
    if isinstance(rehearsal_identity, dict):
        errors.extend(
            _project_identity_errors(
                project_root,
                project_archive,
                rehearsal_identity,
            )
        )
    if isinstance(ceiling_identity, dict):
        errors.extend(
            _ceiling_identity_errors(
                ceiling_root,
                ceiling_archive,
                ceiling_identity,
            )
        )
    project_rows = _read_csv(project_root / "group_runs.csv", errors)
    ceiling_rows = _read_csv(ceiling_root / "group_runs.csv", errors)
    proposed = [row for row in project_rows if row.get("scenario_id") == PROPOSED]
    if len(proposed) != 1:
        errors.append("project root must contain exactly one SAOR rehearsal cell")
    if len(ceiling_rows) != 1:
        errors.append("ceiling root must contain exactly one result cell")
    if errors:
        return {
            "schema_version": 2,
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
        "schema_version": 2,
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
        "evidence_scope": "sealed_artifact_identity_and_feeding_arithmetic",
        "pre_run_clean_gate_evidence_status": (
            "unavailable:not_structured_for_postgresql_and_ray"
        ),
        "paper_reproducibility_complete": False,
        "artifact_identity": {
            "project_root_id": (
                rehearsal_identity.get("root_id")
                if isinstance(rehearsal_identity, dict)
                else None
            ),
            "ceiling_root_id": (
                ceiling_identity.get("root_id")
                if isinstance(ceiling_identity, dict)
                else None
            ),
            "project_group_runs_sha256": _optional_sha256(
                project_root / "group_runs.csv"
            ),
            "ceiling_group_runs_sha256": _optional_sha256(
                ceiling_root / "group_runs.csv"
            ),
            "project_archive_sha256": _optional_sha256(project_archive),
            "ceiling_archive_sha256": _optional_sha256(ceiling_archive),
        },
        "errors": errors,
    }


def _project_identity_errors(
    root: Path,
    archive: Path | None,
    expected: dict[str, object],
) -> list[str]:
    errors = _common_identity_errors(root, archive, expected, "project")
    snapshot = _checked_json(
        root / "project_mechanism_contract.json",
        expected.get("contract_snapshot_sha256"),
        "project contract snapshot",
        errors,
    )
    if snapshot is not None and (
        snapshot.get("contract_sha256") != expected.get("run_contract_sha256")
    ):
        errors.append("project run contract SHA drifted")
    validation_path = root / "rehearsal_validation.json"
    if _optional_sha256(validation_path) != expected.get("validation_sha256"):
        errors.append("project rehearsal validation SHA drifted")
    validation = _read_json(validation_path, "project rehearsal validation", errors)
    if validation is not None:
        if validation.get("status") != "passed":
            errors.append("project rehearsal validation did not pass")
        if validation.get("errors") != []:
            errors.append("project rehearsal validation contains errors")
    return errors


def _ceiling_identity_errors(
    root: Path,
    archive: Path | None,
    expected: dict[str, object],
) -> list[str]:
    errors = _common_identity_errors(root, archive, expected, "ceiling")
    snapshot = _checked_json(
        root / "feeding_ceiling_contract.json",
        expected.get("contract_snapshot_sha256"),
        "ceiling contract snapshot",
        errors,
    )
    if snapshot is not None:
        for field in (
            "reference_contract_sha256",
            "reference_config_sha256",
            "ceiling_config_sha256",
        ):
            if snapshot.get(field) != expected.get(field):
                errors.append(f"ceiling snapshot {field} drifted")
    return errors


def _common_identity_errors(
    root: Path,
    archive: Path | None,
    expected: dict[str, object],
    label: str,
) -> list[str]:
    errors: list[str] = []
    group_path = root / "group_runs.csv"
    if _optional_sha256(group_path) != expected.get("group_runs_sha256"):
        errors.append(f"{label} group_runs SHA drifted")
    manifest = _checked_json(
        root / "manifest.json",
        expected.get("manifest_sha256"),
        f"{label} manifest",
        errors,
    )
    if manifest is not None:
        for field in (
            "experiment_id",
            "repository_commit",
            "config_fingerprint",
        ):
            if manifest.get(field) != expected.get(field):
                errors.append(f"{label} manifest {field} drifted")
        if manifest.get("status") != "completed":
            errors.append(f"{label} manifest is not completed")
        if manifest.get("incidents") != []:
            errors.append(f"{label} manifest contains incidents")
        run_instance_id = manifest.get("run_instance_id")
        root_id = expected.get("root_id")
        if not (
            isinstance(run_instance_id, str)
            and isinstance(root_id, str)
            and run_instance_id.startswith(f"{root_id}-")
        ):
            errors.append(f"{label} manifest root identity drifted")
    if archive is None:
        errors.append(f"{label} archive was not supplied")
    elif _optional_sha256(archive) != expected.get("archive_sha256"):
        errors.append(f"{label} archive SHA drifted")
    return errors


def _checked_json(
    path: Path,
    expected_sha256: object,
    label: str,
    errors: list[str],
) -> dict[str, object] | None:
    if _optional_sha256(path) != expected_sha256:
        errors.append(f"{label} SHA drifted")
    return _read_json(path, label, errors)


def _read_json(
    path: Path,
    label: str,
    errors: list[str],
) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        errors.append(f"{label} is unavailable or invalid")
        return None
    if not isinstance(payload, dict):
        errors.append(f"{label} must be a JSON object")
        return None
    return payload


def _read_csv(
    path: Path,
    errors: list[str],
) -> list[dict[str, str]]:
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        errors.append(f"{path.name} is unavailable")
        return []


def _optional_sha256(path: Path | None) -> str | None:
    if path is None or not path.is_file():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_summary(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
