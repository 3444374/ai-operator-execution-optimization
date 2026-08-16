"""Validate and summarize the isolated D0/D1/P0 feeding-gap diagnostic.

The module answers one bounded question: is the sealed 7.10% feeding gap
primarily associated with the endpoint work envelope or with the Project
PostgreSQL/Daft/Ray/coordinator path? It cannot authorize or revive SAOR formal.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
from pathlib import Path

from src.experiments.scenarios.core import build_scenario_schedule
from src.experiments.shared_vllm.config import SharedVllmConfig


D0 = "feeding_gap_d0_direct_k_only"
D1 = "feeding_gap_d1_direct_k_work"
P0 = "feeding_gap_p0_project_bounded_ready_fifo"
ARM_ORDER = (D0, D1, P0)
ARM_POLICIES = {
    D0: "direct_no_job",
    D1: "direct_work_limited",
    P0: "shared_fifo",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_lf_normalized_text_file(path: Path) -> str:
    """Hash a text contract without platform-specific CRLF differences."""

    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def load_diagnostic_contract(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("feeding-gap diagnostic contract schema must be 1")
    return payload


def validate_prior_failed_lock(
    diagnostic_contract: dict[str, object],
    prior_contract_path: Path,
) -> list[str]:
    """Prove that the diagnostic cannot reinterpret the sealed negative gate."""

    errors: list[str] = []
    if diagnostic_contract.get("status") != "locked_diagnostic_only_pending_run":
        errors.append("diagnostic contract status is not locked")
    if diagnostic_contract.get("may_change_prior_feeding_decision") is not False:
        errors.append("diagnostic contract may not change the prior decision")
    prior_identity = diagnostic_contract.get("prior_failed_feeding_lock")
    if not isinstance(prior_identity, dict):
        return [*errors, "prior failed-feeding identity is missing"]
    if not prior_contract_path.is_file():
        return [*errors, "prior failed-feeding contract is missing"]
    prior = json.loads(prior_contract_path.read_text(encoding="utf-8"))
    if prior.get("status") != "locked_failed_feeding":
        errors.append("prior contract is no longer locked_failed_feeding")
    if prior.get("formal_authorized") is not False:
        errors.append("prior contract unexpectedly authorizes formal")
    if prior_identity.get(
        "contract_sha256"
    ) != sha256_lf_normalized_text_file(prior_contract_path):
        errors.append("prior failed-feeding contract SHA drifted")
    feeding = prior.get("feeding_validation")
    if not isinstance(feeding, dict):
        errors.append("prior feeding validation is missing")
    else:
        for key in ("root_id", "validation_sha256", "archive_sha256"):
            if feeding.get(key) != prior_identity.get(key):
                errors.append(f"prior failed-feeding {key} drifted")
        if feeding.get("feeding_gate_passed") is not False:
            errors.append("prior feeding gate is no longer negative")
    return errors


def validate_diagnostic_config(
    reference: SharedVllmConfig,
    diagnostic: SharedVllmConfig,
    contract: dict[str, object],
) -> list[str]:
    """Freeze the three arms to one workload, service, K, W, and work cost."""

    errors: list[str] = []
    if diagnostic.experiment_id != contract.get("experiment_id"):
        errors.append("diagnostic experiment_id drifted")
    if diagnostic.warmup_runs_per_scenario != 1 or diagnostic.formal_repeats != 3:
        errors.append("diagnostic must contain 1 warm-up and 3 measured repeats")
    if not diagnostic.fail_closed_rehearsal:
        errors.append("diagnostic request evidence must fail closed")
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
        if getattr(reference, field) != getattr(diagnostic, field):
            errors.append(f"diagnostic {field} drifted from the sealed Project run")
    if reference.completion_work_cost != diagnostic.completion_work_cost:
        errors.append("diagnostic typed completion-work cost drifted")

    _decision_ratio_min(contract, errors)

    matrix = contract.get("matrix")
    if not isinstance(matrix, dict):
        errors.append("diagnostic matrix contract is missing")
        return errors
    if diagnostic.request_limit_per_endpoint != matrix.get("k_per_endpoint"):
        errors.append("diagnostic K does not match the frozen matrix")
    if diagnostic.work_limit_per_endpoint != matrix.get("w_per_endpoint"):
        errors.append("diagnostic W does not match the frozen matrix")
    if len(diagnostic.scenarios) != len(ARM_ORDER):
        errors.append("diagnostic must contain exactly D0, D1, and P0")
        return errors
    by_id = {scenario.scenario_id: scenario for scenario in diagnostic.scenarios}
    if set(by_id) != set(ARM_ORDER):
        errors.append("diagnostic scenario identities drifted")
        return errors
    schedule = build_scenario_schedule(
        [scenario.scenario_id for scenario in diagnostic.scenarios],
        diagnostic.warmup_runs_per_scenario,
        diagnostic.formal_repeats,
        diagnostic.seed,
    )
    formal_positions = {arm_id: [] for arm_id in ARM_ORDER}
    for scheduled in schedule:
        if scheduled.phase == "formal":
            formal_positions[scheduled.scenario_id].append(
                scheduled.order_index % len(ARM_ORDER)
            )
    if any(
        sorted(positions) != list(range(len(ARM_ORDER)))
        for positions in formal_positions.values()
    ):
        errors.append("diagnostic measured repeats are not position-balanced")
    try:
        proposed = next(
            scenario
            for scenario in reference.scenarios
            if scenario.policy == "saor_bounded_ready"
        )
    except StopIteration:
        errors.append("reference config lacks the sealed SAOR scenario")
        return errors
    comparable = (
        "job_count",
        "rows_per_job",
        "rows_per_jobs",
        "weights",
        "arrival_offsets_s",
        "source_row_offsets",
        "request_manifests",
    )
    for arm_id in ARM_ORDER:
        arm = by_id[arm_id]
        if arm.policy != ARM_POLICIES[arm_id]:
            errors.append(f"{arm_id} policy drifted")
        for field in comparable:
            if getattr(arm, field) != getattr(proposed, field):
                errors.append(f"{arm_id} {field} drifted")
        request_limit, work_limit = arm.endpoint_limits(
            diagnostic.request_limit_per_endpoint,
            diagnostic.work_limit_per_endpoint,
        )
        if request_limit != matrix.get("k_per_endpoint"):
            errors.append(f"{arm_id} effective K drifted")
        if work_limit != matrix.get("w_per_endpoint"):
            errors.append(f"{arm_id} effective W drifted")
    for arm_id in (D0, D1):
        if by_id[arm_id].ready_observation_contract != "single_head":
            errors.append(f"{arm_id} must not use bounded-ready")
    if by_id[P0].ready_observation_contract != (
        "bounded_concrete_pre_registration"
    ):
        errors.append("P0 must use bounded-ready FIFO")

    expected_manifest_sha = matrix.get("request_manifest_sha256")
    if not isinstance(expected_manifest_sha, list):
        errors.append("diagnostic manifest SHA contract is missing")
    else:
        observed = [
            sha256_file(Path(str(path)).resolve())
            for path in by_id[D0].request_manifests
            if path is not None
        ]
        if observed != expected_manifest_sha:
            errors.append("diagnostic immutable request manifests drifted")
    return errors


def summarize_feeding_gap(
    output_root: Path,
    *,
    prior_contract_path: Path,
    diagnostic_contract: dict[str, object],
    diagnostic_contract_sha256: str,
) -> dict[str, object]:
    """Fail closed, then classify the paired D1/D0 and P0/D1 ratios."""

    errors = validate_prior_failed_lock(
        diagnostic_contract,
        prior_contract_path,
    )
    threshold = _decision_ratio_min(diagnostic_contract, errors)
    snapshot = _read_json(
        output_root / "feeding_gap_contract_snapshot.json",
        errors,
    )
    if snapshot is not None:
        if snapshot.get("status") != "diagnostic_only_ready":
            errors.append("diagnostic runtime contract snapshot is not ready")
        if snapshot.get("diagnostic_contract_sha256") != (
            diagnostic_contract_sha256
        ):
            errors.append("diagnostic runtime contract SHA drifted")
        if snapshot.get(
            "prior_failed_contract_sha256"
        ) != sha256_lf_normalized_text_file(prior_contract_path):
            errors.append("diagnostic runtime prior-contract SHA drifted")
        if snapshot.get("may_change_prior_feeding_decision") is not False:
            errors.append("diagnostic runtime snapshot may change prior decision")
    clean_gate = _read_json(output_root / "pre_run_clean_gate.json", errors)
    if clean_gate is None or clean_gate.get("status") != "passed":
        errors.append("structured PG/Ray/endpoint clean gate did not pass")
    manifest = _read_json(output_root / "manifest.json", errors)
    if manifest is None or manifest.get("status") != "completed":
        errors.append("diagnostic runner manifest is incomplete")
    rows = _read_csv(output_root / "group_runs.csv", errors)
    grouped = _validate_rows(rows, errors)
    if manifest is not None:
        _validate_runtime_schedule(manifest, grouped, errors)
    component_rows = _component_rows(output_root, grouped, errors)
    if errors:
        return _invalid_summary(errors, component_rows)

    paired = []
    for repeat in range(1, 4):
        d0 = grouped[(D0, "formal", repeat)]
        d1 = grouped[(D1, "formal", repeat)]
        p0 = grouped[(P0, "formal", repeat)]
        d0_rate = _positive_float(d0, "tokens_per_s", errors)
        d1_rate = _positive_float(d1, "tokens_per_s", errors)
        p0_rate = _positive_float(p0, "tokens_per_s", errors)
        if errors:
            return _invalid_summary(errors, component_rows)
        paired.append(
            {
                "repeat_index": repeat,
                "d0_tokens_per_s": d0_rate,
                "d1_tokens_per_s": d1_rate,
                "p0_tokens_per_s": p0_rate,
                "d1_over_d0": d1_rate / d0_rate,
                "p0_over_d1": p0_rate / d1_rate,
                "p0_over_d0": p0_rate / d0_rate,
            }
        )
    d1_d0 = [float(row["d1_over_d0"]) for row in paired]
    p0_d1 = [float(row["p0_over_d1"]) for row in paired]
    if threshold is None or errors:
        return _invalid_summary(errors, component_rows)
    d1_passed = statistics.mean(d1_d0) >= threshold
    p0_passed = statistics.mean(p0_d1) >= threshold
    classification = classify_feeding_gap(
        statistics.mean(d1_d0),
        statistics.mean(p0_d1),
        ratio_min=threshold,
    )
    return {
        "schema_version": 1,
        "status": "valid_diagnostic",
        "evidence_valid": True,
        "scope": "feeding_gap_attribution_only",
        "may_change_prior_feeding_decision": False,
        "prior_decision_preserved": "locked_failed_feeding",
        "ratio_min": threshold,
        "d1_over_d0_mean": statistics.mean(d1_d0),
        "d1_over_d0_cv": _cv(d1_d0),
        "p0_over_d1_mean": statistics.mean(p0_d1),
        "p0_over_d1_cv": _cv(p0_d1),
        "d1_over_d0_passed": d1_passed,
        "p0_over_d1_passed": p0_passed,
        "classification": classification,
        "paired_repeats": paired,
        "component_rows": component_rows,
        "errors": [],
    }


def classify_feeding_gap(
    d1_over_d0: float,
    p0_over_d1: float,
    *,
    ratio_min: float = 0.95,
) -> str:
    """Return the preregistered four-way attribution label."""

    if any(
        not math.isfinite(value) or value <= 0
        for value in (d1_over_d0, p0_over_d1, ratio_min)
    ):
        raise ValueError("feeding-gap ratios and threshold must be positive")
    d1_passed = d1_over_d0 >= ratio_min
    p0_passed = p0_over_d1 >= ratio_min
    return (
        "work_envelope_primary"
        if not d1_passed and p0_passed
        else "project_path_primary"
        if d1_passed and not p0_passed
        else "work_envelope_and_project_path"
        if not d1_passed and not p0_passed
        else "original_gap_not_reproduced"
    )


def _validate_rows(
    rows: list[dict[str, str]],
    errors: list[str],
) -> dict[tuple[str, str, int], dict[str, str]]:
    expected = {
        (arm_id, phase, repeat)
        for arm_id in ARM_ORDER
        for phase, repeats in (("warmup", (1,)), ("formal", (1, 2, 3)))
        for repeat in repeats
    }
    grouped: dict[tuple[str, str, int], dict[str, str]] = {}
    for row in rows:
        try:
            key = (
                row["scenario_id"],
                row["phase"],
                int(row["repeat_index"]),
            )
        except (KeyError, TypeError, ValueError):
            errors.append("diagnostic result row has an invalid identity")
            continue
        if key in grouped:
            errors.append(f"diagnostic result cell is duplicated: {key}")
        grouped[key] = row
    if set(grouped) != expected:
        errors.append("diagnostic does not contain the complete 3-arm 1+3 matrix")
    required = (
        "metrics_status",
        "resource_metrics_status",
        "mfu_status",
        "mfu_estimate",
        "tokens_per_s",
        "gpu_utilization_pct_mean",
        "gpu_power_w_mean",
        "gpu_energy_j",
        "energy_j_per_1k_observed_tokens",
        "vllm_running_mean",
        "vllm_waiting_mean",
        "vllm_kv_usage_mean",
        "vllm_time_to_first_token_p99_s",
        "vllm_inter_token_latency_p99_s",
        "job_jct_s",
        "job_p99_s",
        "job_slo_violation_ratio",
        "job_slo_goodput_per_s",
        "job_exactly_once",
    )
    for key, row in grouped.items():
        if row.get("policy") != ARM_POLICIES.get(key[0]):
            errors.append(f"diagnostic policy mismatch: {key}")
        if row.get("metrics_status") != "ok":
            errors.append(f"vLLM metrics unavailable: {key}")
        if row.get("resource_metrics_status") != "ok":
            errors.append(f"resource metrics unavailable: {key}")
        if row.get("mfu_status") != "ok":
            errors.append(f"MFU unavailable: {key}")
        if row.get("job_exactly_once") != "[true, true]":
            errors.append(f"exactly-once failed: {key}")
        for field in required:
            if row.get(field, "") == "":
                errors.append(f"diagnostic metric {field} is missing: {key}")
        for field in (
            "tokens_per_s",
            "gpu_utilization_pct_mean",
            "gpu_power_w_mean",
            "gpu_energy_j",
            "energy_j_per_1k_observed_tokens",
            "vllm_running_mean",
            "vllm_waiting_mean",
            "vllm_kv_usage_mean",
            "vllm_time_to_first_token_p99_s",
            "vllm_inter_token_latency_p99_s",
            "mfu_estimate",
        ):
            _finite_float(row, field, errors)
        for field in (
            "job_jct_s",
            "job_p99_s",
            "job_slo_violation_ratio",
            "job_slo_goodput_per_s",
        ):
            _numeric_json_list(row, field, expected_length=2, errors=errors)
        if key[0] in {D0, D1}:
            if row.get("direct_admission_trace_status") != (
                "ok:lossless_acquire_release_ledger"
            ):
                errors.append(f"direct admission evidence is invalid: {key}")
            expected_work_gate = key[0] == D1
            if _truth(row.get("direct_work_limit_applied")) != expected_work_gate:
                errors.append(f"direct work-gate identity is invalid: {key}")
            for field in (
                "direct_request_occupancy_max",
                "direct_estimated_work_occupancy_max",
                "direct_request_occupancy_fraction_mean",
                "direct_estimated_work_to_reference_w_fraction_mean",
                "direct_admission_wait_p50_s",
                "direct_admission_wait_p95_s",
                "direct_admission_wait_p99_s",
                "direct_admission_wait_max_s",
            ):
                _finite_float(row, field, errors)
        if key[0] == P0:
            if row.get("credit_trace_status") != "ok:sampled_endpoint_credit":
                errors.append(f"P0 credit occupancy is unavailable: {key}")
            if row.get("bounded_ready_event_status") != "ok:actor_event_join":
                errors.append(f"P0 bounded-ready evidence is unavailable: {key}")
    return grouped


def _component_rows(
    output_root: Path,
    grouped: dict[tuple[str, str, int], dict[str, str]],
    errors: list[str],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for key, row in sorted(grouped.items()):
        arm_id, phase, repeat = key
        component = {
            "scenario_id": arm_id,
            "phase": phase,
            "repeat_index": repeat,
            "tokens_per_s": row.get("tokens_per_s", ""),
            "mfu_fraction": row.get("mfu_estimate", ""),
            "gpu_utilization_pct_mean": row.get("gpu_utilization_pct_mean", ""),
            "gpu_energy_j": row.get("gpu_energy_j", ""),
            "vllm_running_mean": row.get("vllm_running_mean", ""),
            "vllm_waiting_mean": row.get("vllm_waiting_mean", ""),
            "vllm_kv_usage_mean": row.get("vllm_kv_usage_mean", ""),
            "ttft_p99_s": row.get("vllm_time_to_first_token_p99_s", ""),
            "itl_p99_s": row.get("vllm_inter_token_latency_p99_s", ""),
            "job_jct_s": row.get("job_jct_s", ""),
            "job_p99_s": row.get("job_p99_s", ""),
            "job_slo_violation_ratio": row.get("job_slo_violation_ratio", ""),
            "direct_request_occupancy_fraction_mean": row.get(
                "direct_request_occupancy_fraction_mean", ""
            ),
            "direct_estimated_work_to_reference_w_fraction_mean": row.get(
                "direct_estimated_work_to_reference_w_fraction_mean", ""
            ),
            "direct_admission_wait_p95_s": row.get(
                "direct_admission_wait_p95_s", ""
            ),
            "project_active_request_occupancy_max": "",
            "project_active_work_occupancy_max": "",
            "project_waiting_request_occupancy_max": "",
            "project_waiting_work_occupancy_max": "",
            "ray_actor_ready_s_max": "",
            "ray_submit_s_max": "",
            "project_bounded_wait_s_max": "",
            "project_avg_bounded_wait_s_max": "",
        }
        if arm_id == P0:
            try:
                order_index = int(row["order_index"])
            except (KeyError, TypeError, ValueError):
                errors.append(f"P0 order_index is invalid: {key}")
                output.append(component)
                continue
            stem = f"{order_index:03d}_{phase}_{repeat}_{arm_id}"
            job_rows = []
            for job_index in range(2):
                path = output_root / "jobs" / f"{stem}_job{job_index}.runs.csv"
                loaded = _read_csv(path, errors)
                if len(loaded) != 1:
                    errors.append(f"P0 Job component evidence is incomplete: {path}")
                else:
                    job_rows.append(loaded[0])
            credit_rows = _read_csv(
                output_root / "traces" / f"{stem}.credits.csv",
                errors,
            )
            if job_rows:
                for source, target in (
                    ("actor_ready_s", "ray_actor_ready_s_max"),
                    ("submit_s", "ray_submit_s_max"),
                    ("bounded_wait_s", "project_bounded_wait_s_max"),
                    (
                        "avg_bounded_wait_s",
                        "project_avg_bounded_wait_s_max",
                    ),
                ):
                    component[target] = max(
                        _finite_float(item, source, errors) for item in job_rows
                    )
            if credit_rows:
                for source, target in (
                    ("active_requests", "project_active_request_occupancy_max"),
                    ("active_work", "project_active_work_occupancy_max"),
                    ("waiting_requests", "project_waiting_request_occupancy_max"),
                    ("waiting_work", "project_waiting_work_occupancy_max"),
                ):
                    component[target] = max(
                        _finite_float(item, source, errors)
                        for item in credit_rows
                    )
        output.append(component)
    return output


def _validate_runtime_schedule(
    manifest: dict[str, object],
    grouped: dict[tuple[str, str, int], dict[str, str]],
    errors: list[str],
) -> None:
    schedule = manifest.get("schedule")
    if not isinstance(schedule, list) or len(schedule) != 12:
        errors.append("diagnostic runtime schedule is incomplete")
        return
    formal_positions = {arm_id: [] for arm_id in ARM_ORDER}
    seen: set[tuple[str, str, int]] = set()
    for expected_order, raw in enumerate(schedule):
        if not isinstance(raw, dict):
            errors.append("diagnostic runtime schedule row is invalid")
            continue
        try:
            key = (
                str(raw["scenario_id"]),
                str(raw["phase"]),
                int(raw["repeat_index"]),
            )
            order_index = int(raw["order_index"])
        except (KeyError, TypeError, ValueError):
            errors.append("diagnostic runtime schedule identity is invalid")
            continue
        if key in seen:
            errors.append(f"diagnostic runtime schedule cell is duplicated: {key}")
        seen.add(key)
        if order_index != expected_order:
            errors.append("diagnostic runtime schedule order_index is not contiguous")
        result = grouped.get(key)
        if result is None:
            errors.append(f"diagnostic runtime schedule lacks result: {key}")
        else:
            try:
                result_order = int(result["order_index"])
            except (KeyError, TypeError, ValueError):
                errors.append(f"diagnostic result order_index is invalid: {key}")
            else:
                if result_order != order_index:
                    errors.append(f"diagnostic schedule/result order drifted: {key}")
        if key[1] == "formal" and key[0] in formal_positions:
            formal_positions[key[0]].append(order_index % len(ARM_ORDER))
    if set(grouped) != seen:
        errors.append("diagnostic schedule and result matrix identities differ")
    if any(
        sorted(positions) != list(range(len(ARM_ORDER)))
        for positions in formal_positions.values()
    ):
        errors.append("diagnostic runtime measured positions are not balanced")


def _read_csv(path: Path, errors: list[str]) -> list[dict[str, str]]:
    if not path.is_file():
        errors.append(f"missing CSV evidence: {path}")
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path, errors: list[str]) -> dict[str, object] | None:
    if not path.is_file():
        errors.append(f"missing JSON evidence: {path}")
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"invalid JSON evidence {path}: {type(exc).__name__}")
        return None
    if not isinstance(payload, dict):
        errors.append(f"JSON evidence is not an object: {path}")
        return None
    return payload


def _positive_float(
    row: dict[str, str],
    field: str,
    errors: list[str],
) -> float:
    value = _finite_float(row, field, errors)
    if value <= 0:
        errors.append(f"{field} must be positive")
    return value


def _finite_float(
    row: dict[str, str],
    field: str,
    errors: list[str],
) -> float:
    try:
        value = float(row[field])
    except (KeyError, TypeError, ValueError):
        errors.append(f"metric {field} is unavailable")
        return math.nan
    if not math.isfinite(value):
        errors.append(f"metric {field} is not finite")
    return value


def _truth(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true"}


def _decision_ratio_min(
    contract: dict[str, object],
    errors: list[str],
) -> float | None:
    decision = contract.get("decision_contract")
    if not isinstance(decision, dict):
        errors.append("diagnostic decision contract is missing")
        return None
    raw = decision.get("ratio_min")
    if isinstance(raw, bool):
        errors.append("diagnostic ratio threshold is invalid")
        return None
    try:
        value = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        errors.append("diagnostic ratio threshold is invalid")
        return None
    if not math.isfinite(value) or not 0 < value <= 1:
        errors.append("diagnostic ratio threshold must be in (0, 1]")
        return None
    return value


def _numeric_json_list(
    row: dict[str, str],
    field: str,
    *,
    expected_length: int,
    errors: list[str],
) -> list[float]:
    try:
        payload = json.loads(row[field])
    except (KeyError, TypeError, json.JSONDecodeError):
        errors.append(f"diagnostic metric {field} is not a JSON list")
        return []
    if not isinstance(payload, list) or len(payload) != expected_length:
        errors.append(
            f"diagnostic metric {field} must contain {expected_length} Jobs"
        )
        return []
    values: list[float] = []
    for item in payload:
        if isinstance(item, bool):
            errors.append(f"diagnostic metric {field} contains a non-number")
            return []
        try:
            value = float(item)
        except (TypeError, ValueError):
            errors.append(f"diagnostic metric {field} contains a non-number")
            return []
        if not math.isfinite(value):
            errors.append(f"diagnostic metric {field} contains a non-finite value")
            return []
        values.append(value)
    return values


def _cv(values: list[float]) -> float:
    mean = statistics.mean(values)
    return statistics.pstdev(values) / mean if mean > 0 else math.inf


def _invalid_summary(
    errors: list[str],
    component_rows: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "status": "invalid_evidence",
        "evidence_valid": False,
        "scope": "feeding_gap_attribution_only",
        "may_change_prior_feeding_decision": False,
        "prior_decision_preserved": "locked_failed_feeding",
        "classification": "unavailable",
        "component_rows": component_rows,
        "errors": sorted(set(errors)),
    }
