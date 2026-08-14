"""Frozen contracts for the Project SAOR mechanism formal matrix.

This module deliberately separates evidence validity from a performance claim:
an experiment can be valid while the proposed selector fails its Pareto gate.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

from src.experiments.scenarios.core import build_scenario_schedule
from src.experiments.shared_vllm.config import (
    CompletionWorkCostConfig,
    SharedVllmConfig,
)
from src.experiments.shared_vllm.metrics import (
    completion_accounted_service_fairness,
)
from src.experiments.shared_vllm.work_evidence import (
    cell_trace_paths,
    joined_cell_work,
    model_artifact_calibration_identity,
    read_csv,
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


# Engineering decision: formal authorization is tied to the one independently
# reviewed rehearsal artifact.  Presence or shape checks are insufficient here:
# changing any identity field must require another explicit review and code
# update before a formal run can be authorized.
REVIEWED_REHEARSAL_EVIDENCE: dict[str, object] = {
    "status": "passed_independent_review",
    "repository_commit": "63d1730058923609808bec6e3b91ed26a2cd581a",
    "root_id": "saor_project_mechanism_rehearsal_63d17300_20260814",
    "validation_sha256": (
        "4f19e0b70c13d4a67a24015ff33444a95a8bab4b773052b62716bfc39540b668"
    ),
    "archive_sha256": (
        "5f267dc5847529e8dcea7a4415d52a3e1675a4a983c5190c164ef67af552cedd"
    ),
    "performance_ranking_decided": False,
    "valid_rehearsal": True,
}


# Engineering decision: this exact-signature direct ceiling is a valid
# negative prerequisite, not a missing measurement. Keep the measured
# throughput values as evidence identity and refuse formal authorization while
# the frozen feeding gate is false. A future candidate must use a new contract
# rather than rewriting this result or lowering the threshold.
FROZEN_FEEDING_EVIDENCE: dict[str, object] = {
    "status": "failed_feeding",
    "repository_commit": "c988622a643699925faeeb3cecc4c351913b728b",
    "root_id": "saor_project_feeding_ceiling_c988622a_20260814_retry2",
    "validation_sha256": (
        "6c656f25b8128fe102a06b65093c8be7e593f182029febc68201af265cdba3d5"
    ),
    "archive_sha256": (
        "ebf5c35a699ff034891855d14c3332dbe42dabef3ede1f0641d3ac18a4079fb2"
    ),
    "evidence_valid": True,
    "feeding_gate_passed": False,
    "ratio_min": 0.95,
    "project_tokens_per_s": 12713.02535346175,
    "ceiling_tokens_per_s": 13684.897101379862,
    "feeding_ratio": 0.9289821661998381,
}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def load_contract(path: Path) -> dict[str, object]:
    payload = json.loads(path.resolve().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Project mechanism contract must be a JSON object")
    return payload


def work_cost_from_contract(
    payload: dict[str, object],
) -> CompletionWorkCostConfig:
    work_cost = payload.get("work_cost_contract")
    if not isinstance(work_cost, dict):
        raise ValueError("mechanism contract lacks work_cost_contract")
    protocol = work_cost.get("completion_protocol")
    if protocol not in {"completions", "chat_completions"}:
        raise ValueError("mechanism completion protocol is invalid")
    overhead = work_cost.get("prompt_token_overhead_per_request")
    if not isinstance(overhead, int) or isinstance(overhead, bool) or overhead < 0:
        raise ValueError("mechanism prompt overhead is invalid")
    output_bound_source = work_cost.get("output_bound_source")
    if output_bound_source != "fixed_output_cap":
        raise ValueError("mechanism output bound source is invalid")
    completion_max_tokens = work_cost.get("completion_max_tokens")
    if (
        not isinstance(completion_max_tokens, int)
        or isinstance(completion_max_tokens, bool)
        or completion_max_tokens <= 0
    ):
        raise ValueError("mechanism completion max tokens is invalid")
    return CompletionWorkCostConfig(
        protocol=protocol,
        prompt_token_overhead_per_request=overhead,
        output_bound_source=output_bound_source,
        completion_max_tokens=completion_max_tokens,
    )


def validate_calibration_artifact(
    payload: dict[str, object],
    config: SharedVllmConfig,
) -> tuple[dict[str, object], list[str]]:
    """Verify the runtime tokenizer/template artifact against the contract."""

    errors: list[str] = []
    work_cost = payload.get("work_cost_contract")
    identity = (
        work_cost.get("calibration_identity")
        if isinstance(work_cost, dict)
        else None
    )
    if not isinstance(identity, dict):
        return {}, ["mechanism work-cost lacks calibration identity"]
    try:
        observed = model_artifact_calibration_identity(
            config.completion_tokenizer_path
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {}, [str(exc)]
    for key, value in observed.items():
        if identity.get(key) != value:
            errors.append(f"runtime calibration artifact {key} drifted")
    expected_shape = (
        "single_user_message_no_system"
        if config.completion_work_cost.protocol == "chat_completions"
        else "raw_prompt_array"
    )
    runtime_contract = {
        "message_shape": expected_shape,
        "prompt_format": config.completion_prompt_format,
        "return_token_ids": config.completion_returns_token_ids,
    }
    for key, value in runtime_contract.items():
        if identity.get(key) != value:
            errors.append(f"runtime completion request {key} drifted")
    return {**observed, **runtime_contract}, errors


def calibration_snapshot_errors(
    snapshot: dict[str, object],
    payload: dict[str, object],
) -> list[str]:
    work_cost = payload.get("work_cost_contract")
    expected = (
        work_cost.get("calibration_identity")
        if isinstance(work_cost, dict)
        else None
    )
    readiness = snapshot.get("readiness")
    observed = (
        readiness.get("work_cost_calibration_identity")
        if isinstance(readiness, dict)
        else None
    )
    if not isinstance(expected, dict) or not isinstance(observed, dict):
        return ["run snapshot lacks work-cost calibration identity"]
    errors = []
    for key, value in observed.items():
        if expected.get(key) != value:
            errors.append(f"run calibration snapshot {key} drifted")
    required = {
        "model_revision",
        "tokenizer_revision",
        "model_config_sha256",
        "tokenizer_config_sha256",
        "tokenizer_json_sha256",
        "chat_template_sha256",
        "message_shape",
        "prompt_format",
        "return_token_ids",
    }
    if set(observed) != required:
        errors.append("run calibration snapshot field set is incomplete")
    return errors


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

    authorized = payload.get("formal_authorized") is True
    work_cost = payload.get("work_cost_contract")
    if not isinstance(work_cost, dict):
        errors.append("mechanism contract lacks work_cost_contract")
    else:
        expected_work_cost = {
            "completion_protocol": "chat_completions",
            "prompt_token_overhead_per_request": 29,
            "output_bound_source": "fixed_output_cap",
            "completion_max_tokens": 256,
            "calibration_method": (
                "endpoint_usage_prompt_tokens_minus_raw_prompt_tokens"
            ),
            "calibration_requests": 6144,
            "observed_min_tokens": 29,
            "observed_max_tokens": 29,
        }
        for key, expected in expected_work_cost.items():
            if work_cost.get(key) != expected:
                errors.append(f"mechanism work-cost {key} drifted")
        try:
            configured_work_cost = config.completion_work_cost
            contract_work_cost = work_cost_from_contract(payload)
        except ValueError as exc:
            errors.append(str(exc))
        else:
            if configured_work_cost != contract_work_cost:
                errors.append(
                    "mechanism typed work-cost config drifted from calibration"
                )
        calibration_identity = work_cost.get("calibration_identity")
        required_identity = {
            "model_id": "Qwen/Qwen2.5-7B-Instruct",
            "model_revision": "a09a35458c702b33eeacc393d103063234e8bc28",
            "model_config_sha256": (
                "7463bb0ea78315365e6c6b74de4e73bbcc8359dfb0c5a737584e077d42c0b03c"
            ),
            "tokenizer_id": "Qwen/Qwen2.5-7B-Instruct",
            "tokenizer_revision": "a09a35458c702b33eeacc393d103063234e8bc28",
            "tokenizer_config_sha256": (
                "5b5d4f65d0acd3b2d56a35b56d374a36cbc1c8fa5cf3b3febbbfabf22f359583"
            ),
            "tokenizer_json_sha256": (
                "c0382117ea329cdf097041132f6d735924b697924d6f6fc3945713e96ce87539"
            ),
            "chat_template_sha256": (
                "cd8e9439f0570856fd70470bf8889ebd8b5d1107207f67a5efb46e342330527f"
            ),
            "message_shape": "single_user_message_no_system",
            "prompt_format": "raw",
            "return_token_ids": True,
        }
        if calibration_identity != required_identity:
            errors.append("mechanism work-cost calibration identity drifted")
        calibration_evidence = work_cost.get("calibration_evidence")
        if not isinstance(calibration_evidence, dict):
            errors.append("mechanism work-cost lacks calibration evidence")
        else:
            predecessor = calibration_evidence.get(
                "predecessor_failed_archive_sha256"
            )
            if not _is_sha256(predecessor):
                errors.append("predecessor calibration archive SHA is invalid")
            evidence_status = calibration_evidence.get("status")
            if evidence_status not in {
                "locked_pending_source_verified_rehearsal",
                "source_verified_rehearsal",
            }:
                errors.append("work-cost calibration evidence status is invalid")
            if authorized or evidence_status == "source_verified_rehearsal":
                for key in (
                    "calibration_artifact_sha256",
                    "input_files_manifest_sha256",
                    "validated_archive_sha256",
                ):
                    if not _is_sha256(calibration_evidence.get(key)):
                        errors.append(f"work-cost calibration {key} is invalid")

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
        if decision.get("debt_repayment_completed_min") != 1:
            errors.append("formal requires at least one completed repayment episode")
        if (
            decision.get("debt_repayment_censored_policy")
            != "explicit_finish_job_after_source_exhausted_and_credit_drained"
        ):
            errors.append("debt repayment censoring policy drifted")
        if (
            decision.get("recovery_projection")
            != "raw_plus_weighted_foreign_residual_minus_weighted_all_own_inflight"
        ):
            errors.append("debt recovery projection contract drifted")
        if (
            decision.get("projection_work_estimate_upper_bound_required")
            is not True
        ):
            errors.append("formal requires projection-work estimate upper bounds")
        if decision.get("projection_offline_recompute_required") is not True:
            errors.append("formal requires independent projection recomputation")
        if (
            decision.get("discrete_overshoot_bound")
            != "(1-phi_i)*candidate_estimated_work"
        ):
            errors.append("discrete recovery overshoot bound drifted")

    feeding = payload.get("feeding_validation")
    if not isinstance(feeding, dict):
        errors.append("mechanism contract lacks frozen feeding evidence")
    else:
        for key, expected in FROZEN_FEEDING_EVIDENCE.items():
            if feeding.get(key) != expected:
                errors.append(f"formal feeding evidence {key} drifted")

    if authorized:
        if payload.get("status") != "formal_ready":
            errors.append("authorized contract status must be formal_ready")
        rehearsal = payload.get("rehearsal_validation")
        if not isinstance(rehearsal, dict):
            errors.append("formal authorization requires frozen rehearsal evidence")
        else:
            for key, expected in REVIEWED_REHEARSAL_EVIDENCE.items():
                if rehearsal.get(key) != expected:
                    errors.append(
                        f"formal rehearsal evidence {key} drifted"
                    )
        if not isinstance(feeding, dict) or (
            feeding.get("evidence_valid") is not True
            or feeding.get("feeding_gate_passed") is not True
        ):
            errors.append(
                "formal authorization requires a valid passed feeding gate"
            )
    else:
        if payload.get("status") != "locked_failed_feeding":
            errors.append(
                "feeding-negative contract must remain locked_failed_feeding"
            )
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


def completion_fairness_from_raw(
    root: Path,
    row: dict[str, str],
    *,
    work_cost: CompletionWorkCostConfig | None = None,
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
    if work_cost is None:
        first_request_path, _first_submission_path = cell_trace_paths(
            root,
            row,
            job_count=job_count,
        )[0]
        summary_path = first_request_path.with_name(
            first_request_path.name.replace(".requests.csv", ".runs.csv")
        )
        if not summary_path.is_file():
            return completion_accounted_service_fairness(unavailable, weights)
        summary_rows = read_csv(summary_path)
        if len(summary_rows) != 1:
            return completion_accounted_service_fairness(unavailable, weights)
        summary = summary_rows[0]
        protocol = summary.get("completion_protocol", "")
        if protocol not in {"completions", "chat_completions"}:
            raise ValueError("raw fairness evidence has invalid protocol")
        work_cost = CompletionWorkCostConfig(
            protocol=protocol,
            prompt_token_overhead_per_request=int(
                summary.get("completion_prompt_token_overhead", "")
            ),
            output_bound_source=summary.get("output_cost_mode", ""),
            completion_max_tokens=int(
                summary.get("completion_max_tokens", "")
            ),
        )
    try:
        joined_by_job, _input_paths = joined_cell_work(
            root,
            row,
            work_cost=work_cost,
            job_count=job_count,
            require_estimate_upper_bound=False,
        )
    except (OSError, TypeError, ValueError):
        raise
    evidence = []
    trace_paths = cell_trace_paths(root, row, job_count=job_count)
    for joined, (_request_path, submission_path) in zip(
        joined_by_job,
        trace_paths,
    ):
        submissions = read_csv(submission_path)
        service_by_id = {
            item.submission_id: (item.completion_epoch_s, item.actual_work)
            for item in joined
        }
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
                    f"{submission_path.name} has an incomplete "
                    "registered-ready service join"
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
                    bool(joined) and len(lifecycle) == len(joined)
                ),
                "ready_lifecycle_rows": lifecycle,
            }
        )
    return completion_accounted_service_fairness(evidence, weights)
