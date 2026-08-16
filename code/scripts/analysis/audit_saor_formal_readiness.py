#!/usr/bin/env python3
"""Fail-closed static preflight for frozen SAOR active-set matrices."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.contracts import ChatRequest  # noqa: E402
from src.baselines.common.manifests import read_manifest  # noqa: E402
from src.experiments.shared_vllm.config import (  # noqa: E402
    _argument_value,
    load_config,
)
from src.infrastructure.config_env import expand_scalar  # noqa: E402
from src.experiments.shared_vllm.direct_control import (  # noqa: E402
    direct_control_contract,
)


EXPECTED = {
    "active_set_direct_no_job": ("direct_no_job", 2),
    "active_set_static_partition": ("static_partition", 2),
    "active_set_shared_fifo": ("shared_fifo", 2),
    "active_set_shared_drr": ("shared_drr", 2),
    "active_set_external_vtc": ("external_vtc", 2),
    "active_set_saor_release": ("saor_release", 2),
    "solo_project_bulk": ("independent_full", 1),
    "solo_project_foreground": ("independent_full", 1),
    "solo_direct_bulk": ("direct_no_job", 1),
    "solo_direct_foreground": ("direct_no_job", 1),
}
PRIORITY_REACHABILITY_EXPECTED = {
    "active_set_static_partition": ("static_partition", 2),
    "active_set_saor_release": ("saor_release", 2),
    "active_set_foreground_strict_priority": (
        "foreground_strict_priority",
        2,
    ),
}
BOUNDED_PRIORITY_EXPECTED = {
    "active_set_static_partition": ("static_partition", 2),
    "active_set_saor_release": ("saor_release", 2),
    "active_set_saor_bounded_priority_0125k": (
        "saor_bounded_priority",
        2,
    ),
    "active_set_saor_bounded_priority_025k": (
        "saor_bounded_priority",
        2,
    ),
}
BOUNDED_READY_EXPECTED = {
    "active_set_static_partition": ("static_partition", 2),
    "active_set_saor_release": ("saor_release", 2),
    "active_set_saor_bounded_ready_0125k": ("saor_bounded_ready", 2),
    "active_set_saor_bounded_ready_025k": ("saor_bounded_ready", 2),
}
MATCHED_READY_ABLATION_EXPECTED = {
    "active_set_project_frozen_static": ("static_partition", 2),
    "active_set_project_bounded_ready_fifo": ("shared_fifo", 2),
    "active_set_project_bounded_ready_drr": ("shared_drr", 2),
    "active_set_project_bounded_ready_vtc_style": ("external_vtc", 2),
    "active_set_project_bounded_ready_strict_priority": (
        "foreground_strict_priority",
        2,
    ),
    "active_set_project_bounded_ready_guarded_debt_0125we": (
        "saor_bounded_ready",
        2,
    ),
}
READY_OBSERVATION_BRIDGE_EXPECTED = {
    "active_set_project_frozen_static": ("static_partition", 2),
    "active_set_project_single_head_shared_fifo": ("shared_fifo", 2),
    "active_set_project_bounded_ready_fifo": ("shared_fifo", 2),
}


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--profile",
        choices=(
            "formal",
            "priority_reachability",
            "bounded_priority_development",
            "bounded_ready_development",
            "matched_ready_selector_ablation",
            "ready_observation_bridge",
        ),
        default="formal",
    )
    return parser.parse_args()


def audit(
    config_path: Path,
    *,
    profile: str = "formal",
) -> dict[str, object]:
    expected = (
        EXPECTED
        if profile == "formal"
        else PRIORITY_REACHABILITY_EXPECTED
        if profile == "priority_reachability"
        else BOUNDED_PRIORITY_EXPECTED
        if profile == "bounded_priority_development"
        else BOUNDED_READY_EXPECTED
        if profile == "bounded_ready_development"
        else MATCHED_READY_ABLATION_EXPECTED
        if profile == "matched_ready_selector_ablation"
        else READY_OBSERVATION_BRIDGE_EXPECTED
        if profile == "ready_observation_bridge"
        else None
    )
    if expected is None:
        raise ValueError(f"unknown readiness profile: {profile}")
    raw_config = json.loads(config_path.resolve().read_text(encoding="utf-8"))
    config = load_config(config_path.resolve())
    errors: list[str] = []
    if config.warmup_runs_per_scenario != 1 or config.formal_repeats != 3:
        errors.append("formal matrix requires exactly 1 warmup + 3 formal")
    if not config.fail_closed_rehearsal:
        errors.append("formal matrix requires fail_closed_rehearsal=true")
    if dict(config.service_metadata).get("scheduling_policy") != "fcfs":
        errors.append("service_metadata.scheduling_policy must be explicit fcfs")
    observed = {
        scenario.scenario_id: (scenario.policy, scenario.job_count)
        for scenario in config.scenarios
    }
    if observed != expected:
        errors.append(f"scenario matrix does not match the frozen {profile} contract")
    expected_observation_contract = (
        "bounded_concrete_pre_registration"
        if profile == "bounded_ready_development"
        else "single_head"
    )
    if config.ready_observation_contract != expected_observation_contract:
        errors.append(
            "ready observation contract does not match the selected profile"
        )
    scenario_resource_contracts = {
        scenario.scenario_id: {
            "request_limit_per_endpoint": scenario.endpoint_limits(
                config.request_limit_per_endpoint,
                config.work_limit_per_endpoint,
            )[0],
            "work_limit_per_endpoint": scenario.endpoint_limits(
                config.request_limit_per_endpoint,
                config.work_limit_per_endpoint,
            )[1],
            "weights": list(scenario.weights),
        }
        for scenario in config.scenarios
    }
    if profile in {
        "matched_ready_selector_ablation",
        "ready_observation_bridge",
    }:
        expected_limits = (
            config.request_limit_per_endpoint,
            config.work_limit_per_endpoint,
        )
        for scenario in config.scenarios:
            if scenario.endpoint_limits(
                config.request_limit_per_endpoint,
                config.work_limit_per_endpoint,
            ) != expected_limits:
                errors.append(
                    f"{scenario.scenario_id} effective request/work limits "
                    "drift from the frozen root contract"
                )
            if scenario.weights != (1, 1):
                errors.append(
                    f"{scenario.scenario_id} weights drift from frozen (1, 1)"
                )
    if profile in {
        "bounded_priority_development",
        "bounded_ready_development",
    }:
        bounded_policy = (
            "saor_bounded_priority"
            if profile == "bounded_priority_development"
            else "saor_bounded_ready"
        )
        bounded = [
            scenario
            for scenario in config.scenarios
            if scenario.policy == bounded_policy
        ]
        if [scenario.priorities for scenario in bounded] != [(0, 1), (0, 1)]:
            errors.append("bounded priority roles must be explicit bulk=0/foreground=1")
        if [scenario.slo_targets_s for scenario in bounded] != [
            (None, 30.0),
            (None, 30.0),
        ]:
            errors.append("bounded priority foreground SLO must be frozen at 30s")
        if [scenario.priority_windows_s for scenario in bounded] != [
            (None, 30.0),
            (None, 30.0),
        ]:
            errors.append("bounded priority window must be frozen at 30s")
        if [scenario.debt_cap_fractions for scenario in bounded] != [
            (0.125, None),
            (0.25, None),
        ]:
            errors.append(
                "bounded debt caps must be frozen at 0.125W_e and 0.25W_e"
            )
    if profile == "matched_ready_selector_ablation":
        project_controls = [
            scenario
            for scenario in config.scenarios
            if scenario.policy != "static_partition"
        ]
        if not project_controls or any(
            scenario.ready_observation_contract
            != "bounded_concrete_pre_registration"
            for scenario in project_controls
        ):
            errors.append(
                "every project selector ablation must use matched bounded-ready"
            )
        static = [
            scenario
            for scenario in config.scenarios
            if scenario.policy == "static_partition"
        ]
        if len(static) != 1 or static[0].ready_observation_contract != "single_head":
            errors.append(
                "project frozen-static reference must not use bounded-ready"
            )
        proposed = [
            scenario
            for scenario in config.scenarios
            if scenario.policy == "saor_bounded_ready"
        ]
        if (
            len(proposed) != 1
            or proposed[0].priorities != (0, 1)
            or proposed[0].slo_targets_s != (None, 30.0)
            or proposed[0].priority_windows_s != (None, 30.0)
            or proposed[0].debt_cap_fractions != (0.125, None)
        ):
            errors.append(
                "proposed must freeze H_B=0.125W_e and foreground SLO at 30s"
            )
    if profile == "ready_observation_bridge":
        by_id = {scenario.scenario_id: scenario for scenario in config.scenarios}
        static = by_id.get("active_set_project_frozen_static")
        single_head = by_id.get("active_set_project_single_head_shared_fifo")
        bounded = by_id.get("active_set_project_bounded_ready_fifo")
        if (
            static is None
            or static.policy != "static_partition"
            or static.ready_observation_contract != "single_head"
        ):
            errors.append("bridge static reference must use single-head observation")
        if (
            single_head is None
            or single_head.policy != "shared_fifo"
            or single_head.ready_observation_contract != "single_head"
        ):
            errors.append("bridge shared-capacity control must use single-head FIFO")
        if (
            bounded is None
            or bounded.policy != "shared_fifo"
            or bounded.ready_observation_contract
            != "bounded_concrete_pre_registration"
        ):
            errors.append("bridge observation control must use bounded-ready FIFO")
    if profile == "formal":
        try:
            direct = direct_control_contract(config)
        except (TypeError, ValueError) as exc:
            errors.append(f"direct request contract is invalid: {exc}")
            direct = None
    else:
        direct = None
    manifests: dict[str, dict[str, object]] = {}
    configured_cap = int(
        _argument_value(config.common_args, "--completion-max-tokens", "-1")
    )
    if configured_cap <= 0:
        errors.append("formal matrix requires a positive completion max token cap")
    try:
        prompt_token_overhead = int(
            _argument_value(
                config.common_args,
                "--completion-prompt-token-overhead",
                "0",
            )
        )
    except ValueError:
        prompt_token_overhead = -1
    if prompt_token_overhead < 0:
        errors.append("completion prompt token overhead must be non-negative")
    try:
        arrival_time_scale = float(
            _argument_value(config.common_args, "--arrival-time-scale", "nan")
        )
    except ValueError:
        arrival_time_scale = math.nan
    if not math.isfinite(arrival_time_scale) or arrival_time_scale <= 0:
        errors.append("arrival-time-scale must be finite and positive")
    readiness = raw_config.get("readiness_contract")
    if not isinstance(readiness, dict):
        errors.append("formal matrix requires a readiness_contract")
        max_effective_span_s = math.nan
        min_pre_foreground_envelopes = math.nan
    else:
        try:
            max_effective_span_s = float(
                expand_scalar(
                    readiness.get("max_effective_manifest_span_s"),
                    "readiness_contract.max_effective_manifest_span_s",
                    environment=os.environ,
                )
            )
        except (TypeError, ValueError):
            max_effective_span_s = math.nan
        if not math.isfinite(max_effective_span_s) or max_effective_span_s <= 0:
            errors.append(
                "readiness max_effective_manifest_span_s must be finite and positive"
            )
        try:
            min_pre_foreground_envelopes = float(
                expand_scalar(
                    readiness.get(
                        "min_pre_foreground_work_envelopes_per_endpoint"
                    ),
                    (
                        "readiness_contract."
                        "min_pre_foreground_work_envelopes_per_endpoint"
                    ),
                    environment=os.environ,
                )
            )
        except (TypeError, ValueError):
            min_pre_foreground_envelopes = math.nan
        if (
            not math.isfinite(min_pre_foreground_envelopes)
            or min_pre_foreground_envelopes <= 0
        ):
            errors.append(
                "readiness pre-foreground work-envelope factor must be "
                "finite and positive"
            )
    active_reference: tuple[str, ...] | None = None
    active_offsets: tuple[float, ...] | None = None
    doc_owners: dict[int, str] = {}
    manifest_requests: dict[str, tuple[ChatRequest, ...]] = {}
    for scenario in config.scenarios:
        paths = tuple(str(path) for path in scenario.request_manifests)
        if not paths or len(paths) != scenario.job_count:
            errors.append(f"{scenario.scenario_id} lacks immutable manifests")
            continue
        if scenario.scenario_id.startswith("active_set_"):
            if active_reference is None:
                active_reference = paths
            elif paths != active_reference:
                errors.append("active-set arms do not use identical manifests in order")
            if active_offsets is None:
                active_offsets = scenario.arrival_offsets_s
            elif scenario.arrival_offsets_s != active_offsets:
                errors.append("active-set arms do not use identical arrival offsets")
            if not (
                len(scenario.arrival_offsets_s) == 2
                and math.isclose(scenario.arrival_offsets_s[0], 0.0, abs_tol=1e-9)
                and scenario.arrival_offsets_s[1] > 0
            ):
                errors.append("active-set contract requires bulk@0 and foreground@positive")
        for index, raw_path in enumerate(paths):
            path = Path(raw_path)
            if not path.is_file():
                errors.append(f"manifest is missing: {path}")
                continue
            requests = read_manifest(path)
            manifest_requests[str(path.resolve())] = requests
            if len(requests) != scenario.row_count(index):
                errors.append(f"{scenario.scenario_id} job {index} row count mismatch")
            ids = {request.doc_id for request in requests}
            for doc_id in ids:
                owner = doc_owners.setdefault(doc_id, str(path.resolve()))
                if owner != str(path.resolve()):
                    errors.append(
                        "bulk/foreground manifests reuse a doc_id across files"
                    )
            endpoint_indices = {request.endpoint_index for request in requests}
            if endpoint_indices != set(range(len(config.endpoint_ids))):
                errors.append(
                    f"{scenario.scenario_id} job {index} does not cover every endpoint"
                )
            if {request.max_output_tokens for request in requests} != {
                configured_cap
            }:
                errors.append(
                    f"{scenario.scenario_id} job {index} output cap mismatch"
                )
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            arrivals = [request.arrival_time_s for request in requests]
            raw_span_s = max(arrivals) - min(arrivals) if arrivals else 0.0
            effective_span_s = raw_span_s * arrival_time_scale
            if (
                math.isfinite(max_effective_span_s)
                and effective_span_s > max_effective_span_s
            ):
                errors.append(
                    f"manifest effective replay span exceeds readiness bound: {path}"
                )
            manifests[str(path.resolve())] = {
                "rows": len(requests),
                "sha256": digest,
                "endpoint_indices": sorted(endpoint_indices),
                "raw_arrival_span_s": raw_span_s,
                "effective_arrival_span_s": effective_span_s,
            }
    pre_foreground_work: dict[str, int] = {}
    if active_reference and active_offsets and len(active_reference) == 2:
        bulk_path = str(Path(active_reference[0]).resolve())
        bulk_requests = manifest_requests.get(bulk_path, ())
        foreground_offset_s = active_offsets[1]
        first_bulk_arrival_s = min(
            (request.arrival_time_s for request in bulk_requests),
            default=0.0,
        )
        for endpoint_index in range(len(config.endpoint_ids)):
            work = sum(
                request.estimated_work + prompt_token_overhead
                for request in bulk_requests
                if request.endpoint_index == endpoint_index
                and (
                    request.arrival_time_s - first_bulk_arrival_s
                )
                * arrival_time_scale
                < foreground_offset_s
            )
            endpoint_id = config.endpoint_ids[endpoint_index]
            pre_foreground_work[endpoint_id] = work
            required = (
                config.work_limit_per_endpoint
                * min_pre_foreground_envelopes
            )
            if math.isfinite(required) and work < required:
                errors.append(
                    "bulk pre-foreground predicted work does not cover the "
                    f"required envelope on {endpoint_id}: {work} < {required}"
                )
    if config.calibration_contract is None:
        errors.append("formal matrix requires a validated calibration contract")
    else:
        calibration_payload = json.loads(
            Path(config.calibration_contract.path).read_text(encoding="utf-8")
        )
        selection = calibration_payload.get("selection", {})
        evidence = calibration_payload.get("evidence", {})
        token_evidence = (
            evidence.get("token_budget", {})
            if isinstance(evidence, dict)
            else {}
        )
        calibrated_budget = selection.get("best_token_budget")
        if calibrated_budget is None:
            calibrated_budget = token_evidence.get("frozen_token_budget")
        try:
            configured_budget = int(
                _argument_value(config.common_args, "--token-budget", "-1")
            )
            calibrated_budget = int(calibrated_budget)
        except (TypeError, ValueError):
            errors.append(
                "calibration contract lacks a numeric selected/frozen token budget"
            )
        else:
            if configured_budget != calibrated_budget:
                errors.append(
                    "configured token budget does not match calibration evidence"
                )
    if config.saor_release_control is None:
        errors.append("formal matrix requires executable SAOR release control")
    elif (
        config.saor_release_control.queue_weight != 0
        or config.saor_release_control.slo_weight != 0
    ):
        errors.append(
            "current formal matrix freezes unwired queue/SLO SAOR weights at zero"
        )
    status = "passed" if not errors else "failed"
    return {
        "schema_version": 1,
        "status": status,
        "errors": errors,
        "experiment_id": config.experiment_id,
        "profile": profile,
        "scenario_count": len(config.scenarios),
        "warmup_runs_per_scenario": config.warmup_runs_per_scenario,
        "formal_repeats": config.formal_repeats,
        "arrival_time_scale": arrival_time_scale,
        "max_effective_manifest_span_s": max_effective_span_s,
        "min_pre_foreground_work_envelopes_per_endpoint": (
            min_pre_foreground_envelopes
        ),
        "pre_foreground_predicted_work_by_endpoint": pre_foreground_work,
        "completion_prompt_token_overhead": prompt_token_overhead,
        "service_metadata": dict(config.service_metadata),
        "scenario_resource_contracts": scenario_resource_contracts,
        "calibration_contract": (
            {
                "path": config.calibration_contract.path,
                "sha256": config.calibration_contract.sha256,
                "selection": dict(config.calibration_contract.selection),
            }
            if config.calibration_contract is not None
            else None
        ),
        "direct_contract": (
            {
                "endpoint_urls": direct.endpoint_urls,
                "model": direct.model,
                "concurrency_per_endpoint": direct.concurrency_per_endpoint,
                "protocol": direct.protocol,
                "prompt_format": direct.prompt_format,
                "return_token_ids": direct.return_token_ids,
                "keepalive_expiry_s": direct.keepalive_expiry_s,
            }
            if direct is not None
            else None
        ),
        "manifests": manifests,
    }


def main() -> int:
    args = _args()
    result = audit(args.config, profile=args.profile)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if result["status"] != "passed":
        raise ValueError("; ".join(result["errors"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
