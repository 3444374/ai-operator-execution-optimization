#!/usr/bin/env python3
"""Fail-closed static preflight for the fixed-envelope SAOR formal matrix."""

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


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def audit(config_path: Path) -> dict[str, object]:
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
    if observed != EXPECTED:
        errors.append("scenario matrix does not match the frozen ten-scenario contract")
    try:
        direct = direct_control_contract(config)
    except (TypeError, ValueError) as exc:
        errors.append(f"direct request contract is invalid: {exc}")
        direct = None
    manifests: dict[str, dict[str, object]] = {}
    configured_cap = int(
        _argument_value(config.common_args, "--completion-max-tokens", "-1")
    )
    if configured_cap <= 0:
        errors.append("formal matrix requires a positive completion max token cap")
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
                request.estimated_work
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
        "scenario_count": len(config.scenarios),
        "warmup_runs_per_scenario": config.warmup_runs_per_scenario,
        "formal_repeats": config.formal_repeats,
        "arrival_time_scale": arrival_time_scale,
        "max_effective_manifest_span_s": max_effective_span_s,
        "min_pre_foreground_work_envelopes_per_endpoint": (
            min_pre_foreground_envelopes
        ),
        "pre_foreground_predicted_work_by_endpoint": pre_foreground_work,
        "service_metadata": dict(config.service_metadata),
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
    result = audit(args.config)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if result["status"] != "passed":
        raise ValueError("; ".join(result["errors"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
