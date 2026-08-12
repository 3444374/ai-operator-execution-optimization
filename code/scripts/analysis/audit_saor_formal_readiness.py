#!/usr/bin/env python3
"""Fail-closed static preflight for the fixed-envelope SAOR formal matrix."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.manifests import read_manifest  # noqa: E402
from src.experiments.shared_vllm.config import (  # noqa: E402
    _argument_value,
    load_config,
)
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
    config = load_config(config_path.resolve())
    errors: list[str] = []
    if config.warmup_runs_per_scenario != 1 or config.formal_repeats != 3:
        errors.append("formal matrix requires exactly 1 warmup + 3 formal")
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
    active_reference: tuple[str, ...] | None = None
    active_offsets: tuple[float, ...] | None = None
    doc_owners: dict[int, str] = {}
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
            manifests[str(path.resolve())] = {
                "rows": len(requests),
                "sha256": digest,
                "endpoint_indices": sorted(endpoint_indices),
            }
    if config.calibration_contract is None:
        errors.append("formal matrix requires a validated calibration contract")
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
