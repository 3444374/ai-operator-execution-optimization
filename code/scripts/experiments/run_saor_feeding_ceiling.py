#!/usr/bin/env python3
"""Run one fail-closed direct feeding ceiling matched to the SAOR matrix."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.saor.feeding_ceiling import (  # noqa: E402
    validate_ceiling_config,
)
from src.experiments.saor.project_mechanism_formal import (  # noqa: E402
    load_contract,
    sha256_file,
    validate_calibration_artifact,
    validate_contract,
)
from src.experiments.shared_vllm.config import load_config  # noqa: E402
from src.experiments.shared_vllm.cli import parse_runner_args  # noqa: E402
from src.experiments.shared_vllm.preflight import wait_for_idle  # noqa: E402
from src.experiments.shared_vllm.runner import run_experiment  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--evaluation-contract", required=True, type=Path)
    parser.add_argument("--reference-config", required=True, type=Path)
    known, remaining = parser.parse_known_args(argv)
    options = parse_runner_args(remaining)
    if not options.rehearsal:
        raise ValueError("feeding ceiling must run with --rehearsal")
    contract_path = known.evaluation_contract.resolve()
    reference_path = known.reference_config.resolve()
    payload = load_contract(contract_path)
    reference = load_config(reference_path)
    ceiling = load_config(options.config_path)
    errors = validate_contract(payload, reference, formal_run=False)
    errors.extend(validate_ceiling_config(reference, ceiling))
    calibration, calibration_errors = validate_calibration_artifact(
        payload,
        ceiling,
    )
    errors.extend(calibration_errors)
    if errors:
        raise ValueError("; ".join(errors))
    snapshot = {
        "schema_version": 1,
        "status": "matched_ready_to_run",
        "reference_contract_sha256": sha256_file(contract_path),
        "reference_config_sha256": sha256_file(reference_path),
        "ceiling_config_sha256": sha256_file(options.config_path),
        "work_cost_calibration_identity": calibration,
    }
    options.output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = options.output_dir / "feeding_ceiling_contract.json"
    if snapshot_path.exists():
        existing = json.loads(snapshot_path.read_text(encoding="utf-8"))
        if existing != snapshot:
            raise ValueError("existing feeding ceiling snapshot drifted")
    else:
        snapshot_path.write_text(
            json.dumps(snapshot, indent=2) + "\n",
            encoding="utf-8",
        )
    return run_experiment(options, idle_gate=wait_for_idle)


if __name__ == "__main__":
    raise SystemExit(main())
