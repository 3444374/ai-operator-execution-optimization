#!/usr/bin/env python3
"""Run the fail-closed Project SAOR mechanism rehearsal/formal matrix."""

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

from scripts.analysis.audit_saor_formal_readiness import audit  # noqa: E402
from scripts.analysis.summarize_saor_project_mechanism_formal import (  # noqa: E402
    validate_rehearsal_root,
)
from scripts.experiments.run_ai_operator_scenarios import (  # noqa: E402
    wait_for_idle,
)
from scripts.experiments.run_shared_vllm_experiment import (  # noqa: E402
    parse_args as parse_runner_args,
)
from src.experiments.saor.project_mechanism_formal import (  # noqa: E402
    contract_snapshot,
    load_contract,
    validate_calibration_artifact,
    validate_contract,
)
from src.experiments.shared_vllm.config import (  # noqa: E402
    RunnerOptions,
    load_config,
)
from src.experiments.shared_vllm.runner import run_experiment  # noqa: E402


def parse_args(
    argv: list[str] | None = None,
) -> tuple[Path, RunnerOptions]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--evaluation-contract", required=True, type=Path)
    known, remaining = parser.parse_known_args(argv)
    return known.evaluation_contract.resolve(), parse_runner_args(remaining)


def prepare(
    contract_path: Path,
    runner_options: RunnerOptions,
) -> dict[str, object]:
    payload = load_contract(contract_path)
    config = load_config(runner_options.config_path)
    errors = validate_contract(
        payload,
        config,
        formal_run=not runner_options.rehearsal,
    )
    readiness = audit(
        runner_options.config_path,
        profile="matched_ready_selector_ablation",
    )
    if readiness["status"] != "passed":
        errors.extend(str(error) for error in readiness["errors"])
    calibration_identity, calibration_errors = validate_calibration_artifact(
        payload,
        config,
    )
    errors.extend(calibration_errors)
    readiness = {
        **readiness,
        "work_cost_calibration_identity": calibration_identity,
    }
    if errors:
        raise ValueError("; ".join(errors))
    return contract_snapshot(contract_path, payload, readiness)


def main(argv: list[str] | None = None) -> int:
    contract_path, options = parse_args(argv)
    snapshot = prepare(contract_path, options)
    options.output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = options.output_dir / "project_mechanism_contract.json"
    if snapshot_path.exists():
        existing = json.loads(snapshot_path.read_text(encoding="utf-8"))
        if existing != snapshot:
            raise ValueError("existing Project mechanism contract snapshot drifted")
    else:
        snapshot_path.write_text(
            json.dumps(snapshot, indent=2) + "\n",
            encoding="utf-8",
        )
    result = run_experiment(options, idle_gate=wait_for_idle)
    if result == 0 and options.rehearsal:
        validate_rehearsal_root(options.output_dir, contract_path)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
