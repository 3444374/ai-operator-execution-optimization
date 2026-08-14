#!/usr/bin/env python3
"""Run the locked D0/D1/P0 diagnostic without touching SAOR formal state."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.saor.feeding_gap_diagnostic import (  # noqa: E402
    load_diagnostic_contract,
    sha256_file,
    validate_diagnostic_config,
    validate_prior_failed_lock,
)
from src.experiments.saor.feeding_gap_preflight import (  # noqa: E402
    collect_pre_run_clean_gate,
    write_pre_run_clean_gate,
)
from src.experiments.shared_vllm.cli import parse_runner_args  # noqa: E402
from src.experiments.shared_vllm.config import load_config  # noqa: E402
from src.experiments.shared_vllm.preflight import wait_for_idle  # noqa: E402
from src.experiments.shared_vllm.runner import run_experiment  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--diagnostic-contract", required=True, type=Path)
    parser.add_argument("--prior-failed-contract", required=True, type=Path)
    parser.add_argument("--reference-config", required=True, type=Path)
    known, remaining = parser.parse_known_args(argv)
    options = parse_runner_args(remaining)
    if options.rehearsal:
        raise ValueError(
            "feeding-gap diagnostic uses its frozen 1+3 matrix; "
            "do not pass --rehearsal"
        )
    contract_path = known.diagnostic_contract.resolve()
    prior_path = known.prior_failed_contract.resolve()
    reference_path = known.reference_config.resolve()
    contract = load_diagnostic_contract(contract_path)
    reference = load_config(reference_path)
    diagnostic = load_config(options.config_path)
    errors = validate_prior_failed_lock(contract, prior_path)
    errors.extend(validate_diagnostic_config(reference, diagnostic, contract))
    if errors:
        raise ValueError("; ".join(errors))

    snapshot = {
        "schema_version": 1,
        "status": "diagnostic_only_ready",
        "may_change_prior_feeding_decision": False,
        "prior_decision_preserved": "locked_failed_feeding",
        "diagnostic_contract_sha256": sha256_file(contract_path),
        "prior_failed_contract_sha256": sha256_file(prior_path),
        "reference_config_sha256": sha256_file(reference_path),
        "diagnostic_config_sha256": sha256_file(options.config_path),
    }
    if options.output_dir.exists() and any(options.output_dir.iterdir()):
        raise ValueError("feeding-gap diagnostic requires an empty output root")
    options.output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = options.output_dir / "feeding_gap_contract_snapshot.json"
    clean_path = options.output_dir / "pre_run_clean_gate.json"
    if snapshot_path.exists() or clean_path.exists():
        raise ValueError("feeding-gap diagnostic requires a fresh output root")
    snapshot_temporary = snapshot_path.with_suffix(
        snapshot_path.suffix + ".tmp"
    )
    snapshot_temporary.write_text(
        json.dumps(snapshot, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(snapshot_temporary, snapshot_path)
    clean = collect_pre_run_clean_gate(
        diagnostic,
        metrics_urls=options.metrics_urls,
        ray_address=options.ray_address,
    )
    write_pre_run_clean_gate(clean_path, clean)
    if clean.get("status") != "passed":
        raise RuntimeError("structured pre-run clean gate failed")
    return run_experiment(options, idle_gate=wait_for_idle)


if __name__ == "__main__":
    raise SystemExit(main())
