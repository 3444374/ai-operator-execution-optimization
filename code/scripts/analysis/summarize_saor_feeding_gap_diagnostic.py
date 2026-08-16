#!/usr/bin/env python3
"""Recompute the D0/D1/P0 attribution and publish diagnostic-only evidence."""

from __future__ import annotations

import argparse
import csv
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
    summarize_feeding_gap,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--diagnostic-contract", required=True, type=Path)
    parser.add_argument("--prior-failed-contract", required=True, type=Path)
    args = parser.parse_args(argv)
    root = args.output_root.resolve()
    diagnostic_contract_path = args.diagnostic_contract.resolve()
    summary = summarize_feeding_gap(
        root,
        prior_contract_path=args.prior_failed_contract.resolve(),
        diagnostic_contract=load_diagnostic_contract(
            diagnostic_contract_path
        ),
        diagnostic_contract_sha256=sha256_file(diagnostic_contract_path),
    )
    _write_csv(root / "diagnostic_components.csv", summary["component_rows"])
    paired = summary.get("paired_repeats")
    if isinstance(paired, list):
        _write_csv(root / "diagnostic_paired_ratios.csv", paired)
    # Publish the verdict last. A passed validation therefore cannot coexist
    # with partially written component or paired-ratio evidence.
    temporary = root / "diagnostic_validation.json.tmp"
    temporary.write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, root / "diagnostic_validation.json")
    return 0 if summary.get("evidence_valid") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
