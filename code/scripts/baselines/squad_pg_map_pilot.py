#!/usr/bin/env python3
"""Prepare SQuAD PG Map files or evaluate JSONL predictions; sends no requests."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[2]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.data.import_squad_workload import (  # noqa: E402
    _validate_dev_count, _validate_dev_sha256, parse_squad_dev,
)
from src.baselines.text.squad_map import (  # noqa: E402
    SPLITS, evaluate_predictions, prepare_manifest, validate_manifest,
)
from src.baselines.common.redact import redact_text  # noqa: E402
from src.baselines.common.private_artifacts import (  # noqa: E402
    new_private_directory, open_private_text, write_private_json,
)
from src.baselines.text.map_inputs import verify_text_roundtrip  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--source", type=Path, required=True)
    prepare.add_argument("--rows-per-split", type=int, default=64)
    prepare.add_argument("--output-dir", type=Path, required=True)
    evaluate = sub.add_parser("evaluate")
    evaluate.add_argument("--manifest", type=Path, required=True)
    evaluate.add_argument("--split", choices=SPLITS, required=True)
    evaluate.add_argument("--predictions", type=Path, required=True)
    evaluate.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        raw = args.source.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        _validate_dev_sha256(digest)
        examples = parse_squad_dev(json.loads(raw))
        _validate_dev_count(len(examples))
        manifest = prepare_manifest(examples, digest, rows_per_split=args.rows_per_split)
        new_private_directory(args.output_dir)
        write_private_json(args.output_dir / "manifest.json", manifest)
        restored = json.loads((args.output_dir / "manifest.json").read_text())
        validate_manifest(restored)
        if restored != manifest:
            raise ValueError("SQuAD prepared manifest roundtrip mismatch")
        for split in SPLITS:
            expected = [(row["source_example_id"], row["input_text"]) for row in manifest["splits"][split]]
            with open_private_text(args.output_dir / f"{split}.csv", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(["source_example_id", "input_text"])
                writer.writerows(expected)
            with (args.output_dir / f"{split}.csv").open(newline="", encoding="utf-8") as stream:
                actual = [(row["source_example_id"], row["input_text"]) for row in csv.DictReader(stream)]
            verify_text_roundtrip(expected, actual)
        print(json.dumps({"status": "prepared", "manifest_sha256": manifest["sha256"], "model_requests": 0}))
        return 0
    manifest = json.loads(args.manifest.read_text())
    with args.predictions.open(encoding="utf-8") as stream:
        report = evaluate_predictions(manifest, args.split, (json.loads(line) for line in stream if line.strip()))
    with args.output.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report))
    return 0 if report["association_complete"] and not report["null_predictions"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, KeyError, OSError) as error:
        print(redact_text(f"{type(error).__name__}: {error}"), file=sys.stderr)
        raise SystemExit(1)
