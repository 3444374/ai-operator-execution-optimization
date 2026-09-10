#!/usr/bin/env python3
"""Prepare verbatim ShareGPT inputs, inspect local tokens, or export a public summary.

No command connects to a database, sends inference requests, or downloads assets.
"""

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys

CODE_ROOT = Path(__file__).resolve().parents[2]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.private_artifacts import (
    content_digest, new_private_directory, write_private_json, workload_summary,
)
from src.baselines.common.redact import redact_text
from src.baselines.text.map_inputs import context_report, verify_text_roundtrip
from src.baselines.text.sharegpt_inputs import prepare_sharegpt_manifest, validate_sharegpt_manifest
from src.baselines.text.squad_map import validate_manifest


def load_workload(path):
    value = json.loads(path.read_text())
    if value.get("schema") == "squad_v11_pg_map_v1":
        validate_manifest(value)
    else:
        validate_sharegpt_manifest(value)
    return value


def prepare(args):
    if args.source.stat().st_size > 1024**3:
        raise ValueError("this bounded source loader accepts at most 1 GiB")
    raw = args.source.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != args.source_sha256:
        raise ValueError("ShareGPT original file SHA256 mismatch")
    manifest = prepare_sharegpt_manifest(json.loads(raw), digest, count=args.count,
        minimum_bytes=args.minimum_bytes, maximum_bytes=args.maximum_bytes)
    new_private_directory(args.output_dir)
    destination = args.output_dir / "manifest.json"
    write_private_json(destination, manifest)
    restored = load_workload(destination)
    verify_text_roundtrip(
        ((row["source_example_id"], row["input_text"]) for row in manifest["rows"]),
        ((row["source_example_id"], row["input_text"]) for row in restored["rows"]),
    )
    print(json.dumps(workload_summary(restored)))
    return 0


def inspect_tokens(args):
    from transformers import AutoTokenizer

    if not args.tokenizer.is_dir():
        raise ValueError("tokenizer must be an existing local directory")
    manifest = load_workload(args.manifest)
    if manifest["schema"] == "squad_v11_pg_map_v1":
        if args.split not in ("tuning", "evaluation") or args.instruction_file:
            raise ValueError("SQuAD requires its recorded instruction and an explicit split")
        rows = manifest["splits"][args.split]
        instruction, output_tokens = manifest["instruction"], manifest["generation"]["max_tokens"]
        if args.output_tokens is not None and args.output_tokens != output_tokens:
            raise ValueError("SQuAD output budget differs from its workload contract")
    else:
        if args.split or not args.instruction_file or args.output_tokens is None:
            raise ValueError("ShareGPT requires an explicit task instruction and output budget")
        rows = manifest["rows"]
        instruction, output_tokens = args.instruction_file.read_bytes().decode("utf-8"), args.output_tokens
    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer, local_files_only=True)
    report = context_report(
        ((row["source_example_id"], row["input_text"]) for row in rows), instruction, tokenizer,
        model_id=args.model_id, model_revision=args.model_revision,
        output_tokens=output_tokens, context_tokens=args.context_tokens,
    )
    report.update(
        scope="local_tokenizer_preflight_not_live_service_qualification",
        source_manifest_sha256=manifest["sha256"],
        tokenizer_template_sha256=content_digest(tokenizer.get_chat_template()),
        tokenizer_artifact_sha256={path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(args.tokenizer.iterdir()) if path.is_file() and
            (path.name.startswith(("tokenizer", "special_tokens", "vocab", "merges", "chat_template")))},
        transformers_version=importlib.metadata.version("transformers"),
    )
    write_private_json(args.output, report)
    print(json.dumps({"fits_context": report["fits_context"], "rows": len(report["rows"]), "model_requests": 0}))
    return 0 if report["fits_context"] else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    source = sub.add_parser("sharegpt")
    source.add_argument("--source", required=True, type=Path)
    source.add_argument("--source-sha256", required=True)
    source.add_argument("--count", type=int, default=16)
    source.add_argument("--minimum-bytes", type=int, default=256)
    source.add_argument("--maximum-bytes", type=int, default=2048)
    source.add_argument("--output-dir", required=True, type=Path)
    tokens = sub.add_parser("tokens")
    tokens.add_argument("--manifest", type=Path, required=True)
    tokens.add_argument("--split", choices=("tuning", "evaluation"))
    tokens.add_argument("--instruction-file", type=Path)
    tokens.add_argument("--output-tokens", type=int)
    tokens.add_argument("--tokenizer", type=Path, required=True)
    tokens.add_argument("--model-id", required=True)
    tokens.add_argument("--model-revision", required=True)
    tokens.add_argument("--context-tokens", type=int, required=True)
    tokens.add_argument("--output", type=Path, required=True)
    summary = sub.add_parser("summary")
    summary.add_argument("--manifest", type=Path, required=True)
    summary.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "sharegpt":
        return prepare(args)
    if args.command == "tokens":
        return inspect_tokens(args)
    value = workload_summary(load_workload(args.manifest))
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, KeyError, OSError) as error:
        print(redact_text(f"{type(error).__name__}: {error}"), file=sys.stderr)
        raise SystemExit(1)
