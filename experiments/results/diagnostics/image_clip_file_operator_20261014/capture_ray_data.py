#!/usr/bin/env python3
"""Prepared diagnostic: capture formal native Ray Data outputs at the audit only.

Not executed in this result. The owning coordinator must hold the existing host
lease, serialize all three arms and independently prove physical process/device
exit. This wrapper does not acquire/recover/release a lease or schedule a graph.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys


MEASURED_SOURCE = "45da0a5451bfcf785d98b9ab5336054bdbaf199f"
FORMAL_IDS_SHA256 = "532dbd946317ae8629dde9d7fda5a26c8c83a781ddf7d4ca29b6a25007d3ce03"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--lease-file", type=Path, required=True)
    parser.add_argument("original_cli_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    cli_args = args.original_cli_args
    if cli_args[:1] == ["--"]:
        cli_args = cli_args[1:]
    sidecar = Path(str(args.capture) + ".manifest.json")
    if args.capture.suffix != ".npz" or args.capture.exists() or sidecar.exists():
        raise ValueError("capture must be a new .npz path, with a new sidecar")
    artifact_root = os.environ.get("ARTIFACT_ROOT")
    if not artifact_root or args.lease_file.resolve() != (Path(artifact_root) / ".host-runner-scope/.runner-lease.json").resolve():
        raise RuntimeError("use the canonical shared ARTIFACT_ROOT host lease")
    lease = json.loads(args.lease_file.read_text())
    token = os.environ.get("SEMLOOM_CLIP_LEASE_OWNER_TOKEN")
    if not token or lease["owner_token"] != token or lease["pid"] != os.getppid():
        raise RuntimeError("direct parent must own the existing native host lease")
    if lease["repository_commit"] != MEASURED_SOURCE or lease["config_fingerprint"] != "host-exclusive-experiment-runner-v1":
        raise RuntimeError("lease source/scope does not match this diagnostic")
    if lease["hostname"] != socket.gethostname() or not lease["process_start_id"]:
        raise RuntimeError("lease host/birth identity is unavailable or changed")
    source_files = json.loads((Path(__file__).parent / "raw/source-files.json").read_text())["files"]
    for item in source_files:
        if hashlib.sha256((args.source_root / item["path"]).read_bytes()).hexdigest() != item["sha256"]:
            raise RuntimeError(f"source mismatch: {item['path']}")
    # Parse through the original CLI, including aliases and last-wins behavior,
    # before starting execution. Never rely on literal flag-count checks.
    sys.path.insert(0, str(args.source_root / "code"))
    spec = importlib.util.spec_from_file_location("clip_original_diagnostic_cli", args.source_root / "code/scripts/experiments/run_image_clip_e2e.py")
    cli = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = cli
    spec.loader.exec_module(cli)
    sys.argv = [str(spec.origin)] + cli_args
    parsed = cli.parse_args()
    required = {"arm": "ray_data_staged", "phase": "gate", "limit": 257, "offset": 0, "dataset_passes": 1, "warmup_rows": 64, "batch_size": 64, "source_kind": "files", "source_shards": 2, "cpu_workers": 4, "gpu_workers": 1, "torch_intraop_threads": 1, "torch_interop_threads": 1, "input_size": 224, "dtype": "float16", "embedding_dimension": 512, "embedding_output_contract": "l2_normalized", "ray_address": "", "ray_data_autoscaling_actor_pools": False, "expected_source_doc_ids_sha256": FORMAL_IDS_SHA256, "expected_input_encoded_bytes": 41184449}
    for name, value in required.items():
        if getattr(parsed, name) != value:
            raise ValueError(f"diagnostic requires {name}={value}")
    if parsed.save_embeddings or parsed.detailed_stage_timing:
        raise ValueError("use only the bounded audit capture in this diagnostic")
    if os.environ.get("RAY_ADDRESS", "") not in ("", "local"):
        raise ValueError("diagnostic must start its own local Ray runtime")
    provenance = json.loads((Path(__file__).parent / "provenance.json").read_text())
    if hashlib.sha256(Path(parsed.source_manifest).read_bytes()).hexdigest() != provenance["shared_manifest_sha256"]:
        raise ValueError("diagnostic requires the existing shared file manifest")
    if Path(parsed.model).resolve() != Path(parsed.processor).resolve():
        raise ValueError("diagnostic requires the same fixed model/processor folder")
    for filename, key in (("pytorch_model.bin", "weight_sha256"), ("config.json", "config_sha256"), ("preprocessor_config.json", "preprocessor_config_sha256")):
        if hashlib.sha256((Path(parsed.model) / filename).read_bytes()).hexdigest() != provenance["model"][key]:
            raise ValueError(f"model/processor file mismatch: {filename}")
    # Reading the native process identity helper does not import an SDK/model.
    lease_spec = importlib.util.spec_from_file_location("clip_diagnostic_lease", args.source_root / "code/src/infrastructure/runner_lease.py")
    lease_module = importlib.util.module_from_spec(lease_spec)
    sys.modules[lease_spec.name] = lease_module
    lease_spec.loader.exec_module(lease_module)
    if lease_module._process_start_id(lease["pid"]) != lease["process_start_id"]:
        raise RuntimeError("lease owner's process birth identity changed")
    from src.baselines.image.frameworks import ray_data
    from src.modalities.image.execution import EmbeddingAudit, EmbeddingCapture
    capture = EmbeddingCapture()
    formal_audits = []

    def capture_audit(*positional, **keywords):
        audit = EmbeddingAudit(*positional, **keywords)
        if len(audit.expected_doc_ids) == 257:
            fingerprint = hashlib.sha256(("\n".join(sorted(audit.expected_doc_ids))+"\n").encode()).hexdigest()
            if fingerprint != FORMAL_IDS_SHA256 or formal_audits:
                raise ValueError("unexpected/repeated formal ID set")
            audit.capture = capture
            formal_audits.append(audit)
        return audit

    original = ray_data.EmbeddingAudit
    ray_data.EmbeddingAudit = capture_audit
    try:
        cli.main()
    finally:
        ray_data.EmbeddingAudit = original
    if len(formal_audits) != 1:
        raise ValueError("formal native output capture did not occur")
    doc_ids, embeddings = capture.finish()
    if embeddings.shape != (257, 512) or len(set(doc_ids)) != 257:
        raise ValueError("capture shape/ID count differs from diagnostic")
    import numpy as np
    args.capture.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive file creation preserves any prior failed or completed output.
    with args.capture.open("xb") as handle:
        np.savez_compressed(handle, embeddings=embeddings, doc_ids=np.asarray(doc_ids, dtype=str))
    with sidecar.open("x") as handle:
        json.dump({"arm": "ray_data_staged", "source_candidate": MEASURED_SOURCE, "rows": 257, "capture_scope": "formal driver audit only; native graph/algorithm unchanged", "diagnostic_only": True, "performance_eligible": False, "npz_sha256": hashlib.sha256(args.capture.read_bytes()).hexdigest()}, handle, indent=2)
        handle.write("\n")


if __name__ == "__main__":
    main()
