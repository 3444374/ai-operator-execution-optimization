#!/usr/bin/env python3
"""Run the original project CLI once with delegating diagnostic observers."""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import socket
import sys

import clip_trace_hooks


def main():
    source_cli = Path(sys.argv[1])
    cli_args = sys.argv[2:]
    artifact_root = os.environ["ARTIFACT_ROOT"]
    lease = json.loads((Path(artifact_root) / ".host-runner-scope/.runner-lease.json").read_text())
    if not os.environ.get("SEMLOOM_CLIP_LEASE_OWNER_TOKEN") or lease["owner_token"] != os.environ["SEMLOOM_CLIP_LEASE_OWNER_TOKEN"] or lease["pid"] != os.getppid():
        raise RuntimeError("direct parent must own the canonical shared native lease")
    if lease["repository_commit"] != "45da0a5451bfcf785d98b9ab5336054bdbaf199f" or lease["hostname"] != socket.gethostname() or lease["config_fingerprint"] != "host-exclusive-experiment-runner-v1":
        raise RuntimeError("shared lease host/source/scope mismatch")
    birth = (Path('/proc') / str(lease['pid']) / 'stat').read_text().rsplit(')', 1)[1].split()[19]
    if not birth or birth != lease["process_start_id"]:
        raise RuntimeError("lease parent birth identity changed")
    spec = importlib.util.spec_from_file_location("original_project_clip_diagnostic", source_cli)
    cli = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = cli
    spec.loader.exec_module(cli)
    sys.argv = [str(source_cli)] + cli_args
    parsed = cli.parse_args()
    required = {"arm": "project_ray", "limit": 257, "offset": 0, "phase": "gate", "dataset_passes": 1, "warmup_rows": 64, "batch_size": 64, "cpu_workers": 4, "gpu_workers": 1, "source_shards": 2, "source_cpu_threads": 2, "torch_intraop_threads": 1, "torch_interop_threads": 1, "dtype": "float16", "input_size": 224, "embedding_dimension": 512, "embedding_output_contract": "l2_normalized", "expected_input_encoded_bytes": 41184449, "ray_address": ""}
    for name, value in required.items():
        if getattr(parsed, name) != value:
            raise ValueError(f"diagnostic requires {name}={value}")
    if parsed.project_execution_mode not in ("direct_dependency", "hse_static") or not parsed.save_embeddings or parsed.detailed_stage_timing:
        raise ValueError("fixed project mode, bounded capture and nonintrusive timing required")
    if os.environ.get("RAY_ADDRESS", "") not in ("", "local"):
        raise ValueError("diagnostic requires its own local Ray runtime")
    restore = clip_trace_hooks.observe_cli(cli)
    try:
        cli.main()
    finally:
        restore()


if __name__ == "__main__":
    main()
