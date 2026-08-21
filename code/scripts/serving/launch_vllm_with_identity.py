#!/usr/bin/env python3
"""Seal the vLLM Python environment identity, then exec the API server."""

from __future__ import annotations

import argparse
import importlib.metadata
import importlib.util
import json
import os
import sys
from pathlib import Path


def _process_start_time_ticks() -> str:
    raw = Path("/proc/self/stat").read_text(encoding="utf-8")
    close = raw.rfind(")")
    fields_after_comm = raw[close + 2:].split()
    if close < 0 or len(fields_after_comm) <= 19:
        raise RuntimeError("/proc/self/stat has no process start time")
    return fields_after_comm[19]


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--identity-output", required=True, type=Path)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("server_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.server_args or args.server_args[0] != "--":
        parser.error("server arguments must follow --")
    args.server_args = args.server_args[1:]
    return args


def main() -> int:
    args = _args()
    spec = importlib.util.find_spec("vllm")
    if spec is None or spec.origin is None:
        raise RuntimeError("vLLM is not installed in VLLM_PYTHON")
    package_root = Path(spec.origin).resolve().parent
    identity = {
        "schema_version": 1,
        "pid": os.getpid(),
        "process_start_time_ticks": _process_start_time_ticks(),
        "port": args.port,
        "python_executable_argv0": sys.executable,
        "sys_prefix": sys.prefix,
        "package_root": str(package_root),
        "package_version": importlib.metadata.version("vllm"),
    }
    args.identity_output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.identity_output.with_suffix(args.identity_output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(identity, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, args.identity_output)
    os.execv(
        sys.executable,
        [
            sys.executable,
            "-m",
            "vllm.entrypoints.openai.api_server",
            *args.server_args,
        ],
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
