#!/usr/bin/env python3
"""Run the offline vLLM CLIP gate with a hard timeout and immutable evidence.

The worker gate can block while vLLM starts engine subprocesses.  This harness
runs it in a new process group, captures stdout/stderr, records the exact exit
status, and terminates the whole group on timeout.  It uses only Python/POSEX
facilities available on both the project's macOS and Linux environments.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument(
        "gate_args",
        nargs=argparse.REMAINDER,
        help="arguments for gate_vllm_clip_pooling.py; place them after --",
    )
    return parser.parse_args()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_process(
    command: list[str],
    *,
    timeout_seconds: int,
    stdout_path: Path,
    stderr_path: Path,
) -> tuple[int, bool]:
    """Run one command, returning ``(exit_code, timed_out)``."""

    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(
            command,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        try:
            return process.wait(timeout=timeout_seconds), False
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            return 124, True


def main() -> int:
    args = parse_args()
    if args.timeout_seconds <= 0:
        raise SystemExit("--timeout-seconds must be positive")
    if args.output_dir.exists():
        raise SystemExit(f"refusing to overwrite output directory: {args.output_dir}")
    args.output_dir.mkdir(parents=True)

    gate_args = list(args.gate_args)
    if gate_args and gate_args[0] == "--":
        gate_args = gate_args[1:]
    script = Path(__file__).with_name("gate_vllm_clip_pooling.py")
    result_json = args.output_dir / "result.json"
    command = [
        sys.executable,
        str(script),
        "--json-out",
        str(result_json),
        *gate_args,
    ]
    started_at = _utc_now()
    exit_code, timed_out = run_process(
        command,
        timeout_seconds=args.timeout_seconds,
        stdout_path=args.output_dir / "stdout.log",
        stderr_path=args.output_dir / "stderr.log",
    )
    _atomic_json(
        args.output_dir / "process_status.json",
        {
            "schema_version": 1,
            "started_at_utc": started_at,
            "finished_at_utc": _utc_now(),
            "command": command,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "timeout_seconds": args.timeout_seconds,
            "result_json_exists": result_json.is_file(),
        },
    )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
