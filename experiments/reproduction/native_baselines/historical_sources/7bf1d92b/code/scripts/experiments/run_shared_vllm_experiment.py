#!/usr/bin/env python3
"""Run preregistered shared-vLLM multi-job scenario groups."""

from __future__ import annotations

import sys
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.shared_vllm.cli import (  # noqa: E402
    parse_runner_args as parse_args,
)
from src.experiments.shared_vllm.preflight import wait_for_idle  # noqa: E402
from src.experiments.shared_vllm.runner import run_experiment  # noqa: E402


def main() -> None:
    raise SystemExit(
        run_experiment(
            parse_args(),
            idle_gate=wait_for_idle,
        )
    )


if __name__ == "__main__":
    main()
