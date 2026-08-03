"""Reproducible process and Ray worker environment helpers."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path


NUMERIC_THREAD_ENV = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
}


def subprocess_env(
    base: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Limit implicit numeric-library pools in each experiment process."""

    resolved = dict(os.environ if base is None else base)
    resolved.update(NUMERIC_THREAD_ENV)
    return resolved


def ray_runtime_env(code_root: Path) -> dict[str, dict[str, str]]:
    """Export project imports and numeric thread limits to Ray workers."""

    pythonpath = str(code_root)
    existing_pythonpath = os.environ.get("PYTHONPATH")
    if existing_pythonpath:
        pythonpath = os.pathsep.join([pythonpath, existing_pythonpath])
    return {
        "env_vars": {
            "PYTHONPATH": pythonpath,
            **NUMERIC_THREAD_ENV,
        }
    }
