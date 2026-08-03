"""Fail-closed calibration contract for downstream strategy experiments."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias


JsonScalar: TypeAlias = str | int | float | bool | None


@dataclass(frozen=True)
class CalibrationContract:
    path: str
    sha256: str
    selection: tuple[tuple[str, JsonScalar], ...]


def load_calibration_contract(
    path: Path,
    expected: dict[str, JsonScalar],
) -> CalibrationContract:
    if not path.is_file():
        raise ValueError(f"calibration selection file does not exist: {path}")
    raw = path.read_bytes()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"calibration selection file is not valid JSON: {path}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise ValueError("calibration selection schema_version must be 1")
    if payload.get("status") != "ready":
        raise ValueError("calibration selection status must be ready")

    selection = payload.get("selection")
    if not isinstance(selection, dict) or not selection:
        raise ValueError("calibration selection must be a non-empty object")
    for key, value in selection.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("calibration selection keys must be non-empty")
        if not isinstance(value, (str, int, float, bool)) and value is not None:
            raise ValueError("calibration selection values must be JSON scalars")

    evidence = payload.get("evidence")
    if not isinstance(evidence, dict):
        raise ValueError("calibration evidence must be an object")
    for required in ("feeding", "token_budget", "actor_pool"):
        item = evidence.get(required)
        if not isinstance(item, dict) or item.get("status") != "passed":
            raise ValueError(
                f"calibration evidence {required} must have status passed"
            )

    missing = sorted(set(expected) - set(selection))
    if missing:
        raise ValueError(
            "calibration selection is missing expected key(s): "
            + ", ".join(missing)
        )
    mismatches = {
        key: (expected_value, selection[key])
        for key, expected_value in expected.items()
        if selection[key] != expected_value
    }
    if mismatches:
        details = ", ".join(
            f"{key}: config={values[0]!r}, selection={values[1]!r}"
            for key, values in sorted(mismatches.items())
        )
        raise ValueError(f"calibration selection mismatch: {details}")

    return CalibrationContract(
        path=str(path.resolve()),
        sha256=hashlib.sha256(raw).hexdigest(),
        selection=tuple(sorted(selection.items())),
    )
