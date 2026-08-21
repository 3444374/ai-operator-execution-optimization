"""Typed top-level parser for the five-arm matched-system contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from src.experiments.saor.native_system_contract import MatchedArm, MatchedSystemConfig
from src.infrastructure.config_env import expand_structure


def parse_matched_system_config(
    path: Path,
    *,
    arm_loader: Callable[[object, Path], MatchedArm],
    path_resolver: Callable[[object, str, Path], str],
    integer: Callable[[object, str], int],
    nonnegative: Callable[[object, str], int],
    string: Callable[[object, str], str],
    boolean: Callable[[object, str], bool],
    mapping: Callable[[object, str], tuple[tuple[str, object], ...]],
) -> MatchedSystemConfig:
    """Parse only; semantic validation is a separate stage."""

    decoded = expand_structure(json.loads(path.read_text(encoding="utf-8")), "config")
    if not isinstance(decoded, dict) or decoded.get("schema_version") != 1:
        raise ValueError("matched-system config schema_version must be 1")
    arms_raw = decoded.get("arms")
    if not isinstance(arms_raw, list):
        raise ValueError("arms must be a list")
    directory = path.parent.resolve()
    return MatchedSystemConfig(
        seed=integer(decoded.get("seed"), "seed"),
        warmup_repeats=nonnegative(decoded.get("warmup_repeats"), "warmup_repeats"),
        formal_repeats=nonnegative(decoded.get("formal_repeats"), "formal_repeats"),
        matrix_output_root=path_resolver(
            decoded.get("matrix_output_root"), "matrix_output_root", directory
        ),
        endpoint_urls=tuple(
            string(item, "endpoint_urls")
            for item in decoded.get("endpoint_urls", [])
        ),
        gpu_formal_locally_authorized=boolean(
            decoded.get("gpu_formal_locally_authorized"),
            "gpu_formal_locally_authorized",
        ),
        matched_manifest_status=string(
            decoded.get("matched_manifest_status"), "matched_manifest_status"
        ),
        service_identity=mapping(
            decoded.get("service_identity"), "service_identity"
        ),
        arms=tuple(arm_loader(item, directory) for item in arms_raw),
    )
