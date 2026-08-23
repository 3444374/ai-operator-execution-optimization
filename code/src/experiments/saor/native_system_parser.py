"""Typed top-level parser for the five-arm matched-system contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from src.experiments.saor.native_system_contract import (
    JobObservationIdentity,
    MatchedArm,
    MatchedSystemConfig,
)
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
    raw_observation = decoded.get(
        "job_observation_contracts",
        [
            {"job_id": "job0", "role": "bulk", "weight": 1.0, "request_slo_s": 30.0, "job_jct_slo_s": None},
            {"job_id": "job1", "role": "foreground", "weight": 1.0, "request_slo_s": 30.0, "job_jct_slo_s": 30.0},
        ],
    )
    if not isinstance(raw_observation, list):
        raise ValueError("job_observation_contracts must be a list")
    observation_contracts: list[JobObservationIdentity] = []
    for index, item in enumerate(raw_observation):
        required = {"job_id", "role", "weight", "request_slo_s"}
        if not isinstance(item, dict) or not required.issubset(item) or set(item) - (
            required | {"job_jct_slo_s"}
        ):
            raise ValueError("job_observation_contracts entries have an invalid schema")
        weight = item["weight"]
        request_slo_s = item["request_slo_s"]
        job_jct_slo_s = item.get("job_jct_slo_s")
        if (
            isinstance(weight, bool)
            or not isinstance(weight, (int, float))
            or float(weight) <= 0
            or isinstance(request_slo_s, bool)
            or not isinstance(request_slo_s, (int, float))
            or float(request_slo_s) <= 0
            or (
                job_jct_slo_s is not None
                and (
                    isinstance(job_jct_slo_s, bool)
                    or not isinstance(job_jct_slo_s, (int, float))
                    or float(job_jct_slo_s) <= 0
                )
            )
        ):
            raise ValueError(
                f"job_observation_contracts[{index}] weight/SLO must be positive"
            )
        observation_contracts.append(
            JobObservationIdentity(
                job_id=string(item["job_id"], "job_observation_contracts.job_id"),
                role=string(item["role"], "job_observation_contracts.role"),
                weight=float(weight),
                request_slo_s=float(request_slo_s),
                job_jct_slo_s=(
                    None if job_jct_slo_s is None else float(job_jct_slo_s)
                ),
            )
        )
    gateway_timeout = decoded.get("observation_gateway_request_timeout_s", 600.0)
    if (
        isinstance(gateway_timeout, bool)
        or not isinstance(gateway_timeout, (int, float))
        or float(gateway_timeout) <= 0
    ):
        raise ValueError("observation_gateway_request_timeout_s must be positive")
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
        job_observation_contracts=tuple(observation_contracts),
        observation_gateway_request_timeout_s=float(gateway_timeout),
    )
