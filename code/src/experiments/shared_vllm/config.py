"""Shared-vLLM configuration, validation, and job-command construction."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path

from src.experiments.calibration.contracts import (
    CalibrationContract,
    JsonScalar,
    load_calibration_contract as validate_calibration_contract,
)
from src.experiments.scenarios.core import validate_service_metadata
from src.infrastructure.config_env import expand_scalar, expand_text


POLICIES = {
    "independent_full",
    "static_partition",
    "shared_drr",
}

_SCENARIO_ID = re.compile(r"^[A-Za-z0-9_.-]+$")

_RUNNER_OWNED_FLAGS = {
    "--admission-scope",
    "--arrival-replay-start-epoch-s",
    "--control-trace-output",
    "--experiment-id",
    "--flush-trace-output",
    "--max-active-work-per-endpoint",
    "--max-inflight",
    "--output",
    "--random-seed",
    "--ray-address",
    "--repeats",
    "--request-trace-output",
    "--request-manifest",
    "--resource-trace-output",
    "--reset-documents",
    "--run-phase",
    "--run-repeat-index",
    "--scenario-id",
    "--setup",
    "--shared-credit-coordinator-name",
    "--shared-credit-job-weight",
    "--shared-credit-namespace",
    "--shared-credit-quantum",
    "--shared-credit-request-limit",
    "--shared-credit-work-limit",
    "--source-row-offset",
    "--submission-granularity",
    "--submission-trace-output",
    "--total-rows",
    "--warmup-runs",
}

@dataclass(frozen=True)
class RunnerOptions:
    config_path: Path
    profiler_path: Path
    python_executable: Path
    output_dir: Path
    health_url: str
    metrics_urls: tuple[str, ...]
    ray_address: str
    idle_timeout_s: float
    start_delay_s: float = 15.0
    max_start_lateness_s: float = 2.0
    max_start_skew_s: float = 0.5
    resume: bool = False
    recover_stale_lease: bool = False

@dataclass(frozen=True)
class GroupRunIdentity:
    phase: str
    repeat_index: int
    order_index: int

@dataclass(frozen=True)
class SharedVllmScenario:
    scenario_id: str
    policy: str
    job_count: int
    rows_per_job: int
    weights: tuple[int, ...]
    arrival_offsets_s: tuple[float, ...]
    source_row_offsets: tuple[int, ...] = ()
    request_manifests: tuple[str | None, ...] = ()

@dataclass(frozen=True)
class SharedVllmConfig:
    experiment_id: str
    seed: int
    warmup_runs_per_scenario: int
    formal_repeats: int
    endpoint_ids: tuple[str, ...]
    request_limit_per_endpoint: int
    work_limit_per_endpoint: int
    credit_quantum: int
    shared_credit_namespace: str
    gpu_peak_tflops: float
    mfu_precision: str
    common_args: tuple[str, ...]
    scenarios: tuple[SharedVllmScenario, ...]
    service_metadata: tuple[tuple[str, object], ...]
    calibration_contract: CalibrationContract | None = None

def load_config(path: Path) -> SharedVllmConfig:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if decoded.get("schema_version") != 1:
        raise ValueError("shared-vLLM config schema_version must be 1")
    experiment_id = _nonempty_string(
        decoded.get("experiment_id"),
        "experiment_id",
    )
    seed = _integer(decoded.get("seed"), "seed")
    warmups = _nonnegative_integer(
        decoded.get("warmup_runs_per_scenario"),
        "warmup_runs_per_scenario",
    )
    repeats = _positive_integer(
        decoded.get("formal_repeats"),
        "formal_repeats",
    )
    endpoint_ids_raw = decoded.get("endpoint_ids")
    if (
        not isinstance(endpoint_ids_raw, list)
        or not endpoint_ids_raw
        or any(
            not isinstance(item, str) or not item.strip()
            for item in endpoint_ids_raw
        )
        or len(set(endpoint_ids_raw)) != len(endpoint_ids_raw)
    ):
        raise ValueError("endpoint_ids must be unique non-empty strings")
    request_limit = _positive_integer(
        _expand_scalar(
            decoded.get("request_limit_per_endpoint"),
            "request_limit_per_endpoint",
        ),
        "request_limit_per_endpoint",
    )
    work_limit = _positive_integer(
        _expand_scalar(
            decoded.get("work_limit_per_endpoint"),
            "work_limit_per_endpoint",
        ),
        "work_limit_per_endpoint",
    )
    quantum = _positive_integer(
        decoded.get("credit_quantum"),
        "credit_quantum",
    )
    namespace = _nonempty_string(
        decoded.get(
            "shared_credit_namespace",
            "ai-operator-shared-vllm",
        ),
        "shared_credit_namespace",
    )
    common_args = _argument_list(
        decoded.get("common_args", []),
        "common_args",
    )
    if "--arrival-replay" not in common_args:
        raise ValueError("common_args must enable --arrival-replay")
    gpu_peak_tflops = _nonnegative_float(
        _argument_value(common_args, "--gpu-peak-tflops", "0"),
        "--gpu-peak-tflops",
    )
    mfu_precision = _argument_value(
        common_args,
        "--mfu-precision",
        "",
    )
    scenarios_raw = decoded.get("scenarios")
    if not isinstance(scenarios_raw, list) or not scenarios_raw:
        raise ValueError("scenarios must be a non-empty list")
    scenarios = tuple(
        _load_scenario(item, request_limit, work_limit)
        for item in scenarios_raw
    )
    scenario_ids = [item.scenario_id for item in scenarios]
    if len(set(scenario_ids)) != len(scenario_ids):
        raise ValueError("scenario_id values must be unique")
    executor = _argument_value(common_args, "--executor", "")
    if executor == "ray_actor":
        expected_endpoint_ids = tuple(
            f"endpoint-{index}" for index in range(len(endpoint_ids_raw))
        )
        if tuple(endpoint_ids_raw) != expected_endpoint_ids:
            raise ValueError(
                "ray_actor endpoint_ids must match profiler actor topology: "
                + ",".join(expected_endpoint_ids)
            )
    if (
        any(scenario.job_count >= 4 for scenario in scenarios)
        and executor == "ray_task"
    ):
        raise ValueError(
            "four-or-more-job shared-vLLM runs require ray_actor with a "
            "bounded persistent actor pool; ray_task can expand to hundreds "
            "of worker processes and exhaust the host VMA map limit"
        )
    service_metadata = decoded.get("service_metadata", {})
    if not isinstance(service_metadata, dict):
        raise ValueError("service_metadata must be an object")
    expanded_metadata = tuple(
        sorted(
            (
                _nonempty_string(key, "service_metadata key"),
                _expand_scalar(value, f"service_metadata.{key}"),
            )
            for key, value in service_metadata.items()
        )
    )
    if decoded.get("require_complete_service_metadata") is True:
        validate_service_metadata(dict(expanded_metadata))
    calibration_contract = _load_calibration_contract(
        decoded.get("calibration_contract")
    )
    return SharedVllmConfig(
        experiment_id=experiment_id,
        seed=seed,
        warmup_runs_per_scenario=warmups,
        formal_repeats=repeats,
        endpoint_ids=tuple(endpoint_ids_raw),
        request_limit_per_endpoint=request_limit,
        work_limit_per_endpoint=work_limit,
        credit_quantum=quantum,
        shared_credit_namespace=namespace,
        gpu_peak_tflops=gpu_peak_tflops,
        mfu_precision=mfu_precision,
        common_args=common_args,
        scenarios=scenarios,
        service_metadata=expanded_metadata,
        calibration_contract=calibration_contract,
    )

def build_job_command(
    options: RunnerOptions,
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    identity: GroupRunIdentity,
    *,
    job_index: int,
    start_epoch_s: float,
    coordinator_name: str,
) -> list[str]:
    if not 0 <= job_index < scenario.job_count:
        raise ValueError("job_index is outside scenario job_count")
    if not options.ray_address:
        raise ValueError("shared-vLLM runner requires an explicit Ray address")
    request_limit, work_limit = _local_limits(
        config,
        scenario,
        job_index,
    )
    run_stem = (
        f"{identity.order_index:03d}_{identity.phase}_"
        f"{identity.repeat_index}_{scenario.scenario_id}"
    )
    job_stem = options.output_dir / "jobs" / f"{run_stem}_job{job_index}"
    command = [
        str(options.python_executable),
        str(options.profiler_path),
        *config.common_args,
        "--total-rows",
        str(scenario.rows_per_job),
        "--db-fetch-rows",
        str(scenario.rows_per_job),
        "--max-inflight",
        str(request_limit),
        "--admission-scope",
        "per_endpoint",
        "--max-active-work-per-endpoint",
        str(work_limit),
        "--ray-address",
        options.ray_address,
        "--arrival-replay-start-epoch-s",
        str(start_epoch_s + scenario.arrival_offsets_s[job_index]),
        "--submission-granularity",
        "request",
        "--experiment-id",
        config.experiment_id,
        "--scenario-id",
        scenario.scenario_id,
        "--random-seed",
        str(config.seed + identity.order_index),
        "--run-phase",
        identity.phase,
        "--run-repeat-index",
        str(identity.repeat_index),
        "--warmup-runs",
        "0",
        "--repeats",
        "1",
        "--output",
        str(job_stem.with_suffix(".runs.csv")),
        "--request-trace-output",
        str(job_stem.with_suffix(".requests.csv")),
        "--submission-trace-output",
        str(job_stem.with_suffix(".submissions.csv")),
        "--flush-trace-output",
        str(job_stem.with_suffix(".flush.csv")),
    ]
    source_row_offset = (
        scenario.source_row_offsets[job_index]
        if scenario.source_row_offsets
        else 0
    )
    command.extend(["--source-row-offset", str(source_row_offset)])
    request_manifest = (
        scenario.request_manifests[job_index]
        if scenario.request_manifests
        else None
    )
    if request_manifest is not None:
        command.extend(["--request-manifest", request_manifest])
    if scenario.policy == "shared_drr":
        if not coordinator_name:
            raise ValueError("shared_drr requires a coordinator name")
        command.extend(
            [
                "--shared-credit-coordinator-name",
                coordinator_name,
                "--shared-credit-namespace",
                config.shared_credit_namespace,
                "--shared-credit-request-limit",
                str(config.request_limit_per_endpoint),
                "--shared-credit-work-limit",
                str(config.work_limit_per_endpoint),
                "--shared-credit-quantum",
                str(config.credit_quantum),
                "--shared-credit-job-weight",
                str(scenario.weights[job_index]),
            ]
        )
    return command

def _load_scenario(
    raw: object,
    request_limit: int,
    work_limit: int,
) -> SharedVllmScenario:
    if not isinstance(raw, dict):
        raise ValueError("each scenario must be an object")
    scenario_id = _nonempty_string(raw.get("scenario_id"), "scenario_id")
    if not _SCENARIO_ID.fullmatch(scenario_id):
        raise ValueError("scenario_id contains unsupported characters")
    policy = _nonempty_string(raw.get("policy"), "policy")
    if policy not in POLICIES:
        raise ValueError(f"unknown shared-vLLM policy: {policy}")
    job_count = _positive_integer(raw.get("job_count"), "job_count")
    rows_per_job = _positive_integer(
        raw.get("rows_per_job"),
        "rows_per_job",
    )
    if (
        policy == "static_partition"
        and (job_count > request_limit or job_count > work_limit)
    ):
        raise ValueError("static partition would assign zero capacity")
    weights = _positive_integer_tuple(
        raw.get("weights", [1] * job_count),
        "weights",
        job_count,
    )
    offsets = _nonnegative_float_tuple(
        raw.get("arrival_offsets_s", [0.0] * job_count),
        "arrival_offsets_s",
        job_count,
    )
    source_row_offsets = _nonnegative_integer_tuple(
        raw.get("source_row_offsets", [0] * job_count),
        "source_row_offsets",
        job_count,
    )
    request_manifests = _optional_path_tuple(
        raw.get("request_manifests", [None] * job_count),
        "request_manifests",
        job_count,
    )
    if any(request_manifests) and not all(request_manifests):
        raise ValueError("request_manifests must be provided for every job or none")
    if all(request_manifests) and any(source_row_offsets):
        raise ValueError(
            "manifest-selected jobs require zero source_row_offsets"
        )
    return SharedVllmScenario(
        scenario_id=scenario_id,
        policy=policy,
        job_count=job_count,
        rows_per_job=rows_per_job,
        weights=weights,
        arrival_offsets_s=offsets,
        source_row_offsets=source_row_offsets,
        request_manifests=request_manifests,
    )

def _local_limits(
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    job_index: int,
) -> tuple[int, int]:
    if scenario.policy != "static_partition":
        return (
            config.request_limit_per_endpoint,
            config.work_limit_per_endpoint,
        )
    return (
        _partition_share(
            config.request_limit_per_endpoint,
            scenario.job_count,
            job_index,
        ),
        _partition_share(
            config.work_limit_per_endpoint,
            scenario.job_count,
            job_index,
        ),
    )

def _partition_share(total: int, count: int, index: int) -> int:
    base, remainder = divmod(total, count)
    return base + (1 if index < remainder else 0)

def _argument_list(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) for item in value
    ):
        raise ValueError(f"{label} must be a list of strings")
    expanded = tuple(_expand_text(item, label) for item in value)
    for item in expanded:
        flag = item.split("=", 1)[0]
        if flag in _RUNNER_OWNED_FLAGS:
            raise ValueError(f"{label} contains runner-owned flag {flag}")
    return expanded

def _argument_value(
    arguments: tuple[str, ...],
    flag: str,
    default: str,
) -> str:
    for index, item in enumerate(arguments):
        if item == flag:
            if index + 1 >= len(arguments):
                raise ValueError(f"{flag} requires a value")
            return arguments[index + 1]
        prefix = f"{flag}="
        if item.startswith(prefix):
            return item[len(prefix) :]
    return default

def _csv_argument_values(
    arguments: tuple[str, ...],
    flag: str,
) -> tuple[str, ...]:
    raw = _argument_value(arguments, flag, "")
    return tuple(item.strip() for item in raw.split(",") if item.strip())

def _expand_text(value: str, label: str) -> str:
    return expand_text(value, label)

def _expand_scalar(value: object, label: str) -> object:
    return expand_scalar(value, label)

def _load_calibration_contract(
    value: object,
) -> CalibrationContract | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("calibration_contract must be an object")
    raw_path = value.get("path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError("calibration_contract.path must be non-empty")
    path = Path(_expand_text(raw_path, "calibration_contract.path"))
    raw_expected = value.get("expected")
    if not isinstance(raw_expected, dict) or not raw_expected:
        raise ValueError(
            "calibration_contract.expected must be a non-empty object"
        )
    expected: dict[str, JsonScalar] = {}
    for key, item in raw_expected.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError(
                "calibration_contract.expected keys must be non-empty"
            )
        expanded = _expand_scalar(
            item,
            f"calibration_contract.expected.{key}",
        )
        if (
            not isinstance(expanded, (str, int, float, bool))
            and expanded is not None
        ):
            raise ValueError(
                "calibration_contract.expected values must be JSON scalars"
            )
        expected[key] = expanded
    return validate_calibration_contract(path, expected)

def _nonempty_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value

def _integer(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    return value

def _positive_integer(value: object, label: str) -> int:
    resolved = _integer(value, label)
    if resolved <= 0:
        raise ValueError(f"{label} must be positive")
    return resolved

def _nonnegative_integer(value: object, label: str) -> int:
    resolved = _integer(value, label)
    if resolved < 0:
        raise ValueError(f"{label} must be non-negative")
    return resolved

def _nonnegative_float(value: object, label: str) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric") from exc
    if not math.isfinite(resolved) or resolved < 0:
        raise ValueError(f"{label} must be finite and non-negative")
    return resolved

def _positive_integer_tuple(
    value: object,
    label: str,
    expected: int,
) -> tuple[int, ...]:
    if not isinstance(value, list) or len(value) != expected:
        raise ValueError(f"{label} must contain one value per job")
    return tuple(_positive_integer(item, label) for item in value)

def _nonnegative_float_tuple(
    value: object,
    label: str,
    expected: int,
) -> tuple[float, ...]:
    if not isinstance(value, list) or len(value) != expected:
        raise ValueError(f"{label} must contain one value per job")
    resolved = []
    for item in value:
        if (
            isinstance(item, bool)
            or not isinstance(item, (int, float))
            or not math.isfinite(float(item))
            or item < 0
        ):
            raise ValueError(f"{label} values must be finite and non-negative")
        resolved.append(float(item))
    return tuple(resolved)

def _nonnegative_integer_tuple(
    value: object,
    label: str,
    expected: int,
) -> tuple[int, ...]:
    if not isinstance(value, list) or len(value) != expected:
        raise ValueError(f"{label} must contain one value per job")
    return tuple(_nonnegative_integer(item, label) for item in value)

def _optional_path_tuple(
    value: object,
    label: str,
    expected: int,
) -> tuple[str | None, ...]:
    if not isinstance(value, list) or len(value) != expected:
        raise ValueError(f"{label} must contain one value per job")
    resolved: list[str | None] = []
    for index, item in enumerate(value):
        if item is None:
            resolved.append(None)
            continue
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{label} values must be paths or null")
        resolved.append(_expand_text(item, f"{label}[{index}]"))
    return tuple(resolved)
