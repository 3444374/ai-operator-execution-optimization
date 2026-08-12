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
from src.scheduling.core.control import CapacityArm
from src.scheduling.runtime.saor_capacity import (
    LinearCostFeature,
    SaorArmEstimate,
    SaorObservationModel,
)


POLICIES = {
    "independent_full",
    "static_partition",
    "shared_drr",
    "shared_fifo",
    "external_vtc",
    "saor_release",
    "state_aware_adaptive",
    "saor_capacity",
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
    "--shared-credit-policy",
    "--shared-credit-quantum",
    "--shared-credit-request-limit",
    "--shared-credit-work-limit",
    "--saor-entitlement-weight",
    "--saor-fairness-weight",
    "--saor-queue-weight",
    "--saor-slo-weight",
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
    rows_per_job: int | None
    weights: tuple[int, ...]
    arrival_offsets_s: tuple[float, ...]
    static_partition_count: int | None = None
    source_row_offsets: tuple[int, ...] = ()
    request_manifests: tuple[str | None, ...] = ()
    rows_per_jobs: tuple[int, ...] = ()
    request_limit_per_endpoint: int | None = None
    work_limit_per_endpoint: int | None = None

    def row_count(self, job_index: int) -> int:
        """Return the immutable request count for one job."""
        if not 0 <= job_index < self.job_count:
            raise ValueError("job_index is outside scenario job_count")
        if self.rows_per_jobs:
            return self.rows_per_jobs[job_index]
        if self.rows_per_job is None:
            raise ValueError("scenario has no row-count contract")
        return self.rows_per_job

    def endpoint_limits(
        self,
        default_request_limit: int,
        default_work_limit: int,
    ) -> tuple[int, int]:
        """Return this arm's frozen endpoint capacity contract."""
        return (
            self.request_limit_per_endpoint or default_request_limit,
            self.work_limit_per_endpoint or default_work_limit,
        )

@dataclass(frozen=True)
class StateAwareControlConfig:
    request_candidates: tuple[int, ...]
    work_candidates: tuple[int, ...]
    initial_request_limit: int
    fallback_request_limit: int
    fallback_work_limit: int
    target_service_rate_tokens_s_per_endpoint: float
    rate_ewma_alpha: float
    congestion_kv_usage: float
    consecutive_samples: int
    increase_consecutive_samples: int
    cooldown_samples: int
    max_state_age_s: float


@dataclass(frozen=True)
class SaorCapacityControlConfig:
    arms: tuple[SaorArmEstimate, ...]
    initial_arm: str
    fallback_arm: str
    observation_model: SaorObservationModel
    ewma_alpha: float
    queue_work_scale: int
    min_dwell_samples: int
    max_state_age_s: float
    v: float
    tail_weight: float
    energy_weight: float
    switch_weight: float


@dataclass(frozen=True)
class SaorReleaseControlConfig:
    entitlement_weight: float
    queue_weight: float
    fairness_weight: float
    slo_weight: float


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
    state_aware_control: StateAwareControlConfig | None = None
    saor_capacity_control: SaorCapacityControlConfig | None = None
    saor_release_control: SaorReleaseControlConfig | None = None

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
    repeats = _nonnegative_integer(
        decoded.get("formal_repeats"),
        "formal_repeats",
    )
    if warmups == 0 and repeats == 0:
        raise ValueError(
            "shared-vLLM config requires at least one warmup or formal run"
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
    state_aware_control = _load_state_aware_control(
        decoded.get("state_aware_control")
    )
    saor_capacity_control = _load_saor_capacity_control(
        decoded.get("saor_capacity_control")
    )
    saor_release_control = _load_saor_release_control(
        decoded.get("saor_release_control")
    )
    uses_state_aware = any(
        scenario.policy == "state_aware_adaptive"
        for scenario in scenarios
    )
    if uses_state_aware and state_aware_control is None:
        raise ValueError(
            "state_aware_adaptive policy requires state_aware_control"
        )
    uses_saor_capacity = any(
        scenario.policy == "saor_capacity" for scenario in scenarios
    )
    if uses_saor_capacity and saor_capacity_control is None:
        raise ValueError("saor_capacity policy requires saor_capacity_control")
    uses_saor_release = any(
        scenario.policy == "saor_release" for scenario in scenarios
    )
    if uses_saor_release and saor_release_control is None:
        raise ValueError("saor_release policy requires saor_release_control")
    if state_aware_control is not None and (
        state_aware_control.initial_request_limit != request_limit
        or state_aware_control.work_candidates[
            state_aware_control.request_candidates.index(
                state_aware_control.initial_request_limit
            )
        ] != work_limit
    ):
        raise ValueError(
            "state-aware initial arm must equal root request/work limits"
        )
    if saor_capacity_control is not None:
        initial_saor_arm = next(
            arm
            for arm in saor_capacity_control.arms
            if arm.name == saor_capacity_control.initial_arm
        )
        if initial_saor_arm.arm != CapacityArm(request_limit, work_limit):
            raise ValueError(
                "SAOR initial arm must equal root request/work limits"
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
        state_aware_control=state_aware_control,
        saor_capacity_control=saor_capacity_control,
        saor_release_control=saor_release_control,
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
    endpoint_request_limit, endpoint_work_limit = scenario.endpoint_limits(
        config.request_limit_per_endpoint,
        config.work_limit_per_endpoint,
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
        str(scenario.row_count(job_index)),
        "--db-fetch-rows",
        str(scenario.row_count(job_index)),
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
    if scenario.policy in {
        "shared_drr",
        "shared_fifo",
        "external_vtc",
        "saor_release",
        "state_aware_adaptive",
        "saor_capacity",
    }:
        if not coordinator_name:
            raise ValueError("shared policies require a coordinator name")
        command.extend(
            [
                "--shared-credit-coordinator-name",
                coordinator_name,
                "--shared-credit-namespace",
                config.shared_credit_namespace,
                "--shared-credit-request-limit",
                str(endpoint_request_limit),
                "--shared-credit-work-limit",
                str(endpoint_work_limit),
                "--shared-credit-quantum",
                str(config.credit_quantum),
                "--shared-credit-policy",
                (
                    "fifo" if scenario.policy == "shared_fifo"
                    else "vtc" if scenario.policy == "external_vtc"
                    else "saor" if scenario.policy == "saor_release"
                    else "drr"
                ),
                "--shared-credit-job-weight",
                str(scenario.weights[job_index]),
            ]
        )
        if scenario.policy == "saor_release":
            control = config.saor_release_control
            if control is None:
                raise ValueError("saor_release control configuration is missing")
            command.extend(
                [
                    "--saor-entitlement-weight",
                    str(control.entitlement_weight),
                    "--saor-queue-weight",
                    str(control.queue_weight),
                    "--saor-fairness-weight",
                    str(control.fairness_weight),
                    "--saor-slo-weight",
                    str(control.slo_weight),
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
    scenario_request_raw = raw.get("request_limit_per_endpoint")
    scenario_work_raw = raw.get("work_limit_per_endpoint")
    if (scenario_request_raw is None) != (scenario_work_raw is None):
        raise ValueError(
            "scenario request/work endpoint limits must be provided together"
        )
    scenario_request_limit = (
        _positive_integer(
            _expand_scalar(
                scenario_request_raw,
                "scenario.request_limit_per_endpoint",
            ),
            "scenario.request_limit_per_endpoint",
        )
        if scenario_request_raw is not None
        else None
    )
    scenario_work_limit = (
        _positive_integer(
            _expand_scalar(
                scenario_work_raw,
                "scenario.work_limit_per_endpoint",
            ),
            "scenario.work_limit_per_endpoint",
        )
        if scenario_work_raw is not None
        else None
    )
    if (
        policy in {"state_aware_adaptive", "saor_capacity"}
        and scenario_request_limit is not None
    ):
        raise ValueError(
            "dynamic capacity policies use the root initial endpoint limits"
        )
    static_partition_count_raw = raw.get("static_partition_count")
    static_partition_count = (
        _positive_integer(
            static_partition_count_raw,
            "static_partition_count",
        )
        if static_partition_count_raw is not None
        else None
    )
    if policy != "static_partition" and static_partition_count is not None:
        raise ValueError(
            "static_partition_count is only valid for static_partition"
        )
    if (
        static_partition_count is not None
        and static_partition_count < job_count
    ):
        raise ValueError(
            "static_partition_count cannot be smaller than job_count"
        )
    partition_count = static_partition_count or job_count
    has_uniform_rows = raw.get("rows_per_job") is not None
    has_per_job_rows = raw.get("rows_per_jobs") is not None
    if has_uniform_rows == has_per_job_rows:
        raise ValueError(
            "provide exactly one of rows_per_job or rows_per_jobs"
        )
    rows_per_job = (
        _positive_integer(raw.get("rows_per_job"), "rows_per_job")
        if has_uniform_rows
        else None
    )
    rows_per_jobs = (
        _positive_integer_tuple(
            raw.get("rows_per_jobs"),
            "rows_per_jobs",
            job_count,
        )
        if has_per_job_rows
        else ()
    )
    effective_request_limit = scenario_request_limit or request_limit
    effective_work_limit = scenario_work_limit or work_limit
    if (
        policy == "static_partition"
        and (
            partition_count > effective_request_limit
            or partition_count > effective_work_limit
        )
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
        static_partition_count=static_partition_count,
        source_row_offsets=source_row_offsets,
        request_manifests=request_manifests,
        rows_per_jobs=rows_per_jobs,
        request_limit_per_endpoint=scenario_request_limit,
        work_limit_per_endpoint=scenario_work_limit,
    )

def _local_limits(
    config: SharedVllmConfig,
    scenario: SharedVllmScenario,
    job_index: int,
) -> tuple[int, int]:
    request_limit, work_limit = scenario.endpoint_limits(
        config.request_limit_per_endpoint,
        config.work_limit_per_endpoint,
    )
    if scenario.policy == "state_aware_adaptive":
        if config.state_aware_control is None:
            raise ValueError(
                "state_aware_adaptive requires state_aware_control"
            )
        # The shared coordinator owns the actuated capacity.  Keep the
        # job-local admission ceiling at the largest calibrated arm so it
        # cannot silently clamp a coordinator upshift at the initial arm.
        return (
            max(config.state_aware_control.request_candidates),
            max(config.state_aware_control.work_candidates),
        )
    if scenario.policy == "saor_capacity":
        if config.saor_capacity_control is None:
            raise ValueError("saor_capacity requires saor_capacity_control")
        return (
            max(item.arm.request_limit for item in config.saor_capacity_control.arms),
            max(item.arm.work_limit for item in config.saor_capacity_control.arms),
        )
    if scenario.policy != "static_partition":
        return request_limit, work_limit
    partition_count = scenario.static_partition_count or scenario.job_count
    return (
        _partition_share(
            request_limit,
            partition_count,
            job_index,
        ),
        _partition_share(
            work_limit,
            partition_count,
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

def _load_state_aware_control(
    raw: object,
) -> StateAwareControlConfig | None:
    if raw is None:
        return None
    required = {
        "request_candidates",
        "work_candidates",
        "initial_request_limit",
        "fallback_request_limit",
        "fallback_work_limit",
        "target_service_rate_tokens_s_per_endpoint",
        "rate_ewma_alpha",
        "congestion_kv_usage",
        "consecutive_samples",
        "cooldown_samples",
        "max_state_age_s",
    }
    allowed = required | {"increase_consecutive_samples"}
    if (
        not isinstance(raw, dict)
        or not required.issubset(raw)
        or not set(raw).issubset(allowed)
    ):
        raise ValueError("state_aware_control fields are invalid")
    raw_candidates = raw["request_candidates"]
    if not isinstance(raw_candidates, list) or not raw_candidates:
        raise ValueError("request_candidates must be a non-empty list")
    candidates = tuple(
        sorted(
            set(
                _positive_integer(
                    _expand_scalar(item, "request_candidates"),
                    "request_candidates",
                )
                for item in raw_candidates
            )
        )
    )
    raw_work_candidates = raw["work_candidates"]
    if (
        not isinstance(raw_work_candidates, list)
        or len(raw_work_candidates) != len(raw_candidates)
    ):
        raise ValueError("work_candidates must align with request_candidates")
    work_by_request: dict[int, int] = {}
    for request, work in zip(raw_candidates, raw_work_candidates):
        request_limit = _positive_integer(
            _expand_scalar(request, "request_candidates"),
            "request_candidates",
        )
        work_limit = _positive_integer(
            _expand_scalar(work, "work_candidates"),
            "work_candidates",
        )
        if (
            request_limit in work_by_request
            and work_by_request[request_limit] != work_limit
        ):
            raise ValueError("duplicate request candidate has conflicting work limit")
        work_by_request[request_limit] = work_limit
    work_candidates = tuple(work_by_request[item] for item in candidates)
    fallback = _positive_integer(
        _expand_scalar(
            raw["fallback_request_limit"],
            "fallback_request_limit",
        ),
        "fallback_request_limit",
    )
    if fallback not in candidates:
        raise ValueError("fallback_request_limit must be a candidate")
    initial = _positive_integer(
        _expand_scalar(
            raw["initial_request_limit"],
            "initial_request_limit",
        ),
        "initial_request_limit",
    )
    if initial not in candidates:
        raise ValueError("initial_request_limit must be a candidate")
    fallback_work = _positive_integer(
        _expand_scalar(
            raw["fallback_work_limit"],
            "fallback_work_limit",
        ),
        "fallback_work_limit",
    )
    if work_by_request[fallback] != fallback_work:
        raise ValueError("fallback request/work limits must form a candidate")

    def positive_float(value: object, label: str) -> float:
        resolved = _nonnegative_float(_expand_scalar(value, label), label)
        if resolved <= 0:
            raise ValueError(f"{label} must be positive")
        return resolved

    alpha = positive_float(raw["rate_ewma_alpha"], "rate_ewma_alpha")
    if alpha > 1:
        raise ValueError("rate_ewma_alpha must be <= 1")
    congestion_kv_usage = positive_float(
        raw["congestion_kv_usage"],
        "congestion_kv_usage",
    )
    if congestion_kv_usage > 1:
        raise ValueError("congestion_kv_usage must be <= 1")
    return StateAwareControlConfig(
        request_candidates=candidates,
        work_candidates=work_candidates,
        initial_request_limit=initial,
        fallback_request_limit=fallback,
        fallback_work_limit=fallback_work,
        target_service_rate_tokens_s_per_endpoint=positive_float(
            raw["target_service_rate_tokens_s_per_endpoint"],
            "target_service_rate_tokens_s_per_endpoint",
        ),
        rate_ewma_alpha=alpha,
        congestion_kv_usage=congestion_kv_usage,
        consecutive_samples=_positive_integer(
            _expand_scalar(
                raw["consecutive_samples"],
                "consecutive_samples",
            ),
            "consecutive_samples",
        ),
        increase_consecutive_samples=_positive_integer(
            _expand_scalar(
                raw.get(
                    "increase_consecutive_samples",
                    raw["consecutive_samples"],
                ),
                "increase_consecutive_samples",
            ),
            "increase_consecutive_samples",
        ),
        cooldown_samples=_nonnegative_integer(
            _expand_scalar(raw["cooldown_samples"], "cooldown_samples"),
            "cooldown_samples",
        ),
        max_state_age_s=positive_float(
            raw["max_state_age_s"],
            "max_state_age_s",
        ),
    )


def _load_saor_capacity_control(
    raw: object,
) -> SaorCapacityControlConfig | None:
    if raw is None:
        return None
    required = {
        "arms",
        "initial_arm",
        "fallback_arm",
        "observation_model",
        "ewma_alpha",
        "queue_work_scale",
        "min_dwell_samples",
        "max_state_age_s",
        "v",
        "tail_weight",
        "energy_weight",
        "switch_weight",
    }
    if not isinstance(raw, dict) or set(raw) != required:
        raise ValueError("saor_capacity_control fields are invalid")
    raw_arms = raw["arms"]
    if not isinstance(raw_arms, list) or not raw_arms:
        raise ValueError("SAOR arms must be a non-empty list")
    arms = []
    arm_fields = {
        "name",
        "request_limit",
        "work_limit",
        "prior_goodput",
        "prior_tail_risk",
        "prior_energy",
    }
    for item in raw_arms:
        if not isinstance(item, dict) or set(item) != arm_fields:
            raise ValueError("SAOR arm fields are invalid")
        arms.append(
            SaorArmEstimate(
                name=_nonempty_string(item["name"], "SAOR arm name"),
                arm=CapacityArm(
                    _positive_integer(
                        _expand_scalar(item["request_limit"], "SAOR request_limit"),
                        "SAOR request_limit",
                    ),
                    _positive_integer(
                        _expand_scalar(item["work_limit"], "SAOR work_limit"),
                        "SAOR work_limit",
                    ),
                ),
                goodput=_nonnegative_float(
                    _expand_scalar(item["prior_goodput"], "SAOR prior_goodput"),
                    "SAOR prior_goodput",
                ),
                tail_risk=_nonnegative_float(
                    _expand_scalar(item["prior_tail_risk"], "SAOR prior_tail_risk"),
                    "SAOR prior_tail_risk",
                ),
                energy=_nonnegative_float(
                    _expand_scalar(item["prior_energy"], "SAOR prior_energy"),
                    "SAOR prior_energy",
                ),
            )
        )
    names = tuple(item.name for item in arms)
    if len(names) != len(set(names)):
        raise ValueError("SAOR arm names must be unique")
    if tuple(sorted(arms, key=lambda item: item.arm)) != tuple(arms):
        raise ValueError("SAOR arms must be ordered by capacity")
    initial_arm = _nonempty_string(raw["initial_arm"], "SAOR initial_arm")
    fallback_arm = _nonempty_string(raw["fallback_arm"], "SAOR fallback_arm")
    if initial_arm not in names or fallback_arm not in names:
        raise ValueError("SAOR initial/fallback arm is not configured")

    observation_raw = raw["observation_model"]
    if not isinstance(observation_raw, dict) or set(observation_raw) != {
        "goodput_field",
        "goodput_scale",
        "tail_features",
        "energy_features",
    }:
        raise ValueError("SAOR observation_model fields are invalid")

    def load_features(value: object, label: str) -> tuple[LinearCostFeature, ...]:
        if not isinstance(value, list):
            raise ValueError(f"{label} must be a list")
        features = []
        for item in value:
            if not isinstance(item, dict) or set(item) != {"field", "scale", "weight"}:
                raise ValueError(f"{label} feature fields are invalid")
            scale = _nonnegative_float(
                _expand_scalar(item["scale"], f"{label}.scale"),
                f"{label}.scale",
            )
            if scale <= 0:
                raise ValueError(f"{label}.scale must be positive")
            features.append(
                LinearCostFeature(
                    _nonempty_string(item["field"], f"{label}.field"),
                    scale,
                    _nonnegative_float(
                        _expand_scalar(item["weight"], f"{label}.weight"),
                        f"{label}.weight",
                    ),
                )
            )
        return tuple(features)

    goodput_scale = _nonnegative_float(
        _expand_scalar(observation_raw["goodput_scale"], "SAOR goodput_scale"),
        "SAOR goodput_scale",
    )
    if goodput_scale <= 0:
        raise ValueError("SAOR goodput_scale must be positive")
    observation_model = SaorObservationModel(
        goodput_field=_nonempty_string(
            observation_raw["goodput_field"],
            "SAOR goodput_field",
        ),
        goodput_scale=goodput_scale,
        tail_features=load_features(
            observation_raw["tail_features"],
            "SAOR tail_features",
        ),
        energy_features=load_features(
            observation_raw["energy_features"],
            "SAOR energy_features",
        ),
    )
    ewma_alpha = _nonnegative_float(
        _expand_scalar(raw["ewma_alpha"], "SAOR ewma_alpha"),
        "SAOR ewma_alpha",
    )
    if not 0 < ewma_alpha <= 1:
        raise ValueError("SAOR ewma_alpha must be in (0, 1]")
    max_state_age_s = _nonnegative_float(
        _expand_scalar(raw["max_state_age_s"], "SAOR max_state_age_s"),
        "SAOR max_state_age_s",
    )
    if max_state_age_s <= 0:
        raise ValueError("SAOR max_state_age_s must be positive")
    return SaorCapacityControlConfig(
        arms=tuple(arms),
        initial_arm=initial_arm,
        fallback_arm=fallback_arm,
        observation_model=observation_model,
        ewma_alpha=ewma_alpha,
        queue_work_scale=_positive_integer(
            _expand_scalar(raw["queue_work_scale"], "SAOR queue_work_scale"),
            "SAOR queue_work_scale",
        ),
        min_dwell_samples=_nonnegative_integer(
            _expand_scalar(raw["min_dwell_samples"], "SAOR min_dwell_samples"),
            "SAOR min_dwell_samples",
        ),
        max_state_age_s=max_state_age_s,
        v=_nonnegative_float(_expand_scalar(raw["v"], "SAOR v"), "SAOR v"),
        tail_weight=_nonnegative_float(
            _expand_scalar(raw["tail_weight"], "SAOR tail_weight"),
            "SAOR tail_weight",
        ),
        energy_weight=_nonnegative_float(
            _expand_scalar(raw["energy_weight"], "SAOR energy_weight"),
            "SAOR energy_weight",
        ),
        switch_weight=_nonnegative_float(
            _expand_scalar(raw["switch_weight"], "SAOR switch_weight"),
            "SAOR switch_weight",
        ),
    )


def _load_saor_release_control(
    raw: object,
) -> SaorReleaseControlConfig | None:
    if raw is None:
        return None
    fields = {
        "entitlement_weight",
        "queue_weight",
        "fairness_weight",
        "slo_weight",
    }
    if not isinstance(raw, dict) or set(raw) != fields:
        raise ValueError("saor_release_control fields are invalid")
    values = {
        field: _nonnegative_float(
            _expand_scalar(raw[field], f"saor_release_control.{field}"),
            f"saor_release_control.{field}",
        )
        for field in fields
    }
    if not any(value > 0 for value in values.values()):
        raise ValueError("at least one SAOR release weight must be positive")
    if values["slo_weight"] > 0:
        raise ValueError(
            "saor_release_control.slo_weight is not executable yet; "
            "keep it at 0 until per-Job SLO debt is connected to release"
        )
    return SaorReleaseControlConfig(**values)


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
    return tuple(
        _positive_integer(
            _expand_scalar(item, f"{label}[{index}]"),
            f"{label}[{index}]",
        )
        for index, item in enumerate(value)
    )

def _nonnegative_float_tuple(
    value: object,
    label: str,
    expected: int,
) -> tuple[float, ...]:
    if not isinstance(value, list) or len(value) != expected:
        raise ValueError(f"{label} must contain one value per job")
    return tuple(
        _nonnegative_float(
            _expand_scalar(item, f"{label}[{index}]"),
            f"{label}[{index}]",
        )
        for index, item in enumerate(value)
    )

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
