"""Typed shared-service research values and evidence argument readers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from src.experiments.calibration.contracts import CalibrationContract
from src.research.multitask.saor_capacity import (
    SaorArmEstimate,
    SaorObservationModel,
)


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
    rehearsal: bool = False
    observation_endpoint_urls_by_job: tuple[tuple[str, ...], ...] = ()

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
    priorities: tuple[int, ...] = ()
    slo_targets_s: tuple[float | None, ...] = ()
    priority_windows_s: tuple[float | None, ...] = ()
    debt_cap_fractions: tuple[float | None, ...] = ()
    ready_observation_contract: str = "single_head"

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

    def job_priority(self, job_index: int) -> int:
        """Return the explicit diagnostic priority for one Job."""

        if not 0 <= job_index < self.job_count:
            raise ValueError("job_index is outside scenario job_count")
        if self.policy in {"saor_bounded_priority", "saor_bounded_ready"}:
            return self.priorities[job_index]
        if self.policy != "foreground_strict_priority":
            return 0
        foreground_offset = max(self.arrival_offsets_s)
        return int(self.arrival_offsets_s[job_index] == foreground_offset)

    def job_slo_target_s(self, job_index: int) -> float | None:
        return self.slo_targets_s[job_index] if self.slo_targets_s else None

    def job_priority_window_s(self, job_index: int) -> float | None:
        return self.priority_windows_s[job_index] if self.priority_windows_s else None

    def job_debt_cap_work(self, job_index: int, work_limit: int) -> float | None:
        if not self.debt_cap_fractions:
            return None
        fraction = self.debt_cap_fractions[job_index]
        return None if fraction is None else fraction * work_limit

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
class CompletionWorkCostConfig:
    """Typed request-work contract shared by admission and evidence audits."""

    protocol: Literal["completions", "chat_completions"]
    prompt_token_overhead_per_request: int
    output_bound_source: Literal[
        "prompt_only",
        "fixed_output_cap",
        "trace_target_output",
    ]
    completion_max_tokens: int

    def __post_init__(self) -> None:
        if self.protocol not in {"completions", "chat_completions"}:
            raise ValueError("completion protocol is unsupported")
        if (
            not isinstance(self.prompt_token_overhead_per_request, int)
            or isinstance(self.prompt_token_overhead_per_request, bool)
            or self.prompt_token_overhead_per_request < 0
        ):
            raise ValueError("prompt token overhead must be non-negative")
        if self.output_bound_source not in {
            "prompt_only",
            "fixed_output_cap",
            "trace_target_output",
        }:
            raise ValueError("output bound source is unsupported")
        if (
            not isinstance(self.completion_max_tokens, int)
            or isinstance(self.completion_max_tokens, bool)
            or self.completion_max_tokens < 0
        ):
            raise ValueError("completion max tokens must be non-negative")

    def validate_estimated_output_tokens(self, value: int) -> None:
        """Reject trace estimates that do not follow the configured source."""

        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(
                "estimated_output_tokens must be a non-negative integer"
            )
        # Engineering decision: the evidence trace must not be able to enlarge
        # its own upper bound after admission. Formal currently freezes the
        # fixed-cap branch; the other modes retain their native constraints.
        if self.output_bound_source == "fixed_output_cap":
            if value != self.completion_max_tokens:
                raise ValueError(
                    "estimated_output_tokens does not equal the configured "
                    "completion cap"
                )
        elif self.output_bound_source == "prompt_only":
            if value != 0:
                raise ValueError(
                    "prompt-only work must estimate zero output tokens"
                )
        elif value > self.completion_max_tokens:
            raise ValueError(
                "trace output estimate exceeds the configured completion cap"
            )

    def estimated_work(
        self,
        raw_prompt_tokens: int,
        estimated_output_tokens: int,
    ) -> int:
        for value, name in (
            (raw_prompt_tokens, "raw_prompt_tokens"),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        self.validate_estimated_output_tokens(estimated_output_tokens)
        return (
            raw_prompt_tokens
            + self.prompt_token_overhead_per_request
            + estimated_output_tokens
        )


@dataclass(frozen=True)
class SharedVllmConfig:
    experiment_id: str
    seed: int
    warmup_runs_per_scenario: int
    formal_repeats: int
    endpoint_ids: tuple[str, ...]
    service_signature: tuple[tuple[str, object], ...]
    request_limit_per_endpoint: int
    work_limit_per_endpoint: int
    credit_quantum: int
    shared_credit_namespace: str
    gpu_peak_tflops: float
    mfu_precision: str
    common_args: tuple[str, ...]
    scenarios: tuple[SharedVllmScenario, ...]
    service_metadata: tuple[tuple[str, object], ...]
    fail_closed_rehearsal: bool = False
    calibration_contract: CalibrationContract | None = None
    state_aware_control: StateAwareControlConfig | None = None
    saor_capacity_control: SaorCapacityControlConfig | None = None
    saor_release_control: SaorReleaseControlConfig | None = None
    ready_observation_contract: str = "single_head"
    ready_payload_bytes_limit_per_job: int = 0
    job_internal_arrival_contract: Literal["manifest_timed", "eager"] = (
        "manifest_timed"
    )
    service_identity: tuple[tuple[str, object], ...] = ()

    @property
    def completion_work_cost(self) -> CompletionWorkCostConfig:
        """Expose the CLI work-cost flags as one validated typed contract."""

        protocol = _argument_value(
            self.common_args,
            "--completion-protocol",
            "completions",
        )
        if protocol not in {"completions", "chat_completions"}:
            raise ValueError("--completion-protocol is unsupported")
        overhead_raw = _argument_value(
            self.common_args,
            "--completion-prompt-token-overhead",
            "0",
        )
        try:
            overhead = int(overhead_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "--completion-prompt-token-overhead must be an integer"
            ) from exc
        if overhead < 0:
            raise ValueError(
                "--completion-prompt-token-overhead must be non-negative"
            )
        return CompletionWorkCostConfig(
            protocol=protocol,
            prompt_token_overhead_per_request=overhead,
            output_bound_source=_argument_value(
                self.common_args,
                "--output-cost-mode",
                "prompt_only",
            ),
            completion_max_tokens=int(
                _argument_value(
                    self.common_args,
                    "--completion-max-tokens",
                    "0",
                )
            ),
        )

    @property
    def completion_tokenizer_path(self) -> Path:
        value = _argument_value(self.common_args, "--cost-tokenizer-id", "")
        if not value:
            raise ValueError("--cost-tokenizer-id is required")
        return Path(value)

    @property
    def completion_prompt_format(self) -> str:
        value = _argument_value(
            self.common_args,
            "--completion-prompt-format",
            "raw",
        )
        if value not in {"raw", "chatml"}:
            raise ValueError("--completion-prompt-format is unsupported")
        return value

    @property
    def completion_returns_token_ids(self) -> bool:
        return "--completion-return-token-ids" in self.common_args


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
