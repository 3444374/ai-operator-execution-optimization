"""Deterministic scheduling for interleaved experiment scenarios."""

from __future__ import annotations

import math
import random
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, Sequence


REQUIRED_SERVICE_METADATA = (
    "vllm_version",
    "enforce_eager",
    "compilation_mode",
    "chunked_prefill",
    "max_num_batched_tokens",
    "max_num_seqs",
    "gpu_memory_utilization",
    "prefix_caching",
    "mfu_metrics",
)


def validate_service_metadata(metadata: Mapping[str, object]) -> None:
    missing = [key for key in REQUIRED_SERVICE_METADATA if key not in metadata]
    if missing:
        raise ValueError(
            "service_metadata missing required keys: " + ", ".join(missing)
        )
    utilization = metadata["gpu_memory_utilization"]
    if (
        isinstance(utilization, bool)
        or not isinstance(utilization, (int, float))
        or not math.isfinite(utilization)
        or not 0.0 < utilization <= 1.0
    ):
        raise ValueError(
            "gpu_memory_utilization must be a finite number in (0, 1]"
        )
    for key in ("max_num_batched_tokens", "max_num_seqs"):
        capacity = metadata[key]
        if isinstance(capacity, str) and capacity == "unknown":
            continue
        if (
            isinstance(capacity, bool)
            or not isinstance(capacity, int)
            or capacity <= 0
        ):
            raise ValueError(
                f"{key} must be a positive integer or 'unknown'"
            )


@dataclass(frozen=True)
class ScheduledScenarioRun:
    scenario_id: str
    phase: Literal["warmup", "formal"]
    repeat_index: int
    order_index: int
    random_seed: int


def build_scenario_schedule(
    scenario_ids: Sequence[str],
    warmup_runs_per_scenario: int,
    formal_repeats: int,
    seed: int,
) -> tuple[ScheduledScenarioRun, ...]:
    if not scenario_ids:
        raise ValueError("scenario_ids must contain at least one scenario")
    if any(not item or not item.strip() for item in scenario_ids):
        raise ValueError("scenario_ids must be non-empty")
    if len(set(scenario_ids)) != len(scenario_ids):
        raise ValueError("scenario_ids must be unique")
    if warmup_runs_per_scenario < 0 or formal_repeats < 0:
        raise ValueError("run counts must be non-negative")

    schedule = []
    for repeat_index in range(1, warmup_runs_per_scenario + 1):
        for scenario_id in scenario_ids:
            schedule.append(
                ScheduledScenarioRun(
                    scenario_id=scenario_id,
                    phase="warmup",
                    repeat_index=repeat_index,
                    order_index=len(schedule),
                    random_seed=seed,
                )
            )

    generator = random.Random(seed)
    for repeat_index in range(1, formal_repeats + 1):
        ordered_ids = list(scenario_ids)
        generator.shuffle(ordered_ids)
        for scenario_id in ordered_ids:
            schedule.append(
                ScheduledScenarioRun(
                    scenario_id=scenario_id,
                    phase="formal",
                    repeat_index=repeat_index,
                    order_index=len(schedule),
                    random_seed=seed,
                )
            )
    return tuple(schedule)
