"""Text output-token cost semantics for complete requests."""

from __future__ import annotations

from typing import Literal


OutputCostMode = Literal[
    "prompt_only",
    "fixed_output_cap",
    "trace_target_output",
]

_OUTPUT_COST_SOURCES = {
    "prompt_only": "configured_zero",
    "fixed_output_cap": "backend_completion_cap",
    "trace_target_output": "burstgpt_unpaired_trace_metadata",
}


def _non_negative_int(value: object, field_name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def output_cost_source(mode: OutputCostMode) -> str:
    try:
        return _OUTPUT_COST_SOURCES[mode]
    except KeyError as exc:
        raise ValueError(f"unknown output cost mode: {mode}") from exc


def resolve_output_tokens(
    mode: OutputCostMode,
    *,
    completion_max_tokens: int,
    target_output_tokens: object,
) -> int:
    cap = _non_negative_int(
        completion_max_tokens,
        "completion_max_tokens",
    )
    output_cost_source(mode)
    if mode == "prompt_only":
        return 0
    if mode == "fixed_output_cap":
        return cap
    target = _non_negative_int(
        target_output_tokens,
        "target_output_tokens",
    )
    return min(target, cap)
