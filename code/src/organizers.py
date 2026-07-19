"""Data organization backends for AI operator execution pipelines.

The organizer boundary is intentionally small: it receives an Arrow table and
returns Arrow tables that downstream Ray/vLLM code can submit as batches. This
keeps Daft-specific APIs out of the strategy and submission layers.
"""

from __future__ import annotations

import time
import warnings
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa


PartitionMode = Literal["none", "into_partitions", "repartition"]
DaftRunner = Literal["native", "ray"]
BatchingPolicy = Literal[
    "fixed_rows",
    "token_budget",
    "length_align_fixed_rows",
    "length_align_token_budget",
    "prefix_aware_fixed_rows",
    "prefix_aware_token_budget",
]
_CONFIGURED_DAFT_RUNNER: DaftRunner | None = None


@dataclass(frozen=True)
class OrganizerConfig:
    batch_size: int
    partition_mode: PartitionMode = "none"
    partitions: int = 0
    runner: DaftRunner = "native"
    batching_policy: BatchingPolicy = "fixed_rows"
    token_budget: int = 0
    completion_max_tokens: int = 0


@dataclass(frozen=True)
class OrganizedBatches:
    batches: list[pa.Table]
    metrics: dict[str, object]


def configure_daft_runner(runner: DaftRunner) -> None:
    """Configure Daft runner once before any Daft dataframe is materialized."""
    import daft

    global _CONFIGURED_DAFT_RUNNER
    if _CONFIGURED_DAFT_RUNNER is None:
        if runner == "ray":
            daft.set_runner_ray(noop_if_initialized=True)
        else:
            daft.set_runner_native()
        _CONFIGURED_DAFT_RUNNER = runner
        return
    if _CONFIGURED_DAFT_RUNNER != runner:
        raise RuntimeError(
            "Daft runner is already configured as "
            f"{_CONFIGURED_DAFT_RUNNER}; cannot switch to {runner} in the same process."
        )


class ArrowOrganizer:
    """Baseline organizer using direct Arrow slicing."""

    name = "arrow"

    def __init__(self, config: OrganizerConfig):
        if config.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        _validate_batching_policy(config)
        self.config = config

    def organize(self, table: pa.Table) -> OrganizedBatches:
        start_s = time.perf_counter()
        batches = organize_arrow_table(table, self.config)
        elapsed_s = time.perf_counter() - start_s
        partition_effective = self.config.partition_mode == "none"
        return OrganizedBatches(
            batches=batches,
            metrics={
                "organizer": self.name,
                "runner": "",
                "partition_mode": self.config.partition_mode,
                "requested_partitions": self.config.partitions,
                "partition_effective": str(partition_effective).lower(),
                "organizer_from_arrow_s": 0.0,
                "organizer_plan_s": round(elapsed_s, 6),
                "organizer_collect_s": 0.0,
                "warnings": "",
                **organization_strategy_metrics(batches, self.config.batching_policy),
            },
        )


class DaftOrganizer:
    """Daft DataFrame organizer for the text-stage engine backend."""

    name = "daft"

    def __init__(self, config: OrganizerConfig):
        if config.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        _validate_batching_policy(config)
        self.config = config

    def organize(self, table: pa.Table) -> OrganizedBatches:
        import daft

        configure_daft_runner(self.config.runner)

        from_arrow_start = time.perf_counter()
        df = daft.from_arrow(table)
        from_arrow_s = time.perf_counter() - from_arrow_start

        warnings_seen: list[str] = []
        plan_start = time.perf_counter()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            df = self._apply_partition_mode(df)
            if self.config.batching_policy == "fixed_rows":
                df = df.into_batches(self.config.batch_size)
        plan_s = time.perf_counter() - plan_start
        warnings_seen.extend(str(item.message) for item in caught)

        collect_start = time.perf_counter()
        if self.config.batching_policy == "fixed_rows":
            batches = list(df.to_arrow_iter())
        else:
            batches = []
            for arrow_table in df.to_arrow_iter():
                batches.extend(organize_arrow_table(arrow_table, self.config))
        collect_s = time.perf_counter() - collect_start

        partition_effective = self.config.partition_mode == "none" or self.config.runner == "ray"
        return OrganizedBatches(
            batches=batches,
            metrics={
                "organizer": self.name,
                "runner": self.config.runner,
                "partition_mode": self.config.partition_mode,
                "requested_partitions": self.config.partitions,
                "partition_effective": str(partition_effective).lower(),
                "organizer_from_arrow_s": round(from_arrow_s, 6),
                "organizer_plan_s": round(plan_s, 6),
                "organizer_collect_s": round(collect_s, 6),
                "warnings": " | ".join(warnings_seen),
                **organization_strategy_metrics(batches, self.config.batching_policy),
            },
        )

    def _apply_partition_mode(self, df):
        if self.config.partitions <= 0 or self.config.partition_mode == "none":
            return df
        if self.config.partition_mode == "into_partitions":
            return df.into_partitions(self.config.partitions)
        return df.repartition(self.config.partitions)


def make_organizer(name: Literal["arrow", "daft"], config: OrganizerConfig):
    if name == "arrow":
        return ArrowOrganizer(config)
    if name == "daft":
        return DaftOrganizer(config)
    raise ValueError(f"Unknown organizer: {name}")


def _validate_batching_policy(config: OrganizerConfig) -> None:
    if config.batching_policy not in (
        "fixed_rows",
        "token_budget",
        "length_align_fixed_rows",
        "length_align_token_budget",
        "prefix_aware_fixed_rows",
        "prefix_aware_token_budget",
    ):
        raise ValueError(f"unknown batching policy: {config.batching_policy}")
    if _uses_token_budget(config.batching_policy) and config.token_budget <= 0:
        raise ValueError("token_budget must be positive when batching_policy is token_budget")


def _uses_token_budget(policy: BatchingPolicy) -> bool:
    return policy in ("token_budget", "length_align_token_budget", "prefix_aware_token_budget")


def _base_policy(policy: BatchingPolicy) -> Literal["fixed_rows", "token_budget"]:
    return "token_budget" if _uses_token_budget(policy) else "fixed_rows"


def _sort_table_for_policy(table: pa.Table, policy: BatchingPolicy) -> pa.Table:
    if table.num_rows == 0:
        return table
    if policy in ("length_align_fixed_rows", "length_align_token_budget"):
        if "prompt_tokens" not in table.column_names:
            return table
        order = sorted(
            range(table.num_rows),
            key=lambda index: (
                _safe_int(table.column("prompt_tokens")[index].as_py()),
                _safe_int(table.column("doc_id")[index].as_py()) if "doc_id" in table.column_names else index,
            ),
        )
        return table.take(pa.array(order, type=pa.int64()))
    if policy in ("prefix_aware_fixed_rows", "prefix_aware_token_budget"):
        if "prefix_key" not in table.column_names:
            return table
        order = sorted(
            range(table.num_rows),
            key=lambda index: (
                str(table.column("prefix_key")[index].as_py() or ""),
                _safe_int(table.column("prompt_tokens")[index].as_py()) if "prompt_tokens" in table.column_names else 0,
                _safe_int(table.column("doc_id")[index].as_py()) if "doc_id" in table.column_names else index,
            ),
        )
        return table.take(pa.array(order, type=pa.int64()))
    return table


def _safe_int(value: object) -> int:
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def _row_token_cost(table: pa.Table, row_index: int, completion_max_tokens: int) -> int:
    if "prompt_tokens" not in table.column_names:
        return max(1, completion_max_tokens)
    value = table.column("prompt_tokens")[row_index].as_py()
    prompt_tokens = int(value) if value is not None else 0
    return max(1, prompt_tokens + max(0, completion_max_tokens))


def _token_budget_batches(table: pa.Table, token_budget: int, completion_max_tokens: int) -> list[pa.Table]:
    batches: list[pa.Table] = []
    start = 0
    current_tokens = 0
    for row_index in range(table.num_rows):
        row_tokens = _row_token_cost(table, row_index, completion_max_tokens)
        if row_index > start and current_tokens + row_tokens > token_budget:
            batches.append(table.slice(start, row_index - start))
            start = row_index
            current_tokens = 0
        current_tokens += row_tokens
    if start < table.num_rows:
        batches.append(table.slice(start, table.num_rows - start))
    return batches


def organize_arrow_table(table: pa.Table, config: OrganizerConfig) -> list[pa.Table]:
    table = _sort_table_for_policy(table, config.batching_policy)
    if _base_policy(config.batching_policy) == "fixed_rows":
        return [table.slice(offset, config.batch_size) for offset in range(0, table.num_rows, config.batch_size)]
    return _token_budget_batches(table, config.token_budget, config.completion_max_tokens)


def organization_strategy_metrics(batches: list[pa.Table], policy: BatchingPolicy) -> dict[str, float | str]:
    token_spreads = []
    prefix_majority_rows = 0
    prefix_rows = 0
    for batch in batches:
        if "prompt_tokens" in batch.column_names and batch.num_rows:
            values = [_safe_int(value.as_py()) for value in batch.column("prompt_tokens")]
            token_spreads.append(max(values) - min(values))
        if "prefix_key" in batch.column_names and batch.num_rows:
            prefixes = [str(value.as_py() or "") for value in batch.column("prefix_key")]
            counts: dict[str, int] = {}
            for prefix in prefixes:
                counts[prefix] = counts.get(prefix, 0) + 1
            prefix_majority_rows += max(counts.values()) if counts else 0
            prefix_rows += len(prefixes)
    return {
        "organization_policy_family": (
            "length_align"
            if policy in ("length_align_fixed_rows", "length_align_token_budget")
            else "prefix_aware"
            if policy in ("prefix_aware_fixed_rows", "prefix_aware_token_budget")
            else "none"
        ),
        "batch_prompt_token_spread_mean": round(sum(token_spreads) / len(token_spreads), 3) if token_spreads else 0.0,
        "prefix_group_ratio": round(prefix_majority_rows / prefix_rows, 6) if prefix_rows else 0.0,
    }


def batch_metrics(batches: list[pa.Table]) -> dict[str, float | int]:
    batch_rows = [batch.num_rows for batch in batches]
    output_rows = sum(batch_rows)
    return {
        "output_rows": output_rows,
        "batch_count": len(batches),
        "min_batch_rows": min(batch_rows) if batch_rows else 0,
        "max_batch_rows": max(batch_rows) if batch_rows else 0,
        "avg_batch_rows": round(output_rows / len(batches), 3) if batches else 0,
    }
