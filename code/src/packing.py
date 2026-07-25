"""Deterministic scalar-capacity packing for complete request rows."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence


def _positive_int(value: object, field_name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value <= 0
    ):
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _non_negative_int(value: object, field_name: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


@dataclass(frozen=True)
class PackItem:
    row_index: int
    stable_id: str
    cost_units: int

    def __post_init__(self) -> None:
        _non_negative_int(self.row_index, "row_index")
        if not isinstance(self.stable_id, str) or not self.stable_id:
            raise ValueError("stable_id must be a non-empty string")
        _non_negative_int(self.cost_units, "cost_units")


@dataclass
class _OpenBatch:
    creation_index: int
    row_indexes: list[int]
    total_cost_units: int


def best_fit_decreasing(
    items: Sequence[PackItem],
    *,
    capacity: int,
    max_rows: int,
) -> tuple[tuple[int, ...], ...]:
    resolved_capacity = _positive_int(capacity, "capacity")
    resolved_max_rows = _positive_int(max_rows, "max_rows")
    row_indexes = [item.row_index for item in items]
    if len(row_indexes) != len(set(row_indexes)):
        raise ValueError("duplicate row_index values are not allowed")

    ordered = sorted(
        items,
        key=lambda item: (
            -item.cost_units,
            item.stable_id,
            item.row_index,
        ),
    )
    batches: list[_OpenBatch] = []
    for item in ordered:
        eligible = [
            batch
            for batch in batches
            if len(batch.row_indexes) < resolved_max_rows
            and batch.total_cost_units + item.cost_units
            <= resolved_capacity
        ]
        if eligible:
            selected = min(
                eligible,
                key=lambda batch: (
                    resolved_capacity
                    - batch.total_cost_units
                    - item.cost_units,
                    batch.creation_index,
                ),
            )
            selected.row_indexes.append(item.row_index)
            selected.total_cost_units += item.cost_units
            continue
        batches.append(
            _OpenBatch(
                creation_index=len(batches),
                row_indexes=[item.row_index],
                total_cost_units=item.cost_units,
            )
        )
    return tuple(tuple(batch.row_indexes) for batch in batches)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil(percentile / 100.0 * len(ordered)) - 1
    return ordered[min(max(index, 0), len(ordered) - 1)]


@dataclass(frozen=True)
class PackingSummary:
    utilization_mean: float
    utilization_p95: float
    oversized_rows: int
    input_rows: int
    batch_count: int
    cost_units_p50: float
    cost_units_p95: float
    cost_units_p99: float
    cost_units_max: int


def summarize_packing(
    batch_cost_units: Sequence[int],
    batch_row_counts: Sequence[int],
    *,
    capacity: int,
) -> PackingSummary:
    resolved_capacity = _non_negative_int(capacity, "capacity")
    if len(batch_cost_units) != len(batch_row_counts):
        raise ValueError("batch costs and row counts must have equal length")
    costs = [
        _non_negative_int(value, "batch cost")
        for value in batch_cost_units
    ]
    rows = [
        _non_negative_int(value, "batch row count")
        for value in batch_row_counts
    ]
    utilization = (
        [
            cost / resolved_capacity
            for cost in costs
            if cost <= resolved_capacity
        ]
        if resolved_capacity > 0
        else []
    )
    oversized_rows = (
        sum(
            row_count
            for cost, row_count in zip(costs, rows)
            if cost > resolved_capacity
        )
        if resolved_capacity > 0
        else 0
    )
    return PackingSummary(
        utilization_mean=(
            sum(utilization) / len(utilization)
            if utilization
            else 0.0
        ),
        utilization_p95=_percentile(utilization, 95),
        oversized_rows=oversized_rows,
        input_rows=sum(rows),
        batch_count=len(costs),
        cost_units_p50=_percentile([float(value) for value in costs], 50),
        cost_units_p95=_percentile([float(value) for value in costs], 95),
        cost_units_p99=_percentile([float(value) for value in costs], 99),
        cost_units_max=max(costs, default=0),
    )
