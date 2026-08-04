"""Distribution summaries for batch-level results."""

from __future__ import annotations

import math
import statistics
from dataclasses import asdict, dataclass


_T_CRITICAL_95 = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    15: 2.131,
    20: 2.086,
    30: 2.042,
}


@dataclass(frozen=True)
class RepeatSummary:
    count: int
    mean: float
    standard_deviation: float
    coefficient_of_variation: float
    ci95_lower: float
    ci95_upper: float


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((p / 100.0) * len(ordered)) - 1
    index = min(max(index, 0), len(ordered) - 1)
    return ordered[index]


def repeat_summary(values: list[float]) -> dict[str, float | int | str]:
    """Summarize formal repeats with a small-sample Student-t interval."""

    if not values:
        return {
            "status": "unavailable:no_formal_repeats",
            **asdict(RepeatSummary(0, 0.0, 0.0, 0.0, 0.0, 0.0)),
        }
    if not all(math.isfinite(value) for value in values):
        raise ValueError("repeat values must be finite")
    mean = statistics.mean(values)
    standard_deviation = statistics.stdev(values) if len(values) > 1 else 0.0
    critical = _student_t_critical_95(len(values) - 1)
    margin = critical * standard_deviation / math.sqrt(len(values))
    return {
        "status": "ok" if len(values) > 1 else "single_repeat:no_interval",
        **asdict(
            RepeatSummary(
                count=len(values),
                mean=mean,
                standard_deviation=standard_deviation,
                coefficient_of_variation=(
                    standard_deviation / abs(mean) if mean else 0.0
                ),
                ci95_lower=mean - margin,
                ci95_upper=mean + margin,
            )
        ),
    }


def paired_performance_regression_count(
    baseline: list[float],
    candidate: list[float],
    *,
    higher_is_better: bool,
    tolerance_ratio: float = 0.0,
) -> int:
    if len(baseline) != len(candidate) or not baseline:
        raise ValueError("baseline and candidate repeats must align")
    if tolerance_ratio < 0 or not math.isfinite(tolerance_ratio):
        raise ValueError("tolerance_ratio must be finite and non-negative")
    if higher_is_better:
        return sum(
            observed < reference * (1.0 - tolerance_ratio)
            for reference, observed in zip(baseline, candidate)
        )
    return sum(
        observed > reference * (1.0 + tolerance_ratio)
        for reference, observed in zip(baseline, candidate)
    )


def _student_t_critical_95(degrees_of_freedom: int) -> float:
    if degrees_of_freedom <= 0:
        return 0.0
    for upper_bound in sorted(_T_CRITICAL_95):
        if degrees_of_freedom <= upper_bound:
            return _T_CRITICAL_95[upper_bound]
    return 1.96

def batch_result_stats(results: list[dict]) -> dict[str, float | int]:
    rows = [int(result.get("rows", 0)) for result in results]
    tokens = [int(result.get("token_count", 0)) for result in results]
    service_s = [float(result.get("service_s", 0.0)) for result in results]
    return {
        "batch_rows_min": min(rows) if rows else 0,
        "batch_rows_max": max(rows) if rows else 0,
        "batch_rows_mean": statistics.mean(rows) if rows else 0.0,
        "batch_tokens_min": min(tokens) if tokens else 0,
        "batch_tokens_max": max(tokens) if tokens else 0,
        "batch_tokens_mean": statistics.mean(tokens) if tokens else 0.0,
        "batch_tokens_p50": percentile([float(value) for value in tokens], 50),
        "batch_tokens_p95": percentile([float(value) for value in tokens], 95),
        "batch_service_s_p50": percentile(service_s, 50),
        "batch_service_s_p95": percentile(service_s, 95),
        "batch_service_s_p99": percentile(service_s, 99),
    }
