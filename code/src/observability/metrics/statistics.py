"""Distribution summaries for batch-level results."""

from __future__ import annotations

import math
import statistics
from dataclasses import asdict, dataclass


# t(0.975, df), rounded upward to six decimals; see ../README.md.
_T_CRITICAL_95 = {
    1: 12.706205,
    2: 4.302653,
    3: 3.182447,
    4: 2.776446,
    5: 2.570582,
    6: 2.446912,
    7: 2.364625,
    8: 2.306005,
    9: 2.262158,
    10: 2.228139,
    11: 2.200986,
    12: 2.178813,
    13: 2.160369,
    14: 2.144787,
    15: 2.131450,
    16: 2.119906,
    17: 2.109816,
    18: 2.100923,
    19: 2.093025,
    20: 2.085964,
    21: 2.079614,
    22: 2.073874,
    23: 2.068658,
    24: 2.063899,
    25: 2.059539,
    26: 2.055530,
    27: 2.051831,
    28: 2.048408,
    29: 2.045230,
    30: 2.042273,
    40: 2.021076,
    60: 2.000298,
    120: 1.979931,
    1000: 1.962340,
    10000: 1.960202,
    1000000: 1.959967,
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


def latency_observations(rows, *, sample_unit_key=None, slowest=3):
    """Offline receipt summary for one declared task/phase/population.

    Batch-copied intervals require a shared sample-unit ID. A hundred completed
    units only permits an empirical rank; it does not establish population P99.
    """
    rows=list(rows);units={};ids=set()
    for row in rows:
        if row['id'] in ids:
            raise ValueError('duplicate observation ID')
        ids.add(row['id'])
        if row['status'] not in ('succeeded','failed','unknown'):
            raise ValueError('explicit observation status required')
        value=row.get('elapsed_s')
        if value is not None and (type(value) not in (int,float) or not math.isfinite(value) or value<0):
            raise ValueError('elapsed time must be finite nonnegative or missing')
        if row['status']=='succeeded' and value is not None:
            key=row['id'] if sample_unit_key is None else row[sample_unit_key]
            if key in units and units[key]!=value:
                raise ValueError('one sample unit has inconsistent copied intervals')
            units[key]=value
    values=list(units.values())
    missing=sum(row['status']!='succeeded' or row.get('elapsed_s') is None for row in rows)
    batch_unknown=sample_unit_key is None and any(row.get('copied_batch_interval') for row in rows)
    eligible=len(values)>=100 and not missing and not batch_unknown
    completed=[row for row in rows if row['status']=='succeeded' and row.get('elapsed_s') is not None]
    domains={row.get('clock_domain') for row in completed}
    timed=completed and None not in domains and len(domains)==1 and all(
        row.get('clock_domain_verified') is True and row.get('end_s') is not None for row in completed)
    last=max(completed,key=lambda row:row['end_s']) if timed else None
    return dict(observations=len(rows),completed_units=len(values),incomplete_observations=missing,
        median_s=statistics.median(values) if values else None,maximum_s=max(values) if values else None,
        p50_s=percentile(values,50) if values else None,p95_s=percentile(values,95) if values else None,
        quantile_method='nearest rank; median_s uses arithmetic middle',
        empirical_p99_s=percentile(values,99) if eligible else None,
        empirical_p99_rank=math.ceil(.99*len(values)) if eligible else None,
        empirical_only=True,population_P99_established=False,
        p99_unavailable_reason=None if eligible else 'insufficient_units_or_incomplete_or_unidentified_batch',
        slowest=sorted(completed,key=lambda row:row['elapsed_s'],reverse=True)[:slowest],
        last_completed=last,clock_correlation_available=bool(timed),
        incomplete=[row for row in rows if row['status']!='succeeded' or row.get('elapsed_s') is None],
        distribution_scope='completed successful units; incomplete observations listed separately',
        scope='caller-declared homogeneous task/phase/population; not pure GPU inference')


def repeat_summary(values: list[float]) -> dict[str, float | int | str]:
    """Summarize repeat means with a conservative two-sided 95% t interval."""

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
    margin = (critical / math.sqrt(len(values))) * standard_deviation
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
    """Use the nearest lower tabulated df; zero denotes a single repeat."""

    if isinstance(degrees_of_freedom, bool) or not isinstance(degrees_of_freedom, int):
        raise TypeError("degrees_of_freedom must be an integer")
    if degrees_of_freedom < 0:
        raise ValueError("degrees_of_freedom must be non-negative")
    if degrees_of_freedom == 0:
        return 0.0
    lower_bound = max(df for df in _T_CRITICAL_95 if df <= degrees_of_freedom)
    return _T_CRITICAL_95[lower_bound]

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
