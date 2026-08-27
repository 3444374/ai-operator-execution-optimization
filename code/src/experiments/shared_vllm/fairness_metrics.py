"""Service-rate and completion-accounted fairness metrics."""

from __future__ import annotations

import json
import math

from src.observability.metrics import percentile


TAIL_PERCENTILE = 95


def jain_fairness(values: list[float]) -> float:
    if not values:
        raise ValueError("fairness values must not be empty")
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
        for value in values
    ):
        raise ValueError("fairness values must be finite and non-negative")
    total = float(sum(values))
    if total == 0:
        return 0.0
    return total * total / (
        len(values) * sum(float(value) ** 2 for value in values)
    )

def normalized_job_service_rates(
    job_evidence: list[dict[str, object]],
    weights: tuple[int, ...],
) -> list[float]:
    if len(job_evidence) != len(weights):
        raise ValueError("job evidence and weights must have equal length")
    rates = []
    for evidence, weight in zip(job_evidence, weights):
        observed_work = float(
            evidence.get("actual_work", evidence["predicted_work"])
        )
        jct_s = float(evidence["jct_s"])
        if (
            not math.isfinite(observed_work)
            or observed_work < 0
            or not math.isfinite(jct_s)
            or jct_s <= 0
            or weight <= 0
        ):
            raise ValueError(
                "service inputs must contain finite non-negative work, "
                "positive JCT, and positive weights"
            )
        rates.append(observed_work / jct_s / weight)
    return rates


def cumulative_service_disparity(
    job_evidence: list[dict[str, object]],
    weights: tuple[int, ...],
) -> dict[str, float | str]:
    """Report the final weighted cumulative-service gap across jobs.

    This is a descriptive trace-derived quantity, not a theoretical VTC/DRR
    lag bound: completed jobs may have different offered work.  Keeping the
    status explicit prevents it from being over-interpreted.
    """

    if len(job_evidence) != len(weights) or not job_evidence:
        raise ValueError("job evidence and weights must be aligned and non-empty")
    normalized = []
    for evidence, weight in zip(job_evidence, weights):
        actual_work = float(evidence["actual_work"])
        if weight <= 0 or not math.isfinite(actual_work) or actual_work < 0:
            raise ValueError("actual work must be finite and weights positive")
        normalized.append(actual_work / weight)
    disparity = max(normalized) - min(normalized)
    mean_service = sum(normalized) / len(normalized)
    cumulative = [0.0] * len(job_evidence)
    events = sorted(
        (
            float(completion_epoch_s),
            job_index,
            float(work),
        )
        for job_index, evidence in enumerate(job_evidence)
        for completion_epoch_s, work in evidence.get(
            "service_completion_events",
            (),
        )
    )
    max_overlap_disparity = 0.0
    max_overlap_ratio = 0.0
    overlap_samples = 0
    for completion_epoch_s, job_index, work in events:
        cumulative[job_index] += work
        active = []
        for index, evidence in enumerate(job_evidence):
            intervals = evidence.get("request_backlog_intervals", ())
            if intervals:
                backlogged = any(
                    float(arrival_epoch_s) <= completion_epoch_s
                    <= float(request_completion_epoch_s)
                    for arrival_epoch_s, request_completion_epoch_s in intervals
                )
            else:
                # Compatibility for historical evidence that only recorded the
                # coarse job lifetime. New formal runs always use request-level
                # backlog intervals.
                backlogged = (
                    float(evidence.get("arrival_start_epoch_s", float("inf")))
                    <= completion_epoch_s
                    <= float(
                        evidence.get("completion_end_epoch_s", float("-inf"))
                    )
                )
            if backlogged:
                active.append(index)
        if len(active) < 2:
            continue
        overlap_samples += 1
        active_service = [cumulative[index] / weights[index] for index in active]
        observed_disparity = max(active_service) - min(active_service)
        observed_mean = sum(active_service) / len(active_service)
        max_overlap_disparity = max(max_overlap_disparity, observed_disparity)
        max_overlap_ratio = max(
            max_overlap_ratio,
            observed_disparity / observed_mean if observed_mean > 0 else 0.0,
        )
    return {
        "service_disparity_status": (
            "ok:simultaneously_backlogged_jobs_descriptive"
            if overlap_samples
            else "unavailable:no_overlapping_completion_samples"
        ),
        "service_disparity_bound_status": (
            "unavailable:not_proven_for_current_credit_implementation"
        ),
        "normalized_cumulative_service_min": min(normalized),
        "normalized_cumulative_service_max": max(normalized),
        "normalized_cumulative_service_disparity": disparity,
        "normalized_cumulative_service_disparity_ratio": (
            disparity / mean_service if mean_service > 0 else 0.0
        ),
        "overlap_service_disparity_samples": overlap_samples,
        "max_overlap_normalized_service_disparity": max_overlap_disparity,
        "max_overlap_normalized_service_disparity_ratio": max_overlap_ratio,
    }

def completion_accounted_service_fairness(
    job_evidence: list[dict[str, object]],
    weights: tuple[int, ...],
) -> dict[str, float | int | str]:
    """Replay empirical weighted service from registered-ready completions.

    Actual work is charged only when a request completes.  Consequently these
    are upstream, completion-granularity empirical metrics, not continuous
    token service or a VTC/GPS theoretical bound.
    """

    unavailable: dict[str, float | int | str] = {
        "completion_service_lag_status": (
            "unavailable:requires_complete_registered_ready_ledger"
        ),
        "completion_service_lag_samples": 0,
        "completion_service_lag_p95_work": 0.0,
        "completion_service_lag_max_work": 0.0,
        "completion_service_lag_job_max_work": "[]",
        "completion_longest_no_service_s": 0.0,
        "completion_longest_no_service_by_job_s": "[]",
    }
    if len(job_evidence) != len(weights) or not job_evidence:
        raise ValueError("job evidence and weights must be aligned and non-empty")
    if any(weight <= 0 for weight in weights):
        raise ValueError("service weights must be positive")
    if not all(
        evidence.get("ready_lifecycle_complete") is True
        and evidence.get("ready_lifecycle_rows")
        for evidence in job_evidence
    ):
        return unavailable

    intervals_by_job: list[list[tuple[float, float]]] = []
    events_by_epoch: dict[float, list[tuple[int, float]]] = {}
    for job_index, evidence in enumerate(job_evidence):
        intervals = []
        for row in evidence.get("ready_lifecycle_rows", ()):
            registered = float(row["registered_epoch_s"])
            completed = float(row["completion_epoch_s"])
            work = float(row["actual_work"])
            if (
                not math.isfinite(registered)
                or not math.isfinite(completed)
                or completed < registered
                or not math.isfinite(work)
                or work < 0
            ):
                raise ValueError("ready lifecycle service evidence is invalid")
            intervals.append((registered, completed))
            events_by_epoch.setdefault(completed, []).append((job_index, work))
        intervals_by_job.append(sorted(intervals))

    ideal = [0.0] * len(job_evidence)
    actual = [0.0] * len(job_evidence)
    positive_lag_samples: list[float] = []
    max_lag_by_job = [0.0] * len(job_evidence)
    for completed_at, completions in sorted(events_by_epoch.items()):
        active = [
            job_index
            for job_index, intervals in enumerate(intervals_by_job)
            if any(start <= completed_at <= end for start, end in intervals)
        ]
        if not active:
            raise ValueError("completion event has no registered backlog owner")
        completed_work = sum(work for _job_index, work in completions)
        active_weight = sum(weights[job_index] for job_index in active)
        for job_index in active:
            ideal[job_index] += (
                completed_work * weights[job_index] / active_weight
            )
        for job_index, work in completions:
            actual[job_index] += work
        if len(active) >= 2:
            for job_index in active:
                lag = max(0.0, ideal[job_index] - actual[job_index])
                positive_lag_samples.append(lag)
                max_lag_by_job[job_index] = max(
                    max_lag_by_job[job_index], lag
                )

    longest_no_service_by_job = []
    for job_index, intervals in enumerate(intervals_by_job):
        merged: list[list[float]] = []
        for start, end in intervals:
            if not merged or start > merged[-1][1]:
                merged.append([start, end])
            else:
                merged[-1][1] = max(merged[-1][1], end)
        completions = sorted(
            completed_at
            for completed_at, rows in events_by_epoch.items()
            if any(owner == job_index for owner, _work in rows)
        )
        longest = 0.0
        for start, end in merged:
            service_epochs = [
                epoch for epoch in completions if start <= epoch <= end
            ]
            points = [start, *service_epochs, end]
            longest = max(
                longest,
                max(
                    (right - left for left, right in zip(points, points[1:])),
                    default=0.0,
                ),
            )
        longest_no_service_by_job.append(longest)

    return {
        "completion_service_lag_status": (
            "ok:registered_backlog_completion_accounted_empirical"
        ),
        "completion_service_lag_samples": len(positive_lag_samples),
        "completion_service_lag_p95_work": (
            percentile(positive_lag_samples, TAIL_PERCENTILE)
            if positive_lag_samples
            else 0.0
        ),
        "completion_service_lag_max_work": max(
            positive_lag_samples, default=0.0
        ),
        "completion_service_lag_job_max_work": json.dumps(max_lag_by_job),
        "completion_longest_no_service_s": max(
            longest_no_service_by_job, default=0.0
        ),
        "completion_longest_no_service_by_job_s": json.dumps(
            longest_no_service_by_job
        ),
    }
