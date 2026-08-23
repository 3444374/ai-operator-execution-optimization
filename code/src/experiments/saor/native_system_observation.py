"""Validate common gateway traces and derive five-arm system/fairness metrics."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence


@dataclass(frozen=True)
class JobObservationContract:
    """Freeze one Job's role, entitlement weight, and request SLO."""

    job_id: str
    role: str
    weight: float
    request_slo_s: float
    job_jct_slo_s: float | None = None


def summarize_gateway_rows(
    rows: Iterable[Mapping[str, object]],
    contracts: Sequence[JobObservationContract],
) -> dict[str, object]:
    """Fail closed on proxy drift and summarize request/service observations."""

    materialized = [dict(row) for row in rows]
    if not materialized:
        raise ValueError("observation gateway trace is empty")
    contract_by_job = {contract.job_id: contract for contract in contracts}
    if len(contract_by_job) != len(contracts) or not contracts:
        raise ValueError("Job observation contracts must be non-empty and unique")
    by_job: dict[str, list[dict[str, object]]] = {
        job_id: [] for job_id in contract_by_job
    }
    request_ids: set[int] = set()
    for row in materialized:
        job_id = str(row.get("job_id", ""))
        if job_id not in by_job:
            raise ValueError("gateway trace contains an unknown Job")
        request_id = _integer(row, "gateway_request_id")
        if request_id in request_ids:
            raise ValueError("gateway request IDs are not unique")
        request_ids.add(request_id)
        if _integer(row, "retry_count") != 0:
            raise ValueError("observation gateway retry is forbidden")
        if row.get("request_body_sha256") != row.get("forwarded_body_sha256"):
            raise ValueError("observation gateway changed the request body")
        if row.get("status") != "completed" or not 200 <= _integer(
            row, "upstream_status"
        ) < 300:
            raise ValueError("observation gateway contains a failed request")
        for field in (
            "received_epoch_s",
            "upstream_start_epoch_s",
            "upstream_response_epoch_s",
            "response_completed_epoch_s",
            "dispatch_delay_s",
        ):
            _finite(row, field)
        received = _finite(row, "received_epoch_s")
        started = _finite(row, "upstream_start_epoch_s")
        completed = _finite(row, "upstream_response_epoch_s")
        response_completed = _finite(row, "response_completed_epoch_s")
        if not received <= started <= completed <= response_completed:
            raise ValueError("gateway lifecycle timestamps are unordered")
        work = _integer(row, "actual_total_tokens")
        prompt = _integer(row, "actual_prompt_tokens")
        output = _integer(row, "actual_output_tokens")
        if work <= 0 or prompt < 0 or output < 0 or prompt + output != work:
            raise ValueError("gateway endpoint usage is incomplete")
        by_job[job_id].append(row)
    if any(not job_rows for job_rows in by_job.values()):
        raise ValueError("gateway trace does not cover every frozen Job")

    jobs: dict[str, dict[str, object]] = {}
    for job_id, job_rows in by_job.items():
        contract = contract_by_job[job_id]
        latencies = [
            _finite(row, "upstream_response_epoch_s")
            - _finite(row, "received_epoch_s")
            for row in job_rows
        ]
        actual_work = sum(_integer(row, "actual_total_tokens") for row in job_rows)
        jobs[job_id] = {
            "role": contract.role,
            "weight": contract.weight,
            "request_count": len(job_rows),
            "t2_first_request_epoch_s": min(
                _finite(row, "received_epoch_s") for row in job_rows
            ),
            "t3_last_request_completion_epoch_s": max(
                _finite(row, "upstream_response_epoch_s") for row in job_rows
            ),
            "request_p50_s": _quantile(latencies, 0.50),
            "request_p95_s": _quantile(latencies, 0.95),
            "request_p99_s": _quantile(latencies, 0.99),
            "request_slo_s": contract.request_slo_s,
            "job_jct_slo_s": contract.job_jct_slo_s,
            "request_slo_violation_ratio": sum(
                latency > contract.request_slo_s for latency in latencies
            ) / len(latencies),
            "actual_prompt_tokens": sum(
                _integer(row, "actual_prompt_tokens") for row in job_rows
            ),
            "actual_output_tokens": sum(
                _integer(row, "actual_output_tokens") for row in job_rows
            ),
            "actual_total_tokens": actual_work,
        }

    fairness = _service_fairness(by_job, contract_by_job)
    return {
        "schema_version": 1,
        "status": "passed",
        "observation_policy": "pass_through_no_queue_no_retry",
        "gateway_integrity": {
            "request_count": len(materialized),
            "retry_count": 0,
            "body_identity_passed": True,
            "dispatch_delay_p99_s": _quantile(
                [_finite(row, "dispatch_delay_s") for row in materialized],
                0.99,
            ),
            "dispatch_delay_max_s": max(
                _finite(row, "dispatch_delay_s") for row in materialized
            ),
        },
        "jobs": jobs,
        "request_observations": [
            {
                "gateway_request_id": _integer(row, "gateway_request_id"),
                "job_id": str(row["job_id"]),
                "received_epoch_s": _finite(row, "received_epoch_s"),
                "completed_epoch_s": _finite(row, "upstream_response_epoch_s"),
                "actual_total_tokens": _integer(row, "actual_total_tokens"),
            }
            for row in materialized
        ],
        "service_fairness": fairness,
        "actual_total_tokens": sum(
            int(job["actual_total_tokens"]) for job in jobs.values()
        ),
    }


def build_system_observation(
    gateway_summary: Mapping[str, object],
    *,
    t0_by_job: Mapping[str, float],
    t1_by_job: Mapping[str, float],
    t4_by_job: Mapping[str, float],
) -> dict[str, object]:
    """Join external release/source/result clocks with passive service clocks."""

    raw_jobs = gateway_summary.get("jobs")
    if not isinstance(raw_jobs, dict) or not raw_jobs:
        raise ValueError("gateway summary jobs are missing")
    if not (
        set(raw_jobs) == set(t0_by_job) == set(t1_by_job) == set(t4_by_job)
    ):
        raise ValueError("system timeline Job identities do not match")
    jobs: dict[str, dict[str, object]] = {}
    for job_id, raw in raw_jobs.items():
        if not isinstance(raw, dict):
            raise ValueError("gateway Job summary must be an object")
        t0 = _finite_value(t0_by_job[job_id], f"{job_id}.T0")
        t1 = _finite_value(t1_by_job[job_id], f"{job_id}.T1")
        t2 = _finite_value(raw["t2_first_request_epoch_s"], f"{job_id}.T2")
        t3 = _finite_value(
            raw["t3_last_request_completion_epoch_s"], f"{job_id}.T3"
        )
        t4 = _finite_value(t4_by_job[job_id], f"{job_id}.T4")
        if not t0 <= t1 <= t2 <= t3 <= t4:
            raise ValueError("system timeline must satisfy T0<=T1<=T2<=T3<=T4")
        jobs[job_id] = {
            **raw,
            "t0_job_release_epoch_s": t0,
            "t1_first_batch_epoch_s": t1,
            "t2_first_request_epoch_s": t2,
            "t3_last_request_completion_epoch_s": t3,
            "t4_result_visible_epoch_s": t4,
            "jct_s": t4 - t0,
            "source_s": t1 - t0,
            "execution_s": t4 - t1,
            "service_span_s": t3 - t2,
            "job_jct_slo_status": (
                "available" if raw.get("job_jct_slo_s") is not None else "unavailable"
            ),
            "job_jct_slo_violation": (
                (t4 - t0) > float(raw["job_jct_slo_s"])
                if raw.get("job_jct_slo_s") is not None else "unavailable"
            ),
        }
    group_jct = max(float(job["t4_result_visible_epoch_s"]) for job in jobs.values()) - min(
        float(job["t0_job_release_epoch_s"]) for job in jobs.values()
    )
    actual_tokens = _integer(gateway_summary, "actual_total_tokens")
    isolation = _observed_interference(
        gateway_summary,
        jobs=jobs,
        t0_by_job=t0_by_job,
    )
    return {
        "schema_version": 1,
        "status": "passed",
        "timed_boundary": "job_release_before_postgres_to_validated_result_visibility",
        "jobs": jobs,
        "group_jct_s": group_jct,
        "correct_throughput_tokens_per_s": actual_tokens / group_jct,
        "actual_total_tokens": actual_tokens,
        "service_fairness": gateway_summary.get("service_fairness"),
        "isolation_observation": isolation,
        "gateway_integrity": gateway_summary.get("gateway_integrity"),
    }


def _observed_interference(
    gateway_summary: Mapping[str, object],
    *,
    jobs: Mapping[str, Mapping[str, object]],
    t0_by_job: Mapping[str, float],
) -> dict[str, object]:
    """Measure within-run victim impact without claiming a solo counterfactual."""

    role_to_job = {str(job["role"]): job_id for job_id, job in jobs.items()}
    victim = role_to_job.get("bulk")
    aggressor = role_to_job.get("foreground")
    observations = gateway_summary.get("request_observations")
    if not victim or not aggressor or not isinstance(observations, list):
        return {"status": "unavailable:roles_or_request_observations_missing"}
    aggressor_release = float(t0_by_job[aggressor])
    victim_rows = [
        row for row in observations
        if isinstance(row, dict) and row.get("job_id") == victim
    ]
    pre = [
        float(row["completed_epoch_s"]) - float(row["received_epoch_s"])
        for row in victim_rows
        if float(row["completed_epoch_s"]) <= aggressor_release
    ]
    post = [
        float(row["completed_epoch_s"]) - float(row["received_epoch_s"])
        for row in victim_rows
        if float(row["received_epoch_s"]) >= aggressor_release
    ]
    first_post_completion = min(
        (
            float(row["completed_epoch_s"])
            for row in victim_rows
            if float(row["completed_epoch_s"]) >= aggressor_release
        ),
        default=None,
    )
    victim_backlogged = any(
        float(row["received_epoch_s"]) <= aggressor_release
        < float(row["completed_epoch_s"])
        for row in victim_rows
    )
    aggressor_service_end = float(jobs[aggressor]["t3_last_request_completion_epoch_s"])
    output: dict[str, object] = {
        "status": (
            "ok:within_run_observation_only"
            if pre and post else "partial:insufficient_pre_or_post_samples"
        ),
        "victim_job_id": victim,
        "aggressor_job_id": aggressor,
        "aggressor_release_epoch_s": aggressor_release,
        "victim_pre_request_count": len(pre),
        "victim_post_request_count": len(post),
        "victim_backlogged_at_aggressor_release": victim_backlogged,
        "victim_no_service_after_aggressor_release_s": (
            max(0.0, first_post_completion - aggressor_release)
            if victim_backlogged and first_post_completion is not None else "unavailable"
        ),
        "counterfactual_scope": "within_run_only_not_full_solo_slowdown",
    }
    if pre:
        output["victim_pre_request_p99_s"] = _quantile(pre, 0.99)
    if post:
        output["victim_post_request_p99_s"] = _quantile(post, 0.99)
    if pre and post:
        baseline = _quantile(pre, 0.99)
        output["victim_request_p99_inflation_ratio"] = (
            _quantile(post, 0.99) / baseline if baseline > 0 else "unavailable"
        )
        recovered = min(
            (
                float(row["completed_epoch_s"])
                for row in victim_rows
                if float(row["received_epoch_s"]) >= aggressor_service_end
                and (
                    float(row["completed_epoch_s"])
                    - float(row["received_epoch_s"])
                ) <= baseline
            ),
            default=None,
        )
        output["victim_recovery_after_aggressor_service_end_s"] = (
            max(0.0, recovered - aggressor_service_end)
            if recovered is not None else "unavailable"
        )
    return output


def _service_fairness(
    rows_by_job: Mapping[str, Sequence[Mapping[str, object]]],
    contracts: Mapping[str, JobObservationContract],
) -> dict[str, object]:
    if len(rows_by_job) < 2:
        return {
            "status": "unavailable:requires_at_least_two_jobs",
            "common_backlog_duration_s": 0.0,
        }
    intervals_by_job = {
        job_id: _backlog_intervals(rows)
        for job_id, rows in rows_by_job.items()
    }
    common = list(intervals_by_job.values())[0]
    for intervals in list(intervals_by_job.values())[1:]:
        common = _intersect_intervals(common, intervals)
    duration = sum(end - start for start, end in common)
    if duration <= 0:
        return {
            "status": "unavailable:no_common_gateway_backlog",
            "common_backlog_duration_s": 0.0,
        }
    completed_work = {
        job_id: sum(
            _integer(row, "actual_total_tokens")
            for row in rows
            if _inside_intervals(
                _finite(row, "upstream_response_epoch_s"), common
            )
        )
        for job_id, rows in rows_by_job.items()
    }
    normalized = {
        job_id: completed_work[job_id] / contracts[job_id].weight
        for job_id in completed_work
    }
    denominator = len(normalized) * sum(value * value for value in normalized.values())
    weighted_jain = (
        sum(normalized.values()) ** 2 / denominator if denominator > 0 else 0.0
    )
    normalized_total = sum(normalized.values())
    shares = {
        job_id: value / normalized_total if normalized_total > 0 else 0.0
        for job_id, value in normalized.items()
    }
    no_service = {
        job_id: _longest_no_service(rows, common)
        for job_id, rows in rows_by_job.items()
    }
    lag_samples = _completion_accounted_lag_samples(
        rows_by_job, contracts, common
    )
    positive_lags = [max(0.0, lag) for lag in lag_samples]
    return {
        "status": "ok:gateway_observed_common_backlog_completion_accounted",
        "common_backlog_intervals": [list(interval) for interval in common],
        "common_backlog_duration_s": duration,
        "completed_work_by_job": completed_work,
        "weighted_service_share_by_job": shares,
        "weighted_jain_fairness": weighted_jain,
        "completion_service_lag_p95_work": _quantile(positive_lags, 0.95),
        "completion_service_lag_max_work": max(positive_lags, default=0.0),
        "longest_no_service_by_job_s": no_service,
        "longest_no_service_s": max(no_service.values(), default=0.0),
    }


def _backlog_intervals(
    rows: Sequence[Mapping[str, object]],
) -> list[tuple[float, float]]:
    events: dict[float, int] = {}
    for row in rows:
        start = _finite(row, "received_epoch_s")
        end = _finite(row, "upstream_response_epoch_s")
        events[start] = events.get(start, 0) + 1
        events[end] = events.get(end, 0) - 1
    points = sorted(events)
    active = 0
    intervals: list[tuple[float, float]] = []
    for index, point in enumerate(points[:-1]):
        active += events[point]
        next_point = points[index + 1]
        if active > 0 and next_point > point:
            if intervals and math.isclose(intervals[-1][1], point):
                intervals[-1] = (intervals[-1][0], next_point)
            else:
                intervals.append((point, next_point))
    return intervals


def _intersect_intervals(
    left: Sequence[tuple[float, float]],
    right: Sequence[tuple[float, float]],
) -> list[tuple[float, float]]:
    output: list[tuple[float, float]] = []
    i = j = 0
    while i < len(left) and j < len(right):
        start = max(left[i][0], right[j][0])
        end = min(left[i][1], right[j][1])
        if end > start:
            output.append((start, end))
        if left[i][1] <= right[j][1]:
            i += 1
        else:
            j += 1
    return output


def _inside_intervals(value: float, intervals: Sequence[tuple[float, float]]) -> bool:
    return any(start <= value <= end for start, end in intervals)


def _longest_no_service(
    rows: Sequence[Mapping[str, object]],
    common: Sequence[tuple[float, float]],
) -> float:
    longest = 0.0
    completions = sorted(
        _finite(row, "upstream_response_epoch_s") for row in rows
    )
    for start, end in common:
        inside = [value for value in completions if start <= value <= end]
        boundaries = [start, *inside, end]
        longest = max(
            longest,
            max(
                (right - left for left, right in zip(boundaries, boundaries[1:])),
                default=0.0,
            ),
        )
    return longest


def _completion_accounted_lag_samples(
    rows_by_job: Mapping[str, Sequence[Mapping[str, object]]],
    contracts: Mapping[str, JobObservationContract],
    common: Sequence[tuple[float, float]],
) -> list[float]:
    events = sorted(
        (
            _finite(row, "upstream_response_epoch_s"),
            job_id,
            _integer(row, "actual_total_tokens"),
        )
        for job_id, rows in rows_by_job.items()
        for row in rows
        if _inside_intervals(_finite(row, "upstream_response_epoch_s"), common)
    )
    total_weight = sum(contract.weight for contract in contracts.values())
    total_work = 0.0
    work_by_job = {job_id: 0.0 for job_id in contracts}
    samples: list[float] = []
    for _epoch, completed_job, work in events:
        total_work += work
        work_by_job[completed_job] += work
        for job_id, contract in contracts.items():
            ideal = total_work * contract.weight / total_weight
            samples.append(ideal - work_by_job[job_id])
    return samples


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _finite(row: Mapping[str, object], field: str) -> float:
    if field not in row:
        raise ValueError(f"gateway field is missing: {field}")
    return _finite_value(row[field], field)


def _finite_value(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be finite")
    return parsed


def _integer(row: Mapping[str, object], field: str) -> int:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    return value
