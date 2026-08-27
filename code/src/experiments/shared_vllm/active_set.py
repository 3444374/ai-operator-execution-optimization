"""Observed active-set lifecycle and shared-credit mechanism audit."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field


ACTIVE_SET_JOB_COUNT = 2
SIMULTANEOUS_START_TOLERANCE_S = 1e-6

@dataclass(frozen=True)
class _ActiveSetLifecycle:
    bulk_index: int
    foreground_index: int
    foreground_start: float
    foreground_end: float
    bulk_end: float
    overlap_s: float
    passed: bool
    foreground_drained_first: bool
    first_drained_index: int
    remaining_index: int
    first_drained_end: float
    remaining_end: float
    bulk_job_id: str
    foreground_job_id: str
    first_drained_job_id: str
    remaining_job_id: str

    @property
    def status(self) -> str:
        return (
            "ok:observed_staggered_two_job_overlap"
            if self.passed
            else "active_set_lifecycle_not_observed"
        )

    @property
    def post_drain_duration_s(self) -> float:
        return max(0.0, self.remaining_end - self.first_drained_end)


@dataclass
class _ActiveSetAggregate:
    work_limit: float = 0.0
    request_limit: float = 0.0
    active_by_job: dict[str, float] = field(default_factory=dict)
    active_work_by_job: dict[str, float] = field(default_factory=dict)
    waiting_work_by_job: dict[str, float] = field(default_factory=dict)
    endpoint_samples: list[dict[str, object]] = field(default_factory=list)


@dataclass
class _ActiveSetMechanismStats:
    pre_samples: int = 0
    overlap_samples: int = 0
    post_samples: int = 0
    overlap_reclaim_competition_samples: int = 0
    post_fit_violation_samples: int = 0
    pre_bulk_fractions: list[float] = field(default_factory=list)
    pre_bulk_dominant_shares: list[float] = field(default_factory=list)
    overlap_bulk_fractions: list[float] = field(default_factory=list)
    overlap_foreground_fractions: list[float] = field(default_factory=list)
    overlap_bulk_dominant_shares: list[float] = field(default_factory=list)
    overlap_foreground_dominant_shares: list[float] = field(default_factory=list)
    post_bulk_fractions: list[float] = field(default_factory=list)
    post_remaining_fractions: list[float] = field(default_factory=list)
    post_remaining_dominant_shares: list[float] = field(default_factory=list)
    post_remaining_waiting_work: list[float] = field(default_factory=list)


def _active_set_unavailable(
    observation_interval_s: float,
) -> dict[str, float | int | bool | str]:
    return {
        "active_set_contract_status": "unavailable:requires_staggered_two_job_trace",
        "active_set_contract_passed": False,
        "active_set_lifecycle_status": "unavailable:requires_staggered_two_job_trace",
        "active_set_lifecycle_passed": False,
        "active_set_mechanism_applicable": False,
        "active_set_mechanism_status": "not_applicable:no_credit_trace",
        "active_set_mechanism_passed": False,
        "active_set_bulk_job_index": -1,
        "active_set_foreground_job_index": -1,
        "active_set_overlap_s": 0.0,
        "active_set_foreground_drained_first": False,
        "active_set_first_drained_job_index": -1,
        "active_set_remaining_job_index": -1,
        "active_set_bulk_only_pre_samples": 0,
        "active_set_overlap_samples": 0,
        "active_set_overlap_reclaim_observed": False,
        "active_set_overlap_reclaim_competition_samples": 0,
        "active_set_overlap_bulk_fraction_min": 0.0,
        "active_set_overlap_foreground_fraction_max": 0.0,
        "active_set_overlap_bulk_dominant_share_min": 0.0,
        "active_set_overlap_foreground_dominant_share_max": 0.0,
        "active_set_bulk_only_post_samples": 0,
        "active_set_bulk_borrow_fraction_max": 0.0,
        "active_set_pre_bulk_dominant_share_max": 0.0,
        "active_set_bulk_reborrow_fraction_max": 0.0,
        "active_set_post_remaining_fraction_max": 0.0,
        "active_set_post_remaining_dominant_share_max": 0.0,
        "active_set_post_remaining_waiting_work_max": 0.0,
        "active_set_post_fit_violation_samples": 0,
        "active_set_post_drain_duration_s": 0.0,
        "active_set_post_drain_observation_interval_s": observation_interval_s,
        "active_set_post_drain_observed_samples": 0,
        "active_set_post_drain_applicable": False,
        "active_set_post_drain_status": "not_applicable:no_credit_trace",
        "active_set_post_work_conserving_passed": False,
    }


def _active_set_lifecycle(
    job_evidence: list[dict[str, object]],
) -> _ActiveSetLifecycle | None:
    if len(job_evidence) != ACTIVE_SET_JOB_COUNT:
        return None
    starts = tuple(float(item["arrival_start_epoch_s"]) for item in job_evidence)
    if math.isclose(
        starts[0],
        starts[1],
        abs_tol=SIMULTANEOUS_START_TOLERANCE_S,
    ):
        return None

    bulk_index = min(range(ACTIVE_SET_JOB_COUNT), key=starts.__getitem__)
    foreground_index = 1 - bulk_index
    bulk = job_evidence[bulk_index]
    foreground = job_evidence[foreground_index]
    foreground_start = float(foreground["arrival_start_epoch_s"])
    foreground_end = float(foreground["completion_end_epoch_s"])
    bulk_end = float(bulk["completion_end_epoch_s"])
    overlap_s = max(0.0, min(bulk_end, foreground_end) - foreground_start)
    passed = bool(
        starts[bulk_index] < foreground_start
        and foreground_start < bulk_end
        and overlap_s > 0
    )
    first_drained_index = min(
        range(ACTIVE_SET_JOB_COUNT),
        key=lambda index: float(job_evidence[index]["completion_end_epoch_s"]),
    )
    remaining_index = 1 - first_drained_index
    return _ActiveSetLifecycle(
        bulk_index=bulk_index,
        foreground_index=foreground_index,
        foreground_start=foreground_start,
        foreground_end=foreground_end,
        bulk_end=bulk_end,
        overlap_s=overlap_s,
        passed=passed,
        foreground_drained_first=foreground_end < bulk_end,
        first_drained_index=first_drained_index,
        remaining_index=remaining_index,
        first_drained_end=float(
            job_evidence[first_drained_index]["completion_end_epoch_s"]
        ),
        remaining_end=float(job_evidence[remaining_index]["completion_end_epoch_s"]),
        bulk_job_id=str(bulk["runtime_job_id"]),
        foreground_job_id=str(foreground["runtime_job_id"]),
        first_drained_job_id=str(
            job_evidence[first_drained_index]["runtime_job_id"]
        ),
        remaining_job_id=str(job_evidence[remaining_index]["runtime_job_id"]),
    )


def _decode_pairs(value: object) -> object:
    return json.loads(value) if isinstance(value, str) else value


def _add_job_values(target: dict[str, float], raw_pairs: object) -> None:
    for job_id, value in _decode_pairs(raw_pairs):
        key = str(job_id)
        target[key] = target.get(key, 0.0) + float(value)


def _aggregate_active_set_samples(
    samples: list[dict[str, object]],
) -> dict[float, _ActiveSetAggregate]:
    by_epoch: dict[float, _ActiveSetAggregate] = {}
    for sample in samples:
        observed_at = float(sample["observed_epoch_s"])
        aggregate = by_epoch.setdefault(observed_at, _ActiveSetAggregate())
        work_limit = float(sample["work_limit"])
        if work_limit <= 0:
            raise ValueError("active-set trace work limit must be positive")
        aggregate.work_limit += work_limit
        request_limit = float(sample["request_limit"])
        if request_limit <= 0:
            raise ValueError("active-set trace request limit must be positive")
        aggregate.request_limit += request_limit
        _add_job_values(aggregate.active_by_job, sample.get("active_by_job", ()))
        _add_job_values(
            aggregate.active_work_by_job,
            sample.get("active_work_by_job", ()),
        )
        _add_job_values(
            aggregate.waiting_work_by_job,
            sample.get("waiting_work_by_job", ()),
        )
        aggregate.endpoint_samples.append(sample)
    return by_epoch


def _dominant_share(
    work: float,
    active_requests: float,
    aggregate: _ActiveSetAggregate,
) -> float:
    return max(
        work / aggregate.work_limit,
        active_requests / aggregate.request_limit,
    )


def _record_lifecycle_phase_sample(
    observed_at: float,
    aggregate: _ActiveSetAggregate,
    lifecycle: _ActiveSetLifecycle,
    stats: _ActiveSetMechanismStats,
) -> None:
    bulk_work = aggregate.active_work_by_job.get(lifecycle.bulk_job_id, 0.0)
    foreground_work = aggregate.active_work_by_job.get(
        lifecycle.foreground_job_id,
        0.0,
    )
    if observed_at < lifecycle.foreground_start:
        if bulk_work <= 0 or foreground_work != 0:
            return
        stats.pre_samples += 1
        stats.pre_bulk_fractions.append(bulk_work / aggregate.work_limit)
        stats.pre_bulk_dominant_shares.append(
            _dominant_share(
                bulk_work,
                aggregate.active_by_job.get(lifecycle.bulk_job_id, 0.0),
                aggregate,
            )
        )
        return
    if observed_at <= lifecycle.foreground_end:
        if bulk_work <= 0 or foreground_work <= 0:
            return
        stats.overlap_samples += 1
        stats.overlap_bulk_fractions.append(bulk_work / aggregate.work_limit)
        stats.overlap_foreground_fractions.append(
            foreground_work / aggregate.work_limit
        )
        stats.overlap_bulk_dominant_shares.append(
            _dominant_share(
                bulk_work,
                aggregate.active_by_job.get(lifecycle.bulk_job_id, 0.0),
                aggregate,
            )
        )
        stats.overlap_foreground_dominant_shares.append(
            _dominant_share(
                foreground_work,
                aggregate.active_by_job.get(lifecycle.foreground_job_id, 0.0),
                aggregate,
            )
        )
        stats.overlap_reclaim_competition_samples += int(
            aggregate.waiting_work_by_job.get(lifecycle.bulk_job_id, 0.0) > 0
        )
        return
    if observed_at > lifecycle.bulk_end:
        return
    if bulk_work <= 0 or foreground_work != 0:
        return
    stats.post_samples += 1
    stats.post_bulk_fractions.append(bulk_work / aggregate.work_limit)


def _has_endpoint_fit_violation(
    endpoint_samples: list[dict[str, object]],
    remaining_job_id: str,
) -> bool:
    fit_violation = False
    for endpoint_sample in endpoint_samples:
        endpoint_waiting = dict(
            _decode_pairs(endpoint_sample.get("waiting_by_job", ()))
        )
        if float(endpoint_waiting.get(remaining_job_id, 0.0)) <= 0:
            continue
        endpoint_heads = dict(
            _decode_pairs(endpoint_sample.get("waiting_head_work_by_job", ()))
        )
        head_work = float(endpoint_heads.get(remaining_job_id, 0.0))
        if head_work <= 0:
            raise ValueError("waiting Job has no positive head-work evidence")
        request_fits = (
            float(endpoint_sample["active_requests"]) + 1
            <= float(endpoint_sample["request_limit"])
        )
        work_fits = (
            float(endpoint_sample["active_work"]) + head_work
            <= float(endpoint_sample["work_limit"])
        )
        fit_violation |= request_fits and work_fits
    return fit_violation


def _record_post_drain_sample(
    observed_at: float,
    aggregate: _ActiveSetAggregate,
    lifecycle: _ActiveSetLifecycle,
    stats: _ActiveSetMechanismStats,
) -> None:
    if not lifecycle.first_drained_end < observed_at <= lifecycle.remaining_end:
        return
    remaining_work = aggregate.active_work_by_job.get(
        lifecycle.remaining_job_id,
        0.0,
    )
    first_drained_work = aggregate.active_work_by_job.get(
        lifecycle.first_drained_job_id,
        0.0,
    )
    remaining_waiting_work = aggregate.waiting_work_by_job.get(
        lifecycle.remaining_job_id,
        0.0,
    )
    if first_drained_work != 0:
        return
    if remaining_work <= 0 and remaining_waiting_work <= 0:
        return

    stats.post_remaining_fractions.append(remaining_work / aggregate.work_limit)
    stats.post_remaining_dominant_shares.append(
        _dominant_share(
            remaining_work,
            aggregate.active_by_job.get(lifecycle.remaining_job_id, 0.0),
            aggregate,
        )
    )
    stats.post_remaining_waiting_work.append(remaining_waiting_work)
    stats.post_fit_violation_samples += int(
        _has_endpoint_fit_violation(
            aggregate.endpoint_samples,
            lifecycle.remaining_job_id,
        )
    )


def _active_set_mechanism_stats(
    lifecycle: _ActiveSetLifecycle,
    by_epoch: dict[float, _ActiveSetAggregate],
) -> _ActiveSetMechanismStats:
    stats = _ActiveSetMechanismStats()
    for observed_at, aggregate in sorted(by_epoch.items()):
        _record_lifecycle_phase_sample(observed_at, aggregate, lifecycle, stats)
        _record_post_drain_sample(observed_at, aggregate, lifecycle, stats)
    return stats


def _maximum(values: list[float]) -> float:
    return max(values) if values else 0.0


def _minimum(values: list[float]) -> float:
    return min(values) if values else 0.0


def _active_set_result(
    lifecycle: _ActiveSetLifecycle,
    stats: _ActiveSetMechanismStats,
    *,
    samples_present: bool,
    observed_epochs: tuple[float, ...],
    observation_interval_s: float,
) -> dict[str, float | int | bool | str]:
    pre_borrow_observed = bool(
        stats.pre_bulk_dominant_shares
        and _maximum(stats.pre_bulk_dominant_shares)
        > 1.0 / ACTIVE_SET_JOB_COUNT
    )
    overlap_reclaim_observed = bool(
        stats.pre_bulk_dominant_shares
        and stats.overlap_bulk_dominant_shares
        and stats.overlap_foreground_dominant_shares
        and stats.overlap_reclaim_competition_samples > 0
        and _minimum(stats.overlap_bulk_dominant_shares)
        < _maximum(stats.pre_bulk_dominant_shares)
    )
    post_drain_observed_samples = sum(
        lifecycle.first_drained_end < observed_at <= lifecycle.remaining_end
        for observed_at in observed_epochs
    )
    post_drain_applicable = bool(
        post_drain_observed_samples > 0
        or lifecycle.post_drain_duration_s >= observation_interval_s
    )
    post_work_conserving = bool(
        stats.post_remaining_fractions
        and stats.post_fit_violation_samples == 0
    )
    post_drain_status = (
        "ok:observed_work_conserving_drain"
        if post_work_conserving
        else "active_set_post_drain_not_observed"
        if post_drain_applicable
        else "not_applicable:drain_below_trace_resolution"
    )
    mechanism_passed = bool(
        samples_present
        and lifecycle.passed
        and pre_borrow_observed
        and overlap_reclaim_observed
        and (post_work_conserving or not post_drain_applicable)
    )
    mechanism_status = "not_applicable:no_credit_trace"
    if samples_present:
        mechanism_status = "active_set_mechanism_not_observed"
    if mechanism_passed:
        mechanism_status = (
            "ok:observed_borrow_reclaim_work_conserving_drain"
            if post_drain_applicable
            else "ok:observed_borrow_reclaim_post_drain_not_applicable"
        )
    return {
        "active_set_contract_status": lifecycle.status,
        "active_set_contract_passed": lifecycle.passed,
        "active_set_lifecycle_status": lifecycle.status,
        "active_set_lifecycle_passed": lifecycle.passed,
        "active_set_mechanism_applicable": samples_present,
        "active_set_mechanism_status": mechanism_status,
        "active_set_mechanism_passed": mechanism_passed,
        "active_set_bulk_job_index": lifecycle.bulk_index,
        "active_set_foreground_job_index": lifecycle.foreground_index,
        "active_set_overlap_s": lifecycle.overlap_s,
        "active_set_foreground_drained_first": lifecycle.foreground_drained_first,
        "active_set_first_drained_job_index": lifecycle.first_drained_index,
        "active_set_remaining_job_index": lifecycle.remaining_index,
        "active_set_bulk_only_pre_samples": stats.pre_samples,
        "active_set_overlap_samples": stats.overlap_samples,
        "active_set_overlap_reclaim_observed": overlap_reclaim_observed,
        "active_set_overlap_reclaim_competition_samples": (
            stats.overlap_reclaim_competition_samples
        ),
        "active_set_overlap_bulk_fraction_min": _minimum(
            stats.overlap_bulk_fractions
        ),
        "active_set_overlap_foreground_fraction_max": _maximum(
            stats.overlap_foreground_fractions
        ),
        "active_set_overlap_bulk_dominant_share_min": _minimum(
            stats.overlap_bulk_dominant_shares
        ),
        "active_set_overlap_foreground_dominant_share_max": _maximum(
            stats.overlap_foreground_dominant_shares
        ),
        "active_set_bulk_only_post_samples": stats.post_samples,
        "active_set_bulk_borrow_fraction_max": _maximum(stats.pre_bulk_fractions),
        "active_set_pre_bulk_dominant_share_max": _maximum(
            stats.pre_bulk_dominant_shares
        ),
        "active_set_bulk_reborrow_fraction_max": _maximum(
            stats.post_bulk_fractions
        ),
        "active_set_post_remaining_fraction_max": _maximum(
            stats.post_remaining_fractions
        ),
        "active_set_post_remaining_dominant_share_max": _maximum(
            stats.post_remaining_dominant_shares
        ),
        "active_set_post_remaining_waiting_work_max": _maximum(
            stats.post_remaining_waiting_work
        ),
        "active_set_post_fit_violation_samples": stats.post_fit_violation_samples,
        "active_set_post_drain_duration_s": lifecycle.post_drain_duration_s,
        "active_set_post_drain_observation_interval_s": observation_interval_s,
        "active_set_post_drain_observed_samples": post_drain_observed_samples,
        "active_set_post_drain_applicable": post_drain_applicable,
        "active_set_post_drain_status": post_drain_status,
        "active_set_post_work_conserving_passed": post_work_conserving,
    }


def active_set_phase_summary(
    job_evidence: list[dict[str, object]],
    samples: list[dict[str, object]],
    *,
    observation_interval_s: float = 0.25,
) -> dict[str, float | int | bool | str]:
    """Audit workload lifecycle separately from credit-policy mechanism.

    The audit derives phase boundaries from request arrival/completion evidence,
    never from configured labels. Lifecycle applies to every arm. Credit
    borrow/reclaim/work-conserving drain only applies to policies that emit a
    credit trace.
    Neither gate claims that the selected policy improved performance.
    """

    if not math.isfinite(observation_interval_s) or observation_interval_s <= 0:
        raise ValueError("observation_interval_s must be finite and positive")
    lifecycle = _active_set_lifecycle(job_evidence)
    if lifecycle is None:
        return _active_set_unavailable(observation_interval_s)

    by_epoch = _aggregate_active_set_samples(samples)
    stats = _active_set_mechanism_stats(lifecycle, by_epoch)
    return _active_set_result(
        lifecycle,
        stats,
        samples_present=bool(samples),
        observed_epochs=tuple(by_epoch),
        observation_interval_s=observation_interval_s,
    )
