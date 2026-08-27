"""Bounded ready-window lifecycle and event-join metrics."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass


RequestKey = tuple[str, str]
ReadyInterval = tuple[str, dict[str, object]]
WaitingInterval = tuple[str, int, int]
ReadyMetric = float | int | bool | str


@dataclass(frozen=True)
class _ReadyEvidenceSummary:
    metrics: dict[str, ReadyMetric]
    intervals: tuple[ReadyInterval, ...]
    complete: bool
    foreground_job_id: str


def _ready_summary_base() -> dict[str, ReadyMetric]:
    return {
        "bounded_ready_event_status": "unavailable:no_ready_lifecycle",
        "bounded_ready_lifecycle_complete": False,
        "bounded_ready_intervals": 0,
        "bounded_ready_jobs_with_intervals": 0,
        "bounded_ready_max_ready_requests_seen": 0,
        "bounded_ready_max_ready_work_seen": 0,
        "bounded_ready_max_ready_payload_bytes_seen": 0,
        "bounded_ready_requests_transition_mean_max": 0.0,
        "bounded_ready_requests_transition_p95_max": 0.0,
        "bounded_ready_work_transition_mean_max": 0.0,
        "bounded_ready_work_transition_p95_max": 0.0,
        "bounded_ready_payload_bytes_transition_mean_max": 0.0,
        "bounded_ready_payload_bytes_transition_p95_max": 0.0,
        "bounded_ready_foreground_intervals": 0,
        "bounded_ready_foreign_fallback_events": 0,
        "bounded_ready_foreground_max_ready_requests_seen": 0,
        "bounded_ready_foreground_max_ready_work_seen": 0,
    }


def _evidence_max(
    job_evidence: list[dict[str, object]],
    field: str,
    converter: Callable[[object], float | int],
) -> float | int:
    return max(
        (converter(item.get(field, 0)) for item in job_evidence),
        default=converter(0),
    )


def _summarize_ready_evidence(
    job_evidence: list[dict[str, object]],
    foreground_job_index: int,
) -> _ReadyEvidenceSummary:
    if not 0 <= foreground_job_index < len(job_evidence):
        raise ValueError("foreground_job_index is outside job evidence")
    foreground = job_evidence[foreground_job_index]
    intervals = tuple(
        (str(item["runtime_job_id"]), interval)
        for item in job_evidence
        for interval in item.get("ready_lifecycle_rows", ())
    )
    complete = bool(job_evidence) and all(
        item.get("ready_lifecycle_complete") is True
        and bool(item.get("ready_lifecycle_rows"))
        for item in job_evidence
    )
    metrics = _ready_summary_base()
    metrics.update(
        {
            "bounded_ready_lifecycle_complete": complete,
            "bounded_ready_intervals": len(intervals),
            "bounded_ready_jobs_with_intervals": sum(
                bool(item.get("ready_lifecycle_rows")) for item in job_evidence
            ),
            "bounded_ready_max_ready_requests_seen": _evidence_max(
                job_evidence,
                "max_ready_requests_seen",
                int,
            ),
            "bounded_ready_max_ready_work_seen": _evidence_max(
                job_evidence,
                "max_ready_work_seen",
                int,
            ),
            "bounded_ready_max_ready_payload_bytes_seen": _evidence_max(
                job_evidence,
                "max_ready_payload_bytes_seen",
                int,
            ),
            "bounded_ready_requests_transition_mean_max": _evidence_max(
                job_evidence,
                "ready_requests_transition_mean",
                float,
            ),
            "bounded_ready_requests_transition_p95_max": _evidence_max(
                job_evidence,
                "ready_requests_transition_p95",
                float,
            ),
            "bounded_ready_work_transition_mean_max": _evidence_max(
                job_evidence,
                "ready_work_transition_mean",
                float,
            ),
            "bounded_ready_work_transition_p95_max": _evidence_max(
                job_evidence,
                "ready_work_transition_p95",
                float,
            ),
            "bounded_ready_payload_bytes_transition_mean_max": _evidence_max(
                job_evidence,
                "ready_payload_bytes_transition_mean",
                float,
            ),
            "bounded_ready_payload_bytes_transition_p95_max": _evidence_max(
                job_evidence,
                "ready_payload_bytes_transition_p95",
                float,
            ),
            "bounded_ready_foreground_max_ready_requests_seen": int(
                foreground.get("max_ready_requests_seen", 0)
            ),
            "bounded_ready_foreground_max_ready_work_seen": int(
                foreground.get("max_ready_work_seen", 0)
            ),
        }
    )
    return _ReadyEvidenceSummary(
        metrics=metrics,
        intervals=intervals,
        complete=complete,
        foreground_job_id=str(foreground["runtime_job_id"]),
    )


def _invalid_ready_summary(
    metrics: dict[str, ReadyMetric],
    status: str,
) -> dict[str, ReadyMetric]:
    return {
        **metrics,
        "bounded_ready_event_status": status,
        "bounded_ready_lifecycle_complete": True,
    }


def _event_request_key(event: dict[str, object]) -> RequestKey:
    return (
        str(event.get("selected_job_id", "")),
        str(event.get("selected_request_id", "")),
    )


def _index_ready_events(
    events: list[dict[str, object]],
    intervals: tuple[ReadyInterval, ...],
) -> tuple[
    dict[RequestKey, dict[str, object]],
    dict[RequestKey, dict[str, object]],
    dict[RequestKey, dict[str, object]],
    str | None,
]:
    registration_events = [
        event for event in events if event.get("action") == "register"
    ]
    grant_events = [event for event in events if event.get("action") == "grant"]
    registration_ids = [_event_request_key(event) for event in registration_events]
    grant_ids = [_event_request_key(event) for event in grant_events]
    lifecycle_ids = [
        (job_id, str(interval["request_id"])) for job_id, interval in intervals
    ]
    if (
        any("" in item for item in registration_ids)
        or any("" in item for item in grant_ids)
        or any("" in item for item in lifecycle_ids)
        or len(set(registration_ids)) != len(registration_ids)
        or len(set(grant_ids)) != len(grant_ids)
        or len(set(lifecycle_ids)) != len(lifecycle_ids)
    ):
        return {}, {}, {}, "invalid:event_request_duplicate"

    registrations = dict(zip(registration_ids, registration_events))
    grants = dict(zip(grant_ids, grant_events))
    lifecycle_by_request = {
        (job_id, str(interval["request_id"])): interval
        for job_id, interval in intervals
    }
    lifecycle_request_ids = set(lifecycle_by_request)
    if set(registrations) != lifecycle_request_ids or set(grants) != lifecycle_request_ids:
        return {}, {}, {}, "invalid:event_request_join_incomplete"
    return registrations, grants, lifecycle_by_request, None


def _foreground_waiting_intervals(
    registrations: dict[RequestKey, dict[str, object]],
    grants: dict[RequestKey, dict[str, object]],
    lifecycle_by_request: dict[RequestKey, dict[str, object]],
    foreground_job_id: str,
) -> tuple[list[WaitingInterval], str | None]:
    waiting_intervals: list[WaitingInterval] = []
    for request_key in sorted(lifecycle_by_request):
        registered = registrations[request_key]
        granted = grants[request_key]
        lifecycle = lifecycle_by_request[request_key]
        if registered.get("event_epoch_s") in (None, "") or granted.get(
            "event_epoch_s"
        ) in (None, ""):
            return [], "invalid:event_epoch_missing"
        endpoint_id = str(registered.get("endpoint_id", ""))
        if endpoint_id != str(granted.get("endpoint_id", "")) or endpoint_id != str(
            lifecycle.get("endpoint_id", "")
        ):
            return [], "invalid:event_endpoint_mismatch"
        registered_epoch_s = float(registered["event_epoch_s"])
        granted_epoch_s = float(granted["event_epoch_s"])
        registered_seq = int(registered["event_seq"])
        granted_seq = int(granted["event_seq"])
        if (
            not math.isfinite(registered_epoch_s)
            or not math.isfinite(granted_epoch_s)
            or granted_epoch_s < registered_epoch_s
            or granted_seq <= registered_seq
        ):
            return [], "invalid:event_order"
        if request_key[0] == foreground_job_id:
            waiting_intervals.append((endpoint_id, registered_seq, granted_seq))
    if not waiting_intervals:
        return [], "invalid:no_actor_wait_interval"
    return waiting_intervals, None


def _foreign_fallback_count(
    events: list[dict[str, object]],
    waiting_intervals: list[WaitingInterval],
    foreground_job_id: str,
) -> int:
    violations = 0
    for event in events:
        if event.get("action") != "grant" or event.get("tier") != "saor_fallback":
            continue
        event_seq = int(event["event_seq"])
        endpoint_id = str(event["endpoint_id"])
        foreground_registered = any(
            interval_endpoint == endpoint_id
            and registered_seq < event_seq < granted_seq
            for interval_endpoint, registered_seq, granted_seq in waiting_intervals
        )
        violations += int(
            foreground_registered
            and str(event.get("selected_job_id", "")) != foreground_job_id
        )
    return violations


def bounded_ready_event_summary(
    events: list[dict[str, object]],
    job_evidence: list[dict[str, object]],
    *,
    foreground_job_index: int,
) -> dict[str, ReadyMetric]:
    """Prove the project ready-window lifecycle for every participating Job.

    This audit is selector-neutral.  It is valid only for project-internal
    controls that explicitly enable bounded concrete pre-registration; native
    system baselines never enter this evidence domain.
    """

    evidence = _summarize_ready_evidence(
        job_evidence,
        foreground_job_index,
    )
    if not evidence.complete or not evidence.intervals:
        return evidence.metrics

    registrations, grants, lifecycle_by_request, invalid_status = (
        _index_ready_events(events, evidence.intervals)
    )
    if invalid_status is not None:
        return _invalid_ready_summary(evidence.metrics, invalid_status)

    waiting_intervals, invalid_status = _foreground_waiting_intervals(
        registrations,
        grants,
        lifecycle_by_request,
        evidence.foreground_job_id,
    )
    if invalid_status is not None:
        return _invalid_ready_summary(evidence.metrics, invalid_status)

    lifecycle_ids = tuple(lifecycle_by_request)
    return {
        **evidence.metrics,
        "bounded_ready_event_status": "ok:actor_event_join",
        "bounded_ready_lifecycle_complete": True,
        "bounded_ready_foreground_intervals": sum(
            job_id == evidence.foreground_job_id for job_id, _ in lifecycle_ids
        ),
        "bounded_ready_foreign_fallback_events": _foreign_fallback_count(
            events,
            waiting_intervals,
            evidence.foreground_job_id,
        ),
    }
