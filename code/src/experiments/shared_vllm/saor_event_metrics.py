"""Lossless bounded-SAOR release-ledger metrics."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from src.experiments.shared_vllm.saor_projection_evidence import (
    audit_saor_debt_projections,
)
from src.observability.metrics import percentile


TAIL_PERCENTILE = 95


def _saor_summary_base() -> dict[str, float | int | bool | str]:
    return {
        "bounded_saor_event_status": "unavailable:no_event_ledger",
        "bounded_saor_event_sequence_complete": False,
        "bounded_saor_event_count": 0,
        "bounded_saor_slo_priority_grants": 0,
        "bounded_saor_debt_recovery_grants": 0,
        "bounded_saor_recovery_completions": 0,
        "bounded_saor_unmatched_recovery_grants": 0,
        "bounded_saor_recovery_completion_p95_s": 0.0,
        "bounded_saor_recovery_completion_max_s": 0.0,
        "bounded_saor_debt_repayment_episodes": 0,
        "bounded_saor_debt_repayment_completed": 0,
        "bounded_saor_debt_repayment_censored_no_demand": 0,
        "bounded_saor_debt_repayment_unresolved": 0,
        "bounded_saor_debt_repayment_p95_s": 0.0,
        "bounded_saor_debt_repayment_max_s": 0.0,
        "bounded_saor_fallback_grants": 0,
        "bounded_saor_hold_count": 0,
        "bounded_saor_hold_completed_count": 0,
        "bounded_saor_hold_duration_total_s": 0.0,
        "bounded_saor_hold_duration_p95_s": 0.0,
        "bounded_saor_hold_duration_max_s": 0.0,
        "bounded_saor_reclaim_debt_max": 0,
        "bounded_saor_constraint_conflicts": 0,
        "bounded_saor_recovery_inflight_max": 0,
        "bounded_saor_recovery_inflight_work_max": 0.0,
        "bounded_saor_recovery_inflight_work_at_repayment_max": 0.0,
        "bounded_saor_debt_repayment_overshoot_work_max": 0.0,
        "bounded_saor_projection_status": "unavailable:no_projection_evidence",
        "bounded_saor_projection_checked_events": 0,
        "bounded_saor_projection_expected_events": 0,
        "bounded_saor_projection_violation_events": 0,
        "bounded_saor_projected_overshoot_work_max": 0.0,
        "bounded_saor_projected_overshoot_bound_max": 0.0,
        "bounded_saor_projected_overshoot_bound_violation_events": 0,
        "bounded_saor_projection_estimation_overrun_events": 0,
        "bounded_saor_projection_estimation_overrun_work_max": 0,
        "bounded_saor_recovery_estimation_overrun_events": 0,
        "bounded_saor_recovery_estimation_overrun_work_max": 0,
        "bounded_saor_avoidable_idle_events": 0,
        "bounded_saor_foreign_grant_over_debt_critical_events": 0,
    }


def _pairs(value: object) -> dict[str, float]:
    decoded = json.loads(value) if isinstance(value, str) else value
    return {str(key): float(item) for key, item in (decoded or ())}


def _truth(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true"}
    return bool(value)


@dataclass
class _SaorLedgerStats:
    sequences: dict[str, list[int]] = field(default_factory=dict)
    hold_durations: list[float] = field(default_factory=list)
    max_recovery: int = 0
    max_recovery_work: float = 0.0
    recovery_grants: dict[tuple[str, str], float] = field(default_factory=dict)
    recovery_grants_missing_id: int = 0
    recovery_completion_durations: list[float] = field(default_factory=list)
    debt_episode_starts: dict[tuple[str, str], float] = field(default_factory=dict)
    debt_episode_durations: list[float] = field(default_factory=list)
    debt_episode_overshoots: list[float] = field(default_factory=list)
    repayment_inflight_work: list[float] = field(default_factory=list)
    debt_episode_count: int = 0
    debt_episode_censored: int = 0

    @property
    def sequence_complete(self) -> bool:
        return all(
            sequence == list(range(1, len(sequence) + 1))
            for sequence in self.sequences.values()
        )

    def record_unordered(self, event: dict[str, object]) -> None:
        endpoint_id = str(event["endpoint_id"])
        self.sequences.setdefault(endpoint_id, []).append(int(event["event_seq"]))
        raw_recovery = event.get("recovery_inflight_by_job", ())
        recovery = (
            json.loads(raw_recovery)
            if isinstance(raw_recovery, str)
            else raw_recovery
        )
        recovery_request_count = sum(
            len(request_ids) if isinstance(request_ids, (list, tuple)) else 1
            for _job_id, request_ids in recovery
        )
        self.max_recovery = max(self.max_recovery, recovery_request_count)
        recovery_work = _pairs(event.get("recovery_inflight_work_by_job", ()))
        self.max_recovery_work = max(
            self.max_recovery_work,
            sum(recovery_work.values()),
        )
        if event.get("action") == "hold_end":
            self.hold_durations.append(float(event.get("hold_duration_s", 0.0)))

    def record_ordered(self, endpoint_id: str, event: dict[str, object]) -> None:
        event_time = float(
            event.get("event_epoch_s", event.get("event_time_s", 0.0))
        )
        request_id = str(event.get("selected_request_id", ""))
        self._record_recovery_request(endpoint_id, event, event_time, request_id)
        self._record_debt_episodes(endpoint_id, event, event_time)

    def _record_recovery_request(
        self,
        endpoint_id: str,
        event: dict[str, object],
        event_time: float,
        request_id: str,
    ) -> None:
        if event.get("action") == "grant" and event.get("tier") == "debt_recovery":
            if request_id:
                self.recovery_grants[(endpoint_id, request_id)] = event_time
            else:
                self.recovery_grants_missing_id += 1
        if event.get("action") != "completion" or not request_id:
            return
        started = self.recovery_grants.pop((endpoint_id, request_id), None)
        if started is not None:
            self.recovery_completion_durations.append(max(0.0, event_time - started))

    def _record_debt_episodes(
        self,
        endpoint_id: str,
        event: dict[str, object],
        event_time: float,
    ) -> None:
        debts = _pairs(event.get("debt_by_job", ()))
        caps = _pairs(event.get("debt_cap_by_job", ()))
        recovery_work = _pairs(event.get("recovery_inflight_work_by_job", ()))
        lifecycle_complete_job = (
            str(event.get("selected_job_id", ""))
            if event.get("action") == "finish_job"
            else ""
        )
        for job_id, cap in caps.items():
            key = (endpoint_id, job_id)
            critical = debts.get(job_id, 0.0) >= cap
            if critical and lifecycle_complete_job == job_id:
                if key in self.debt_episode_starts:
                    self.debt_episode_starts.pop(key)
                    self.debt_episode_censored += 1
                continue
            if critical and key not in self.debt_episode_starts:
                self.debt_episode_starts[key] = event_time
                self.debt_episode_count += 1
                continue
            if critical or key not in self.debt_episode_starts:
                continue
            started = self.debt_episode_starts.pop(key)
            self.debt_episode_durations.append(max(0.0, event_time - started))
            self.debt_episode_overshoots.append(
                max(0.0, cap - debts.get(job_id, 0.0))
            )
            self.repayment_inflight_work.append(recovery_work.get(job_id, 0.0))


def _collect_ledger_stats(events: list[dict[str, object]]) -> _SaorLedgerStats:
    stats = _SaorLedgerStats()
    for event in events:
        stats.record_unordered(event)
    for endpoint_id in stats.sequences:
        endpoint_events = sorted(
            (event for event in events if str(event["endpoint_id"]) == endpoint_id),
            key=lambda event: int(event["event_seq"]),
        )
        for event in endpoint_events:
            stats.record_ordered(endpoint_id, event)
    return stats


def _count_mechanism_events(
    mechanism_events: list[dict[str, object]],
    stats: _SaorLedgerStats,
) -> dict[str, float | int]:
    return {
        "bounded_saor_slo_priority_grants": sum(
            event.get("action") == "grant" and event.get("tier") == "slo_priority"
            for event in mechanism_events
        ),
        "bounded_saor_debt_recovery_grants": sum(
            event.get("action") == "grant" and event.get("tier") == "debt_recovery"
            for event in mechanism_events
        ),
        "bounded_saor_fallback_grants": sum(
            event.get("action") == "grant" and event.get("tier") == "saor_fallback"
            for event in mechanism_events
        ),
        "bounded_saor_hold_count": sum(
            event.get("action") == "hold_start" for event in mechanism_events
        ),
        "bounded_saor_hold_completed_count": sum(
            event.get("action") == "hold_end" for event in mechanism_events
        ),
        "bounded_saor_hold_duration_total_s": sum(stats.hold_durations),
        "bounded_saor_hold_duration_p95_s": (
            percentile(stats.hold_durations, TAIL_PERCENTILE)
            if stats.hold_durations
            else 0.0
        ),
        "bounded_saor_hold_duration_max_s": max(stats.hold_durations, default=0.0),
        "bounded_saor_reclaim_debt_max": max(
            (int(event.get("reclaim_debt", 0)) for event in mechanism_events),
            default=0,
        ),
        "bounded_saor_constraint_conflicts": sum(
            _truth(event.get("constraint_conflict")) for event in mechanism_events
        ),
        "bounded_saor_avoidable_idle_events": sum(
            _truth(event.get("avoidable_idle")) for event in mechanism_events
        ),
        "bounded_saor_foreign_grant_over_debt_critical_events": sum(
            _truth(event.get("foreign_grant_over_debt_critical"))
            for event in mechanism_events
        ),
    }


def _recovery_and_debt_metrics(
    stats: _SaorLedgerStats,
) -> dict[str, float | int]:
    return {
        "bounded_saor_recovery_completions": len(
            stats.recovery_completion_durations
        ),
        "bounded_saor_unmatched_recovery_grants": (
            len(stats.recovery_grants) + stats.recovery_grants_missing_id
        ),
        "bounded_saor_recovery_completion_p95_s": (
            percentile(stats.recovery_completion_durations, TAIL_PERCENTILE)
            if stats.recovery_completion_durations
            else 0.0
        ),
        "bounded_saor_recovery_completion_max_s": max(
            stats.recovery_completion_durations,
            default=0.0,
        ),
        "bounded_saor_debt_repayment_episodes": stats.debt_episode_count,
        "bounded_saor_debt_repayment_completed": len(stats.debt_episode_durations),
        "bounded_saor_debt_repayment_censored_no_demand": (
            stats.debt_episode_censored
        ),
        "bounded_saor_debt_repayment_unresolved": len(stats.debt_episode_starts),
        "bounded_saor_debt_repayment_p95_s": (
            percentile(stats.debt_episode_durations, TAIL_PERCENTILE)
            if stats.debt_episode_durations
            else 0.0
        ),
        "bounded_saor_debt_repayment_max_s": max(
            stats.debt_episode_durations,
            default=0.0,
        ),
        "bounded_saor_recovery_inflight_max": stats.max_recovery,
        "bounded_saor_recovery_inflight_work_max": stats.max_recovery_work,
        "bounded_saor_recovery_inflight_work_at_repayment_max": max(
            stats.repayment_inflight_work,
            default=0.0,
        ),
        "bounded_saor_debt_repayment_overshoot_work_max": max(
            stats.debt_episode_overshoots,
            default=0.0,
        ),
    }


def _projection_metrics(
    projection_audit,
    events: list[dict[str, object]],
) -> dict[str, float | int | str]:
    return {
        "bounded_saor_projection_status": (
            "ok:offline_recomputed"
            if projection_audit.schema_detected
            and projection_audit.violation_events == 0
            else "invalid:offline_projection_mismatch"
            if projection_audit.schema_detected
            else "unavailable:legacy_event_schema"
        ),
        "bounded_saor_projection_checked_events": projection_audit.checked_events,
        "bounded_saor_projection_expected_events": projection_audit.expected_events,
        "bounded_saor_projection_violation_events": projection_audit.violation_events,
        "bounded_saor_projected_overshoot_work_max": max(
            projection_audit.projected_overshoots,
            default=0.0,
        ),
        "bounded_saor_projected_overshoot_bound_max": max(
            projection_audit.projected_overshoot_bounds,
            default=0.0,
        ),
        "bounded_saor_projected_overshoot_bound_violation_events": (
            projection_audit.overshoot_bound_violation_events
        ),
        "bounded_saor_recovery_estimation_overrun_events": sum(
            int(event.get("recovery_estimation_overrun_work", 0)) > 0
            for event in events
        ),
        "bounded_saor_projection_estimation_overrun_events": sum(
            int(event.get("projection_estimation_overrun_work", 0)) > 0
            for event in events
        ),
        "bounded_saor_projection_estimation_overrun_work_max": max(
            (int(event.get("projection_estimation_overrun_work", 0)) for event in events),
            default=0,
        ),
        "bounded_saor_recovery_estimation_overrun_work_max": max(
            (int(event.get("recovery_estimation_overrun_work", 0)) for event in events),
            default=0,
        ),
    }


def bounded_saor_event_summary(
    events: list[dict[str, object]],
) -> dict[str, float | int | bool | str]:
    """Summarize bounded-SAOR mechanics only from the lossless ledger."""

    base = _saor_summary_base()
    if not events:
        return base
    mechanism_events = [
        event
        for event in events
        if event.get("action") in {"hold_start", "hold_end"}
        or event.get("tier") in {"slo_priority", "debt_recovery", "saor_fallback"}
    ]
    if not mechanism_events:
        return base

    stats = _collect_ledger_stats(events)
    projection_audit = audit_saor_debt_projections(mechanism_events)
    base.update(
        {
            "bounded_saor_event_status": (
                "ok:lossless_ledger"
                if stats.sequence_complete
                else "invalid:event_sequence_gap_or_duplicate"
            ),
            "bounded_saor_event_sequence_complete": stats.sequence_complete,
            "bounded_saor_event_count": len(events),
            **_count_mechanism_events(mechanism_events, stats),
            **_recovery_and_debt_metrics(stats),
            **_projection_metrics(projection_audit, events),
        }
    )
    return base
