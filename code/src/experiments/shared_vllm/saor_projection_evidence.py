"""Independently audit bounded-SAOR debt projections from raw event fields."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class SaorProjectionAudit:
    """Offline projection evidence, independent of the runtime selector."""

    schema_detected: bool
    checked_events: int
    expected_events: int
    violation_events: int
    projected_overshoots: tuple[float, ...]
    projected_overshoot_bounds: tuple[float, ...]
    overshoot_bound_violation_events: int


def _pairs(value: object) -> dict[str, float]:
    decoded = json.loads(value) if isinstance(value, str) else value
    return {str(key): float(item) for key, item in (decoded or ())}


def _values(value: object) -> tuple[str, ...]:
    decoded = json.loads(value) if isinstance(value, str) else value
    return tuple(str(item) for item in (decoded or ()))


def _truth(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true"}
    return bool(value)


def _event_invalid(
    event: dict[str, object],
) -> tuple[bool, float | None, float | None, bool]:
    """Recompute one decision; return invalid, overshoot, bound, violation."""

    weights = _pairs(event.get("weight_by_job", ()))
    own = _pairs(event.get("projection_own_inflight_work_by_job", ()))
    heads = _pairs(event.get("projection_candidate_work_by_job", ()))
    debts = _pairs(event.get("debt_by_job", ()))
    caps = _pairs(event.get("debt_cap_by_job", ()))
    recorded = {
        "foreign": _pairs(
            event.get("projection_foreign_residual_work_by_job", ())
        ),
        "share": _pairs(event.get("projection_target_share_by_job", ())),
        "before": _pairs(event.get("projected_debt_before_by_job", ())),
        "after": _pairs(event.get("projected_debt_after_by_job", ())),
        "bound": _pairs(event.get("projected_overshoot_bound_by_job", ())),
    }
    active_set = _values(event.get("active_set_jobs", ()))
    ready = set(_values(event.get("ready_jobs", ())))
    fitting = set(_values(event.get("fitting_jobs", ())))
    jobs = set(active_set)
    invalid = bool(
        not jobs
        or len(active_set) != len(jobs)
        or any(
            jobs != set(values)
            for values in (weights, own, heads, debts, *recorded.values())
        )
        or any(weight <= 0 for weight in weights.values())
        or any(work < 0 for work in own.values())
        or any(work < 0 for work in heads.values())
        or any(debt < 0 for debt in debts.values())
        or any(cap <= 0 for cap in caps.values())
        or not ready <= jobs
        or not fitting <= ready
        or any((heads[job_id] > 0) != (job_id in ready) for job_id in jobs)
    )
    if invalid:
        return True, None, None, False
    total_weight = sum(weights.values())
    if total_weight <= 0 or not math.isclose(
        float(event.get("active_set_weight_sum", -1.0)),
        total_weight,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        return True, None, None, False
    total_own = sum(own.values())
    before_by_job: dict[str, float] = {}
    after_by_job: dict[str, float] = {}
    for job_id in jobs:
        phi = weights[job_id] / total_weight
        foreign = total_own - own[job_id]
        before = debts[job_id] + phi * foreign - (1.0 - phi) * own[job_id]
        after = before - (1.0 - phi) * heads[job_id]
        bound = (1.0 - phi) * heads[job_id]
        before_by_job[job_id] = before
        after_by_job[job_id] = after
        for observed, expected in (
            (recorded["foreign"][job_id], foreign),
            (recorded["share"][job_id], phi),
            (recorded["before"][job_id], before),
            (recorded["after"][job_id], after),
            (recorded["bound"][job_id], bound),
        ):
            if not math.isclose(
                observed,
                expected,
                rel_tol=1e-9,
                abs_tol=1e-7,
            ):
                invalid = True
    critical = {
        job_id
        for job_id, cap in caps.items()
        if job_id in ready
        and weights[job_id] < total_weight
        and before_by_job.get(job_id, -math.inf) >= cap
    }
    action = str(event.get("action", ""))
    tier = str(event.get("tier", ""))
    selected = str(event.get("selected_job_id", ""))
    target = str(event.get("target_job_id", ""))
    try:
        observed_active_work = float(event.get("active_work", -1))
    except (TypeError, ValueError):
        observed_active_work = -1
    expected_active_work = total_own
    if action == "grant":
        if selected not in ready or selected not in fitting:
            invalid = True
        else:
            expected_active_work += heads[selected]
    if not math.isclose(
        observed_active_work,
        expected_active_work,
        rel_tol=0.0,
        abs_tol=1e-7,
    ):
        invalid = True
    overshoot: float | None = None
    overshoot_bound: float | None = None
    bound_violation = False
    if action == "grant":
        if tier == "debt_recovery":
            if selected not in critical or selected not in fitting:
                invalid = True
            elif after_by_job[selected] < caps[selected]:
                overshoot = caps[selected] - after_by_job[selected]
                overshoot_bound = recorded["bound"][selected]
                bound_violation = overshoot > overshoot_bound + 1e-7
                invalid = invalid or bound_violation
        elif critical and selected not in critical:
            invalid = True
    elif action == "hold_start" and (
        target not in critical or target in fitting
    ):
        invalid = True
    recomputed_foreign_grant = bool(
        action == "grant" and critical and selected not in critical
    )
    if _truth(
        event.get("foreign_grant_over_debt_critical", False)
    ) != recomputed_foreign_grant:
        invalid = True
    return invalid, overshoot, overshoot_bound, bound_violation


def audit_saor_debt_projections(
    events: list[dict[str, object]],
) -> SaorProjectionAudit:
    """Audit every schema-5 grant/hold decision from raw ledger columns."""

    expected = sum(
        event.get("action") in {"grant", "hold_start"} for event in events
    )
    schema_detected = False
    checked = 0
    violations = 0
    overshoots: list[float] = []
    bounds: list[float] = []
    bound_violations = 0
    for event in events:
        try:
            schema_version = int(event.get("schema_version", 0))
        except (TypeError, ValueError):
            schema_version = 0
        schema_detected = schema_detected or schema_version >= 5
        if schema_version < 5 or event.get("action") not in {
            "grant",
            "hold_start",
        }:
            continue
        checked += 1
        invalid, overshoot, bound, bound_violation = _event_invalid(event)
        violations += invalid
        bound_violations += bound_violation
        if overshoot is not None and bound is not None:
            overshoots.append(overshoot)
            bounds.append(bound)
    if schema_detected and checked != expected:
        violations += abs(expected - checked)
    return SaorProjectionAudit(
        schema_detected=schema_detected,
        checked_events=checked,
        expected_events=expected,
        violation_events=violations,
        projected_overshoots=tuple(overshoots),
        projected_overshoot_bounds=tuple(bounds),
        overshoot_bound_violation_events=bound_violations,
    )
