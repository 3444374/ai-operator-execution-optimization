"""Queue selection and completion accounting on the existing shared credit store.

These functions select and update the existing coordinator's policy state.
The coordinator alone retains leases, queues, activation and common events.
Pure SAOR ranking and debt projections remain in saor.py.
"""
from __future__ import annotations

from dataclasses import dataclass
import math

from .saor import (
    SaorBoundedHeadState,
    SaorReleaseState,
    bounded_saor_recovery_required,
    select_bounded_saor_release,
    select_saor_release_job,
)


@dataclass(frozen=True)
class _GuardHold:
    target_job_id: str
    target_request_id: str
    head_work: int
    reclaim_debt: int
    started_at_s: float
    constraint_conflict: bool


def _grant_waiters(credit, endpoint_id: str) -> None:
    if credit._policy == "fifo":
        _grant_fifo_waiters(credit, endpoint_id)
        return
    if credit._policy == "vtc":
        _grant_vtc_waiters(credit, endpoint_id)
        return
    if credit._policy == "saor":
        _grant_saor_waiters(credit, endpoint_id)
        return
    if credit._policy in {"saor_bounded_priority", "saor_bounded_ready"}:
        _grant_bounded_saor_waiters(credit, endpoint_id)
        return
    if credit._policy == "strict_priority":
        _grant_strict_priority_waiters(credit, endpoint_id)
        return
    request_limit, work_limit = credit._capacities[endpoint_id]
    job_order = credit._job_order[endpoint_id]
    if not job_order:
        return
    visits_without_grant = 0
    while (
        credit._active_requests[endpoint_id] < request_limit
    ):
        if visits_without_grant >= len(job_order):
            nonempty_jobs = [
                job_id
                for job_id in job_order
                if credit._waiting[endpoint_id][job_id]
            ]
            if not nonempty_jobs:
                return
            if all(
                credit._waiting[endpoint_id][job_id][0].estimated_work
                <= credit._deficits.get((endpoint_id, job_id), 0)
                for job_id in nonempty_jobs
            ):
                return
            visits_without_grant = 0
        cursor = credit._cursor[endpoint_id] % len(job_order)
        job_id = job_order[cursor]
        credit._cursor[endpoint_id] = (cursor + 1) % len(job_order)
        queue = credit._waiting[endpoint_id][job_id]
        if not queue:
            visits_without_grant += 1
            continue
        deficit_key = (endpoint_id, job_id)
        credit._deficits[deficit_key] = (
            credit._deficits.get(deficit_key, 0)
            + credit._quantum * credit._weights[job_id]
        )
        lease = queue[0]
        if lease.estimated_work > credit._deficits[deficit_key]:
            visits_without_grant += 1
            continue
        current_work = credit._active_work[endpoint_id]
        fits_work = current_work + lease.estimated_work <= work_limit
        if not fits_work:
            visits_without_grant += 1
            continue
        queue.popleft()
        request_key = (lease.job_id, lease.request_id)
        credit._queued_request_keys.remove(request_key)
        credit._deficits[deficit_key] -= lease.estimated_work
        credit._activate(endpoint_id, lease)
        visits_without_grant = 0


def _grant_fifo_waiters(credit, endpoint_id: str) -> None:
    """Grant one global ready-enqueue FIFO, preserving head-of-line order."""
    request_limit, work_limit = credit._capacities[endpoint_id]
    fifo = credit._fifo_order[endpoint_id]
    while fifo and credit._active_requests[endpoint_id] < request_limit:
        job_id, request_id = fifo[0]
        queue = credit._waiting[endpoint_id][job_id]
        if not queue:
            fifo.popleft()
            continue
        lease = queue[0]
        if lease.request_id != request_id:
            raise RuntimeError("FIFO and per-job credit queues diverged")
        if credit._active_work[endpoint_id] + lease.estimated_work > work_limit:
            return
        fifo.popleft()
        queue.popleft()
        request_key = (job_id, request_id)
        credit._queued_request_keys.remove(request_key)
        credit._activate(endpoint_id, lease)


def _grant_vtc_waiters(credit, endpoint_id: str) -> None:
    """Grant the least normalized attained-service job that fits.

    This is an upstream VTC-style baseline. Unlike in-engine VTC it cannot
    charge output tokens incrementally during decoding; release-time actual
    work correction is therefore part of the recorded baseline identity.
    """
    request_limit, work_limit = credit._capacities[endpoint_id]
    while credit._active_requests[endpoint_id] < request_limit:
        candidates = [
            job_id
            for job_id in credit._job_order[endpoint_id]
            if credit._waiting[endpoint_id][job_id]
            and credit._active_work[endpoint_id]
            + credit._waiting[endpoint_id][job_id][0].estimated_work
            <= work_limit
        ]
        if not candidates:
            return
        order = {
            job_id: index
            for index, job_id in enumerate(credit._job_order[endpoint_id])
        }
        job_id = min(
            candidates,
            key=lambda item: (
                credit._attained_service[endpoint_id].get(item, 0)
                / credit._weights[item],
                order[item],
            ),
        )
        lease = credit._waiting[endpoint_id][job_id].popleft()
        credit._queued_request_keys.remove((lease.job_id, lease.request_id))
        credit._attained_service[endpoint_id][job_id] = (
            credit._attained_service[endpoint_id].get(job_id, 0)
            + lease.estimated_work
        )
        credit._activate(endpoint_id, lease)


def _grant_strict_priority_waiters(credit, endpoint_id: str) -> None:
    """Grant the highest-priority fitting head without revoking leases.

    This policy is a diagnostic upper bound for release-only foreground
    protection.  It is non-preemptive: priority changes only which queued
    Job receives newly freed credit.  While a higher-priority Job remains
    unfinished, newly freed capacity is held for that Job instead of being
    refilled by a lower-priority Job.
    """

    request_limit, work_limit = credit._capacities[endpoint_id]
    order = {
        job_id: index
        for index, job_id in enumerate(credit._job_order[endpoint_id])
    }
    while credit._active_requests[endpoint_id] < request_limit:
        unfinished_priorities = [
            priority
            for job_id, priority in credit._priorities.items()
            if job_id not in credit._finished_jobs
        ]
        active_priority = max(unfinished_priorities, default=0)
        candidates = [
            job_id
            for job_id in credit._job_order[endpoint_id]
            if credit._waiting[endpoint_id][job_id]
            and credit._priorities[job_id] == active_priority
            and credit._active_work[endpoint_id]
            + credit._waiting[endpoint_id][job_id][0].estimated_work
            <= work_limit
        ]
        if not candidates:
            return
        job_id = min(
            candidates,
            key=lambda item: (-credit._priorities[item], order[item]),
        )
        lease = credit._waiting[endpoint_id][job_id].popleft()
        credit._queued_request_keys.remove((lease.job_id, lease.request_id))
        credit._activate(endpoint_id, lease)


def _vtc_job_is_inactive(credit, endpoint_id: str, job_id: str) -> bool:
    return (
        not credit._waiting[endpoint_id].get(job_id)
        and not any(
            lease.endpoint_id == endpoint_id and lease.job_id == job_id
            for lease in credit._active.values()
        )
    )


def _active_vtc_normalized_counters(
    credit,
    endpoint_id: str,
    *,
    exclude_job_id: str = "",
) -> list[float]:
    active_jobs = {
        lease.job_id
        for lease in credit._active.values()
        if lease.endpoint_id == endpoint_id
        and lease.job_id != exclude_job_id
    }
    active_jobs.update(
        job_id
        for job_id, queue in credit._waiting[endpoint_id].items()
        if queue and job_id != exclude_job_id
    )
    return [
        credit._attained_service[endpoint_id][job_id]
        / credit._weights[job_id]
        for job_id in active_jobs
        if job_id in credit._attained_service[endpoint_id]
    ]


def _grant_saor_waiters(credit, endpoint_id: str) -> None:
    """Grant fitting Job heads using fixed-envelope active-set release."""

    request_limit, work_limit = credit._capacities[endpoint_id]
    while credit._active_requests[endpoint_id] < request_limit:
        ordered_jobs = [
            job_id
            for job_id in credit._job_order[endpoint_id]
            if credit._waiting[endpoint_id][job_id]
        ]
        if not ordered_jobs:
            return
        order_by_job = {
            job_id: order for order, job_id in enumerate(ordered_jobs)
        }
        active_requests_by_job, active_work_by_job = credit._active_usage_by_job(endpoint_id)
        active_set = set(ordered_jobs) | set(active_requests_by_job)
        states = []
        for job_id in sorted(active_set):
            queue = credit._waiting[endpoint_id].get(job_id, ())
            waiting_work = sum(lease.estimated_work for lease in queue)
            head_fits = bool(queue) and (
                credit._active_work[endpoint_id]
                + queue[0].estimated_work
                <= work_limit
            )
            target = credit._saor_slo_target_s.get(job_id, 0.0)
            states.append(
                SaorReleaseState(
                    job_id=job_id,
                    weight=float(credit._weights[job_id]),
                    active_requests=active_requests_by_job.get(job_id, 0),
                    active_work=active_work_by_job.get(job_id, 0),
                    waiting_work=waiting_work,
                    fairness_debt=credit._fairness_debt[endpoint_id].get(
                        job_id,
                        0.0,
                    ),
                    oldest_waiting_age_s=(
                        max(0.0, credit._clock() - queue[0].enqueued_at_s)
                        if queue
                        else 0.0
                    ),
                    slo_target_s=target or None,
                    arrival_order=order_by_job.get(
                        job_id,
                        len(order_by_job),
                    ),
                    eligible=head_fits,
                )
            )
        if not any(state.eligible for state in states):
            return
        selected = select_saor_release_job(
            tuple(states),
            request_limit=request_limit,
            work_limit=work_limit,
            config=credit._saor_release_config,
        )
        lease = credit._waiting[endpoint_id][selected.job_id].popleft()
        credit._queued_request_keys.remove((lease.job_id, lease.request_id))
        credit._activate(endpoint_id, lease)


def _grant_bounded_saor_waiters(credit, endpoint_id: str) -> None:
    """Apply debt recovery, bounded SLO priority, then SAOR fallback."""

    request_limit, work_limit = credit._capacities[endpoint_id]
    while credit._active_requests[endpoint_id] < request_limit:
        if _resume_guard_hold(credit, endpoint_id):
            continue
        if credit._guard_holds[endpoint_id] is not None:
            return
        states = _bounded_saor_states(credit, endpoint_id)
        if not states:
            return
        has_critical = any(
            bounded_saor_recovery_required(state, states)
            for state in states
        )
        if not any(state.release.eligible for state in states) and not has_critical:
            return
        selected = select_bounded_saor_release(
            states,
            request_limit=request_limit,
            work_limit=work_limit,
            active_work=credit._active_work[endpoint_id],
            config=credit._saor_release_config,
        )
        if selected.action == "hold":
            job_id = selected.job_id or ""
            lease = credit._waiting[endpoint_id][job_id][0]
            hold = _GuardHold(
                job_id,
                lease.request_id,
                lease.estimated_work,
                selected.reclaim_debt,
                credit._clock(),
                selected.constraint_conflict,
            )
            credit._guard_holds[endpoint_id] = hold
            credit._record_release_event(
                endpoint_id,
                action="hold_start",
                tier=selected.tier,
                target=hold,
                states=states,
            )
            return
        job_id = selected.job_id or ""
        lease = credit._waiting[endpoint_id][job_id].popleft()
        credit._queued_request_keys.remove((lease.job_id, lease.request_id))
        if selected.tier == "debt_recovery":
            credit._recovery_inflight[endpoint_id].setdefault(
                job_id,
                {},
            )[lease.request_id] = lease.estimated_work
        credit._activate(endpoint_id, lease)
        credit._record_release_event(
            endpoint_id,
            action="grant",
            tier=selected.tier,
            lease=lease,
            constraint_conflict=selected.constraint_conflict,
            states=states,
        )


def _resume_guard_hold(credit, endpoint_id: str) -> bool:
    hold = credit._guard_holds[endpoint_id]
    if hold is None:
        return False
    queue = credit._waiting[endpoint_id].get(hold.target_job_id)
    if not queue or queue[0].request_id != hold.target_request_id:
        _close_guard_hold(credit, endpoint_id, hold)
        return False
    _, work_limit = credit._capacities[endpoint_id]
    lease = queue[0]
    reclaim = max(
        0,
        lease.estimated_work - (work_limit - credit._active_work[endpoint_id]),
    )
    if reclaim > 0:
        credit._guard_holds[endpoint_id] = _GuardHold(
            hold.target_job_id,
            hold.target_request_id,
            hold.head_work,
            reclaim,
            hold.started_at_s,
            hold.constraint_conflict,
        )
        return False
    states = _bounded_saor_states(credit, endpoint_id)
    held_state = next(
        (
            state
            for state in states
            if state.release.job_id == hold.target_job_id
        ),
        None,
    )
    if held_state is None or not bounded_saor_recovery_required(
        held_state,
        states,
    ):
        _close_guard_hold(credit, endpoint_id, hold, states=states)
        return False
    _close_guard_hold(credit, endpoint_id, hold, states=states)
    queue.popleft()
    credit._queued_request_keys.remove((lease.job_id, lease.request_id))
    credit._recovery_inflight[endpoint_id].setdefault(
        lease.job_id,
        {},
    )[lease.request_id] = lease.estimated_work
    credit._activate(endpoint_id, lease)
    credit._record_release_event(
        endpoint_id,
        action="grant",
        tier="debt_recovery",
        lease=lease,
        constraint_conflict=hold.constraint_conflict,
        states=states,
    )
    return True


def _close_guard_hold(
    credit,
    endpoint_id: str,
    hold: _GuardHold,
    *,
    states: tuple[SaorBoundedHeadState, ...] | None = None,
) -> None:
    credit._guard_holds[endpoint_id] = None
    credit._record_release_event(
        endpoint_id,
        action="hold_end",
        tier="guard_reclaim_hold",
        target=hold,
        hold_duration_s=max(0.0, credit._clock() - hold.started_at_s),
        states=states or _bounded_saor_states(credit, endpoint_id),
    )


def _bounded_saor_states(
    credit,
    endpoint_id: str,
    *,
    include_job_ids: tuple[str, ...] = (),
) -> tuple[SaorBoundedHeadState, ...]:
    request_limit, work_limit = credit._capacities[endpoint_id]
    active_requests, active_work = credit._active_usage_by_job(endpoint_id)
    jobs = set(active_requests)
    jobs.update(
        job_id
        for job_id, queue in credit._waiting[endpoint_id].items()
        if queue
    )
    jobs.update(include_job_ids)
    order = {
        job_id: index for index, job_id in enumerate(credit._job_order[endpoint_id])
    }
    now = credit._clock()
    states = []
    for job_id in sorted(jobs):
        queue = credit._waiting[endpoint_id].get(job_id, ())
        head = queue[0] if queue else None
        head_work = head.estimated_work if head is not None else 0
        fits = bool(head) and (
            credit._active_requests[endpoint_id] + 1 <= request_limit
            and credit._active_work[endpoint_id] + head_work <= work_limit
        )
        states.append(
            SaorBoundedHeadState(
                release=SaorReleaseState(
                    job_id=job_id,
                    weight=float(credit._weights[job_id]),
                    active_requests=active_requests.get(job_id, 0),
                    active_work=active_work.get(job_id, 0),
                    waiting_work=sum(item.estimated_work for item in queue),
                    fairness_debt=credit._fairness_debt[endpoint_id].get(
                        job_id, 0.0
                    ),
                    oldest_waiting_age_s=(
                        max(0.0, now - head.enqueued_at_s) if head else 0.0
                    ),
                    slo_target_s=None,
                    arrival_order=order.get(job_id, len(order)),
                    eligible=fits,
                ),
                priority=credit._priorities[job_id],
                remaining_slo_budget_s=(
                    head.slo_deadline_s - now
                    if head is not None and head.slo_deadline_s is not None
                    else None
                ),
                priority_window_s=credit._priority_windows_s.get(job_id) or None,
                debt_cap=credit._fairness_debt_caps.get(job_id) or None,
                head_work=head_work,
                ready=head is not None,
                recovery_inflight=(
                    job_id in credit._recovery_inflight[endpoint_id]
                ),
                recovery_inflight_work=sum(
                    credit._recovery_inflight[endpoint_id]
                    .get(job_id, {})
                    .values()
                ),
            )
        )
    return tuple(states)


def _update_saor_fairness_debt(
    credit,
    endpoint_id: str,
    *,
    completed_job_id: str,
    completed_work: int,
) -> None:
    active_jobs = {
        lease.job_id
        for lease in credit._active.values()
        if lease.endpoint_id == endpoint_id
    }
    active_jobs.update(
        job_id
        for job_id, queue in credit._waiting[endpoint_id].items()
        if queue
    )
    active_jobs.add(completed_job_id)
    total_weight = sum(credit._weights[job_id] for job_id in active_jobs)
    debts = credit._fairness_debt[endpoint_id]
    for job_id in active_jobs:
        target = completed_work * credit._weights[job_id] / total_weight
        received = completed_work if job_id == completed_job_id else 0.0
        debts[job_id] = max(0.0, debts.get(job_id, 0.0) + target - received)


def _lift_vtc_floor(credit, endpoint_id: str, job_id: str, weight: int) -> None:
    if _vtc_job_is_inactive(credit, endpoint_id, job_id):
        active_counters = _active_vtc_normalized_counters(
            credit,
            endpoint_id,
            exclude_job_id=job_id,
        )
        floor = min(active_counters) if active_counters else 0
        counters = credit._attained_service[endpoint_id]
        weighted_floor = math.ceil(floor * weight)
        counters[job_id] = max(
            counters.get(job_id, 0),
            weighted_floor,
        )


def _record_completion(credit, lease, actual_work: int | None) -> None:
    endpoint_id = lease.endpoint_id
    job_id, request_id = lease.job_id, lease.request_id
    if actual_work is not None and credit._policy == "vtc":
        counters = credit._attained_service[endpoint_id]
        counters[job_id] = counters.get(job_id, 0) + (
            actual_work - lease.estimated_work
        )
    recovery_estimated_work = 0
    if credit._policy in {
        "saor",
        "saor_bounded_priority",
        "saor_bounded_ready",
    }:
        recovery = credit._recovery_inflight[endpoint_id]
        recovery_requests = recovery.get(job_id)
        if recovery_requests is not None:
            recovery_estimated_work = recovery_requests.pop(request_id, 0)
            if not recovery_requests:
                del recovery[job_id]
        completed_work = (
            lease.estimated_work if actual_work is None else actual_work
        )
        _update_saor_fairness_debt(
            credit,
            endpoint_id,
            completed_job_id=job_id,
            completed_work=completed_work,
        )
        if credit._policy in {
            "saor_bounded_priority",
            "saor_bounded_ready",
        }:
            credit._record_release_event(
                endpoint_id,
                action="completion",
                tier="service_completion",
                lease=lease,
                states=_bounded_saor_states(
                    credit,
                    endpoint_id,
                    include_job_ids=(job_id,),
                ),
                projection_estimation_overrun_work=max(
                    0,
                    completed_work - lease.estimated_work,
                ),
                recovery_estimation_overrun_work=max(
                    0,
                    completed_work - recovery_estimated_work,
                )
                if recovery_estimated_work
                else 0,
            )
