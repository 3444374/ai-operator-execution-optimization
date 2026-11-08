"""Shared endpoint-local work credits for concurrent database AI jobs."""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from collections.abc import Callable

from .saor import (
    SaorBoundedHeadState,
    SaorReleaseConfig,
    bounded_saor_recovery_required,
    project_bounded_saor_debt,
)

from .credit_policies import (
    _GuardHold,
    _bounded_saor_states,
    _close_guard_hold,
    _grant_waiters,
    _lift_vtc_floor,
    _record_completion,
)


@dataclass(frozen=True)
class CreditLease:
    request_id: str
    job_id: str
    endpoint_id: str
    estimated_work: int
    enqueued_at_s: float = 0.0
    slo_deadline_s: float | None = None

    def __post_init__(self) -> None:
        if not self.request_id or not self.job_id or not self.endpoint_id:
            raise ValueError("lease identifiers must be non-empty")
        if self.estimated_work <= 0:
            raise ValueError("estimated_work must be positive")
        if not math.isfinite(self.enqueued_at_s) or self.enqueued_at_s < 0:
            raise ValueError("enqueued_at_s must be finite and non-negative")
        if self.slo_deadline_s is not None and not math.isfinite(
            self.slo_deadline_s
        ):
            raise ValueError("slo_deadline_s must be finite when present")


@dataclass(frozen=True)
class SaorReleaseEvent:
    """Lossless evidence for one bounded-SAOR grant or hold transition."""

    event_seq: int
    event_time_s: float
    event_epoch_s: float
    endpoint_id: str
    action: str
    tier: str
    selected_job_id: str = ""
    selected_request_id: str = ""
    target_job_id: str = ""
    head_work: int = 0
    reclaim_debt: int = 0
    hold_duration_s: float = 0.0
    constraint_conflict: bool = False
    ready_jobs: tuple[str, ...] = ()
    fitting_jobs: tuple[str, ...] = ()
    debt_by_job: tuple[tuple[str, float], ...] = ()
    debt_cap_by_job: tuple[tuple[str, float], ...] = ()
    active_set_jobs: tuple[str, ...] = ()
    active_set_weight_sum: float = 0.0
    weight_by_job: tuple[tuple[str, float], ...] = ()
    projection_own_inflight_work_by_job: tuple[tuple[str, int], ...] = ()
    projection_foreign_residual_work_by_job: tuple[tuple[str, int], ...] = ()
    projection_candidate_work_by_job: tuple[tuple[str, int], ...] = ()
    projection_target_share_by_job: tuple[tuple[str, float], ...] = ()
    projected_debt_before_by_job: tuple[tuple[str, float], ...] = ()
    projected_debt_after_by_job: tuple[tuple[str, float], ...] = ()
    projected_overshoot_bound_by_job: tuple[tuple[str, float], ...] = ()
    recovery_inflight_by_job: tuple[tuple[str, tuple[str, ...]], ...] = ()
    recovery_inflight_work_by_job: tuple[tuple[str, int], ...] = ()
    projection_estimation_overrun_work: int = 0
    recovery_estimation_overrun_work: int = 0
    active_requests: int = 0
    active_work: int = 0
    avoidable_idle: bool = False
    foreign_grant_over_debt_critical: bool = False


@dataclass(frozen=True)
class EndpointCreditSnapshot:
    endpoint_id: str
    request_limit: int
    work_limit: int
    active_requests: int
    active_work: int
    waiting_requests: int
    waiting_work: int
    oldest_waiting_age_s: float
    active_by_job: tuple[tuple[str, int], ...]
    active_work_by_job: tuple[tuple[str, int], ...]
    waiting_by_job: tuple[tuple[str, int], ...]
    waiting_work_by_job: tuple[tuple[str, int], ...]
    waiting_head_work_by_job: tuple[tuple[str, int], ...]
    max_active_requests_seen: int
    max_active_work_seen: int
    granted_requests_by_job: tuple[tuple[str, int], ...]
    granted_work_by_job: tuple[tuple[str, int], ...]
    attained_service_by_job: tuple[tuple[str, int], ...]
    fairness_debt_by_job: tuple[tuple[str, float], ...]
    recovery_inflight_by_job: tuple[tuple[str, tuple[str, ...]], ...] = ()
    recovery_inflight_work_by_job: tuple[tuple[str, int], ...] = ()
    guard_hold_target_job_id: str = ""
    guard_hold_target_request_id: str = ""
    guard_reclaim_debt: int = 0
    guard_hold_age_s: float = 0.0


class FairEndpointCreditCoordinator:
    """Shared endpoint credit with fair and diagnostic release controls.

    This class is engine independent. A Ray named actor can own one instance so
    schedulers from separate database jobs observe the same endpoint capacity.
    """

    def __init__(
        self,
        capacities: dict[str, tuple[int, int]],
        *,
        quantum: int,
        policy: str = "drr",
        saor_release_config: SaorReleaseConfig | None = None,
        record_ready_lifecycle_events: bool = False,
        clock: Callable[[], float] = time.monotonic,
        epoch_clock: Callable[[], float] = time.time,
    ) -> None:
        if not capacities:
            raise ValueError("capacities must not be empty")
        if quantum <= 0:
            raise ValueError("quantum must be positive")
        if policy not in {
            "drr",
            "fifo",
            "vtc",
            "saor",
            "saor_bounded_priority",
            "saor_bounded_ready",
            "strict_priority",
        }:
            raise ValueError(
                "shared credit policy must be drr, fifo, vtc, saor, "
                "saor_bounded_priority, saor_bounded_ready, or strict_priority"
            )
        if (
            policy in {"saor", "saor_bounded_priority", "saor_bounded_ready"}
            and saor_release_config is None
        ):
            raise ValueError("saor shared credit requires release configuration")
        if (
            policy not in {"saor", "saor_bounded_priority", "saor_bounded_ready"}
            and saor_release_config is not None
        ):
            raise ValueError(
                "SAOR release configuration is only valid for SAOR policies"
            )
        for endpoint_id, (request_limit, work_limit) in capacities.items():
            if not endpoint_id:
                raise ValueError("endpoint IDs must be non-empty")
            if request_limit <= 0 or work_limit <= 0:
                raise ValueError("endpoint capacity limits must be positive")
        self._capacities = dict(capacities)
        self._quantum = quantum
        self._policy = policy
        self._saor_release_config = saor_release_config
        self._record_ready_lifecycle_events = bool(
            record_ready_lifecycle_events
        )
        self._clock = clock
        self._epoch_clock = epoch_clock
        self._active: dict[tuple[str, str], CreditLease] = {}
        self._active_work: dict[str, int] = {
            endpoint_id: 0 for endpoint_id in capacities
        }
        self._active_requests: dict[str, int] = {
            endpoint_id: 0 for endpoint_id in capacities
        }
        self._max_active_work: dict[str, int] = {
            endpoint_id: 0 for endpoint_id in capacities
        }
        self._max_active_requests: dict[str, int] = {
            endpoint_id: 0 for endpoint_id in capacities
        }
        self._granted_requests: dict[str, dict[str, int]] = {
            endpoint_id: {} for endpoint_id in capacities
        }
        self._granted_work: dict[str, dict[str, int]] = {
            endpoint_id: {} for endpoint_id in capacities
        }
        self._waiting: dict[str, dict[str, deque[CreditLease]]] = {
            endpoint_id: {} for endpoint_id in capacities
        }
        self._queued_request_keys: set[tuple[str, str]] = set()
        self._fifo_order: dict[str, deque[tuple[str, str]]] = {
            endpoint_id: deque() for endpoint_id in capacities
        }
        self._weights: dict[str, int] = {}
        self._priorities: dict[str, int] = {}
        self._finished_jobs: set[str] = set()
        self._deficits: dict[tuple[str, str], int] = {}
        self._job_order: dict[str, list[str]] = {
            endpoint_id: [] for endpoint_id in capacities
        }
        self._cursor: dict[str, int] = {
            endpoint_id: 0 for endpoint_id in capacities
        }
        # External VTC-style service counters. Work is charged optimistically
        # at grant time so the upstream scheduler remains work-conserving, then
        # corrected to actual prompt+output token work at completion.
        self._attained_service: dict[str, dict[str, int]] = {
            endpoint_id: {} for endpoint_id in capacities
        }
        self._fairness_debt: dict[str, dict[str, float]] = {
            endpoint_id: {} for endpoint_id in capacities
        }
        self._saor_slo_target_s: dict[str, float] = {}
        self._priority_windows_s: dict[str, float] = {}
        self._fairness_debt_caps: dict[str, float] = {}
        self._recovery_inflight: dict[str, dict[str, dict[str, int]]] = {
            endpoint_id: {} for endpoint_id in capacities
        }
        self._guard_holds: dict[str, _GuardHold | None] = {
            endpoint_id: None for endpoint_id in capacities
        }
        self._release_events: dict[str, deque[SaorReleaseEvent]] = {
            endpoint_id: deque() for endpoint_id in capacities
        }
        self._release_event_seq: dict[str, int] = {
            endpoint_id: 0 for endpoint_id in capacities
        }

    def try_acquire(
        self,
        *,
        request_id: str,
        job_id: str,
        endpoint_id: str,
        estimated_work: int,
        weight: int = 1,
        priority: int = 0,
        slo_target_s: float | None = None,
        slo_budget_remaining_s: float | None = None,
        priority_window_s: float | None = None,
        fairness_debt_cap: float | None = None,
    ) -> bool:
        request_key = (job_id, request_id)
        if job_id in self._finished_jobs:
            raise ValueError("a finished job cannot acquire new credit")
        if request_key in self._active:
            return True
        if endpoint_id not in self._capacities:
            raise ValueError(f"unknown endpoint_id: {endpoint_id}")
        if not request_id or not job_id:
            raise ValueError("request_id and job_id must be non-empty")
        if estimated_work <= 0:
            raise ValueError("estimated_work must be positive")
        work_limit = self._capacities[endpoint_id][1]
        if estimated_work > work_limit:
            raise ValueError(
                "estimated_work exceeds endpoint work limit: "
                f"{estimated_work} > {work_limit}"
            )
        if weight <= 0:
            raise ValueError("weight must be positive")
        if not isinstance(priority, int) or isinstance(priority, bool) or priority < 0:
            raise ValueError("priority must be a non-negative integer")
        previous_weight = self._weights.setdefault(job_id, weight)
        if previous_weight != weight:
            raise ValueError("a job must use one stable weight")
        previous_priority = self._priorities.setdefault(job_id, priority)
        if previous_priority != priority:
            raise ValueError("a job must use one stable priority")
        if self._policy == "saor":
            if slo_target_s is not None and (
                not math.isfinite(slo_target_s) or slo_target_s <= 0
            ):
                raise ValueError("slo_target_s must be finite and positive")
            previous_target = self._saor_slo_target_s.setdefault(
                job_id,
                0.0 if slo_target_s is None else float(slo_target_s),
            )
            current_target = 0.0 if slo_target_s is None else float(slo_target_s)
            if previous_target != current_target:
                raise ValueError("a job must use one stable SLO target")
        if self._policy in {"saor_bounded_priority", "saor_bounded_ready"}:
            if priority > 0 and (
                slo_budget_remaining_s is None or priority_window_s is None
            ):
                raise ValueError(
                    "bounded SAOR priority requires SLO budget and priority window"
                )
            if slo_budget_remaining_s is not None and not math.isfinite(
                slo_budget_remaining_s
            ):
                raise ValueError("SLO budget must be finite when present")
            window = 0.0 if priority_window_s is None else float(priority_window_s)
            if priority_window_s is not None and (
                not math.isfinite(window) or window <= 0
            ):
                raise ValueError("priority window must be finite and positive")
            cap = 0.0 if fairness_debt_cap is None else float(fairness_debt_cap)
            if fairness_debt_cap is not None and (
                not math.isfinite(cap) or cap <= 0
            ):
                raise ValueError("fairness debt cap must be finite and positive")
            previous_window = self._priority_windows_s.setdefault(job_id, window)
            if previous_window != window:
                raise ValueError("a job must use one stable priority window")
            previous_cap = self._fairness_debt_caps.setdefault(job_id, cap)
            if previous_cap != cap:
                raise ValueError("a job must use one stable fairness debt cap")
        if request_key not in self._queued_request_keys:
            enqueued_at_s = self._clock()
            lease = CreditLease(
                request_id,
                job_id,
                endpoint_id,
                estimated_work,
                enqueued_at_s,
                (
                    enqueued_at_s + slo_budget_remaining_s
                    if slo_budget_remaining_s is not None
                    else None
                ),
            )
            job_queues = self._waiting[endpoint_id]
            if job_id not in job_queues:
                job_queues[job_id] = deque()
                self._job_order[endpoint_id].append(job_id)
            if self._policy == "vtc":
                _lift_vtc_floor(self, endpoint_id, job_id, weight)
            job_queues[job_id].append(lease)
            self._queued_request_keys.add(request_key)
            if self._policy == "fifo":
                self._fifo_order[endpoint_id].append(request_key)
            if (
                self._policy == "saor_bounded_ready"
                or self._record_ready_lifecycle_events
            ):
                self._record_release_event(
                    endpoint_id,
                    action="register",
                    tier="ready_registration",
                    lease=lease,
                    states=(
                        _bounded_saor_states(self, endpoint_id)
                        if self._policy == "saor_bounded_ready"
                        else ()
                    ),
                )
        _grant_waiters(self, endpoint_id)
        return request_key in self._active

    def cancel_waiter(self, request_id: str, *, job_id: str) -> bool:
        """Remove one queued acquisition without revoking an active lease."""

        request_key = (job_id, request_id)
        if request_key in self._active:
            return False
        if request_key not in self._queued_request_keys:
            return False
        endpoint_id = next(
            endpoint_id
            for endpoint_id, job_queues in self._waiting.items()
            if any(
                lease.request_id == request_id
                for lease in job_queues.get(job_id, ())
            )
        )
        queue = self._waiting[endpoint_id][job_id]
        self._waiting[endpoint_id][job_id] = deque(
            lease for lease in queue if lease.request_id != request_id
        )
        self._queued_request_keys.remove(request_key)
        if self._policy == "fifo":
            self._fifo_order[endpoint_id] = deque(
                key for key in self._fifo_order[endpoint_id] if key != request_key
            )
        hold = self._guard_holds[endpoint_id]
        if hold is not None and (
            hold.target_job_id,
            hold.target_request_id,
        ) == request_key:
            _close_guard_hold(self, endpoint_id, hold)
        _grant_waiters(self, endpoint_id)
        return True

    def release(
        self,
        request_id: str,
        *,
        job_id: str,
        actual_work: int | None = None,
    ) -> None:
        request_key = (job_id, request_id)
        try:
            lease = self._active[request_key]
        except KeyError as exc:
            raise ValueError(
                f"request has no active lease: {job_id}/{request_id}"
            ) from exc
        if actual_work is not None and (
            not isinstance(actual_work, int)
            or isinstance(actual_work, bool)
            or actual_work <= 0
        ):
            raise ValueError("actual_work must be a positive integer when present")
        del self._active[request_key]
        endpoint_id = lease.endpoint_id
        self._active_requests[endpoint_id] -= 1
        self._active_work[endpoint_id] -= lease.estimated_work
        _record_completion(self, lease, actual_work)
        _grant_waiters(self, endpoint_id)

    def finish_job(self, job_id: str) -> None:
        """Close one Job lifecycle after every lease and waiter drains."""

        has_active = any(lease.job_id == job_id for lease in self._active.values())
        has_waiting = any(
            job_queues.get(job_id)
            for job_queues in self._waiting.values()
        )
        if has_active or has_waiting:
            raise ValueError("cannot finish a job with outstanding credit")
        if self._policy in {"saor_bounded_priority", "saor_bounded_ready"}:
            for endpoint_id in self._capacities:
                self._record_release_event(
                    endpoint_id,
                    action="finish_job",
                    tier="job_lifecycle_complete",
                    event_job_id=job_id,
                    states=_bounded_saor_states(
                        self,
                        endpoint_id,
                        include_job_ids=(job_id,),
                    ),
                )
        self._finished_jobs.add(job_id)
        for endpoint_id in self._capacities:
            _grant_waiters(self, endpoint_id)

    @property
    def incremental_safe(self) -> bool:
        """The first streaming integration only qualifies local FIFO without history events."""
        return self._policy == "fifo" and not self._record_ready_lifecycle_events

    def forget_finished_job(self, job_id: str) -> None:
        """Drop retired FIFO accounting after its owner has consumed any needed summary.

        Legacy finish_job retains history by default. Streaming callers explicitly
        retire it; active leases/waiters are never eligible for this operation.
        """
        if not self.incremental_safe or job_id not in self._finished_jobs:
            raise ValueError("only finished incremental FIFO jobs can be forgotten")
        for mapping in (
            self._weights, self._priorities, self._saor_slo_target_s,
            self._priority_windows_s, self._fairness_debt_caps,
        ):
            mapping.pop(job_id, None)
        for endpoint_id in self._capacities:
            for mapping in (
                self._granted_requests, self._granted_work, self._attained_service,
                self._fairness_debt, self._recovery_inflight, self._waiting,
            ):
                mapping[endpoint_id].pop(job_id, None)
            self._deficits.pop((endpoint_id, job_id), None)
            order = self._job_order[endpoint_id]
            if job_id in order:
                order.remove(job_id)
                self._cursor[endpoint_id] %= max(1, len(order))
        self._finished_jobs.remove(job_id)

    def _active_usage_by_job(
        self, endpoint_id: str,
    ) -> tuple[dict[str, int], dict[str, int]]:
        """Read current lease reservations for one endpoint, grouped by Job."""
        requests: dict[str, int] = {}
        work: dict[str, int] = {}
        for lease in self._active.values():
            if lease.endpoint_id != endpoint_id:
                continue
            requests[lease.job_id] = requests.get(lease.job_id, 0) + 1
            work[lease.job_id] = work.get(lease.job_id, 0) + lease.estimated_work
        return requests, work

    def snapshot(self, endpoint_id: str) -> EndpointCreditSnapshot:
        if endpoint_id not in self._capacities:
            raise ValueError(f"unknown endpoint_id: {endpoint_id}")
        active_by_job, active_work_by_job = self._active_usage_by_job(endpoint_id)
        waiting_by_job = {
            job_id: len(queue)
            for job_id, queue in self._waiting[endpoint_id].items()
            if queue
        }
        waiting_work_by_job = {
            job_id: sum(lease.estimated_work for lease in queue)
            for job_id, queue in self._waiting[endpoint_id].items()
            if queue
        }
        waiting_head_work_by_job = {
            job_id: queue[0].estimated_work
            for job_id, queue in self._waiting[endpoint_id].items()
            if queue
        }
        waiting_leases = [
            lease
            for queue in self._waiting[endpoint_id].values()
            for lease in queue
        ]
        oldest_waiting_age_s = (
            max(
                0.0,
                self._clock()
                - min(lease.enqueued_at_s for lease in waiting_leases),
            )
            if waiting_leases
            else 0.0
        )
        request_limit, work_limit = self._capacities[endpoint_id]
        return EndpointCreditSnapshot(
            endpoint_id=endpoint_id,
            request_limit=request_limit,
            work_limit=work_limit,
            active_requests=self._active_requests[endpoint_id],
            active_work=self._active_work[endpoint_id],
            waiting_requests=sum(waiting_by_job.values()),
            waiting_work=sum(waiting_work_by_job.values()),
            oldest_waiting_age_s=oldest_waiting_age_s,
            active_by_job=tuple(sorted(active_by_job.items())),
            active_work_by_job=tuple(sorted(active_work_by_job.items())),
            waiting_by_job=tuple(sorted(waiting_by_job.items())),
            waiting_work_by_job=tuple(
                sorted(waiting_work_by_job.items())
            ),
            waiting_head_work_by_job=tuple(
                sorted(waiting_head_work_by_job.items())
            ),
            max_active_requests_seen=self._max_active_requests[endpoint_id],
            max_active_work_seen=self._max_active_work[endpoint_id],
            granted_requests_by_job=tuple(
                sorted(self._granted_requests[endpoint_id].items())
            ),
            granted_work_by_job=tuple(
                sorted(self._granted_work[endpoint_id].items())
            ),
            attained_service_by_job=tuple(
                sorted(self._attained_service[endpoint_id].items())
            ),
            fairness_debt_by_job=tuple(
                sorted(self._fairness_debt[endpoint_id].items())
            ),
            recovery_inflight_by_job=tuple(
                (job_id, tuple(sorted(request_work)))
                for job_id, request_work in sorted(
                    self._recovery_inflight[endpoint_id].items()
                )
                if request_work
            ),
            recovery_inflight_work_by_job=tuple(
                (job_id, sum(request_work.values()))
                for job_id, request_work in sorted(
                    self._recovery_inflight[endpoint_id].items()
                )
                if request_work
            ),
            guard_hold_target_job_id=(
                self._guard_holds[endpoint_id].target_job_id
                if self._guard_holds[endpoint_id] is not None
                else ""
            ),
            guard_hold_target_request_id=(
                self._guard_holds[endpoint_id].target_request_id
                if self._guard_holds[endpoint_id] is not None
                else ""
            ),
            guard_reclaim_debt=(
                self._guard_holds[endpoint_id].reclaim_debt
                if self._guard_holds[endpoint_id] is not None
                else 0
            ),
            guard_hold_age_s=(
                max(
                    0.0,
                    self._clock() - self._guard_holds[endpoint_id].started_at_s,
                )
                if self._guard_holds[endpoint_id] is not None
                else 0.0
            ),
        )

    def drain_release_events(
        self,
        endpoint_id: str,
    ) -> tuple[SaorReleaseEvent, ...]:
        """Return each bounded-SAOR event once, in coordinator order."""

        if endpoint_id not in self._capacities:
            raise ValueError(f"unknown endpoint_id: {endpoint_id}")
        events = tuple(self._release_events[endpoint_id])
        self._release_events[endpoint_id].clear()
        return events

    def update_capacity(
        self,
        endpoint_id: str,
        *,
        request_limit: int,
        work_limit: int,
    ) -> EndpointCreditSnapshot:
        """Change future admission capacity without revoking active leases."""
        if endpoint_id not in self._capacities:
            raise ValueError(f"unknown endpoint_id: {endpoint_id}")
        if request_limit <= 0 or work_limit <= 0:
            raise ValueError("endpoint capacity limits must be positive")
        oversized_waiters = [
            lease.estimated_work
            for queue in self._waiting[endpoint_id].values()
            for lease in queue
            if lease.estimated_work > work_limit
        ]
        if oversized_waiters:
            raise ValueError(
                "updated work limit is smaller than queued request work"
            )
        self._capacities[endpoint_id] = (request_limit, work_limit)
        _grant_waiters(self, endpoint_id)
        return self.snapshot(endpoint_id)


    def _record_release_event(
        self,
        endpoint_id: str,
        *,
        action: str,
        tier: str,
        states: tuple[SaorBoundedHeadState, ...],
        lease: CreditLease | None = None,
        target: _GuardHold | None = None,
        event_job_id: str = "",
        constraint_conflict: bool = False,
        hold_duration_s: float = 0.0,
        projection_estimation_overrun_work: int = 0,
        recovery_estimation_overrun_work: int = 0,
    ) -> None:
        projections = {
            state.release.job_id: project_bounded_saor_debt(state, states)
            for state in states
        }
        debt_critical = {
            state.release.job_id
            for state in states
            if bounded_saor_recovery_required(state, states)
        }
        target_is_concrete = (
            target is not None
            and target.target_job_id in debt_critical
            and target.reclaim_debt > 0
        )
        selected_lease = (
            lease
            if action in {"register", "grant", "completion"}
            else None
        )
        seq = self._release_event_seq[endpoint_id] + 1
        self._release_event_seq[endpoint_id] = seq
        self._release_events[endpoint_id].append(
            SaorReleaseEvent(
                event_seq=seq,
                event_time_s=self._clock(),
                event_epoch_s=self._epoch_clock(),
                endpoint_id=endpoint_id,
                action=action,
                tier=tier,
                selected_job_id=(
                    selected_lease.job_id
                    if selected_lease is not None
                    else event_job_id
                ),
                selected_request_id=(
                    selected_lease.request_id
                    if selected_lease is not None
                    else ""
                ),
                target_job_id=target.target_job_id if target is not None else "",
                head_work=target.head_work if target is not None else 0,
                reclaim_debt=target.reclaim_debt if target is not None else 0,
                hold_duration_s=hold_duration_s,
                constraint_conflict=(
                    target.constraint_conflict
                    if target is not None
                    else constraint_conflict
                ),
                ready_jobs=tuple(
                    state.release.job_id for state in states if state.ready
                ),
                fitting_jobs=tuple(
                    state.release.job_id
                    for state in states
                    if state.release.eligible
                ),
                debt_by_job=tuple(
                    sorted(
                        (
                            state.release.job_id,
                            state.release.fairness_debt,
                        )
                        for state in states
                    )
                ),
                debt_cap_by_job=tuple(
                    sorted(
                        (state.release.job_id, state.debt_cap)
                        for state in states
                        if state.debt_cap is not None
                    )
                ),
                active_set_jobs=tuple(
                    state.release.job_id for state in states
                ),
                active_set_weight_sum=sum(
                    state.release.weight for state in states
                ),
                weight_by_job=tuple(
                    (state.release.job_id, state.release.weight)
                    for state in states
                ),
                projection_own_inflight_work_by_job=tuple(
                    (state.release.job_id, state.release.active_work)
                    for state in states
                ),
                projection_foreign_residual_work_by_job=tuple(
                    (
                        job_id,
                        projection.foreign_residual_work,
                    )
                    for job_id, projection in projections.items()
                ),
                projection_candidate_work_by_job=tuple(
                    (state.release.job_id, state.head_work)
                    for state in states
                ),
                projection_target_share_by_job=tuple(
                    (job_id, projection.target_share)
                    for job_id, projection in projections.items()
                ),
                projected_debt_before_by_job=tuple(
                    (job_id, projection.debt_before_candidate)
                    for job_id, projection in projections.items()
                ),
                projected_debt_after_by_job=tuple(
                    (job_id, projection.debt_after_candidate)
                    for job_id, projection in projections.items()
                ),
                projected_overshoot_bound_by_job=tuple(
                    (job_id, projection.discrete_overshoot_bound)
                    for job_id, projection in projections.items()
                ),
                recovery_inflight_by_job=tuple(
                    (job_id, tuple(sorted(request_work)))
                    for job_id, request_work in sorted(
                        self._recovery_inflight[endpoint_id].items()
                    )
                    if request_work
                ),
                recovery_estimation_overrun_work=(
                    recovery_estimation_overrun_work
                ),
                projection_estimation_overrun_work=(
                    projection_estimation_overrun_work
                ),
                recovery_inflight_work_by_job=tuple(
                    (job_id, sum(request_work.values()))
                    for job_id, request_work in sorted(
                        self._recovery_inflight[endpoint_id].items()
                    )
                    if request_work
                ),
                active_requests=self._active_requests[endpoint_id],
                active_work=self._active_work[endpoint_id],
                avoidable_idle=(
                    action == "hold_start" and not target_is_concrete
                ),
                foreign_grant_over_debt_critical=(
                    action == "grant"
                    and bool(debt_critical)
                    and lease is not None
                    and lease.job_id not in debt_critical
                ),
            )
        )


    def _activate(self, endpoint_id: str, lease: CreditLease) -> None:
        request_key = (lease.job_id, lease.request_id)
        self._active[request_key] = lease
        self._active_requests[endpoint_id] += 1
        self._active_work[endpoint_id] += lease.estimated_work
        self._max_active_requests[endpoint_id] = max(
            self._max_active_requests[endpoint_id],
            self._active_requests[endpoint_id],
        )
        self._max_active_work[endpoint_id] = max(
            self._max_active_work[endpoint_id],
            self._active_work[endpoint_id],
        )
        granted_requests = self._granted_requests[endpoint_id]
        granted_requests[lease.job_id] = granted_requests.get(lease.job_id, 0) + 1
        granted_work = self._granted_work[endpoint_id]
        granted_work[lease.job_id] = (
            granted_work.get(lease.job_id, 0) + lease.estimated_work
        )
        if (
            self._record_ready_lifecycle_events
            and self._policy
            not in {"saor_bounded_priority", "saor_bounded_ready"}
        ):
            self._record_release_event(
                endpoint_id,
                action="grant",
                tier=f"selector_grant:{self._policy}",
                lease=lease,
                states=(),
            )
