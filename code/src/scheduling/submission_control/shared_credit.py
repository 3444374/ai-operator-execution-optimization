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
    SaorReleaseState,
    select_bounded_saor_release,
    select_saor_release_job,
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
    recovery_inflight_by_job: tuple[tuple[str, str], ...] = ()
    active_requests: int = 0
    active_work: int = 0
    avoidable_idle: bool = False
    foreign_grant_over_debt_critical: bool = False


@dataclass(frozen=True)
class _GuardHold:
    target_job_id: str
    target_request_id: str
    head_work: int
    reclaim_debt: int
    started_at_s: float
    constraint_conflict: bool


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
    recovery_inflight_by_job: tuple[tuple[str, str], ...] = ()
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
        self._recovery_inflight: dict[str, dict[str, str]] = {
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
            if self._policy == "vtc" and self._vtc_job_is_inactive(
                endpoint_id,
                job_id,
            ):
                active_counters = self._active_vtc_normalized_counters(
                    endpoint_id,
                    exclude_job_id=job_id,
                )
                floor = min(active_counters) if active_counters else 0
                counters = self._attained_service[endpoint_id]
                weighted_floor = math.ceil(floor * weight)
                counters[job_id] = max(
                    counters.get(job_id, 0),
                    weighted_floor,
                )
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
                        self._bounded_saor_states(endpoint_id)
                        if self._policy == "saor_bounded_ready"
                        else ()
                    ),
                )
        self._grant_waiters(endpoint_id)
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
            self._close_guard_hold(endpoint_id, hold)
        self._grant_waiters(endpoint_id)
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
            lease = self._active.pop(request_key)
        except KeyError as exc:
            raise ValueError(
                f"request has no active lease: {job_id}/{request_id}"
            ) from exc
        endpoint_id = lease.endpoint_id
        self._active_requests[endpoint_id] -= 1
        self._active_work[endpoint_id] -= lease.estimated_work
        if actual_work is not None:
            if (
                not isinstance(actual_work, int)
                or isinstance(actual_work, bool)
                or actual_work <= 0
            ):
                raise ValueError("actual_work must be a positive integer when present")
            if self._policy == "vtc":
                counters = self._attained_service[endpoint_id]
                counters[job_id] = counters.get(job_id, 0) + (
                    actual_work - lease.estimated_work
                )
        if self._policy in {
            "saor",
            "saor_bounded_priority",
            "saor_bounded_ready",
        }:
            recovery = self._recovery_inflight[endpoint_id]
            if recovery.get(job_id) == request_id:
                del recovery[job_id]
            self._update_saor_fairness_debt(
                endpoint_id,
                completed_job_id=job_id,
                completed_work=(
                    lease.estimated_work if actual_work is None else actual_work
                ),
            )
        self._grant_waiters(endpoint_id)

    def finish_job(self, job_id: str) -> None:
        """Close one Job lifecycle after every lease and waiter drains."""

        has_active = any(lease.job_id == job_id for lease in self._active.values())
        has_waiting = any(
            job_queues.get(job_id)
            for job_queues in self._waiting.values()
        )
        if has_active or has_waiting:
            raise ValueError("cannot finish a job with outstanding credit")
        self._finished_jobs.add(job_id)
        for endpoint_id in self._capacities:
            self._grant_waiters(endpoint_id)

    def snapshot(self, endpoint_id: str) -> EndpointCreditSnapshot:
        if endpoint_id not in self._capacities:
            raise ValueError(f"unknown endpoint_id: {endpoint_id}")
        active_by_job: dict[str, int] = {}
        active_work_by_job: dict[str, int] = {}
        for lease in self._active.values():
            if lease.endpoint_id == endpoint_id:
                active_by_job[lease.job_id] = (
                    active_by_job.get(lease.job_id, 0) + 1
                )
                active_work_by_job[lease.job_id] = (
                    active_work_by_job.get(lease.job_id, 0)
                    + lease.estimated_work
                )
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
                sorted(self._recovery_inflight[endpoint_id].items())
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
        self._grant_waiters(endpoint_id)
        return self.snapshot(endpoint_id)

    def _grant_waiters(self, endpoint_id: str) -> None:
        if self._policy == "fifo":
            self._grant_fifo_waiters(endpoint_id)
            return
        if self._policy == "vtc":
            self._grant_vtc_waiters(endpoint_id)
            return
        if self._policy == "saor":
            self._grant_saor_waiters(endpoint_id)
            return
        if self._policy in {"saor_bounded_priority", "saor_bounded_ready"}:
            self._grant_bounded_saor_waiters(endpoint_id)
            return
        if self._policy == "strict_priority":
            self._grant_strict_priority_waiters(endpoint_id)
            return
        request_limit, work_limit = self._capacities[endpoint_id]
        job_order = self._job_order[endpoint_id]
        if not job_order:
            return
        visits_without_grant = 0
        while (
            self._active_requests[endpoint_id] < request_limit
        ):
            if visits_without_grant >= len(job_order):
                nonempty_jobs = [
                    job_id
                    for job_id in job_order
                    if self._waiting[endpoint_id][job_id]
                ]
                if not nonempty_jobs:
                    return
                if all(
                    self._waiting[endpoint_id][job_id][0].estimated_work
                    <= self._deficits.get((endpoint_id, job_id), 0)
                    for job_id in nonempty_jobs
                ):
                    return
                visits_without_grant = 0
            cursor = self._cursor[endpoint_id] % len(job_order)
            job_id = job_order[cursor]
            self._cursor[endpoint_id] = (cursor + 1) % len(job_order)
            queue = self._waiting[endpoint_id][job_id]
            if not queue:
                visits_without_grant += 1
                continue
            deficit_key = (endpoint_id, job_id)
            self._deficits[deficit_key] = (
                self._deficits.get(deficit_key, 0)
                + self._quantum * self._weights[job_id]
            )
            lease = queue[0]
            if lease.estimated_work > self._deficits[deficit_key]:
                visits_without_grant += 1
                continue
            current_work = self._active_work[endpoint_id]
            fits_work = current_work + lease.estimated_work <= work_limit
            if not fits_work:
                visits_without_grant += 1
                continue
            queue.popleft()
            request_key = (lease.job_id, lease.request_id)
            self._queued_request_keys.remove(request_key)
            self._deficits[deficit_key] -= lease.estimated_work
            self._activate(endpoint_id, lease)
            visits_without_grant = 0

    def _grant_vtc_waiters(self, endpoint_id: str) -> None:
        """Grant the least normalized attained-service job that fits.

        This is an upstream VTC-style baseline. Unlike in-engine VTC it cannot
        charge output tokens incrementally during decoding; release-time actual
        work correction is therefore part of the recorded baseline identity.
        """
        request_limit, work_limit = self._capacities[endpoint_id]
        while self._active_requests[endpoint_id] < request_limit:
            candidates = [
                job_id
                for job_id in self._job_order[endpoint_id]
                if self._waiting[endpoint_id][job_id]
                and self._active_work[endpoint_id]
                + self._waiting[endpoint_id][job_id][0].estimated_work
                <= work_limit
            ]
            if not candidates:
                return
            order = {
                job_id: index
                for index, job_id in enumerate(self._job_order[endpoint_id])
            }
            job_id = min(
                candidates,
                key=lambda item: (
                    self._attained_service[endpoint_id].get(item, 0)
                    / self._weights[item],
                    order[item],
                ),
            )
            lease = self._waiting[endpoint_id][job_id].popleft()
            self._queued_request_keys.remove((lease.job_id, lease.request_id))
            self._attained_service[endpoint_id][job_id] = (
                self._attained_service[endpoint_id].get(job_id, 0)
                + lease.estimated_work
            )
            self._activate(endpoint_id, lease)

    def _grant_strict_priority_waiters(self, endpoint_id: str) -> None:
        """Grant the highest-priority fitting head without revoking leases.

        This policy is a diagnostic upper bound for release-only foreground
        protection.  It is non-preemptive: priority changes only which queued
        Job receives newly freed credit.  While a higher-priority Job remains
        unfinished, newly freed capacity is held for that Job instead of being
        refilled by a lower-priority Job.
        """

        request_limit, work_limit = self._capacities[endpoint_id]
        order = {
            job_id: index
            for index, job_id in enumerate(self._job_order[endpoint_id])
        }
        while self._active_requests[endpoint_id] < request_limit:
            unfinished_priorities = [
                priority
                for job_id, priority in self._priorities.items()
                if job_id not in self._finished_jobs
            ]
            active_priority = max(unfinished_priorities, default=0)
            candidates = [
                job_id
                for job_id in self._job_order[endpoint_id]
                if self._waiting[endpoint_id][job_id]
                and self._priorities[job_id] == active_priority
                and self._active_work[endpoint_id]
                + self._waiting[endpoint_id][job_id][0].estimated_work
                <= work_limit
            ]
            if not candidates:
                return
            job_id = min(
                candidates,
                key=lambda item: (-self._priorities[item], order[item]),
            )
            lease = self._waiting[endpoint_id][job_id].popleft()
            self._queued_request_keys.remove((lease.job_id, lease.request_id))
            self._activate(endpoint_id, lease)

    def _vtc_job_is_inactive(self, endpoint_id: str, job_id: str) -> bool:
        return (
            not self._waiting[endpoint_id].get(job_id)
            and not any(
                lease.endpoint_id == endpoint_id and lease.job_id == job_id
                for lease in self._active.values()
            )
        )

    def _active_vtc_normalized_counters(
        self,
        endpoint_id: str,
        *,
        exclude_job_id: str = "",
    ) -> list[float]:
        active_jobs = {
            lease.job_id
            for lease in self._active.values()
            if lease.endpoint_id == endpoint_id
            and lease.job_id != exclude_job_id
        }
        active_jobs.update(
            job_id
            for job_id, queue in self._waiting[endpoint_id].items()
            if queue and job_id != exclude_job_id
        )
        return [
            self._attained_service[endpoint_id][job_id]
            / self._weights[job_id]
            for job_id in active_jobs
            if job_id in self._attained_service[endpoint_id]
        ]

    def _grant_saor_waiters(self, endpoint_id: str) -> None:
        """Grant fitting Job heads using fixed-envelope active-set release."""

        if self._saor_release_config is None:
            raise RuntimeError("SAOR release configuration is missing")
        request_limit, work_limit = self._capacities[endpoint_id]
        while self._active_requests[endpoint_id] < request_limit:
            ordered_jobs = [
                job_id
                for job_id in self._job_order[endpoint_id]
                if self._waiting[endpoint_id][job_id]
            ]
            if not ordered_jobs:
                return
            order_by_job = {
                job_id: order for order, job_id in enumerate(ordered_jobs)
            }
            active_requests_by_job: dict[str, int] = {}
            active_work_by_job: dict[str, int] = {}
            for lease in self._active.values():
                if lease.endpoint_id != endpoint_id:
                    continue
                active_requests_by_job[lease.job_id] = (
                    active_requests_by_job.get(lease.job_id, 0) + 1
                )
                active_work_by_job[lease.job_id] = (
                    active_work_by_job.get(lease.job_id, 0)
                    + lease.estimated_work
                )
            active_set = set(ordered_jobs) | set(active_requests_by_job)
            states = []
            for job_id in sorted(active_set):
                queue = self._waiting[endpoint_id].get(job_id, ())
                waiting_work = sum(lease.estimated_work for lease in queue)
                head_fits = bool(queue) and (
                    self._active_work[endpoint_id]
                    + queue[0].estimated_work
                    <= work_limit
                )
                target = self._saor_slo_target_s.get(job_id, 0.0)
                states.append(
                    SaorReleaseState(
                        job_id=job_id,
                        weight=float(self._weights[job_id]),
                        active_requests=active_requests_by_job.get(job_id, 0),
                        active_work=active_work_by_job.get(job_id, 0),
                        waiting_work=waiting_work,
                        fairness_debt=self._fairness_debt[endpoint_id].get(
                            job_id,
                            0.0,
                        ),
                        oldest_waiting_age_s=(
                            max(0.0, self._clock() - queue[0].enqueued_at_s)
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
                config=self._saor_release_config,
            )
            lease = self._waiting[endpoint_id][selected.job_id].popleft()
            self._queued_request_keys.remove((lease.job_id, lease.request_id))
            self._activate(endpoint_id, lease)

    def _grant_bounded_saor_waiters(self, endpoint_id: str) -> None:
        """Apply debt recovery, bounded SLO priority, then SAOR fallback."""

        if self._saor_release_config is None:
            raise RuntimeError("SAOR release configuration is missing")
        request_limit, work_limit = self._capacities[endpoint_id]
        while self._active_requests[endpoint_id] < request_limit:
            if self._resume_guard_hold(endpoint_id):
                continue
            if self._guard_holds[endpoint_id] is not None:
                return
            states = self._bounded_saor_states(endpoint_id)
            if not states:
                return
            has_critical = any(
                state.ready
                and state.debt_cap is not None
                and state.release.fairness_debt >= state.debt_cap
                and not state.recovery_inflight
                for state in states
            )
            if not any(state.release.eligible for state in states) and not has_critical:
                return
            selected = select_bounded_saor_release(
                states,
                request_limit=request_limit,
                work_limit=work_limit,
                active_work=self._active_work[endpoint_id],
                config=self._saor_release_config,
            )
            if selected.action == "hold":
                job_id = selected.job_id or ""
                lease = self._waiting[endpoint_id][job_id][0]
                hold = _GuardHold(
                    job_id,
                    lease.request_id,
                    lease.estimated_work,
                    selected.reclaim_debt,
                    self._clock(),
                    selected.constraint_conflict,
                )
                self._guard_holds[endpoint_id] = hold
                self._record_release_event(
                    endpoint_id,
                    action="hold_start",
                    tier=selected.tier,
                    target=hold,
                    states=states,
                )
                return
            job_id = selected.job_id or ""
            lease = self._waiting[endpoint_id][job_id].popleft()
            self._queued_request_keys.remove((lease.job_id, lease.request_id))
            if selected.tier == "debt_recovery":
                self._recovery_inflight[endpoint_id][job_id] = lease.request_id
            self._activate(endpoint_id, lease)
            self._record_release_event(
                endpoint_id,
                action="grant",
                tier=selected.tier,
                lease=lease,
                constraint_conflict=selected.constraint_conflict,
                states=states,
            )

    def _resume_guard_hold(self, endpoint_id: str) -> bool:
        hold = self._guard_holds[endpoint_id]
        if hold is None:
            return False
        queue = self._waiting[endpoint_id].get(hold.target_job_id)
        if not queue or queue[0].request_id != hold.target_request_id:
            self._close_guard_hold(endpoint_id, hold)
            return False
        _, work_limit = self._capacities[endpoint_id]
        lease = queue[0]
        reclaim = max(
            0,
            lease.estimated_work - (work_limit - self._active_work[endpoint_id]),
        )
        if reclaim > 0:
            self._guard_holds[endpoint_id] = _GuardHold(
                hold.target_job_id,
                hold.target_request_id,
                hold.head_work,
                reclaim,
                hold.started_at_s,
                hold.constraint_conflict,
            )
            return False
        states = self._bounded_saor_states(endpoint_id)
        self._close_guard_hold(endpoint_id, hold, states=states)
        queue.popleft()
        self._queued_request_keys.remove((lease.job_id, lease.request_id))
        self._recovery_inflight[endpoint_id][lease.job_id] = lease.request_id
        self._activate(endpoint_id, lease)
        self._record_release_event(
            endpoint_id,
            action="grant",
            tier="debt_recovery",
            lease=lease,
            constraint_conflict=hold.constraint_conflict,
            states=states,
        )
        return True

    def _close_guard_hold(
        self,
        endpoint_id: str,
        hold: _GuardHold,
        *,
        states: tuple[SaorBoundedHeadState, ...] | None = None,
    ) -> None:
        self._guard_holds[endpoint_id] = None
        self._record_release_event(
            endpoint_id,
            action="hold_end",
            tier="guard_reclaim_hold",
            target=hold,
            hold_duration_s=max(0.0, self._clock() - hold.started_at_s),
            states=states or self._bounded_saor_states(endpoint_id),
        )

    def _bounded_saor_states(
        self,
        endpoint_id: str,
    ) -> tuple[SaorBoundedHeadState, ...]:
        request_limit, work_limit = self._capacities[endpoint_id]
        active_requests: dict[str, int] = {}
        active_work: dict[str, int] = {}
        for lease in self._active.values():
            if lease.endpoint_id != endpoint_id:
                continue
            active_requests[lease.job_id] = active_requests.get(lease.job_id, 0) + 1
            active_work[lease.job_id] = (
                active_work.get(lease.job_id, 0) + lease.estimated_work
            )
        jobs = set(active_requests)
        jobs.update(
            job_id
            for job_id, queue in self._waiting[endpoint_id].items()
            if queue
        )
        order = {
            job_id: index for index, job_id in enumerate(self._job_order[endpoint_id])
        }
        now = self._clock()
        states = []
        for job_id in sorted(jobs):
            queue = self._waiting[endpoint_id].get(job_id, ())
            head = queue[0] if queue else None
            head_work = head.estimated_work if head is not None else 0
            fits = bool(head) and (
                self._active_requests[endpoint_id] + 1 <= request_limit
                and self._active_work[endpoint_id] + head_work <= work_limit
            )
            states.append(
                SaorBoundedHeadState(
                    release=SaorReleaseState(
                        job_id=job_id,
                        weight=float(self._weights[job_id]),
                        active_requests=active_requests.get(job_id, 0),
                        active_work=active_work.get(job_id, 0),
                        waiting_work=sum(item.estimated_work for item in queue),
                        fairness_debt=self._fairness_debt[endpoint_id].get(
                            job_id, 0.0
                        ),
                        oldest_waiting_age_s=(
                            max(0.0, now - head.enqueued_at_s) if head else 0.0
                        ),
                        slo_target_s=None,
                        arrival_order=order.get(job_id, len(order)),
                        eligible=fits,
                    ),
                    priority=self._priorities[job_id],
                    remaining_slo_budget_s=(
                        head.slo_deadline_s - now
                        if head is not None and head.slo_deadline_s is not None
                        else None
                    ),
                    priority_window_s=self._priority_windows_s.get(job_id) or None,
                    debt_cap=self._fairness_debt_caps.get(job_id) or None,
                    head_work=head_work,
                    ready=head is not None,
                    recovery_inflight=(
                        job_id in self._recovery_inflight[endpoint_id]
                    ),
                )
            )
        return tuple(states)

    def _record_release_event(
        self,
        endpoint_id: str,
        *,
        action: str,
        tier: str,
        states: tuple[SaorBoundedHeadState, ...],
        lease: CreditLease | None = None,
        target: _GuardHold | None = None,
        constraint_conflict: bool = False,
        hold_duration_s: float = 0.0,
    ) -> None:
        debt_critical = {
            state.release.job_id
            for state in states
            if state.ready
            and state.debt_cap is not None
            and state.release.fairness_debt >= state.debt_cap
            and not state.recovery_inflight
        }
        target_is_concrete = (
            target is not None
            and target.target_job_id in debt_critical
            and target.reclaim_debt > 0
        )
        selected_lease = lease if action in {"register", "grant"} else None
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
                    selected_lease.job_id if selected_lease is not None else ""
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
                recovery_inflight_by_job=tuple(
                    sorted(self._recovery_inflight[endpoint_id].items())
                ),
                active_requests=self._active_requests[endpoint_id],
                active_work=self._active_work[endpoint_id],
                avoidable_idle=(action == "hold" and not target_is_concrete),
                foreign_grant_over_debt_critical=(
                    action == "grant"
                    and bool(debt_critical)
                    and lease is not None
                    and lease.job_id not in debt_critical
                ),
            )
        )

    def _update_saor_fairness_debt(
        self,
        endpoint_id: str,
        *,
        completed_job_id: str,
        completed_work: int,
    ) -> None:
        active_jobs = {
            lease.job_id
            for lease in self._active.values()
            if lease.endpoint_id == endpoint_id
        }
        active_jobs.update(
            job_id
            for job_id, queue in self._waiting[endpoint_id].items()
            if queue
        )
        active_jobs.add(completed_job_id)
        total_weight = sum(self._weights[job_id] for job_id in active_jobs)
        debts = self._fairness_debt[endpoint_id]
        for job_id in active_jobs:
            target = completed_work * self._weights[job_id] / total_weight
            received = completed_work if job_id == completed_job_id else 0.0
            debts[job_id] = max(0.0, debts.get(job_id, 0.0) + target - received)

    def _grant_fifo_waiters(self, endpoint_id: str) -> None:
        """Grant one global ready-enqueue FIFO, preserving head-of-line order."""
        request_limit, work_limit = self._capacities[endpoint_id]
        fifo = self._fifo_order[endpoint_id]
        while fifo and self._active_requests[endpoint_id] < request_limit:
            job_id, request_id = fifo[0]
            queue = self._waiting[endpoint_id][job_id]
            if not queue:
                fifo.popleft()
                continue
            lease = queue[0]
            if lease.request_id != request_id:
                raise RuntimeError("FIFO and per-job credit queues diverged")
            if self._active_work[endpoint_id] + lease.estimated_work > work_limit:
                return
            fifo.popleft()
            queue.popleft()
            request_key = (job_id, request_id)
            self._queued_request_keys.remove(request_key)
            self._activate(endpoint_id, lease)

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
