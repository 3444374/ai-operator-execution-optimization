"""Shared endpoint-local work credits for concurrent database AI jobs."""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from collections.abc import Callable

from .saor import (
    SaorReleaseConfig,
    SaorReleaseState,
    select_saor_release_job,
)


@dataclass(frozen=True)
class CreditLease:
    request_id: str
    job_id: str
    endpoint_id: str
    estimated_work: int
    enqueued_at_s: float = 0.0

    def __post_init__(self) -> None:
        if not self.request_id or not self.job_id or not self.endpoint_id:
            raise ValueError("lease identifiers must be non-empty")
        if self.estimated_work <= 0:
            raise ValueError("estimated_work must be positive")
        if not math.isfinite(self.enqueued_at_s) or self.enqueued_at_s < 0:
            raise ValueError("enqueued_at_s must be finite and non-negative")


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
    max_active_requests_seen: int
    max_active_work_seen: int
    granted_requests_by_job: tuple[tuple[str, int], ...]
    granted_work_by_job: tuple[tuple[str, int], ...]
    attained_service_by_job: tuple[tuple[str, int], ...]
    fairness_debt_by_job: tuple[tuple[str, float], ...]


class FairEndpointCreditCoordinator:
    """Shared endpoint credit with DRR or an arrival-ordered FIFO control.

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
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not capacities:
            raise ValueError("capacities must not be empty")
        if quantum <= 0:
            raise ValueError("quantum must be positive")
        if policy not in {"drr", "fifo", "vtc", "saor"}:
            raise ValueError(
                "shared credit policy must be drr, fifo, vtc, or saor"
            )
        if policy == "saor" and saor_release_config is None:
            raise ValueError("saor shared credit requires release configuration")
        if policy != "saor" and saor_release_config is not None:
            raise ValueError("SAOR release configuration is only valid for saor")
        for endpoint_id, (request_limit, work_limit) in capacities.items():
            if not endpoint_id:
                raise ValueError("endpoint IDs must be non-empty")
            if request_limit <= 0 or work_limit <= 0:
                raise ValueError("endpoint capacity limits must be positive")
        self._capacities = dict(capacities)
        self._quantum = quantum
        self._policy = policy
        self._saor_release_config = saor_release_config
        self._clock = clock
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

    def try_acquire(
        self,
        *,
        request_id: str,
        job_id: str,
        endpoint_id: str,
        estimated_work: int,
        weight: int = 1,
        slo_target_s: float | None = None,
    ) -> bool:
        request_key = (job_id, request_id)
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
        previous_weight = self._weights.setdefault(job_id, weight)
        if previous_weight != weight:
            raise ValueError("a job must use one stable weight")
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
        if request_key not in self._queued_request_keys:
            lease = CreditLease(
                request_id,
                job_id,
                endpoint_id,
                estimated_work,
                self._clock(),
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
        self._grant_waiters(endpoint_id)
        return request_key in self._active

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
        if self._policy == "saor":
            self._update_saor_fairness_debt(
                endpoint_id,
                completed_job_id=job_id,
                completed_work=(
                    lease.estimated_work if actual_work is None else actual_work
                ),
            )
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
        )

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
            granted_requests[lease.job_id] = (
                granted_requests.get(lease.job_id, 0) + 1
            )
            granted_work = self._granted_work[endpoint_id]
            granted_work[lease.job_id] = (
                granted_work.get(lease.job_id, 0)
                + lease.estimated_work
            )
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
