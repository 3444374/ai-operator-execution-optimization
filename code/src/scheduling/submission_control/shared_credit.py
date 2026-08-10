"""Shared endpoint-local work credits for concurrent database AI jobs."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class CreditLease:
    request_id: str
    job_id: str
    endpoint_id: str
    estimated_work: int

    def __post_init__(self) -> None:
        if not self.request_id or not self.job_id or not self.endpoint_id:
            raise ValueError("lease identifiers must be non-empty")
        if self.estimated_work <= 0:
            raise ValueError("estimated_work must be positive")


@dataclass(frozen=True)
class EndpointCreditSnapshot:
    endpoint_id: str
    active_requests: int
    active_work: int
    waiting_requests: int
    waiting_work: int
    active_by_job: tuple[tuple[str, int], ...]
    active_work_by_job: tuple[tuple[str, int], ...]
    waiting_by_job: tuple[tuple[str, int], ...]
    waiting_work_by_job: tuple[tuple[str, int], ...]
    max_active_requests_seen: int
    max_active_work_seen: int
    granted_requests_by_job: tuple[tuple[str, int], ...]
    granted_work_by_job: tuple[tuple[str, int], ...]


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
    ) -> None:
        if not capacities:
            raise ValueError("capacities must not be empty")
        if quantum <= 0:
            raise ValueError("quantum must be positive")
        if policy not in {"drr", "fifo"}:
            raise ValueError("shared credit policy must be drr or fifo")
        for endpoint_id, (request_limit, work_limit) in capacities.items():
            if not endpoint_id:
                raise ValueError("endpoint IDs must be non-empty")
            if request_limit <= 0 or work_limit <= 0:
                raise ValueError("endpoint capacity limits must be positive")
        self._capacities = dict(capacities)
        self._quantum = quantum
        self._policy = policy
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

    def try_acquire(
        self,
        *,
        request_id: str,
        job_id: str,
        endpoint_id: str,
        estimated_work: int,
        weight: int = 1,
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
        if request_key not in self._queued_request_keys:
            lease = CreditLease(
                request_id,
                job_id,
                endpoint_id,
                estimated_work,
            )
            job_queues = self._waiting[endpoint_id]
            if job_id not in job_queues:
                job_queues[job_id] = deque()
                self._job_order[endpoint_id].append(job_id)
            job_queues[job_id].append(lease)
            self._queued_request_keys.add(request_key)
            if self._policy == "fifo":
                self._fifo_order[endpoint_id].append(request_key)
        self._grant_waiters(endpoint_id)
        return request_key in self._active

    def release(self, request_id: str, *, job_id: str) -> None:
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
        return EndpointCreditSnapshot(
            endpoint_id=endpoint_id,
            active_requests=self._active_requests[endpoint_id],
            active_work=self._active_work[endpoint_id],
            waiting_requests=sum(waiting_by_job.values()),
            waiting_work=sum(waiting_work_by_job.values()),
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
        )

    def _grant_waiters(self, endpoint_id: str) -> None:
        if self._policy == "fifo":
            self._grant_fifo_waiters(endpoint_id)
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

    def _grant_fifo_waiters(self, endpoint_id: str) -> None:
        """Grant one global arrival-ordered queue without work borrowing."""
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
