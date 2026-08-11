"""Engine-neutral multi-job queues and monotonic ordered release state."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Mapping

from ..core.control import CapacityArm
from ..core.models import PayloadEnvelope
from .saor import (
    SaorDecision,
    SaorJobState,
    SaorReleaseCandidate,
)


@dataclass(frozen=True)
class ReleasedSubmission:
    release_seq: int
    endpoint_id: str
    envelope: PayloadEnvelope

    @property
    def request_id(self) -> str:
        return self.envelope.request.request_id

    @property
    def job_id(self) -> str:
        return self.envelope.request.job_id

    @property
    def estimated_work(self) -> int:
        return self.envelope.request.estimated_work_units


@dataclass(frozen=True)
class OrderedReleaseSnapshot:
    ready_requests: int
    ready_work: int
    active_requests: int
    active_work: int
    ready_by_job: tuple[tuple[str, int, int], ...]
    active_by_job: tuple[tuple[str, int, int], ...]
    active_by_endpoint: tuple[tuple[str, int, int], ...]
    current_arms: tuple[tuple[str, CapacityArm], ...]
    next_release_seq: int


class OrderedReleaseCoordinator:
    """Own ready/active state; policies only choose validated release actions."""

    def __init__(self, endpoint_ids: tuple[str, ...]) -> None:
        if not endpoint_ids or any(not item for item in endpoint_ids):
            raise ValueError("endpoint IDs must be non-empty")
        if len(endpoint_ids) != len(set(endpoint_ids)):
            raise ValueError("endpoint IDs must be unique")
        self._endpoint_ids = endpoint_ids
        self._ready: dict[str, deque[PayloadEnvelope]] = {}
        self._active: dict[str, ReleasedSubmission] = {}
        self._seen_request_ids: set[str] = set()
        self._completed_work: dict[str, int] = {}
        self._current_arms: dict[str, CapacityArm] = {}
        self._next_release_seq = 0

    def enqueue(self, envelope: PayloadEnvelope) -> None:
        request_id = envelope.request.request_id
        if request_id in self._seen_request_ids:
            raise ValueError(f"duplicate request_id: {request_id}")
        self._seen_request_ids.add(request_id)
        self._ready.setdefault(envelope.request.job_id, deque()).append(envelope)

    def ready_heads(
        self,
        endpoint_id: str,
    ) -> tuple[SaorReleaseCandidate, ...]:
        self._validate_endpoint(endpoint_id)
        heads = []
        for job_id, queue in self._ready.items():
            if not queue:
                continue
            request = queue[0].request
            if (
                request.preferred_endpoint_id
                and request.preferred_endpoint_id != endpoint_id
            ):
                continue
            heads.append(
                SaorReleaseCandidate(
                    request.request_id,
                    job_id,
                    endpoint_id,
                    request.estimated_work_units,
                )
            )
        return tuple(heads)

    def job_states(
        self,
        weights: Mapping[str, float],
        fairness_debts: Mapping[str, float] | None = None,
    ) -> tuple[SaorJobState, ...]:
        debts = fairness_debts or {}
        job_ids = set(self._ready) | {item.job_id for item in self._active.values()}
        missing = job_ids - set(weights)
        if missing:
            raise ValueError(f"missing weights for jobs: {sorted(missing)}")
        return tuple(
            SaorJobState(
                job_id,
                float(weights[job_id]),
                ready_work=sum(
                    item.request.estimated_work_units
                    for item in self._ready.get(job_id, ())
                ),
                active_work=sum(
                    item.estimated_work
                    for item in self._active.values()
                    if item.job_id == job_id
                ),
                fairness_debt=float(debts.get(job_id, 0.0)),
            )
            for job_id in sorted(job_ids)
        )

    def publish(
        self,
        decision: SaorDecision,
    ) -> tuple[ReleasedSubmission, ...]:
        action = decision.action
        self._validate_endpoint(action.endpoint_id)
        planned = self._validate_action(action.endpoint_id, action.arm, action.releases)
        self._current_arms[action.endpoint_id] = action.arm
        released = []
        for candidate, envelope in planned:
            self._ready[candidate.job_id].popleft()
            item = ReleasedSubmission(
                self._next_release_seq,
                candidate.endpoint_id,
                envelope,
            )
            self._next_release_seq += 1
            self._active[candidate.request_id] = item
            released.append(item)
        return tuple(released)

    def complete(
        self,
        request_id: str,
        *,
        actual_work: int | None = None,
        succeeded: bool = True,
    ) -> ReleasedSubmission:
        if actual_work is not None and (
            not isinstance(actual_work, int)
            or isinstance(actual_work, bool)
            or actual_work <= 0
        ):
            raise ValueError("actual_work must be a positive integer")
        try:
            released = self._active[request_id]
        except KeyError as exc:
            raise ValueError(f"request is not active: {request_id}") from exc
        del self._active[request_id]
        if succeeded:
            work = released.estimated_work if actual_work is None else actual_work
            self._completed_work[released.job_id] = (
                self._completed_work.get(released.job_id, 0) + work
            )
        return released

    def drain_completed_work(self) -> dict[str, int]:
        completed = self._completed_work
        self._completed_work = {}
        return completed

    def active_requests(self, endpoint_id: str) -> int:
        self._validate_endpoint(endpoint_id)
        return sum(
            item.endpoint_id == endpoint_id for item in self._active.values()
        )

    def active_work(self, endpoint_id: str) -> int:
        self._validate_endpoint(endpoint_id)
        return sum(
            item.estimated_work
            for item in self._active.values()
            if item.endpoint_id == endpoint_id
        )

    def snapshot(self) -> OrderedReleaseSnapshot:
        ready_by_job = tuple(
            (
                job_id,
                len(queue),
                sum(item.request.estimated_work_units for item in queue),
            )
            for job_id, queue in sorted(self._ready.items())
            if queue
        )
        active_jobs = sorted({item.job_id for item in self._active.values()})
        active_by_job = tuple(
            (
                job_id,
                sum(item.job_id == job_id for item in self._active.values()),
                sum(
                    item.estimated_work
                    for item in self._active.values()
                    if item.job_id == job_id
                ),
            )
            for job_id in active_jobs
        )
        active_by_endpoint = tuple(
            (
                endpoint_id,
                self.active_requests(endpoint_id),
                self.active_work(endpoint_id),
            )
            for endpoint_id in self._endpoint_ids
        )
        return OrderedReleaseSnapshot(
            ready_requests=sum(item[1] for item in ready_by_job),
            ready_work=sum(item[2] for item in ready_by_job),
            active_requests=len(self._active),
            active_work=sum(item.estimated_work for item in self._active.values()),
            ready_by_job=ready_by_job,
            active_by_job=active_by_job,
            active_by_endpoint=active_by_endpoint,
            current_arms=tuple(sorted(self._current_arms.items())),
            next_release_seq=self._next_release_seq,
        )

    def _validate_action(
        self,
        endpoint_id: str,
        arm: CapacityArm,
        releases: tuple[SaorReleaseCandidate, ...],
    ) -> list[tuple[SaorReleaseCandidate, PayloadEnvelope]]:
        request_count = self.active_requests(endpoint_id)
        work = self.active_work(endpoint_id)
        offsets: dict[str, int] = {}
        planned = []
        for candidate in releases:
            queue = self._ready.get(candidate.job_id)
            offset = offsets.get(candidate.job_id, 0)
            if not queue or offset >= len(queue):
                raise ValueError("selected request is not at its Job queue head")
            envelope = queue[offset]
            request = envelope.request
            if request.request_id != candidate.request_id:
                raise ValueError("selected request is not at its Job queue head")
            if candidate.endpoint_id != endpoint_id:
                raise ValueError("release targets the wrong endpoint")
            if (
                request.preferred_endpoint_id
                and request.preferred_endpoint_id != endpoint_id
            ):
                raise ValueError("release violates preferred endpoint")
            if (
                request.job_id != candidate.job_id
                or request.estimated_work_units != candidate.estimated_work
            ):
                raise ValueError("release metadata does not match queued request")
            request_count += 1
            work += candidate.estimated_work
            if request_count > arm.request_limit or work > arm.work_limit:
                raise ValueError("release exceeds selected capacity arm")
            offsets[candidate.job_id] = offset + 1
            planned.append((candidate, envelope))
        return planned

    def _validate_endpoint(self, endpoint_id: str) -> None:
        if endpoint_id not in self._endpoint_ids:
            raise ValueError(f"unknown endpoint_id: {endpoint_id}")
