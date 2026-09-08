"""Adapt a single immutable task to the existing admission and endpoint policies."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable, Protocol

from .errors import EndpointCapacityUnavailable
from .models import BatchRequest, TopologySnapshot
from .scheduler import AdmissionPolicy, EndpointRouter, PoolRouter, SharedCreditPolicy
from .session_capacity import TaskRecord
from .session_contract import OfferedTask, SessionSpec, TaskKey


class IncrementalCreditPolicy(SharedCreditPolicy, Protocol):
    incremental_safe: bool

    def forget_finished_job(self, job_id: str) -> None: ...


@dataclass(frozen=True)
class TaskCandidate:
    """Read-only accepted-task view; selectors cannot mutate reservations or phases."""

    key: TaskKey
    task: OfferedTask
    spec: SessionSpec | None = None


def fifo_task(candidates: tuple[TaskCandidate, ...]) -> TaskKey | None:
    return candidates[0].key if candidates else None


@dataclass(frozen=True)
class SessionPolicies:
    """Only local bounded policies are eligible; remote credit adapters stay in legacy."""

    admission: AdmissionPolicy
    router: EndpointRouter
    topology: TopologySnapshot
    pool_id: str
    pool_router: PoolRouter | None = None
    choose_task: Callable[[tuple[TaskCandidate, ...]], TaskKey | None] = fifo_task

    organize: Callable[[tuple[TaskCandidate, ...]], tuple[TaskKey, ...]] | None = None

    def select_task(self, queued: tuple[TaskRecord, ...]) -> TaskRecord | None:
        candidates = tuple(TaskCandidate(record.key, record.task, record.spec) for record in queued)
        key = self.choose_task(candidates)
        if key is None:
            return None
        if (
            type(key) is not TaskKey
            or type(key.session_id) is not int
            or type(key.sequence) is not int
        ):
            raise ValueError("selector returned an invalid task identity")
        for record in queued:
            if record.key == key:
                return record
        raise ValueError("selector returned a task outside the ready window")

    def select_batch(self, queued: tuple[TaskRecord, ...]) -> tuple[TaskRecord, ...]:
        if self.organize is None:
            selected = self.select_task(queued)
            return () if selected is None else (selected,)
        keys = self.organize(tuple(TaskCandidate(r.key, r.task, r.spec) for r in queued))
        if type(keys) is not tuple or len(keys) > len(queued):
            raise ValueError("organizer exceeded its accepted window")
        if any(
            type(k) is not TaskKey or type(k.session_id) is not int or type(k.sequence) is not int
            for k in keys
        ):
            raise ValueError("invalid organized task identity")
        records = {r.key: r for r in queued}
        if len(set(keys)) != len(keys) or any(k not in records for k in keys):
            raise ValueError("organizer returned duplicate or unowned members")
        return tuple(records[k] for k in keys)

    def select(self, record: TaskRecord, active: tuple[TaskRecord, ...], now: float) -> str | None:
        oldest = max((now - r.since for r in active), default=0.0)
        if not self.admission.decide(len(active), hol_age_s=max(0.0, oldest)).allowed:
            return None
        request = BatchRequest(
            request_id=f"{record.key.session_id}:{record.key.sequence}",
            job_id=record.spec.job_id,
            operator=record.spec.operator,
            row_count=1,
            prompt_tokens=0,
            estimated_output_tokens=0,
            prefix_key=record.task.info.work.locality_key if record.task.info else "",
            first_arrival_s=record.since,
            oldest_arrival_s=record.since,
            payload_id=f"{record.key.session_id}:{record.key.sequence}",
            work_units=record.task.estimated_work,
            work_unit=record.spec.work_unit,
            estimated_payload_bytes=len(record.task.payload),
            work_descriptor=record.task.info.work if record.task.info else None,
        )
        endpoints = tuple(
            replace(
                e,
                running=e.running + sum(r.endpoint == e.endpoint_id for r in active),
                estimated_active_work=e.estimated_active_work
                + sum(r.task.estimated_work for r in active if r.endpoint == e.endpoint_id),
            )
            for e in self.topology.endpoints
        )
        topology = replace(self.topology, endpoints=endpoints)
        pool = (
            self.pool_router.route(request, topology).pool_id if self.pool_router else self.pool_id
        )
        try:
            route = self.router.route(request, topology, pool)
        except EndpointCapacityUnavailable:
            return None
        if not any(
            e.endpoint_id == route.endpoint_id and e.pool_id == pool and e.healthy and e.available
            for e in endpoints
        ):
            raise ValueError("policy selected unavailable endpoint")
        return route.endpoint_id
