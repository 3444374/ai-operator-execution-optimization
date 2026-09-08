"""Admission, routing and shared-credit interfaces consumed by both execution loops."""

from typing import Protocol
from .models import (
    AdmissionDecision,
    BatchRequest,
    PoolRoutingDecision,
    RoutingDecision,
    TopologySnapshot,
)


class AdmissionPolicy(Protocol):
    limit: int

    def decide(self, inflight: int, *, hol_age_s: float | None = None) -> AdmissionDecision: ...


class PoolRouter(Protocol):
    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
    ) -> PoolRoutingDecision: ...


class EndpointRouter(Protocol):
    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
        pool_id: str,
    ) -> RoutingDecision: ...


class SharedCreditPolicy(Protocol):
    def try_acquire(
        self,
        *,
        request_id: str,
        job_id: str,
        endpoint_id: str,
        estimated_work: int,
        weight: int = 1,
        priority: int = 0,
        slo_budget_remaining_s: float | None = None,
        priority_window_s: float | None = None,
        fairness_debt_cap: float | None = None,
    ) -> bool: ...

    def release(
        self,
        request_id: str,
        *,
        job_id: str,
        actual_work: int | None = None,
    ) -> None: ...

    def finish_job(self, job_id: str) -> None: ...

    def cancel_waiter(self, request_id: str, *, job_id: str) -> bool: ...
