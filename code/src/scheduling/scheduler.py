"""Synchronous policy-composition scheduler."""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from typing import Iterable, Protocol

from .models import (
    AdmissionDecision,
    BatchRequest,
    CollectedSubmission,
    PayloadEnvelope,
    PoolRoutingDecision,
    RoutingDecision,
    SubmissionCompletion,
    TopologySnapshot,
)


class SubmissionAdapter(Protocol):
    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object:
        ...

    def wait_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
    ) -> CollectedSubmission:
        ...


class AdmissionPolicy(Protocol):
    limit: int

    def decide(self, inflight: int) -> AdmissionDecision:
        ...


class PoolRouter(Protocol):
    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
    ) -> PoolRoutingDecision:
        ...


class EndpointRouter(Protocol):
    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
        pool_id: str,
    ) -> RoutingDecision:
        ...


@dataclass(frozen=True)
class SchedulerResult:
    completions: tuple[SubmissionCompletion, ...]
    operator_invocations: int
    max_inflight_seen: int
    applied_limit: int
    bounded_wait_s: float
    avg_bounded_wait_s: float
    fanin_s: float
    submit_s: float


class SynchronousScheduler:
    def __init__(
        self,
        admission: AdmissionPolicy,
        router: EndpointRouter,
        adapter: SubmissionAdapter,
        pool_id: str,
        *,
        pool_router: PoolRouter | None = None,
    ):
        self.admission = admission
        self.router = router
        self.adapter = adapter
        self.pool_id = pool_id
        self.pool_router = pool_router

    def run(
        self,
        envelopes: Iterable[PayloadEnvelope],
        topology: TopologySnapshot,
    ) -> SchedulerResult:
        pending: list[tuple[object, PayloadEnvelope]] = []
        completions: list[SubmissionCompletion] = []
        submission_order: dict[str, int] = {}
        max_inflight_seen = 0
        bounded_wait_samples: list[float] = []
        fanin_s = 0.0
        submit_s = 0.0

        for envelope in envelopes:
            request_id = envelope.request.request_id
            if request_id in submission_order:
                raise ValueError(f"duplicate request_id: {request_id}")
            submission_order[request_id] = len(submission_order)
            while not self.admission.decide(len(pending)).allowed:
                collected = self._collect_one(pending, completions)
                bounded_wait_samples.append(collected.wait_s)
                fanin_s += collected.result_s
            pool_id = (
                self.pool_router.route(envelope.request, topology).pool_id
                if self.pool_router is not None
                else self.pool_id
            )
            route = self.router.route(envelope.request, topology, pool_id)
            submit_start = time.perf_counter()
            handle = self.adapter.submit(envelope, route.endpoint_id)
            submit_s += time.perf_counter() - submit_start
            pending.append((handle, envelope))
            max_inflight_seen = max(max_inflight_seen, len(pending))

        while pending:
            collected = self._collect_one(pending, completions)
            fanin_s += collected.result_s

        return SchedulerResult(
            completions=tuple(
                sorted(
                    completions,
                    key=lambda completion: submission_order[completion.request_id],
                )
            ),
            operator_invocations=len(completions),
            max_inflight_seen=max_inflight_seen,
            applied_limit=self.admission.limit,
            bounded_wait_s=sum(bounded_wait_samples),
            avg_bounded_wait_s=(
                statistics.mean(bounded_wait_samples) if bounded_wait_samples else 0.0
            ),
            fanin_s=fanin_s,
            submit_s=submit_s,
        )

    def _collect_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
        completions: list[SubmissionCompletion],
    ) -> CollectedSubmission:
        collected = self.adapter.wait_one(pending)
        matching = [
            index for index, (item, _) in enumerate(pending) if item == collected.handle
        ]
        if len(matching) != 1:
            raise RuntimeError("adapter returned an unknown or duplicate pending handle")
        _, pending_envelope = pending.pop(matching[0])
        if collected.completion.request_id != pending_envelope.request.request_id:
            raise RuntimeError("completion request_id does not match pending request")
        completions.append(collected.completion)
        return collected
