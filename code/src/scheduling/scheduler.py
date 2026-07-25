"""Synchronous policy-composition scheduler."""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from typing import Iterable, Protocol

from .models import (
    AdmissionDecision,
    CollectedSubmission,
    PayloadEnvelope,
    SubmissionCompletion,
    TopologySnapshot,
)
from .routing import RoundRobinEndpointRouter


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
        router: RoundRobinEndpointRouter,
        adapter: SubmissionAdapter,
        pool_id: str,
    ):
        self.admission = admission
        self.router = router
        self.adapter = adapter
        self.pool_id = pool_id

    def run(
        self,
        envelopes: Iterable[PayloadEnvelope],
        topology: TopologySnapshot,
    ) -> SchedulerResult:
        pending: list[tuple[object, PayloadEnvelope]] = []
        completions: list[SubmissionCompletion] = []
        max_inflight_seen = 0
        bounded_wait_samples: list[float] = []
        fanin_s = 0.0
        submit_s = 0.0

        for envelope in envelopes:
            while not self.admission.decide(len(pending)).allowed:
                collected = self._collect_one(pending, completions)
                bounded_wait_samples.append(collected.wait_s)
                fanin_s += collected.result_s
            route = self.router.route(envelope.request, topology, self.pool_id)
            submit_start = time.perf_counter()
            handle = self.adapter.submit(envelope, route.endpoint_id)
            submit_s += time.perf_counter() - submit_start
            pending.append((handle, envelope))
            max_inflight_seen = max(max_inflight_seen, len(pending))

        while pending:
            collected = self._collect_one(pending, completions)
            fanin_s += collected.result_s

        return SchedulerResult(
            completions=tuple(completions),
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
