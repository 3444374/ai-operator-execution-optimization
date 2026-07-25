"""Synchronous policy-composition scheduler."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from .admission import StaticAdmissionController
from .models import PayloadEnvelope, SubmissionCompletion, TopologySnapshot
from .routing import RoundRobinEndpointRouter


class SubmissionAdapter(Protocol):
    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object:
        ...

    def wait_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
    ) -> tuple[object, SubmissionCompletion]:
        ...


@dataclass(frozen=True)
class SchedulerResult:
    completions: tuple[SubmissionCompletion, ...]
    max_inflight_seen: int


class SynchronousScheduler:
    def __init__(
        self,
        admission: StaticAdmissionController,
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

        for envelope in envelopes:
            while not self.admission.decide(len(pending)).allowed:
                self._collect_one(pending, completions)
            route = self.router.route(envelope.request, topology, self.pool_id)
            handle = self.adapter.submit(envelope, route.endpoint_id)
            pending.append((handle, envelope))
            max_inflight_seen = max(max_inflight_seen, len(pending))

        while pending:
            self._collect_one(pending, completions)

        return SchedulerResult(tuple(completions), max_inflight_seen)

    def _collect_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
        completions: list[SubmissionCompletion],
    ) -> None:
        handle, completion = self.adapter.wait_one(pending)
        matching = [index for index, (item, _) in enumerate(pending) if item == handle]
        if len(matching) != 1:
            raise RuntimeError("adapter returned an unknown or duplicate pending handle")
        pending.pop(matching[0])
        completions.append(completion)
