"""Synchronous policy-composition scheduler."""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass, replace
from typing import Callable, Iterable, Protocol

from .models import (
    AdmissionDecision,
    BatchRequest,
    CollectedSubmission,
    PayloadEnvelope,
    PoolRoutingDecision,
    RoutingDecision,
    SubmissionCompletion,
    SubmissionLifecycleEvent,
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

    def decide(
        self, inflight: int, *, hol_age_s: float | None = None
    ) -> AdmissionDecision:
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
    submission_events: tuple[SubmissionLifecycleEvent, ...] = ()


class SynchronousScheduler:
    def __init__(
        self,
        admission: AdmissionPolicy,
        router: EndpointRouter,
        adapter: SubmissionAdapter,
        pool_id: str,
        *,
        pool_router: PoolRouter | None = None,
        epoch_clock: Callable[[], float] = time.time,
        per_endpoint_limit: int | None = None,
    ):
        if per_endpoint_limit is not None and per_endpoint_limit <= 0:
            raise ValueError("per_endpoint_limit must be positive")
        self.admission = admission
        self.router = router
        self.adapter = adapter
        self.pool_id = pool_id
        self.pool_router = pool_router
        self.epoch_clock = epoch_clock
        self.per_endpoint_limit = per_endpoint_limit

    def run(
        self,
        envelopes: Iterable[PayloadEnvelope],
        topology: TopologySnapshot,
    ) -> SchedulerResult:
        pending: list[tuple[object, PayloadEnvelope]] = []
        completions: list[SubmissionCompletion] = []
        submission_events: list[SubmissionLifecycleEvent] = []
        submission_context: dict[str, tuple[str, str, str, float]] = {}
        submission_order: dict[str, int] = {}
        endpoints_by_id = {
            endpoint.endpoint_id: endpoint for endpoint in topology.endpoints
        }
        max_inflight_seen = 0
        bounded_wait_samples: list[float] = []
        fanin_s = 0.0
        submit_s = 0.0

        for envelope in envelopes:
            request_id = envelope.request.request_id
            if request_id in submission_order:
                raise ValueError(f"duplicate request_id: {request_id}")
            submission_order[request_id] = len(submission_order)
            while True:
                hol_age_s = self._head_of_line_age_s(submission_context)
                globally_allowed = self.admission.decide(
                    len(pending), hol_age_s=hol_age_s
                ).allowed
                capacity_topology = self._topology_with_local_inflight(
                    topology,
                    submission_context,
                    per_endpoint_limit=self.per_endpoint_limit,
                )
                if globally_allowed and any(
                    endpoint.healthy for endpoint in capacity_topology.endpoints
                ):
                    break
                if not pending:
                    raise RuntimeError(
                        "admission denied or no endpoint has capacity with no "
                        "in-flight submission to collect; the scheduler cannot "
                        "make progress"
                    )
                collected = self._collect_one(
                    pending,
                    completions,
                    submission_events,
                    submission_context,
                )
                bounded_wait_samples.append(collected.wait_s)
                fanin_s += collected.result_s
            pool_id = (
                self.pool_router.route(
                    envelope.request,
                    self._topology_with_local_inflight(
                        topology,
                        submission_context,
                        per_endpoint_limit=self.per_endpoint_limit,
                    ),
                ).pool_id
                if self.pool_router is not None
                else self.pool_id
            )
            route = self.router.route(
                envelope.request,
                self._topology_with_local_inflight(
                    topology,
                    submission_context,
                    per_endpoint_limit=self.per_endpoint_limit,
                ),
                pool_id,
            )
            endpoint = endpoints_by_id.get(route.endpoint_id)
            if endpoint is None:
                raise RuntimeError("router selected an endpoint outside the topology")
            submit_start = time.perf_counter()
            handle = self.adapter.submit(envelope, route.endpoint_id)
            submit_s += time.perf_counter() - submit_start
            submission_context[request_id] = (
                pool_id,
                route.endpoint_id,
                endpoint.gpu_id,
                self.epoch_clock(),
            )
            pending.append((handle, envelope))
            max_inflight_seen = max(max_inflight_seen, len(pending))

        while pending:
            collected = self._collect_one(
                pending,
                completions,
                submission_events,
                submission_context,
            )
            fanin_s += collected.result_s

        return SchedulerResult(
            completions=tuple(
                sorted(
                    completions,
                    key=lambda completion: submission_order[completion.request_id],
                )
            ),
            submission_events=tuple(
                sorted(
                    submission_events,
                    key=lambda event: submission_order[event.submission_id],
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

    def _head_of_line_age_s(
        self,
        submission_context: dict[str, tuple[str, str, str, float]],
    ) -> float:
        # This is the age of the oldest in-flight submission, including both
        # Ray-side waiting and normal endpoint service time. It is directly
        # observable but must not be interpreted as pure queue delay.
        if not submission_context:
            return 0.0
        now = self.epoch_clock()
        oldest_submit_s = min(
            submit_epoch_s
            for _pool_id, _endpoint_id, _gpu_id, submit_epoch_s in submission_context.values()
        )
        return max(0.0, now - oldest_submit_s)

    @staticmethod
    def _topology_with_local_inflight(
        topology: TopologySnapshot,
        submission_context: dict[str, tuple[str, str, str, float]],
        *,
        per_endpoint_limit: int | None = None,
    ) -> TopologySnapshot:
        inflight_by_endpoint: dict[str, int] = {}
        for _pool_id, endpoint_id, _gpu_id, _submit_epoch_s in (
            submission_context.values()
        ):
            inflight_by_endpoint[endpoint_id] = (
                inflight_by_endpoint.get(endpoint_id, 0) + 1
            )
        return replace(
            topology,
            endpoints=tuple(
                replace(
                    endpoint,
                    healthy=(
                        endpoint.healthy
                        and (
                            per_endpoint_limit is None
                            or inflight_by_endpoint.get(endpoint.endpoint_id, 0)
                            < per_endpoint_limit
                        )
                    ),
                    running=(
                        endpoint.running
                        + inflight_by_endpoint.get(endpoint.endpoint_id, 0)
                    ),
                )
                for endpoint in topology.endpoints
            ),
        )

    def _collect_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
        completions: list[SubmissionCompletion],
        submission_events: list[SubmissionLifecycleEvent],
        submission_context: dict[str, tuple[str, str, str, float]],
    ) -> CollectedSubmission:
        collected = self.adapter.wait_one(pending)
        completion_epoch_s = self.epoch_clock()
        matching = [
            index for index, (item, _) in enumerate(pending) if item is collected.handle
        ]
        if len(matching) != 1:
            raise RuntimeError("adapter returned an unknown or duplicate pending handle")
        _, pending_envelope = pending.pop(matching[0])
        if collected.completion.request_id != pending_envelope.request.request_id:
            raise RuntimeError("completion request_id does not match pending request")
        completions.append(collected.completion)
        request_id = collected.completion.request_id
        pool_id, endpoint_id, gpu_id, submit_epoch_s = submission_context.pop(
            request_id
        )
        submission_events.append(
            SubmissionLifecycleEvent(
                submission_id=request_id,
                pool_id=pool_id,
                endpoint_id=endpoint_id,
                gpu_id=gpu_id,
                submit_epoch_s=submit_epoch_s,
                completion_epoch_s=completion_epoch_s,
                status=collected.completion.status,
                error=collected.completion.error,
            )
        )
        return collected
