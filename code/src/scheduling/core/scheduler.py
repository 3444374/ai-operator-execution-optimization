"""Synchronous policy-composition scheduler."""

from __future__ import annotations

import statistics
import time
from collections.abc import Mapping
from dataclasses import replace
from typing import Callable, Iterable

from .errors import EndpointCapacityUnavailable
from .execution import SubmissionContext, SubmissionExecutionLedger
from .models import (
    BatchRequest,
    CollectedSubmission,
    EndpointSnapshot,
    PayloadEnvelope,
    RoutingDecision,
    SubmissionCompletion,
    TopologySnapshot,
)
from .policy_contracts import (
    AdmissionPolicy as AdmissionPolicy,
    EndpointRouter as EndpointRouter,
    PoolRouter as PoolRouter,
)
from .scheduler_types import (
    DEFAULT_SOURCE_POLL_S,
    SubmissionLifecycle,
    EndpointCapacityConfig as EndpointCapacityConfig,
    SchedulerConfig as SchedulerConfig,
    SchedulerResult as SchedulerResult,
    SubmissionAdapter as SubmissionAdapter,
)
from .topology import with_local_inflight
from .envelope_source import _ConcurrentEnvelopeSource


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
        per_endpoint_work_limit: int | None = None,
        per_endpoint_admission: Mapping[str, AdmissionPolicy] | None = None,
        source_poll_interval_s: float = DEFAULT_SOURCE_POLL_S,
        lifecycle: SubmissionLifecycle | None = None,
        actual_work_extractor: Callable[[SubmissionCompletion], int | None] | None = None,
    ):
        config = SchedulerConfig(
            endpoint_capacity=EndpointCapacityConfig(
                request_limit=per_endpoint_limit,
                work_limit=per_endpoint_work_limit,
                admission_by_endpoint=per_endpoint_admission or {},
            ),
            source_poll_interval_s=source_poll_interval_s,
            lifecycle=lifecycle,
            actual_work_extractor=actual_work_extractor,
        )
        self._initialize(
            admission=admission,
            router=router,
            adapter=adapter,
            pool_id=pool_id,
            pool_router=pool_router,
            epoch_clock=epoch_clock,
            config=config,
        )

    @classmethod
    def from_config(
        cls,
        admission: AdmissionPolicy,
        router: EndpointRouter,
        adapter: SubmissionAdapter,
        pool_id: str,
        *,
        config: SchedulerConfig,
        pool_router: PoolRouter | None = None,
        epoch_clock: Callable[[], float] = time.time,
    ) -> SynchronousScheduler:
        """Construct from grouped policy/capacity contracts."""

        scheduler = cls.__new__(cls)
        scheduler._initialize(
            admission=admission,
            router=router,
            adapter=adapter,
            pool_id=pool_id,
            pool_router=pool_router,
            epoch_clock=epoch_clock,
            config=config,
        )
        return scheduler

    def _initialize(
        self,
        *,
        admission: AdmissionPolicy,
        router: EndpointRouter,
        adapter: SubmissionAdapter,
        pool_id: str,
        pool_router: PoolRouter | None,
        epoch_clock: Callable[[], float],
        config: SchedulerConfig,
    ) -> None:
        endpoint_capacity = config.endpoint_capacity
        self.admission = admission
        self.router = router
        self.adapter = adapter
        self.pool_id = pool_id
        self.pool_router = pool_router
        self.epoch_clock = epoch_clock
        self.per_endpoint_limit = endpoint_capacity.request_limit
        self.per_endpoint_work_limit = endpoint_capacity.work_limit
        self.per_endpoint_admission = dict(endpoint_capacity.admission_by_endpoint)
        self.source_poll_interval_s = config.source_poll_interval_s
        self.lifecycle = config.lifecycle
        self.actual_work_extractor = config.actual_work_extractor

    def run(
        self,
        envelopes: Iterable[PayloadEnvelope],
        topology: TopologySnapshot,
    ) -> SchedulerResult:
        endpoints_by_id = self.validate_topology(topology)
        ledger = SubmissionExecutionLedger(
            actual_work_extractor=self.actual_work_extractor,
        )
        max_inflight_seen = 0
        max_active_work_per_endpoint_seen = 0
        bounded_wait_samples: list[float] = []
        fanin_s = 0.0
        submit_s = 0.0

        source = _ConcurrentEnvelopeSource(
            envelopes,
            poll_interval_s=self.source_poll_interval_s,
        )
        for source_item in source:
            if source_item is _ConcurrentEnvelopeSource._POLL:
                collected = self.poll_one(ledger)
                if collected is not None:
                    bounded_wait_samples.append(collected.wait_s)
                    fanin_s += collected.result_s
                continue
            if not isinstance(source_item, PayloadEnvelope):
                raise ValueError("envelopes must contain PayloadEnvelope values")
            envelope = source_item

            ledger.observe(envelope)
            pool_id: str | None = None
            route: RoutingDecision | None = None
            while True:
                hol_age_s = (
                    ledger.oldest_inflight_age_s(now_s=self.epoch_clock())
                    if ledger.contexts
                    else 0.0
                )
                globally_allowed = self.admission.decide(
                    ledger.inflight_count, hol_age_s=hol_age_s
                ).allowed
                capacity_topology = with_local_inflight(
                    topology,
                    ledger.contexts,
                    per_endpoint_limit=self.per_endpoint_limit,
                    per_endpoint_work_limit=self.per_endpoint_work_limit,
                    request_work=envelope.request.estimated_work_units,
                )
                if self.per_endpoint_admission:
                    capacity_topology = self.topology_with_endpoint_admission(
                        capacity_topology,
                        ledger.contexts,
                    )
                if globally_allowed and any(
                    endpoint.healthy and endpoint.available
                    for endpoint in capacity_topology.endpoints
                ):
                    pool_id = self.select_pool(
                        envelope.request, capacity_topology, endpoints_by_id
                    )
                    try:
                        route = self.router.route(
                            envelope.request,
                            capacity_topology,
                            pool_id,
                        )
                    except EndpointCapacityUnavailable:
                        route = None
                    if route is not None:
                        break
                if not ledger.pending:
                    raise RuntimeError(
                        "admission denied or no endpoint has capacity with no "
                        "in-flight submission to collect; the scheduler cannot "
                        "make progress"
                    )
                collected = self.collect_one(ledger)
                bounded_wait_samples.append(collected.wait_s)
                fanin_s += collected.result_s
            if pool_id is None or route is None:
                raise AssertionError("routing decision missing after admission")
            endpoint = endpoints_by_id.get(route.endpoint_id)
            if endpoint is None:
                raise RuntimeError("router selected an endpoint outside the topology")
            if self.lifecycle is not None:
                def collect_pending():
                    nonlocal fanin_s
                    if not ledger.pending:
                        return False
                    collected = self.collect_one(ledger)
                    bounded_wait_samples.append(collected.wait_s)
                    fanin_s += collected.result_s
                    return True
                self.lifecycle.before_submit(envelope, route.endpoint_id, collect_pending)
            submit_start = time.perf_counter()
            try:
                handle = self.adapter.submit(envelope, route.endpoint_id)
            except Exception:
                if self.lifecycle is not None:
                    self.lifecycle.submit_failed(envelope)
                raise
            submit_s += time.perf_counter() - submit_start
            ledger.submitted(
                handle,
                envelope,
                pool_id=pool_id,
                endpoint_id=route.endpoint_id,
                gpu_id=endpoint.gpu_id,
                submit_epoch_s=self.epoch_clock(),
            )
            max_inflight_seen = max(max_inflight_seen, ledger.inflight_count)
            max_active_work_per_endpoint_seen = max(
                max_active_work_per_endpoint_seen,
                max(ledger.active_work_by_endpoint().values(), default=0),
            )

        while ledger.pending:
            collected = self.collect_one(ledger)
            fanin_s += collected.result_s

        if self.lifecycle is not None:
            self.lifecycle.finish()

        completions = ledger.ordered_completions()
        return SchedulerResult(
            completions=completions,
            submission_events=ledger.ordered_events(),
            operator_invocations=len(completions),
            max_inflight_seen=max_inflight_seen,
            applied_limit=self.admission.limit,
            bounded_wait_s=sum(bounded_wait_samples),
            avg_bounded_wait_s=(
                statistics.mean(bounded_wait_samples) if bounded_wait_samples else 0.0
            ),
            fanin_s=fanin_s,
            submit_s=submit_s,
            max_active_work_per_endpoint_seen=(max_active_work_per_endpoint_seen),
        )

    def validate_topology(self, topology: TopologySnapshot) -> dict[str, EndpointSnapshot]:
        endpoints_by_id = {endpoint.endpoint_id: endpoint for endpoint in topology.endpoints}
        unknown_admission_endpoints = set(self.per_endpoint_admission) - set(endpoints_by_id)
        if unknown_admission_endpoints:
            raise ValueError(
                "per-endpoint admission contains endpoints outside topology: "
                + ", ".join(sorted(unknown_admission_endpoints))
            )
        if self.per_endpoint_admission and (
            set(self.per_endpoint_admission) != set(endpoints_by_id)
        ):
            raise ValueError(
                "per-endpoint admission must define exactly one policy for every topology endpoint"
            )
        return endpoints_by_id

    def select_pool(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
        endpoints_by_id: Mapping[str, EndpointSnapshot],
    ) -> str:
        preferred_endpoint = endpoints_by_id.get(request.preferred_endpoint_id)
        if preferred_endpoint is not None:
            return preferred_endpoint.pool_id
        if self.pool_router is not None:
            return self.pool_router.route(request, topology).pool_id
        return self.pool_id

    def topology_with_endpoint_admission(
        self,
        topology: TopologySnapshot,
        submission_context: Mapping[str, SubmissionContext],
    ) -> TopologySnapshot:
        inflight_by_endpoint: dict[str, int] = {}
        oldest_submit_by_endpoint: dict[str, float] = {}
        for item in submission_context.values():
            endpoint_id = item.endpoint_id
            submit_epoch_s = item.submit_epoch_s
            inflight_by_endpoint[endpoint_id] = inflight_by_endpoint.get(endpoint_id, 0) + 1
            oldest_submit_by_endpoint[endpoint_id] = min(
                submit_epoch_s,
                oldest_submit_by_endpoint.get(endpoint_id, submit_epoch_s),
            )
        now = self.epoch_clock()
        return replace(
            topology,
            endpoints=tuple(
                replace(
                    endpoint,
                    available=(
                        endpoint.available
                        and self.per_endpoint_admission[endpoint.endpoint_id]
                        .decide(
                            inflight_by_endpoint.get(endpoint.endpoint_id, 0),
                            hol_age_s=max(
                                0.0,
                                now
                                - oldest_submit_by_endpoint.get(
                                    endpoint.endpoint_id,
                                    now,
                                ),
                            ),
                        )
                        .allowed
                    ),
                )
                for endpoint in topology.endpoints
            ),
        )

    def collect_one(
        self,
        ledger: SubmissionExecutionLedger,
    ) -> CollectedSubmission:
        collected = self.adapter.wait_one(ledger.pending)
        return self._record_collected(collected, ledger)

    def _record_collected(
        self,
        collected: CollectedSubmission,
        ledger: SubmissionExecutionLedger,
    ) -> CollectedSubmission:
        recorded = ledger.record(
            collected,
            completion_epoch_s=self.epoch_clock(),
        )
        if self.lifecycle is not None:
            self.lifecycle.completed(recorded)
        return collected

    def poll_one(
        self,
        ledger: SubmissionExecutionLedger,
    ) -> CollectedSubmission | None:
        if not ledger.pending:
            return None
        poll_one = getattr(self.adapter, "poll_one", None)
        if not callable(poll_one):
            return None
        collected = poll_one(ledger.pending)
        if collected is None:
            return None
        return self._record_collected(collected, ledger)
