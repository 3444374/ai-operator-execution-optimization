"""Synchronous policy-composition scheduler."""

from __future__ import annotations

import math
import statistics
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from queue import Empty, Full, Queue
from typing import Callable, Iterable, Iterator, Protocol

from .errors import EndpointCapacityUnavailable
from .execution import (
    SubmissionContext,
    SubmissionExecutionLedger,
)
from .models import (
    AdmissionDecision,
    BatchRequest,
    CollectedSubmission,
    EndpointSnapshot,
    PayloadEnvelope,
    PoolRoutingDecision,
    RoutingDecision,
    SubmissionCompletion,
    SubmissionLifecycleEvent,
    TopologySnapshot,
)
from .ready_window import BoundedReadyWindow, ReadySubmission


class SubmissionAdapter(Protocol):
    def submit(self, envelope: PayloadEnvelope, endpoint_id: str) -> object:
        ...

    def wait_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
    ) -> CollectedSubmission:
        ...

    def poll_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
    ) -> CollectedSubmission | None:
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
    ) -> bool:
        ...

    def release(
        self,
        request_id: str,
        *,
        job_id: str,
        actual_work: int | None = None,
    ) -> None:
        ...

    def finish_job(self, job_id: str) -> None:
        ...

    def cancel_waiter(self, request_id: str, *, job_id: str) -> bool:
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
    max_active_work_per_endpoint_seen: int = 0
    max_ready_requests_seen: int = 0
    max_ready_work_seen: int = 0
    submission_events: tuple[SubmissionLifecycleEvent, ...] = ()


@dataclass(frozen=True)
class _SourceFailure:
    error: BaseException


class _ConcurrentEnvelopeSource:
    """Expose a blocking input iterator without blocking completion polling."""

    _END = object()
    _POLL = object()

    def __init__(
        self,
        envelopes: Iterable[PayloadEnvelope],
        *,
        poll_interval_s: float,
    ) -> None:
        self._queue: Queue[object] = Queue(maxsize=1)
        self._cancelled = threading.Event()
        self._poll_interval_s = poll_interval_s
        self._thread = threading.Thread(
            target=self._produce,
            args=(envelopes,),
            daemon=True,
            name="scheduler-envelope-source",
        )
        self._thread.start()

    def __iter__(self) -> Iterator[object]:
        try:
            while True:
                try:
                    item = self._queue.get(timeout=self._poll_interval_s)
                except Empty:
                    yield self._POLL
                    continue
                if item is self._END:
                    return
                if isinstance(item, _SourceFailure):
                    raise item.error
                yield item
        finally:
            self.close()

    def close(self) -> None:
        self._cancelled.set()
        self._thread.join(timeout=0.1)

    def _produce(self, envelopes: Iterable[PayloadEnvelope]) -> None:
        try:
            for envelope in envelopes:
                if not self._put(envelope):
                    return
        except BaseException as exc:
            self._put(_SourceFailure(exc))
        finally:
            self._put(self._END)

    def _put(self, item: object) -> bool:
        while not self._cancelled.is_set():
            try:
                self._queue.put(item, timeout=0.01)
                return True
            except Full:
                continue
        return False


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
        shared_credit: SharedCreditPolicy | None = None,
        shared_credit_poll_s: float = 0.001,
        shared_credit_acquire_timeout_s: float | None = None,
        shared_ready_request_limit: int = 1,
        shared_ready_work_limit: int | None = None,
        job_weight: int = 1,
        job_priority: int = 0,
        job_slo_target_s: float | None = None,
        job_priority_window_s: float | None = None,
        job_fairness_debt_cap: float | None = None,
        actual_work_extractor: Callable[
            [SubmissionCompletion], int | None
        ] | None = None,
    ):
        if per_endpoint_limit is not None and per_endpoint_limit <= 0:
            raise ValueError("per_endpoint_limit must be positive")
        if per_endpoint_work_limit is not None and per_endpoint_work_limit <= 0:
            raise ValueError("per_endpoint_work_limit must be positive")
        if shared_credit_poll_s <= 0:
            raise ValueError("shared_credit_poll_s must be positive")
        if (
            not isinstance(shared_ready_request_limit, int)
            or isinstance(shared_ready_request_limit, bool)
            or shared_ready_request_limit <= 0
        ):
            raise ValueError("shared_ready_request_limit must be positive")
        if shared_ready_work_limit is not None and (
            not isinstance(shared_ready_work_limit, int)
            or isinstance(shared_ready_work_limit, bool)
            or shared_ready_work_limit <= 0
        ):
            raise ValueError("shared_ready_work_limit must be positive")
        if shared_ready_request_limit > 1 and shared_ready_work_limit is None:
            raise ValueError(
                "a multi-candidate ready window requires a work limit"
            )
        if shared_credit_acquire_timeout_s is not None and (
            not isinstance(shared_credit_acquire_timeout_s, (int, float))
            or isinstance(shared_credit_acquire_timeout_s, bool)
            or not math.isfinite(shared_credit_acquire_timeout_s)
            or shared_credit_acquire_timeout_s <= 0
        ):
            raise ValueError("shared credit acquire timeout must be positive")
        if job_weight <= 0:
            raise ValueError("job_weight must be positive")
        if (
            not isinstance(job_priority, int)
            or isinstance(job_priority, bool)
            or job_priority < 0
        ):
            raise ValueError("job_priority must be a non-negative integer")
        if job_slo_target_s is not None and (
            not math.isfinite(job_slo_target_s) or job_slo_target_s <= 0
        ):
            raise ValueError("job_slo_target_s must be finite and positive")
        if job_priority_window_s is not None and (
            not math.isfinite(job_priority_window_s)
            or job_priority_window_s <= 0
        ):
            raise ValueError("job_priority_window_s must be finite and positive")
        if job_fairness_debt_cap is not None and (
            not math.isfinite(job_fairness_debt_cap)
            or job_fairness_debt_cap <= 0
        ):
            raise ValueError("job_fairness_debt_cap must be finite and positive")
        if job_priority_window_s is not None and job_slo_target_s is None:
            raise ValueError("a priority window requires a Job SLO target")
        self.admission = admission
        self.router = router
        self.adapter = adapter
        self.pool_id = pool_id
        self.pool_router = pool_router
        self.epoch_clock = epoch_clock
        self.per_endpoint_limit = per_endpoint_limit
        self.per_endpoint_work_limit = per_endpoint_work_limit
        self.per_endpoint_admission = dict(per_endpoint_admission or {})
        self.shared_credit = shared_credit
        self.shared_credit_poll_s = shared_credit_poll_s
        self.shared_credit_acquire_timeout_s = shared_credit_acquire_timeout_s
        self.shared_ready_request_limit = shared_ready_request_limit
        self.shared_ready_work_limit = shared_ready_work_limit
        self.job_weight = job_weight
        self.job_priority = job_priority
        self.job_slo_target_s = job_slo_target_s
        self.job_priority_window_s = job_priority_window_s
        self.job_fairness_debt_cap = job_fairness_debt_cap
        self.actual_work_extractor = actual_work_extractor

    def run(
        self,
        envelopes: Iterable[PayloadEnvelope],
        topology: TopologySnapshot,
    ) -> SchedulerResult:
        ledger = SubmissionExecutionLedger(
            actual_work_extractor=self.actual_work_extractor,
        )
        endpoints_by_id = {
            endpoint.endpoint_id: endpoint for endpoint in topology.endpoints
        }
        unknown_admission_endpoints = (
            set(self.per_endpoint_admission) - set(endpoints_by_id)
        )
        if unknown_admission_endpoints:
            raise ValueError(
                "per-endpoint admission contains endpoints outside topology: "
                + ", ".join(sorted(unknown_admission_endpoints))
            )
        if self.per_endpoint_admission and (
            set(self.per_endpoint_admission) != set(endpoints_by_id)
        ):
            raise ValueError(
                "per-endpoint admission must define exactly one policy for "
                "every topology endpoint"
            )
        if self.shared_credit is not None and self.shared_ready_request_limit > 1:
            return self._run_with_shared_ready_window(
                envelopes,
                topology,
                endpoints_by_id,
            )
        max_inflight_seen = 0
        max_active_work_per_endpoint_seen = 0
        bounded_wait_samples: list[float] = []
        fanin_s = 0.0
        submit_s = 0.0
        shared_credit_jobs: set[str] = set()

        source = _ConcurrentEnvelopeSource(
            envelopes,
            poll_interval_s=self.shared_credit_poll_s,
        )
        for source_item in source:
            if source_item is _ConcurrentEnvelopeSource._POLL:
                collected = self._poll_one(ledger)
                if collected is not None:
                    bounded_wait_samples.append(collected.wait_s)
                    fanin_s += collected.result_s
                continue
            if not isinstance(source_item, PayloadEnvelope):
                raise ValueError(
                    "envelopes must contain PayloadEnvelope values"
                )
            envelope = source_item
            if self.shared_credit is not None:
                shared_credit_jobs.add(envelope.request.job_id)

            request_id = envelope.request.request_id
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
                capacity_topology = self._topology_with_local_inflight(
                    topology,
                    ledger.contexts,
                    per_endpoint_limit=self.per_endpoint_limit,
                    per_endpoint_work_limit=self.per_endpoint_work_limit,
                    request_work=envelope.request.estimated_work_units,
                )
                if self.per_endpoint_admission:
                    capacity_topology = self._topology_with_endpoint_admission(
                        capacity_topology,
                        ledger.contexts,
                    )
                if globally_allowed and any(
                    endpoint.healthy and endpoint.available
                    for endpoint in capacity_topology.endpoints
                ):
                    preferred_endpoint = endpoints_by_id.get(
                        envelope.request.preferred_endpoint_id
                    )
                    pool_id = (
                        preferred_endpoint.pool_id
                        if preferred_endpoint is not None
                        else (
                            self.pool_router.route(
                                envelope.request,
                                capacity_topology,
                            ).pool_id
                            if self.pool_router is not None
                            else self.pool_id
                        )
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
                collected = self._collect_one(ledger)
                bounded_wait_samples.append(collected.wait_s)
                fanin_s += collected.result_s
            if pool_id is None or route is None:
                raise AssertionError("routing decision missing after admission")
            endpoint = endpoints_by_id.get(route.endpoint_id)
            if endpoint is None:
                raise RuntimeError("router selected an endpoint outside the topology")
            credit_wait_started_s = time.monotonic()
            while self.shared_credit is not None and not (
                self.shared_credit.try_acquire(
                    request_id=request_id,
                    job_id=envelope.request.job_id,
                    endpoint_id=route.endpoint_id,
                    estimated_work=max(
                        1,
                        envelope.request.estimated_work_units,
                    ),
                    weight=self.job_weight,
                    priority=self.job_priority,
                    **self._bounded_credit_arguments(envelope.request),
                )
            ):
                if (
                    self.shared_credit_acquire_timeout_s is not None
                    and time.monotonic() - credit_wait_started_s
                    >= self.shared_credit_acquire_timeout_s
                ):
                    cancel_waiter = getattr(
                        self.shared_credit,
                        "cancel_waiter",
                        None,
                    )
                    if cancel_waiter is not None:
                        cancel_waiter(
                            request_id,
                            job_id=envelope.request.job_id,
                        )
                    raise TimeoutError(
                        "shared credit acquire timed out for "
                        f"{envelope.request.job_id}/{request_id}"
                    )
                if ledger.pending:
                    collected = self._collect_one(ledger)
                    bounded_wait_samples.append(collected.wait_s)
                    fanin_s += collected.result_s
                else:
                    time.sleep(self.shared_credit_poll_s)
            submit_start = time.perf_counter()
            try:
                handle = self.adapter.submit(envelope, route.endpoint_id)
            except Exception:
                if self.shared_credit is not None:
                    self.shared_credit.release(
                        request_id,
                        job_id=envelope.request.job_id,
                    )
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
            collected = self._collect_one(ledger)
            fanin_s += collected.result_s

        finish_shared_job = (
            getattr(self.shared_credit, "finish_job", None)
            if self.shared_credit is not None
            else None
        )
        if finish_shared_job is not None:
            for job_id in sorted(shared_credit_jobs):
                finish_shared_job(job_id)

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
            max_active_work_per_endpoint_seen=(
                max_active_work_per_endpoint_seen
            ),
        )

    def _run_with_shared_ready_window(
        self,
        envelopes: Iterable[PayloadEnvelope],
        topology: TopologySnapshot,
        endpoints_by_id: Mapping[str, EndpointSnapshot],
    ) -> SchedulerResult:
        """Pre-register a bounded concrete ready set before waiting on credit.

        The shared coordinator remains the admission owner.  This loop only
        makes more than one already-arrived candidate per Job visible to it;
        it does not speculate beyond the configured request/work window.
        """

        if self.shared_credit is None:
            raise AssertionError("ready-window execution requires shared credit")
        if self.shared_ready_work_limit is None:
            raise AssertionError("ready-window execution requires a work limit")
        ledger = SubmissionExecutionLedger(
            actual_work_extractor=self.actual_work_extractor,
        )
        ready = BoundedReadyWindow(
            request_limit=self.shared_ready_request_limit,
            work_limit=self.shared_ready_work_limit,
        )
        deferred_envelope: PayloadEnvelope | None = None
        source_exhausted = False
        shared_credit_jobs: set[str] = set()
        max_ready_requests_seen = 0
        max_ready_work_seen = 0
        max_inflight_seen = 0
        max_active_work_per_endpoint_seen = 0
        bounded_wait_samples: list[float] = []
        fanin_s = 0.0
        submit_s = 0.0
        source = _ConcurrentEnvelopeSource(
            envelopes,
            poll_interval_s=self.shared_credit_poll_s,
        )
        source_iter = iter(source)

        try:
            while not source_exhausted or deferred_envelope is not None or ready:
                made_progress = False
                source_waiting = False

                while len(ready) < self.shared_ready_request_limit:
                    if deferred_envelope is None:
                        if source_exhausted:
                            break
                        try:
                            source_item = next(source_iter)
                        except StopIteration:
                            source_exhausted = True
                            break
                        if source_item is _ConcurrentEnvelopeSource._POLL:
                            source_waiting = True
                            break
                        if not isinstance(source_item, PayloadEnvelope):
                            raise ValueError(
                                "envelopes must contain PayloadEnvelope values"
                            )
                        deferred_envelope = source_item

                    envelope = deferred_envelope
                    request_work = max(
                        1,
                        envelope.request.estimated_work_units,
                    )
                    if not ready.can_accept(request_work):
                        break

                    ready_epoch_s = self.epoch_clock()
                    ledger.observe(envelope, ready_epoch_s=ready_epoch_s)
                    shared_credit_jobs.add(envelope.request.job_id)
                    candidate = self._register_ready_candidate(
                        envelope,
                        topology,
                        endpoints_by_id,
                        ledger,
                        ready,
                    )
                    ready.append(candidate)
                    deferred_envelope = None
                    made_progress = True
                    max_ready_requests_seen = max(
                        max_ready_requests_seen,
                        len(ready),
                    )
                    max_ready_work_seen = max(
                        max_ready_work_seen,
                        ready.work,
                    )

                now_monotonic_s = time.monotonic()
                for candidate in ready.snapshot():
                    request = candidate.envelope.request
                    request_id = request.request_id
                    if (
                        not candidate.credit_granted
                        and self.shared_credit_acquire_timeout_s is not None
                        and now_monotonic_s - candidate.wait_started_s
                        >= self.shared_credit_acquire_timeout_s
                    ):
                        raise TimeoutError(
                            "shared credit acquire timed out for "
                            f"{request.job_id}/{request_id}"
                        )
                    if not candidate.credit_granted:
                        candidate.credit_granted = self.shared_credit.try_acquire(
                            request_id=request_id,
                            job_id=request.job_id,
                            endpoint_id=candidate.endpoint_id,
                            estimated_work=candidate.estimated_work,
                            weight=self.job_weight,
                            priority=self.job_priority,
                            **candidate.credit_arguments,
                        )
                        if candidate.credit_granted:
                            ledger.credit_granted(
                                request_id,
                                epoch_s=self.epoch_clock(),
                            )
                            made_progress = True
                    if not candidate.credit_granted or not self._can_submit_ready(
                        candidate,
                        topology,
                        ledger,
                    ):
                        continue

                    submit_start = time.perf_counter()
                    try:
                        handle = self.adapter.submit(
                            candidate.envelope,
                            candidate.endpoint_id,
                        )
                    except Exception:
                        self.shared_credit.release(
                            request_id,
                            job_id=request.job_id,
                        )
                        ready.remove(candidate)
                        raise
                    submit_s += time.perf_counter() - submit_start
                    ledger.submitted(
                        handle,
                        candidate.envelope,
                        pool_id=candidate.pool_id,
                        endpoint_id=candidate.endpoint_id,
                        gpu_id=candidate.gpu_id,
                        submit_epoch_s=self.epoch_clock(),
                    )
                    ready.remove(candidate)
                    made_progress = True
                    max_inflight_seen = max(
                        max_inflight_seen,
                        ledger.inflight_count,
                    )
                    max_active_work_per_endpoint_seen = max(
                        max_active_work_per_endpoint_seen,
                        max(
                            ledger.active_work_by_endpoint().values(),
                            default=0,
                        ),
                    )

                while True:
                    collected = self._poll_one(ledger)
                    if collected is None:
                        break
                    bounded_wait_samples.append(collected.wait_s)
                    fanin_s += collected.result_s
                    made_progress = True

                if not made_progress:
                    if source_waiting and not source_exhausted:
                        continue
                    if ledger.pending:
                        collected = self._collect_one(ledger)
                        bounded_wait_samples.append(collected.wait_s)
                        fanin_s += collected.result_s
                    elif ready:
                        time.sleep(self.shared_credit_poll_s)

            while ledger.pending:
                collected = self._collect_one(ledger)
                fanin_s += collected.result_s
        except BaseException:
            self._cancel_ready_candidates(ready)
            raise
        finally:
            source.close()

        finish_shared_job = getattr(self.shared_credit, "finish_job", None)
        if finish_shared_job is not None:
            for job_id in sorted(shared_credit_jobs):
                finish_shared_job(job_id)

        completions = ledger.ordered_completions()
        return SchedulerResult(
            completions=completions,
            submission_events=ledger.ordered_events(),
            operator_invocations=len(completions),
            max_inflight_seen=max_inflight_seen,
            applied_limit=self.admission.limit,
            bounded_wait_s=sum(bounded_wait_samples),
            avg_bounded_wait_s=(
                statistics.mean(bounded_wait_samples)
                if bounded_wait_samples
                else 0.0
            ),
            fanin_s=fanin_s,
            submit_s=submit_s,
            max_active_work_per_endpoint_seen=(
                max_active_work_per_endpoint_seen
            ),
            max_ready_requests_seen=max_ready_requests_seen,
            max_ready_work_seen=max_ready_work_seen,
        )

    def _register_ready_candidate(
        self,
        envelope: PayloadEnvelope,
        topology: TopologySnapshot,
        endpoints_by_id: Mapping[str, EndpointSnapshot],
        ledger: SubmissionExecutionLedger,
        ready: BoundedReadyWindow,
    ) -> ReadySubmission:
        routing_context = {
            **ledger.contexts,
            **ready.routing_contexts(),
        }
        routing_topology = self._topology_with_local_inflight(
            topology,
            routing_context,
        )
        preferred_endpoint = endpoints_by_id.get(
            envelope.request.preferred_endpoint_id
        )
        pool_id = (
            preferred_endpoint.pool_id
            if preferred_endpoint is not None
            else (
                self.pool_router.route(
                    envelope.request,
                    routing_topology,
                ).pool_id
                if self.pool_router is not None
                else self.pool_id
            )
        )
        route = self.router.route(
            envelope.request,
            routing_topology,
            pool_id,
        )
        endpoint = endpoints_by_id.get(route.endpoint_id)
        if endpoint is None:
            raise RuntimeError("router selected an endpoint outside the topology")
        request = envelope.request
        credit_arguments = self._bounded_credit_arguments(request)
        registered_epoch_s = self.epoch_clock()
        ledger.credit_registered(
            request.request_id,
            epoch_s=registered_epoch_s,
        )
        granted = self.shared_credit.try_acquire(
            request_id=request.request_id,
            job_id=request.job_id,
            endpoint_id=route.endpoint_id,
            estimated_work=max(1, request.estimated_work_units),
            weight=self.job_weight,
            priority=self.job_priority,
            **credit_arguments,
        )
        if granted:
            ledger.credit_granted(
                request.request_id,
                epoch_s=self.epoch_clock(),
            )
        return ReadySubmission(
            envelope=envelope,
            pool_id=pool_id,
            endpoint_id=route.endpoint_id,
            gpu_id=endpoint.gpu_id,
            wait_started_s=time.monotonic(),
            credit_arguments=credit_arguments,
            credit_granted=granted,
        )

    def _can_submit_ready(
        self,
        candidate: ReadySubmission,
        topology: TopologySnapshot,
        ledger: SubmissionExecutionLedger,
    ) -> bool:
        hol_age_s = (
            ledger.oldest_inflight_age_s(now_s=self.epoch_clock())
            if ledger.contexts
            else 0.0
        )
        if not self.admission.decide(
            ledger.inflight_count,
            hol_age_s=hol_age_s,
        ).allowed:
            return False
        capacity_topology = self._topology_with_local_inflight(
            topology,
            ledger.contexts,
            per_endpoint_limit=self.per_endpoint_limit,
            per_endpoint_work_limit=self.per_endpoint_work_limit,
            request_work=candidate.estimated_work,
        )
        if self.per_endpoint_admission:
            capacity_topology = self._topology_with_endpoint_admission(
                capacity_topology,
                ledger.contexts,
            )
        endpoint = next(
            item
            for item in capacity_topology.endpoints
            if item.endpoint_id == candidate.endpoint_id
        )
        return endpoint.healthy and endpoint.available

    def _cancel_ready_candidates(
        self,
        ready: Iterable[ReadySubmission],
    ) -> None:
        if self.shared_credit is None:
            return
        cancel_waiter = getattr(self.shared_credit, "cancel_waiter", None)
        for candidate in tuple(ready):
            request = candidate.envelope.request
            if candidate.credit_granted:
                self.shared_credit.release(
                    request.request_id,
                    job_id=request.job_id,
                )
                continue
            cancelled = (
                cancel_waiter(
                    request.request_id,
                    job_id=request.job_id,
                )
                if cancel_waiter is not None
                else False
            )
            if not cancelled:
                acquired = self.shared_credit.try_acquire(
                    request_id=request.request_id,
                    job_id=request.job_id,
                    endpoint_id=candidate.endpoint_id,
                    estimated_work=candidate.estimated_work,
                    weight=self.job_weight,
                    priority=self.job_priority,
                    **candidate.credit_arguments,
                )
                if acquired:
                    self.shared_credit.release(
                        request.request_id,
                        job_id=request.job_id,
                    )

    def _bounded_credit_arguments(self, request: BatchRequest) -> dict[str, float | None]:
        if (
            self.job_slo_target_s is None
            and self.job_priority_window_s is None
            and self.job_fairness_debt_cap is None
        ):
            return {}
        if self.job_priority_window_s is not None:
            if request.oldest_arrival_epoch_s is None:
                raise ValueError(
                    "bounded priority requires an explicit oldest arrival epoch"
                )
            remaining_slo_budget_s = self.job_slo_target_s - max(
                0.0,
                self.epoch_clock() - request.oldest_arrival_epoch_s,
            )
        else:
            remaining_slo_budget_s = None
        return {
            "slo_budget_remaining_s": remaining_slo_budget_s,
            "priority_window_s": self.job_priority_window_s,
            "fairness_debt_cap": self.job_fairness_debt_cap,
        }

    @staticmethod
    def _topology_with_local_inflight(
        topology: TopologySnapshot,
        submission_context: Mapping[str, SubmissionContext],
        *,
        per_endpoint_limit: int | None = None,
        per_endpoint_work_limit: int | None = None,
        request_work: int = 0,
    ) -> TopologySnapshot:
        inflight_by_endpoint: dict[str, int] = {}
        work_by_endpoint: dict[str, int] = {}
        for item in submission_context.values():
            endpoint_id = item.endpoint_id
            inflight_by_endpoint[endpoint_id] = (
                inflight_by_endpoint.get(endpoint_id, 0) + 1
            )
            work_by_endpoint[endpoint_id] = (
                work_by_endpoint.get(endpoint_id, 0) + item.estimated_work
            )
        return replace(
            topology,
            endpoints=tuple(
                replace(
                    endpoint,
                    available=(
                        endpoint.available
                        and (
                            per_endpoint_limit is None
                            or inflight_by_endpoint.get(endpoint.endpoint_id, 0)
                            < per_endpoint_limit
                        )
                        and (
                            per_endpoint_work_limit is None
                            or (
                                endpoint.estimated_active_work
                                + work_by_endpoint.get(endpoint.endpoint_id, 0)
                                == 0
                            )
                            or (
                                endpoint.estimated_active_work
                                + work_by_endpoint.get(endpoint.endpoint_id, 0)
                                + request_work
                                <= per_endpoint_work_limit
                            )
                        )
                    ),
                    running=(
                        endpoint.running
                        + inflight_by_endpoint.get(endpoint.endpoint_id, 0)
                    ),
                    estimated_active_work=(
                        endpoint.estimated_active_work
                        + work_by_endpoint.get(endpoint.endpoint_id, 0)
                    ),
                )
                for endpoint in topology.endpoints
            ),
        )

    def _topology_with_endpoint_admission(
        self,
        topology: TopologySnapshot,
        submission_context: Mapping[str, SubmissionContext],
    ) -> TopologySnapshot:
        inflight_by_endpoint: dict[str, int] = {}
        oldest_submit_by_endpoint: dict[str, float] = {}
        for item in submission_context.values():
            endpoint_id = item.endpoint_id
            submit_epoch_s = item.submit_epoch_s
            inflight_by_endpoint[endpoint_id] = (
                inflight_by_endpoint.get(endpoint_id, 0) + 1
            )
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
                        and self.per_endpoint_admission[
                            endpoint.endpoint_id
                        ].decide(
                            inflight_by_endpoint.get(endpoint.endpoint_id, 0),
                            hol_age_s=max(
                                0.0,
                                now
                                - oldest_submit_by_endpoint.get(
                                    endpoint.endpoint_id,
                                    now,
                                ),
                            ),
                        ).allowed
                    ),
                )
                for endpoint in topology.endpoints
            ),
        )

    def _collect_one(
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
        request_id = recorded.envelope.request.request_id
        if self.shared_credit is not None:
            self.shared_credit.release(
                request_id,
                job_id=recorded.envelope.request.job_id,
                actual_work=recorded.actual_work,
            )
        return collected

    def _poll_one(
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
