"""Synchronous policy-composition scheduler."""

from __future__ import annotations

import statistics
import threading
import time
from dataclasses import dataclass, replace
from queue import Empty, Full, Queue
from typing import Callable, Iterable, Iterator, Protocol

from .errors import EndpointCapacityUnavailable
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
    ) -> bool:
        ...

    def release(self, request_id: str, *, job_id: str) -> None:
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
        shared_credit: SharedCreditPolicy | None = None,
        shared_credit_poll_s: float = 0.001,
        job_weight: int = 1,
    ):
        if per_endpoint_limit is not None and per_endpoint_limit <= 0:
            raise ValueError("per_endpoint_limit must be positive")
        if per_endpoint_work_limit is not None and per_endpoint_work_limit <= 0:
            raise ValueError("per_endpoint_work_limit must be positive")
        if shared_credit_poll_s <= 0:
            raise ValueError("shared_credit_poll_s must be positive")
        if job_weight <= 0:
            raise ValueError("job_weight must be positive")
        self.admission = admission
        self.router = router
        self.adapter = adapter
        self.pool_id = pool_id
        self.pool_router = pool_router
        self.epoch_clock = epoch_clock
        self.per_endpoint_limit = per_endpoint_limit
        self.per_endpoint_work_limit = per_endpoint_work_limit
        self.shared_credit = shared_credit
        self.shared_credit_poll_s = shared_credit_poll_s
        self.job_weight = job_weight

    def run(
        self,
        envelopes: Iterable[PayloadEnvelope],
        topology: TopologySnapshot,
    ) -> SchedulerResult:
        pending: list[tuple[object, PayloadEnvelope]] = []
        completions: list[SubmissionCompletion] = []
        submission_events: list[SubmissionLifecycleEvent] = []
        submission_context: dict[
            str,
            tuple[str, str, str, float, int],
        ] = {}
        submission_order: dict[str, int] = {}
        endpoints_by_id = {
            endpoint.endpoint_id: endpoint for endpoint in topology.endpoints
        }
        max_inflight_seen = 0
        max_active_work_per_endpoint_seen = 0
        bounded_wait_samples: list[float] = []
        fanin_s = 0.0
        submit_s = 0.0

        source = _ConcurrentEnvelopeSource(
            envelopes,
            poll_interval_s=self.shared_credit_poll_s,
        )
        for source_item in source:
            if source_item is _ConcurrentEnvelopeSource._POLL:
                collected = self._poll_one(
                    pending,
                    completions,
                    submission_events,
                    submission_context,
                )
                if collected is not None:
                    bounded_wait_samples.append(collected.wait_s)
                    fanin_s += collected.result_s
                continue
            if not isinstance(source_item, PayloadEnvelope):
                raise ValueError(
                    "envelopes must contain PayloadEnvelope values"
                )
            envelope = source_item

            request_id = envelope.request.request_id
            if request_id in submission_order:
                raise ValueError(f"duplicate request_id: {request_id}")
            submission_order[request_id] = len(submission_order)
            pool_id: str | None = None
            route: RoutingDecision | None = None
            while True:
                hol_age_s = self._head_of_line_age_s(submission_context)
                globally_allowed = self.admission.decide(
                    len(pending), hol_age_s=hol_age_s
                ).allowed
                capacity_topology = self._topology_with_local_inflight(
                    topology,
                    submission_context,
                    per_endpoint_limit=self.per_endpoint_limit,
                    per_endpoint_work_limit=self.per_endpoint_work_limit,
                    request_work=envelope.request.estimated_total_tokens,
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
            if pool_id is None or route is None:
                raise AssertionError("routing decision missing after admission")
            endpoint = endpoints_by_id.get(route.endpoint_id)
            if endpoint is None:
                raise RuntimeError("router selected an endpoint outside the topology")
            while self.shared_credit is not None and not (
                self.shared_credit.try_acquire(
                    request_id=request_id,
                    job_id=envelope.request.job_id,
                    endpoint_id=route.endpoint_id,
                    estimated_work=max(
                        1,
                        envelope.request.estimated_total_tokens,
                    ),
                    weight=self.job_weight,
                )
            ):
                if pending:
                    collected = self._collect_one(
                        pending,
                        completions,
                        submission_events,
                        submission_context,
                    )
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
            submission_context[request_id] = (
                pool_id,
                route.endpoint_id,
                endpoint.gpu_id,
                self.epoch_clock(),
                envelope.request.estimated_total_tokens,
            )
            pending.append((handle, envelope))
            max_inflight_seen = max(max_inflight_seen, len(pending))
            max_active_work_per_endpoint_seen = max(
                max_active_work_per_endpoint_seen,
                self._max_endpoint_active_work(submission_context),
            )

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
            max_active_work_per_endpoint_seen=(
                max_active_work_per_endpoint_seen
            ),
        )

    def _head_of_line_age_s(
        self,
        submission_context: dict[str, tuple[str, str, str, float, int]],
    ) -> float:
        # This is the age of the oldest in-flight submission, including both
        # Ray-side waiting and normal endpoint service time. It is directly
        # observable but must not be interpreted as pure queue delay.
        if not submission_context:
            return 0.0
        now = self.epoch_clock()
        oldest_submit_s = min(
            submit_epoch_s
            for (
                _pool_id,
                _endpoint_id,
                _gpu_id,
                submit_epoch_s,
                _work,
            ) in submission_context.values()
        )
        return max(0.0, now - oldest_submit_s)

    @staticmethod
    def _topology_with_local_inflight(
        topology: TopologySnapshot,
        submission_context: dict[str, tuple[str, str, str, float, int]],
        *,
        per_endpoint_limit: int | None = None,
        per_endpoint_work_limit: int | None = None,
        request_work: int = 0,
    ) -> TopologySnapshot:
        inflight_by_endpoint: dict[str, int] = {}
        work_by_endpoint: dict[str, int] = {}
        for _pool_id, endpoint_id, _gpu_id, _submit_epoch_s, work in (
            submission_context.values()
        ):
            inflight_by_endpoint[endpoint_id] = (
                inflight_by_endpoint.get(endpoint_id, 0) + 1
            )
            work_by_endpoint[endpoint_id] = (
                work_by_endpoint.get(endpoint_id, 0) + work
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

    @staticmethod
    def _max_endpoint_active_work(
        submission_context: dict[str, tuple[str, str, str, float, int]],
    ) -> int:
        work_by_endpoint: dict[str, int] = {}
        for _pool_id, endpoint_id, _gpu_id, _submit_s, work in (
            submission_context.values()
        ):
            work_by_endpoint[endpoint_id] = (
                work_by_endpoint.get(endpoint_id, 0) + work
            )
        return max(work_by_endpoint.values(), default=0)

    def _collect_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
        completions: list[SubmissionCompletion],
        submission_events: list[SubmissionLifecycleEvent],
        submission_context: dict[str, tuple[str, str, str, float, int]],
    ) -> CollectedSubmission:
        collected = self.adapter.wait_one(pending)
        return self._record_collected(
            collected,
            pending,
            completions,
            submission_events,
            submission_context,
        )

    def _record_collected(
        self,
        collected: CollectedSubmission,
        pending: list[tuple[object, PayloadEnvelope]],
        completions: list[SubmissionCompletion],
        submission_events: list[SubmissionLifecycleEvent],
        submission_context: dict[str, tuple[str, str, str, float, int]],
    ) -> CollectedSubmission:
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
        (
            pool_id,
            endpoint_id,
            gpu_id,
            submit_epoch_s,
            _work,
        ) = submission_context.pop(request_id)
        if self.shared_credit is not None:
            self.shared_credit.release(
                request_id,
                job_id=pending_envelope.request.job_id,
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
                planning_batch_id=pending_envelope.request.planning_batch_id,
                service_quantum_index=(
                    pending_envelope.request.service_quantum_index
                ),
                service_quantum_oversized=(
                    pending_envelope.request.service_quantum_oversized
                ),
                actor_worker_id=collected.actor_worker_id,
                actor_worker_index=collected.actor_worker_index,
                actor_worker_pid=collected.actor_worker_pid,
            )
        )
        return collected

    def _poll_one(
        self,
        pending: list[tuple[object, PayloadEnvelope]],
        completions: list[SubmissionCompletion],
        submission_events: list[SubmissionLifecycleEvent],
        submission_context: dict[str, tuple[str, str, str, float, int]],
    ) -> CollectedSubmission | None:
        if not pending:
            return None
        poll_one = getattr(self.adapter, "poll_one", None)
        if not callable(poll_one):
            return None
        collected = poll_one(pending)
        if collected is None:
            return None
        return self._record_collected(
            collected,
            pending,
            completions,
            submission_events,
            submission_context,
        )
