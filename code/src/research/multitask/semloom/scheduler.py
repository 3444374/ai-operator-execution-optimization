"""Opt-in shared-credit orchestration over SemLoom's ordinary submission executor."""

from __future__ import annotations

import statistics
import time
from collections.abc import Mapping
from typing import Iterable

from ....scheduling.core.execution import SubmissionExecutionLedger
from ....scheduling.core.policy_contracts import AdmissionPolicy, EndpointRouter
from ....scheduling.core.scheduler_types import SubmissionAdapter
from ....scheduling.core.models import BatchRequest, EndpointSnapshot, PayloadEnvelope, TopologySnapshot
from ....scheduling.core.scheduler import SynchronousScheduler
from ....scheduling.core.scheduler_types import SchedulerResult
from ....scheduling.core.topology import with_local_inflight
from ....scheduling.core.envelope_source import _ConcurrentEnvelopeSource
from .ready_window import BoundedReadyWindow, ReadySubmission
from .scheduler_types import (DEFAULT_SHARED_CREDIT_POLL_S, JobSchedulingContract,
                              ReadyWindowConfig, SharedCreditConfig, SharedSchedulerConfig)


class SharedCreditScheduler(SynchronousScheduler):
    def __init__(
        self,
        admission: AdmissionPolicy,
        router: EndpointRouter,
        adapter: SubmissionAdapter,
        pool_id: str,
        *,
        shared_credit: object | None = None,
        shared_credit_poll_s: float = DEFAULT_SHARED_CREDIT_POLL_S,
        shared_credit_acquire_timeout_s: float | None = None,
        shared_ready_request_limit: int = 1,
        shared_ready_work_limit: int | None = None,
        shared_ready_payload_bytes_limit: int | None = None,
        job_weight: int = 1,
        job_priority: int = 0,
        job_slo_target_s: float | None = None,
        job_priority_window_s: float | None = None,
        job_fairness_debt_cap: float | None = None,
        **execution_options,
    ):
        shared = SharedCreditConfig(
            policy=shared_credit, poll_interval_s=shared_credit_poll_s,
            acquire_timeout_s=shared_credit_acquire_timeout_s,
            ready_window=ReadyWindowConfig(shared_ready_request_limit, shared_ready_work_limit,
                                           shared_ready_payload_bytes_limit),
            job=JobSchedulingContract(job_weight, job_priority, job_slo_target_s,
                                      job_priority_window_s, job_fairness_debt_cap),
        )
        self._set_shared(shared)
        super().__init__(admission, router, adapter, pool_id,
                         source_poll_interval_s=shared.poll_interval_s,
                         lifecycle=self, **execution_options)

    def _set_shared(self, shared):
        self.shared_credit = shared.policy
        self.shared_credit_poll_s = shared.poll_interval_s
        self.shared_credit_acquire_timeout_s = shared.acquire_timeout_s
        self.shared_ready_request_limit = shared.ready_window.request_limit
        self.shared_ready_work_limit = shared.ready_window.work_limit
        self.shared_ready_payload_bytes_limit = shared.ready_window.payload_bytes_limit
        self.job_weight = shared.job.weight
        self.job_priority = shared.job.priority
        self.job_slo_target_s = shared.job.slo_target_s
        self.job_priority_window_s = shared.job.priority_window_s
        self.job_fairness_debt_cap = shared.job.fairness_debt_cap
        self._credit_jobs = set()

    @classmethod
    def from_config(cls, admission, router, adapter, pool_id, *, config: SharedSchedulerConfig,
                    pool_router=None, epoch_clock=time.time):
        scheduler = super().from_config(
            admission, router, adapter, pool_id, config=config.execution,
            pool_router=pool_router, epoch_clock=epoch_clock,
        )
        scheduler._set_shared(config.shared_credit)
        scheduler.lifecycle = scheduler
        scheduler.source_poll_interval_s = config.shared_credit.poll_interval_s
        return scheduler

    def run(self, envelopes, topology):
        self._credit_jobs = set()
        if self.shared_credit is not None and self.shared_ready_request_limit > 1:
            return self._run_with_shared_ready_window(envelopes, topology,
                                                       self.validate_topology(topology))
        return super().run(envelopes, topology)

    def before_submit(self, envelope, endpoint_id, collect_pending):
        if self.shared_credit is None:
            return
        request = envelope.request
        self._credit_jobs.add(request.job_id)
        credit_wait_started_s = time.monotonic()
        while not self.shared_credit.try_acquire(
            request_id=request.request_id, job_id=request.job_id, endpoint_id=endpoint_id,
            estimated_work=max(1, request.estimated_work_units), weight=self.job_weight,
            priority=self.job_priority, **self._bounded_credit_arguments(request),
        ):
            if (self.shared_credit_acquire_timeout_s is not None
                    and time.monotonic() - credit_wait_started_s >= self.shared_credit_acquire_timeout_s):
                cancel_waiter = getattr(self.shared_credit, "cancel_waiter", None)
                if cancel_waiter is not None:
                    cancel_waiter(request.request_id, job_id=request.job_id)
                raise TimeoutError(f"shared credit acquire timed out for {request.job_id}/{request.request_id}")
            if not collect_pending():
                time.sleep(self.shared_credit_poll_s)

    def submit_failed(self, envelope):
        if self.shared_credit is not None:
            self.shared_credit.release(envelope.request.request_id, job_id=envelope.request.job_id)

    def completed(self, recorded):
        if self.shared_credit is not None:
            self.shared_credit.release(recorded.envelope.request.request_id,
                                       job_id=recorded.envelope.request.job_id,
                                       actual_work=recorded.actual_work)

    def finish(self):
        finish = getattr(self.shared_credit, "finish_job", None)
        if finish is not None:
            for job_id in sorted(self._credit_jobs):
                finish(job_id)

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

        ledger = SubmissionExecutionLedger(
            actual_work_extractor=self.actual_work_extractor,
        )
        ready = BoundedReadyWindow(
            request_limit=self.shared_ready_request_limit,
            work_limit=self.shared_ready_work_limit,
            payload_bytes_limit=self.shared_ready_payload_bytes_limit,
        )
        deferred_envelope: PayloadEnvelope | None = None
        source_exhausted = False
        shared_credit_jobs: set[str] = set()
        max_ready_requests_seen = 0
        max_ready_work_seen = 0
        max_ready_payload_bytes_seen = 0
        ready_requests_transition_samples = [0]
        ready_work_transition_samples = [0]
        ready_payload_bytes_transition_samples = [0]
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
                            raise ValueError("envelopes must contain PayloadEnvelope values")
                        deferred_envelope = source_item

                    envelope = deferred_envelope
                    request_work = max(
                        1,
                        envelope.request.estimated_work_units,
                    )
                    request_payload_bytes = envelope.request.estimated_payload_bytes
                    if not ready.can_accept(
                        request_work,
                        request_payload_bytes,
                    ):
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
                    max_ready_payload_bytes_seen = max(
                        max_ready_payload_bytes_seen,
                        ready.payload_bytes,
                    )
                    ready_requests_transition_samples.append(len(ready))
                    ready_work_transition_samples.append(ready.work)
                    ready_payload_bytes_transition_samples.append(ready.payload_bytes)

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
                            f"shared credit acquire timed out for {request.job_id}/{request_id}"
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
                    ready_requests_transition_samples.append(len(ready))
                    ready_work_transition_samples.append(ready.work)
                    ready_payload_bytes_transition_samples.append(ready.payload_bytes)
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
                    collected = self.poll_one(ledger)
                    if collected is None:
                        break
                    bounded_wait_samples.append(collected.wait_s)
                    fanin_s += collected.result_s
                    made_progress = True

                if not made_progress:
                    if source_waiting and not source_exhausted:
                        continue
                    if ledger.pending:
                        collected = self.collect_one(ledger)
                        bounded_wait_samples.append(collected.wait_s)
                        fanin_s += collected.result_s
                    elif ready:
                        time.sleep(self.shared_credit_poll_s)

            while ledger.pending:
                collected = self.collect_one(ledger)
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
                statistics.mean(bounded_wait_samples) if bounded_wait_samples else 0.0
            ),
            fanin_s=fanin_s,
            submit_s=submit_s,
            max_active_work_per_endpoint_seen=(max_active_work_per_endpoint_seen),
            max_ready_requests_seen=max_ready_requests_seen,
            max_ready_work_seen=max_ready_work_seen,
            max_ready_payload_bytes_seen=max_ready_payload_bytes_seen,
            ready_requests_transition_samples=tuple(ready_requests_transition_samples),
            ready_work_transition_samples=tuple(ready_work_transition_samples),
            ready_payload_bytes_transition_samples=tuple(ready_payload_bytes_transition_samples),
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
        routing_topology = with_local_inflight(
            topology,
            routing_context,
        )
        pool_id = self.select_pool(
            envelope.request, routing_topology, endpoints_by_id
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
            ledger.oldest_inflight_age_s(now_s=self.epoch_clock()) if ledger.contexts else 0.0
        )
        if not self.admission.decide(
            ledger.inflight_count,
            hol_age_s=hol_age_s,
        ).allowed:
            return False
        capacity_topology = with_local_inflight(
            topology,
            ledger.contexts,
            per_endpoint_limit=self.per_endpoint_limit,
            per_endpoint_work_limit=self.per_endpoint_work_limit,
            request_work=candidate.estimated_work,
        )
        if self.per_endpoint_admission:
            capacity_topology = self.topology_with_endpoint_admission(
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
                raise ValueError("bounded priority requires an explicit oldest arrival epoch")
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
