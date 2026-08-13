"""Shared pending/completion ledger for scheduling execution loops."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping

from .models import (
    CollectedSubmission,
    PayloadEnvelope,
    SubmissionCompletion,
    SubmissionLifecycleEvent,
)


@dataclass(frozen=True)
class SubmissionContext:
    pool_id: str
    endpoint_id: str
    gpu_id: str
    submit_epoch_s: float
    estimated_work: int


@dataclass(frozen=True)
class RecordedCompletion:
    collected: CollectedSubmission
    envelope: PayloadEnvelope
    context: SubmissionContext
    actual_work: int | None
    event: SubmissionLifecycleEvent


class SubmissionExecutionLedger:
    """Own exactly-once pending state and lifecycle evidence."""

    def __init__(
        self,
        *,
        actual_work_extractor: Callable[
            [SubmissionCompletion], int | None
        ] | None = None,
    ) -> None:
        self._pending: list[tuple[object, PayloadEnvelope]] = []
        self._contexts: dict[str, SubmissionContext] = {}
        self._order: dict[str, int] = {}
        self._ready_epoch_s: dict[str, float] = {}
        self._credit_registered_epoch_s: dict[str, float] = {}
        self._credit_granted_epoch_s: dict[str, float] = {}
        self._completions: list[SubmissionCompletion] = []
        self._events: list[SubmissionLifecycleEvent] = []
        self._actual_work_extractor = actual_work_extractor

    @property
    def pending(self) -> list[tuple[object, PayloadEnvelope]]:
        return self._pending

    @property
    def contexts(self) -> Mapping[str, SubmissionContext]:
        return self._contexts

    @property
    def inflight_count(self) -> int:
        return len(self._pending)

    def observe(
        self,
        envelope: PayloadEnvelope,
        *,
        ready_epoch_s: float | None = None,
    ) -> None:
        request_id = envelope.request.request_id
        if request_id in self._order:
            raise ValueError(f"duplicate request_id: {request_id}")
        self._order[request_id] = len(self._order)
        if ready_epoch_s is not None:
            self._ready_epoch_s[request_id] = ready_epoch_s

    def credit_registered(self, request_id: str, *, epoch_s: float) -> None:
        self._record_pre_submission_time(
            self._credit_registered_epoch_s,
            request_id,
            epoch_s,
            "credit registration",
        )

    def credit_granted(self, request_id: str, *, epoch_s: float) -> None:
        self._record_pre_submission_time(
            self._credit_granted_epoch_s,
            request_id,
            epoch_s,
            "credit grant",
        )

    def submitted(
        self,
        handle: object,
        envelope: PayloadEnvelope,
        *,
        pool_id: str,
        endpoint_id: str,
        gpu_id: str,
        submit_epoch_s: float,
    ) -> None:
        request_id = envelope.request.request_id
        if request_id not in self._order:
            raise ValueError("request must be observed before submission")
        if request_id in self._contexts:
            raise ValueError(f"request is already active: {request_id}")
        self._contexts[request_id] = SubmissionContext(
            pool_id,
            endpoint_id,
            gpu_id,
            submit_epoch_s,
            envelope.request.estimated_work_units,
        )
        self._pending.append((handle, envelope))

    def oldest_inflight_age_s(self, *, now_s: float) -> float:
        if not self._contexts:
            return 0.0
        oldest = min(item.submit_epoch_s for item in self._contexts.values())
        return max(0.0, now_s - oldest)

    def active_work_by_endpoint(self) -> dict[str, int]:
        totals: dict[str, int] = {}
        for item in self._contexts.values():
            totals[item.endpoint_id] = (
                totals.get(item.endpoint_id, 0) + item.estimated_work
            )
        return totals

    def record(
        self,
        collected: CollectedSubmission,
        *,
        completion_epoch_s: float,
    ) -> RecordedCompletion:
        matching = [
            index
            for index, (handle, _) in enumerate(self._pending)
            if handle is collected.handle
        ]
        if len(matching) != 1:
            raise RuntimeError(
                "adapter returned an unknown or duplicate pending handle"
            )
        _, envelope = self._pending[matching[0]]
        request_id = envelope.request.request_id
        if collected.completion.request_id != request_id:
            raise RuntimeError(
                "completion request_id does not match pending request"
            )
        context = self._contexts[request_id]
        actual_work = (
            self._actual_work_extractor(collected.completion)
            if self._actual_work_extractor is not None
            else None
        )
        event = SubmissionLifecycleEvent(
            submission_id=request_id,
            pool_id=context.pool_id,
            endpoint_id=context.endpoint_id,
            gpu_id=context.gpu_id,
            submit_epoch_s=context.submit_epoch_s,
            completion_epoch_s=completion_epoch_s,
            status=collected.completion.status,
            error=collected.completion.error,
            planning_batch_id=envelope.request.planning_batch_id,
            service_quantum_index=envelope.request.service_quantum_index,
            service_quantum_oversized=(
                envelope.request.service_quantum_oversized
            ),
            actor_worker_id=collected.actor_worker_id,
            actor_worker_index=collected.actor_worker_index,
            actor_worker_pid=collected.actor_worker_pid,
            ready_epoch_s=self._ready_epoch_s.get(request_id),
            credit_registered_epoch_s=(
                self._credit_registered_epoch_s.get(request_id)
            ),
            credit_granted_epoch_s=self._credit_granted_epoch_s.get(request_id),
        )
        self._pending.pop(matching[0])
        del self._contexts[request_id]
        self._completions.append(collected.completion)
        self._events.append(event)
        return RecordedCompletion(
            collected,
            envelope,
            context,
            actual_work,
            event,
        )

    def ordered_completions(self) -> tuple[SubmissionCompletion, ...]:
        return tuple(
            sorted(
                self._completions,
                key=lambda item: self._order[item.request_id],
            )
        )

    def ordered_events(self) -> tuple[SubmissionLifecycleEvent, ...]:
        return tuple(
            sorted(
                self._events,
                key=lambda item: self._order[item.submission_id],
            )
        )

    def _record_pre_submission_time(
        self,
        destination: dict[str, float],
        request_id: str,
        epoch_s: float,
        label: str,
    ) -> None:
        if request_id not in self._order:
            raise ValueError(f"request must be observed before {label}")
        if request_id in destination:
            return
        destination[request_id] = epoch_s
