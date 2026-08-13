"""Bound concrete, already-arrived requests before shared-credit admission."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from .execution import SubmissionContext
from .models import PayloadEnvelope


@dataclass
class ReadySubmission:
    """One routed concrete request registered with shared credit."""

    envelope: PayloadEnvelope
    pool_id: str
    endpoint_id: str
    gpu_id: str
    wait_started_s: float
    credit_arguments: dict[str, float | None]
    credit_granted: bool = False

    @property
    def estimated_work(self) -> int:
        return max(1, self.envelope.request.estimated_work_units)


class BoundedReadyWindow:
    """Hold concrete ready candidates under request and work bounds."""

    def __init__(self, *, request_limit: int, work_limit: int) -> None:
        if (
            not isinstance(request_limit, int)
            or isinstance(request_limit, bool)
            or request_limit <= 0
        ):
            raise ValueError("request_limit must be a positive integer")
        if (
            not isinstance(work_limit, int)
            or isinstance(work_limit, bool)
            or work_limit <= 0
        ):
            raise ValueError("work_limit must be a positive integer")
        self.request_limit = request_limit
        self.work_limit = work_limit
        self._items: list[ReadySubmission] = []
        self._work = 0

    def __iter__(self) -> Iterator[ReadySubmission]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    @property
    def work(self) -> int:
        return self._work

    def can_accept(self, estimated_work: int) -> bool:
        if estimated_work > self.work_limit:
            raise ValueError(
                "request estimated work exceeds the shared ready window "
                "work limit"
            )
        return (
            len(self._items) < self.request_limit
            and self._work + estimated_work <= self.work_limit
        )

    def append(self, candidate: ReadySubmission) -> None:
        if not self.can_accept(candidate.estimated_work):
            raise ValueError("shared ready window has no remaining capacity")
        self._items.append(candidate)
        self._work += candidate.estimated_work

    def remove(self, candidate: ReadySubmission) -> None:
        self._items.remove(candidate)
        self._work -= candidate.estimated_work

    def snapshot(self) -> tuple[ReadySubmission, ...]:
        return tuple(self._items)

    def routing_contexts(self) -> dict[str, SubmissionContext]:
        """Expose ready work as synthetic load for endpoint routing only."""

        return {
            f"__ready__:{index}": SubmissionContext(
                candidate.pool_id,
                candidate.endpoint_id,
                candidate.gpu_id,
                0.0,
                candidate.estimated_work,
            )
            for index, candidate in enumerate(self._items)
        }
