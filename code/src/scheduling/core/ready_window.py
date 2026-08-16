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

    @property
    def payload_bytes(self) -> int:
        """Return logical payload bytes, not Python/Ray physical RSS."""

        return self.envelope.request.estimated_payload_bytes


class BoundedReadyWindow:
    """Hold concrete ready candidates under request and work bounds."""

    def __init__(
        self,
        *,
        request_limit: int,
        work_limit: int,
        payload_bytes_limit: int | None = None,
    ) -> None:
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
        if payload_bytes_limit is not None and (
            not isinstance(payload_bytes_limit, int)
            or isinstance(payload_bytes_limit, bool)
            or payload_bytes_limit <= 0
        ):
            raise ValueError("payload_bytes_limit must be a positive integer")
        self.request_limit = request_limit
        self.work_limit = work_limit
        self.payload_bytes_limit = payload_bytes_limit
        self._items: list[ReadySubmission] = []
        self._work = 0
        self._payload_bytes = 0

    def __iter__(self) -> Iterator[ReadySubmission]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    @property
    def work(self) -> int:
        return self._work

    @property
    def payload_bytes(self) -> int:
        return self._payload_bytes

    def can_accept(
        self,
        estimated_work: int,
        payload_bytes: int = 0,
    ) -> bool:
        if estimated_work > self.work_limit:
            raise ValueError(
                "request estimated work exceeds the shared ready window "
                "work limit"
            )
        if payload_bytes < 0:
            raise ValueError("request payload bytes must be non-negative")
        if (
            self.payload_bytes_limit is not None
            and payload_bytes > self.payload_bytes_limit
        ):
            raise ValueError(
                "request logical payload bytes exceed the shared ready "
                "window byte limit"
            )
        return (
            len(self._items) < self.request_limit
            and self._work + estimated_work <= self.work_limit
            and (
                self.payload_bytes_limit is None
                or self._payload_bytes + payload_bytes
                <= self.payload_bytes_limit
            )
        )

    def append(self, candidate: ReadySubmission) -> None:
        if not self.can_accept(
            candidate.estimated_work,
            candidate.payload_bytes,
        ):
            raise ValueError("shared ready window has no remaining capacity")
        self._items.append(candidate)
        self._work += candidate.estimated_work
        self._payload_bytes += candidate.payload_bytes

    def remove(self, candidate: ReadySubmission) -> None:
        self._items.remove(candidate)
        self._work -= candidate.estimated_work
        self._payload_bytes -= candidate.payload_bytes

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
