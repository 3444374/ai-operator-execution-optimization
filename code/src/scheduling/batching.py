"""Engine-independent pending batch construction from complete row requests."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Callable, Generic, Iterable, Iterator, Protocol, TypeVar

from .flush import FlushDecision, FlushObservation


def _is_non_negative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


@dataclass(frozen=True)
class RowArrival:
    """Metadata for one complete request entering the pending batch."""

    row_id: str
    arrival_s: float
    prompt_tokens: int
    estimated_output_tokens: int
    prefix_key: str
    payload_ref: object

    def __post_init__(self) -> None:
        if not isinstance(self.row_id, str) or not self.row_id:
            raise ValueError("row_id must be non-empty")
        if not isinstance(self.arrival_s, (int, float)) or isinstance(
            self.arrival_s, bool
        ) or not math.isfinite(self.arrival_s) or self.arrival_s < 0:
            raise ValueError("arrival_s must be finite and non-negative")
        if not _is_non_negative_int(self.prompt_tokens):
            raise ValueError("prompt token count must be a non-negative integer")
        if not _is_non_negative_int(self.estimated_output_tokens):
            raise ValueError(
                "estimated output token count must be a non-negative integer"
            )
        if not isinstance(self.prefix_key, str):
            raise ValueError("prefix_key must be a string")
        if self.payload_ref is None:
            raise ValueError("payload_ref must not be None")

    @property
    def estimated_total_tokens(self) -> int:
        return self.prompt_tokens + self.estimated_output_tokens


@dataclass(frozen=True)
class PendingBatch:
    """A closed, ordered batch of complete row requests."""

    rows: tuple[RowArrival, ...]
    prompt_tokens: int
    estimated_output_tokens: int
    oldest_arrival_s: float

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def estimated_total_tokens(self) -> int:
        return self.prompt_tokens + self.estimated_output_tokens


class PendingBatchBuilder:
    """Accumulates complete rows until a row or token capacity is reached."""

    def __init__(self, max_rows: int, token_budget: int) -> None:
        if not _is_non_negative_int(max_rows) or max_rows == 0:
            raise ValueError("max_rows must be a positive integer")
        if not _is_non_negative_int(token_budget):
            raise ValueError("token_budget must be a non-negative integer")
        self._max_rows = max_rows
        self._token_budget = token_budget
        self._rows: list[RowArrival] = []
        self._prompt_tokens = 0
        self._estimated_output_tokens = 0
        self._capacity_reached = False

    def would_exceed_token_budget(self, row: RowArrival) -> bool:
        """Report whether a later complete row requires a new token batch."""
        if not isinstance(row, RowArrival):
            raise ValueError("row must be a RowArrival")
        return (
            bool(self._rows)
            and self._token_budget > 0
            and (
                self._prompt_tokens
                + self._estimated_output_tokens
                + row.estimated_total_tokens
                > self._token_budget
            )
        )

    def add(self, row: RowArrival) -> bool:
        """Add a complete row and report whether this batch must now close."""
        if self._capacity_reached:
            raise RuntimeError("batch capacity reached; call close() before adding")
        if not isinstance(row, RowArrival):
            raise ValueError("row must be a RowArrival")

        self._rows.append(row)
        self._prompt_tokens += row.prompt_tokens
        self._estimated_output_tokens += row.estimated_output_tokens
        self._capacity_reached = (
            len(self._rows) >= self._max_rows
            or (
                self._token_budget > 0
                and self._prompt_tokens + self._estimated_output_tokens
                >= self._token_budget
            )
        )
        return self._capacity_reached

    def close(self) -> PendingBatch:
        """Close the current batch and reset the builder for the next one."""
        if not self._rows:
            raise ValueError("cannot close an empty pending batch")

        batch = PendingBatch(
            rows=tuple(self._rows),
            prompt_tokens=self._prompt_tokens,
            estimated_output_tokens=self._estimated_output_tokens,
            oldest_arrival_s=min(row.arrival_s for row in self._rows),
        )
        self._rows = []
        self._prompt_tokens = 0
        self._estimated_output_tokens = 0
        self._capacity_reached = False
        return batch


class ReplayClock(Protocol):
    """Monotonic clock boundary used by arrival replay."""

    def now(self) -> float:
        ...

    def wait_until(self, deadline_s: float) -> None:
        ...


class SystemReplayClock:
    """Wall-clock replay implementation backed only by monotonic time."""

    def now(self) -> float:
        return time.monotonic()

    def wait_until(self, deadline_s: float) -> None:
        delay_s = deadline_s - self.now()
        if delay_s > 0:
            time.sleep(delay_s)


@dataclass(frozen=True)
class ReplayServiceObservation:
    fresh: bool
    running: int | None
    waiting: int | None
    kv_usage: float | None


@dataclass(frozen=True)
class FlushTraceEvent:
    elapsed_s: float
    pending_rows: int
    pending_tokens: int
    oldest_age_s: float
    action: str
    reason: str


class _FlushPolicy(Protocol):
    def decide(self, observation: FlushObservation) -> FlushDecision:
        ...


ReplayResult = TypeVar("ReplayResult")


class ArrivalReplayBatcher(Generic[ReplayResult]):
    """Lazily replay row arrivals and close batches through a flush policy."""

    def __init__(
        self,
        rows: Iterable[RowArrival],
        builder_factory: Callable[[], PendingBatchBuilder],
        flush_policy: _FlushPolicy,
        close_batch: Callable[[PendingBatch], ReplayResult],
        service_observation: Callable[[], ReplayServiceObservation],
        clock: ReplayClock,
        arrival_time_scale: float = 1.0,
    ) -> None:
        if (
            not isinstance(arrival_time_scale, (int, float))
            or isinstance(arrival_time_scale, bool)
            or not math.isfinite(arrival_time_scale)
            or arrival_time_scale <= 0
        ):
            raise ValueError("arrival_time_scale must be finite and positive")
        self._rows = rows
        self._builder_factory = builder_factory
        self._flush_policy = flush_policy
        self._close_batch = close_batch
        self._service_observation = service_observation
        self._clock = clock
        self._arrival_time_scale = float(arrival_time_scale)
        self._trace: list[FlushTraceEvent] = []

    @property
    def trace(self) -> tuple[FlushTraceEvent, ...]:
        return tuple(self._trace)

    def __iter__(self) -> Iterator[ReplayResult]:
        self._trace = []
        source = iter(self._rows)
        first = self._next_validated(source, previous_arrival_s=None)
        if first is None:
            return

        origin_arrival_s = first.arrival_s
        previous_arrival_s = first.arrival_s
        replay_start_s = self._clock.now()
        next_row: RowArrival | None = first
        next_deadline_s = replay_start_s
        builder = self._builder_factory()
        if not isinstance(builder, PendingBatchBuilder):
            raise ValueError("builder_factory must return PendingBatchBuilder")

        pending_rows = 0
        pending_tokens = 0
        pending_oldest_deadline_s = 0.0
        budget_reached = False

        while next_row is not None or pending_rows:
            if pending_rows:
                service = self._service_observation()
                now_s = self._clock.now()
                policy_deadline = self._policy_deadline(
                    pending_oldest_deadline_s
                )
                if (
                    policy_deadline is not None
                    and now_s >= policy_deadline[0]
                ):
                    decision = FlushDecision(
                        True,
                        "flush",
                        policy_deadline[1],
                        now_s - pending_oldest_deadline_s,
                    )
                else:
                    decision = self._flush_policy.decide(
                        FlushObservation(
                            now_s=now_s,
                            oldest_arrival_s=pending_oldest_deadline_s,
                            pending_rows=pending_rows,
                            pending_cost=pending_tokens,
                            budget_reached=budget_reached,
                            metrics_fresh=service.fresh,
                            running=service.running,
                            waiting=service.waiting,
                            kv_usage=service.kv_usage,
                        )
                    )
                self._record_trace(
                    replay_start_s,
                    now_s,
                    pending_rows,
                    pending_tokens,
                    pending_oldest_deadline_s,
                    decision.action,
                    decision.reason,
                )
                if decision.flush:
                    closed = builder.close()
                    pending_rows = 0
                    pending_tokens = 0
                    budget_reached = False
                    yield self._close_batch(closed)
                    continue

            else:
                now_s = self._clock.now()
                policy_deadline = None

            if next_row is None:
                self._record_trace(
                    replay_start_s,
                    now_s,
                    pending_rows,
                    pending_tokens,
                    pending_oldest_deadline_s,
                    "flush",
                    "end_of_input",
                )
                closed = builder.close()
                pending_rows = 0
                pending_tokens = 0
                budget_reached = False
                yield self._close_batch(closed)
                continue

            if next_deadline_s <= now_s:
                if builder.would_exceed_token_budget(next_row):
                    self._record_trace(
                        replay_start_s,
                        now_s,
                        pending_rows,
                        pending_tokens,
                        pending_oldest_deadline_s,
                        "flush",
                        "token_budget_membership",
                    )
                    closed = builder.close()
                    pending_rows = 0
                    pending_tokens = 0
                    budget_reached = False
                    yield self._close_batch(closed)
                    continue

                if pending_rows == 0:
                    pending_oldest_deadline_s = next_deadline_s
                pending_rows += 1
                pending_tokens += next_row.estimated_total_tokens
                budget_reached = builder.add(next_row)

                following = self._next_validated(
                    source,
                    previous_arrival_s=previous_arrival_s,
                )
                if following is None:
                    next_row = None
                else:
                    previous_arrival_s = following.arrival_s
                    next_row = following
                    next_deadline_s = replay_start_s + (
                        following.arrival_s - origin_arrival_s
                    ) * self._arrival_time_scale
                continue

            wake_deadline_s = next_deadline_s
            if policy_deadline is not None:
                policy_deadline_s = policy_deadline[0]
                if (
                    now_s < policy_deadline_s <= wake_deadline_s
                ):
                    wake_deadline_s = policy_deadline_s
            if wake_deadline_s > now_s:
                self._clock.wait_until(wake_deadline_s)

    @staticmethod
    def _next_validated(
        source: Iterator[RowArrival],
        previous_arrival_s: float | None,
    ) -> RowArrival | None:
        try:
            row = next(source)
        except StopIteration:
            return None
        try:
            arrival_s = row.arrival_s
        except (AttributeError, TypeError):
            raise ValueError(
                "arrival_s must be present, finite, and non-negative"
            ) from None
        if (
            not isinstance(arrival_s, (int, float))
            or isinstance(arrival_s, bool)
            or not math.isfinite(arrival_s)
            or arrival_s < 0
        ):
            raise ValueError("arrival_s must be finite and non-negative")
        if previous_arrival_s is not None and arrival_s < previous_arrival_s:
            raise ValueError("arrival_s values must be non-decreasing")
        if not isinstance(row, RowArrival):
            raise ValueError("rows must contain RowArrival values")
        return row

    def _policy_deadline(
        self,
        oldest_deadline_s: float,
    ) -> tuple[float, str] | None:
        deadline_fields = (
            ("timeout_s", "fixed_timeout"),
            ("max_wait_s", "hard_max_wait"),
        )
        for attribute, reason in deadline_fields:
            wait_s = getattr(self._flush_policy, attribute, None)
            if isinstance(wait_s, (int, float)) and not isinstance(wait_s, bool):
                return oldest_deadline_s + float(wait_s), reason
        return None

    def _record_trace(
        self,
        replay_start_s: float,
        now_s: float,
        pending_rows: int,
        pending_tokens: int,
        oldest_deadline_s: float,
        action: str,
        reason: str,
    ) -> None:
        self._trace.append(
            FlushTraceEvent(
                elapsed_s=now_s - replay_start_s,
                pending_rows=pending_rows,
                pending_tokens=pending_tokens,
                oldest_age_s=now_s - oldest_deadline_s,
                action=action,
                reason=reason,
            )
        )
