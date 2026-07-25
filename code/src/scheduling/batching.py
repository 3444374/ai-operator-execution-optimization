"""Engine-independent pending batch construction from complete row requests."""

from __future__ import annotations

import math
from dataclasses import dataclass


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
