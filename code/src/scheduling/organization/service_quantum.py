"""Split ordered rows into bounded service-completion units.

A service quantum groups complete rows for one HTTP/Ray completion event.  It
never splits a row's prompt, so model context and request semantics are
preserved.  This is an upstream scheduling boundary, not token-level prefill
chunking inside vLLM.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceQuantumSlice:
    """A half-open slice of complete input rows."""

    start: int
    stop: int
    estimated_work: int
    oversized: bool

    @property
    def row_count(self) -> int:
        return self.stop - self.start


def slice_service_quanta(
    row_costs: Sequence[int],
    target_tokens: int,
) -> tuple[ServiceQuantumSlice, ...]:
    """Accumulate ordered complete rows up to ``target_tokens``.

    A row whose predicted cost exceeds the target is emitted alone and marked
    oversized.  Zero-cost rows remain in their original order and are attached
    to the next or current non-empty quantum.
    """

    if (
        isinstance(target_tokens, bool)
        or not isinstance(target_tokens, int)
        or target_tokens <= 0
    ):
        raise ValueError("target_tokens must be a positive integer")

    costs = tuple(row_costs)
    if any(
        isinstance(cost, bool) or not isinstance(cost, int) or cost < 0
        for cost in costs
    ):
        raise ValueError("row costs must be non-negative integers")

    slices: list[ServiceQuantumSlice] = []
    start = 0
    work = 0
    for index, cost in enumerate(costs):
        if index > start and work + cost > target_tokens:
            slices.append(ServiceQuantumSlice(start, index, work, False))
            start = index
            work = 0

        if cost > target_tokens:
            if work:
                slices.append(ServiceQuantumSlice(start, index, work, False))
            slices.append(ServiceQuantumSlice(index, index + 1, cost, True))
            start = index + 1
            work = 0
        else:
            work += cost

    if start < len(costs):
        slices.append(ServiceQuantumSlice(start, len(costs), work, False))
    return tuple(slices)
