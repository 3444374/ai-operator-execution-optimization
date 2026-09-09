"""Fixed driver grants from one owner-thread payload budget; no execution scheduling."""

from dataclasses import dataclass

from .continuation import MethodLimits


@dataclass(frozen=True)
class MethodCapacity:
    runs: int
    bytes: int

    def __post_init__(self):
        if any(type(v) is not int or v <= 0 for v in (self.runs, self.bytes)):
            raise ValueError("method capacity must contain positive integers")


def row_reservation(limits: MethodLimits) -> int:
    # Old and new continuations may coexist during resume. Include bounded identity/profile text.
    return (
        2 * (limits.input_bytes + limits.state_bytes + limits.result_bytes + 256) + 256
    )


class MethodBudgetPool:
    """The service owner allocates all driver grants here, including sibling flows of a Job.

    Grants reserve worst-case capacity for their lifetime, so a slow consumer cannot take
    another driver's allocation. This counts retained payload envelopes, not Python RSS.
    """

    def __init__(self, capacity: MethodCapacity):
        self.capacity = capacity
        self._grants = set()

    @property
    def allocated(self) -> tuple[int, int]:
        return (
            sum(g.capacity.runs for g in self._grants),
            sum(g.capacity.bytes for g in self._grants),
        )

    @property
    def used(self) -> tuple[int, int]:
        return (sum(g.runs for g in self._grants), sum(g.bytes for g in self._grants))

    def allocate(self, capacity: MethodCapacity) -> "MethodBudget":
        runs, size = self.allocated
        if (
            runs + capacity.runs > self.capacity.runs
            or size + capacity.bytes > self.capacity.bytes
        ):
            raise ValueError("method pool capacity exhausted")
        grant = MethodBudget(self, capacity)
        self._grants.add(grant)
        return grant


class MethodBudget:
    """A single driver owns a grant; per-row reservations stay charged through final release."""

    def __init__(self, pool, capacity):
        self._pool, self.capacity = pool, capacity
        self.runs = self.bytes = 0
        self._claimed = False
        self._closed = False

    def claim(self):
        if self._closed or self._claimed:
            raise ValueError("method budget already claimed or closed")
        self._claimed = True

    def reserve(self, size):
        if self._closed or not self._claimed:
            raise RuntimeError("method budget is not active")
        if self.runs >= self.capacity.runs or self.bytes + size > self.capacity.bytes:
            return False
        self.runs += 1
        self.bytes += size
        return True

    def release(self, size):
        if self.runs <= 0 or size > self.bytes:
            raise RuntimeError("method reservation underflow")
        self.runs -= 1
        self.bytes -= size

    def close(self):
        if self._closed:
            return
        if self.runs or self.bytes:
            raise RuntimeError("method budget still holds rows")
        self._pool._grants.remove(self)
        self._closed = True
