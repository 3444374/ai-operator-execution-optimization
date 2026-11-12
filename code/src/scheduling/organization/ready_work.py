"""Small same-information priority comparators for already legal method work."""
from dataclasses import dataclass
from fractions import Fraction
import math


@dataclass(frozen=True)
class ReadyCost:
    service_ns: int
    remaining_path_ns: int
    source: str


class WorkProgressPriority:
    """The caller supplies calibrated costs; this policy never parses a payload.

    Remaining-path time is an estimate, not permission to run a successor. Missing
    costs return explicit FIFO. Aging retains the oldest request's opportunity.
    """
    def __init__(self, estimate, *, mode, max_wait_s):
        if mode not in ('shortest_work', 'longest_path', 'progress_per_work'):
            raise ValueError('unknown ready-work comparator')
        if not callable(estimate) or not math.isfinite(max_wait_s) or max_wait_s <= 0:
            raise ValueError('a cost provider and positive aging time are required')
        self.estimate, self.mode, self.max_wait_s = estimate, mode, max_wait_s

    def __call__(self, candidates, now_s):
        oldest = candidates[0]
        if now_s - oldest.ready_at >= self.max_wait_s:
            return oldest.sequence, 'aged_fifo'
        costs = [self.estimate(candidate) for candidate in candidates]
        if any(cost is None for cost in costs):
            return oldest.sequence, 'costs_unavailable_fifo'
        # Estimate inputs are checked once at the policy's external cost seam.
        if any(cost.service_ns <= 0 or cost.remaining_path_ns < 0 or not cost.source for cost in costs):
            raise ValueError('costs require positive service, nonnegative remaining path and a source')
        if self.mode == 'shortest_work':
            rank = lambda i: costs[i].service_ns
        elif self.mode == 'longest_path':
            rank = lambda i: -(costs[i].service_ns + costs[i].remaining_path_ns)
        else:
            rank = lambda i: -Fraction(costs[i].service_ns + costs[i].remaining_path_ns, costs[i].service_ns)
        selected = min(range(len(candidates)), key=rank)
        return candidates[selected].sequence, self.mode
