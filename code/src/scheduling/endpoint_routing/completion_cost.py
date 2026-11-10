"""Route using explicit queue, representation transfer and rebuild estimates.

The cost provider owns cache/data-location observations. Routing chooses an
existing endpoint; it neither moves KV nor changes resource admission.
"""
from dataclasses import dataclass

from ..core.models import RoutingDecision
from .policies import LeastQueuedEndpointRouter, _schedulable_or_raise


@dataclass(frozen=True)
class EndpointCost:
    queue_ns: int
    transfer_ns: int
    rebuild_ns: int
    source: str

    @property
    def total_ns(self):
        return self.queue_ns + self.transfer_ns + self.rebuild_ns


class CompletionCostEndpointRouter:
    def __init__(self, estimate):
        if not callable(estimate):
            raise TypeError('an explicit endpoint-cost provider is required')
        self.estimate = estimate
        self.least_queued = LeastQueuedEndpointRouter()

    def route(self, request, topology, pool_id):
        candidates = _schedulable_or_raise(topology, pool_id)
        costs = [self.estimate(request, endpoint) for endpoint in candidates]
        if any(cost is None for cost in costs):
            fallback = self.least_queued.route(request, topology, pool_id)
            return RoutingDecision(fallback.endpoint_id, pool_id, 'costs_unavailable_least_queued')
        if any(min(cost.queue_ns, cost.transfer_ns, cost.rebuild_ns) < 0 or not cost.source for cost in costs):
            raise ValueError('endpoint costs require nonnegative durations and a source')
        choice = min(range(len(candidates)), key=lambda i: costs[i].total_ns)
        return RoutingDecision(candidates[choice].endpoint_id, pool_id, 'estimated_queue_transfer_rebuild')
