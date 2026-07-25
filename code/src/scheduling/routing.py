"""Endpoint routing policies."""

from __future__ import annotations

from .models import BatchRequest, RoutingDecision, TopologySnapshot
from .topology import healthy_endpoints


class RoundRobinEndpointRouter:
    def __init__(self) -> None:
        self._next_index_by_pool: dict[str, int] = {}

    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
        pool_id: str,
    ) -> RoutingDecision:
        del request
        candidates = healthy_endpoints(topology, pool_id)
        if not candidates:
            raise RuntimeError(f"no healthy endpoint in pool {pool_id}")
        index = self._next_index_by_pool.get(pool_id, 0)
        endpoint = candidates[index % len(candidates)]
        self._next_index_by_pool[pool_id] = (index + 1) % len(candidates)
        return RoutingDecision(endpoint.endpoint_id, pool_id, "round_robin")
