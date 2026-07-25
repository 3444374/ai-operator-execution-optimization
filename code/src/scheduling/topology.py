"""Endpoint topology queries."""

from __future__ import annotations

from .models import EndpointSnapshot, TopologySnapshot


def healthy_endpoints(topology: TopologySnapshot, pool_id: str) -> tuple[EndpointSnapshot, ...]:
    return tuple(
        endpoint
        for endpoint in topology.endpoints
        if endpoint.pool_id == pool_id and endpoint.healthy
    )
