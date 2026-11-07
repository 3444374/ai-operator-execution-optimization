"""Endpoint topology queries and local occupancy projection."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace

from .execution import SubmissionContext
from .models import EndpointSnapshot, TopologySnapshot


def healthy_endpoints(topology: TopologySnapshot, pool_id: str) -> tuple[EndpointSnapshot, ...]:
    return tuple(
        endpoint
        for endpoint in topology.endpoints
        if endpoint.pool_id == pool_id and endpoint.healthy
    )


def schedulable_endpoints(
    topology: TopologySnapshot,
    pool_id: str,
) -> tuple[EndpointSnapshot, ...]:
    """Return healthy endpoints that have credit for the current request."""
    return tuple(
        endpoint
        for endpoint in healthy_endpoints(topology, pool_id)
        if endpoint.available
    )


def with_local_inflight(
    topology: TopologySnapshot,
    submission_context: Mapping[str, SubmissionContext],
    *,
    per_endpoint_limit: int | None = None,
    per_endpoint_work_limit: int | None = None,
    request_work: int = 0,
) -> TopologySnapshot:
    inflight_by_endpoint: dict[str, int] = {}
    work_by_endpoint: dict[str, int] = {}
    for item in submission_context.values():
        endpoint_id = item.endpoint_id
        inflight_by_endpoint[endpoint_id] = inflight_by_endpoint.get(endpoint_id, 0) + 1
        work_by_endpoint[endpoint_id] = (
            work_by_endpoint.get(endpoint_id, 0) + item.estimated_work
        )
    return replace(
        topology,
        endpoints=tuple(
            replace(
                endpoint,
                available=(
                    endpoint.available
                    and (
                        per_endpoint_limit is None
                        or inflight_by_endpoint.get(endpoint.endpoint_id, 0)
                        < per_endpoint_limit
                    )
                    and (
                        per_endpoint_work_limit is None
                        or (
                            endpoint.estimated_active_work
                            + work_by_endpoint.get(endpoint.endpoint_id, 0)
                            == 0
                        )
                        or (
                            endpoint.estimated_active_work
                            + work_by_endpoint.get(endpoint.endpoint_id, 0)
                            + request_work
                            <= per_endpoint_work_limit
                        )
                    )
                ),
                running=(endpoint.running + inflight_by_endpoint.get(endpoint.endpoint_id, 0)),
                estimated_active_work=(
                    endpoint.estimated_active_work
                    + work_by_endpoint.get(endpoint.endpoint_id, 0)
                ),
            )
            for endpoint in topology.endpoints
        ),
    )
