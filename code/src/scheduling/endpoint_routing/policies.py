"""Endpoint routing policies."""

from __future__ import annotations

import hashlib

from ..errors import EndpointCapacityUnavailable
from ..models import (
    BatchRequest,
    EndpointSnapshot,
    PoolRoutingDecision,
    RoutingDecision,
    TopologySnapshot,
)
from ..topology import healthy_endpoints, schedulable_endpoints


def _schedulable_or_raise(
    topology: TopologySnapshot,
    pool_id: str,
) -> tuple[EndpointSnapshot, ...]:
    candidates = schedulable_endpoints(topology, pool_id)
    if candidates:
        return candidates
    if healthy_endpoints(topology, pool_id):
        raise EndpointCapacityUnavailable(
            f"no endpoint in pool {pool_id} has admission capacity"
        )
    raise RuntimeError(f"no healthy endpoint in pool {pool_id}")


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
        candidates = _schedulable_or_raise(topology, pool_id)
        index = self._next_index_by_pool.get(pool_id, 0)
        endpoint = candidates[index % len(candidates)]
        self._next_index_by_pool[pool_id] = (index + 1) % len(candidates)
        return RoutingDecision(endpoint.endpoint_id, pool_id, "round_robin")


class LeastQueuedEndpointRouter:
    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
        pool_id: str,
    ) -> RoutingDecision:
        del request
        candidates = _schedulable_or_raise(topology, pool_id)
        endpoint = min(
            candidates,
            key=lambda item: (item.running + item.waiting, item.endpoint_id),
        )
        return RoutingDecision(endpoint.endpoint_id, pool_id, "least_queued")


class LeastWorkEndpointRouter:
    """Route by predicted endpoint drain work instead of request count."""

    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
        pool_id: str,
    ) -> RoutingDecision:
        candidates = _schedulable_or_raise(topology, pool_id)

        def predicted_finish(endpoint) -> tuple[float, str]:
            work = (
                endpoint.estimated_active_work
                + request.estimated_total_tokens
            )
            drain_s = (
                work / endpoint.service_rate_tokens_s
                if endpoint.service_rate_tokens_s is not None
                else float(work)
            )
            return drain_s, endpoint.endpoint_id

        endpoint = min(candidates, key=predicted_finish)
        return RoutingDecision(endpoint.endpoint_id, pool_id, "least_work")


class PinnedEndpointRouter:
    """Route a request only to the endpoint fixed by its input manifest."""

    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
        pool_id: str,
    ) -> RoutingDecision:
        endpoint_id = request.preferred_endpoint_id
        if not endpoint_id:
            raise RuntimeError("missing preferred endpoint")
        endpoint = next(
            (
                candidate
                for candidate in topology.endpoints
                if candidate.endpoint_id == endpoint_id
            ),
            None,
        )
        if endpoint is None:
            raise RuntimeError(f"unknown preferred endpoint {endpoint_id}")
        if endpoint.pool_id != pool_id:
            raise RuntimeError(
                f"preferred endpoint {endpoint_id} is outside pool {pool_id}"
            )
        if not endpoint.healthy:
            raise RuntimeError(
                f"preferred endpoint {endpoint_id} is not healthy"
            )
        if not endpoint.available:
            raise EndpointCapacityUnavailable(
                f"preferred endpoint {endpoint_id} has no admission capacity"
            )
        return RoutingDecision(endpoint_id, pool_id, "manifest_pinned")


class RequestPoolRouter:
    def __init__(
        self,
        long_request_tokens: int,
        *,
        prefix_pool_id: str = "prefix",
        long_pool_id: str = "long",
        short_pool_id: str = "short",
    ):
        if long_request_tokens <= 0:
            raise ValueError("long_request_tokens must be positive")
        self.long_request_tokens = long_request_tokens
        self.prefix_pool_id = prefix_pool_id
        self.long_pool_id = long_pool_id
        self.short_pool_id = short_pool_id

    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
    ) -> PoolRoutingDecision:
        available = {
            endpoint.pool_id
            for endpoint in topology.endpoints
            if endpoint.healthy and endpoint.available
        }
        if not available:
            raise RuntimeError("no healthy endpoint in any pool")

        if request.prefix_key:
            desired_pool = self.prefix_pool_id
            desired_reason = "prefix_request"
        elif request.estimated_total_tokens >= self.long_request_tokens:
            desired_pool = self.long_pool_id
            desired_reason = "long_request"
        else:
            desired_pool = self.short_pool_id
            desired_reason = "short_request"
        if desired_pool in available:
            return PoolRoutingDecision(desired_pool, desired_reason)

        fallback_order = (
            self.short_pool_id,
            self.long_pool_id,
            self.prefix_pool_id,
        )
        fallback = next(
            (pool_id for pool_id in fallback_order if pool_id in available),
            sorted(available)[0],
        )
        return PoolRoutingDecision(fallback, "fallback_available_pool")


class PrefixAffinityEndpointRouter:
    def __init__(self) -> None:
        self._least_queued = LeastQueuedEndpointRouter()

    def route(
        self,
        request: BatchRequest,
        topology: TopologySnapshot,
        pool_id: str,
    ) -> RoutingDecision:
        if not request.prefix_key:
            fallback = self._least_queued.route(request, topology, pool_id)
            return RoutingDecision(
                fallback.endpoint_id,
                pool_id,
                "missing_prefix_least_queued_fallback",
            )

        pool_endpoints = tuple(
            endpoint
            for endpoint in topology.endpoints
            if endpoint.pool_id == pool_id
        )
        healthy = tuple(
            endpoint
            for endpoint in pool_endpoints
            if endpoint.healthy and endpoint.available
        )
        if not healthy:
            if any(endpoint.healthy for endpoint in pool_endpoints):
                raise EndpointCapacityUnavailable(
                    f"no endpoint in pool {pool_id} has admission capacity"
                )
            raise RuntimeError(f"no healthy endpoint in pool {pool_id}")
        affinity_endpoint = max(
            pool_endpoints,
            key=lambda endpoint: self._rendezvous_score(
                request.prefix_key,
                endpoint.endpoint_id,
            ),
        )
        if affinity_endpoint.healthy and affinity_endpoint.available:
            return RoutingDecision(
                affinity_endpoint.endpoint_id,
                pool_id,
                "prefix_affinity",
            )
        fallback = self._least_queued.route(request, topology, pool_id)
        return RoutingDecision(
            fallback.endpoint_id,
            pool_id,
            "prefix_unhealthy_least_queued_fallback",
        )

    @staticmethod
    def _rendezvous_score(prefix_key: str, endpoint_id: str) -> int:
        digest = hashlib.sha256(
            f"{prefix_key}\0{endpoint_id}".encode("utf-8")
        ).digest()
        return int.from_bytes(digest, byteorder="big", signed=False)
