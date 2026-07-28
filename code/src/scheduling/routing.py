"""Compatibility imports for endpoint-routing policies."""

from .endpoint_routing.policies import (
    LeastQueuedEndpointRouter,
    LeastWorkEndpointRouter,
    PrefixAffinityEndpointRouter,
    RequestPoolRouter,
    RoundRobinEndpointRouter,
)

__all__ = [
    "LeastQueuedEndpointRouter",
    "LeastWorkEndpointRouter",
    "PrefixAffinityEndpointRouter",
    "RequestPoolRouter",
    "RoundRobinEndpointRouter",
]
