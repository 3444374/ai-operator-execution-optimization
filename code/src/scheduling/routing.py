"""Compatibility imports for endpoint-routing policies."""

from .endpoint_routing.policies import (
    LeastQueuedEndpointRouter,
    LeastWorkEndpointRouter,
    PinnedEndpointRouter,
    PrefixAffinityEndpointRouter,
    RequestPoolRouter,
    RoundRobinEndpointRouter,
)

__all__ = [
    "LeastQueuedEndpointRouter",
    "LeastWorkEndpointRouter",
    "PinnedEndpointRouter",
    "PrefixAffinityEndpointRouter",
    "RequestPoolRouter",
    "RoundRobinEndpointRouter",
]
