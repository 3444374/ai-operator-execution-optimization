"""Compatibility imports for profiler configuration."""

from .profiling.config import (
    completion_endpoint_urls,
    configured_urls,
    embedding_endpoint_urls,
    model_metrics_urls,
    ray_worker_options,
    resolve_actor_workers_per_endpoint,
    validate_ray_worker_resources,
)

__all__ = [
    "completion_endpoint_urls",
    "configured_urls",
    "embedding_endpoint_urls",
    "model_metrics_urls",
    "ray_worker_options",
    "resolve_actor_workers_per_endpoint",
    "validate_ray_worker_resources",
]
