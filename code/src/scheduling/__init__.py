"""Lazy compatibility facade for composable scheduling modules.

Importing one policy must not load every legacy policy or runtime adapter. New
code should import from the owning submodule; these lazy exports preserve the
maintained package-level API while the runners migrate. The unused UCB
prototype is retained only in historical reproduction sources.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORT_GROUPS: dict[str, tuple[str, ...]] = {
    ".organization.batching": (
        "PendingBatch",
        "PendingBatchBuilder",
        "RowArrival",
    ),
    ".submission_control.adaptive": (
        "AimdAdmissionController",
        "AimdConfig",
    ),
    ".submission_control.admission": (
        "DynamicAdmissionGate",
        "StaticAdmissionController",
        "WindowController",
    ),
    ".core.errors": ("EndpointCapacityUnavailable",),
    ".core.control": ("CapacityArm",),
    ".core.execution": (
        "RecordedCompletion",
        "SubmissionContext",
        "SubmissionExecutionLedger",
    ),
    ".core.models": (
        "AdmissionDecision",
        "AdmissionObservation",
        "BatchRequest",
        "CollectedSubmission",
        "ControlDiagnostics",
        "EndpointSnapshot",
        "PayloadEnvelope",
        "PoolRoutingDecision",
        "RoutingDecision",
        "SubmissionCompletion",
        "SubmissionLifecycleEvent",
        "TopologySnapshot",
        "WindowDecision",
    ),
    ".organization": ("ServiceQuantumSlice", "slice_service_quanta"),
    ".runtime.observations": (
        "AdmissionTraceEvent",
        "CachedMetricsObservationProvider",
        "NonBlockingMetricsObservationProvider",
        "ServiceMetricsSnapshot",
    ),
    ".runtime.ray_adapter": (
        "ActorSubmissionState",
        "ActorWorkerAssignment",
        "ActorWorkerPoolSubmitter",
        "ActorWorkerSnapshot",
        "RaySubmissionAdapter",
    ),
    ".endpoint_routing.policies": (
        "LeastQueuedEndpointRouter",
        "LeastWorkEndpointRouter",
        "PinnedEndpointRouter",
        "PrefixAffinityEndpointRouter",
        "RequestPoolRouter",
        "RoundRobinEndpointRouter",
    ),
    ".core.policy_contracts": ("AdmissionPolicy", "EndpointRouter", "PoolRouter"),
    ".core.scheduler": (
        "EndpointCapacityConfig",
        "SchedulerConfig",
        "SchedulerResult",
        "SubmissionAdapter",
        "SynchronousScheduler",
    ),
    ".runtime.execution": ("SynchronousExecutionEngine",),
    ".core.topology": ("healthy_endpoints", "schedulable_endpoints"),
}

_EXPORTS = {name: module for module, names in _EXPORT_GROUPS.items() for name in names}
__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Resolve a compatibility export only when a caller requests it."""

    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted((*globals(), *__all__))
