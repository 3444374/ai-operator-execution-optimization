"""Lazy compatibility facade for composable scheduling modules.

Importing one policy must not load every legacy policy or runtime adapter. New
code should import from the owning submodule; these lazy exports preserve the
existing package-level API while the runners migrate.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORT_GROUPS: dict[str, tuple[str, ...]] = {
    ".organization.batching": (
        "ArrivalReplayBatcher",
        "FlushTraceEvent",
        "PendingBatch",
        "PendingBatchBuilder",
        "ReplayClock",
        "ReplayServiceObservation",
        "RowArrival",
        "SystemReplayClock",
    ),
    ".submission_control.adaptive": (
        "AimdAdmissionController",
        "AimdConfig",
        "EwmaAimdAdmissionController",
        "HolAgeAimdAdmissionController",
        "HolAgeAimdConfig",
    ),
    ".submission_control.admission": (
        "DynamicAdmissionGate",
        "StaticAdmissionController",
        "WindowController",
    ),
    ".submission_control.flush": (
        "FixedTimeoutFlush",
        "FlushDecision",
        "FlushObservation",
        "FlushWindow",
        "ImmediateFlush",
        "QueueAdaptiveFlush",
        "SloAwareEwmaFlush",
    ),
    ".core.errors": ("EndpointCapacityUnavailable",),
    ".core.control": ("CapacityArm",),
    ".core.execution": (
        "RecordedCompletion",
        "SubmissionContext",
        "SubmissionExecutionLedger",
    ),
    ".core.lifecycle": (
        "MonotonicEpochClock",
        "RequestLifecycleSeed",
        "RequestTraceRow",
        "SubmissionServiceTiming",
        "build_request_trace_rows",
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
    ".submission_control.pid": ("PidAdmissionController", "PidConfig"),
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
        "RoundRobinSubmitter",
    ),
    ".runtime.ray_runtime": ("RayWorkerOptions",),
    ".endpoint_routing.policies": (
        "LeastQueuedEndpointRouter",
        "LeastWorkEndpointRouter",
        "PinnedEndpointRouter",
        "PrefixAffinityEndpointRouter",
        "RequestPoolRouter",
        "RoundRobinEndpointRouter",
    ),
    ".core.scheduler": (
        "AdmissionPolicy",
        "EndpointRouter",
        "PoolRouter",
        "SchedulerResult",
        "SubmissionAdapter",
        "SynchronousScheduler",
    ),
    ".submission_control.shared_credit": (
        "CreditLease",
        "EndpointCreditSnapshot",
        "FairEndpointCreditCoordinator",
    ),
    ".submission_control.ordered_release": (
        "OrderedReleaseCoordinator",
        "OrderedReleaseSnapshot",
        "ReleasedSubmission",
    ),
    ".submission_control.saor": (
        "SaorAction",
        "SaorControlState",
        "SaorDecision",
        "SaorJobState",
        "SaorPolicy",
        "SaorReleaseConfig",
        "SaorReleaseCandidate",
        "SaorReleaseSelection",
        "SaorReleaseState",
        "build_single_release_actions",
        "select_saor_release_job",
        "update_fairness_debts",
    ),
    ".submission_control.capacity": (
        "BoundedCapacityController",
        "CapacityDecision",
    ),
    ".core.topology": ("healthy_endpoints", "schedulable_endpoints"),
    ".organization.token_budget": (
        "ArrivalRateEwma",
        "ServiceQuantumTokenBudgetController",
        "StaticTokenBudgetController",
        "TokenBudgetDecision",
        "TokenBudgetObservation",
    ),
    ".submission_control.ucb": (
        "SloRewardInput",
        "UcbAdmissionController",
        "UcbConfig",
        "slo_constrained_reward",
    ),
}

_EXPORTS = {
    name: module
    for module, names in _EXPORT_GROUPS.items()
    for name in names
}
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
