"""Effective local CPU capacity for preflight and explicit launch budgets."""

from dataclasses import dataclass
import math
import os
import sys

from .cgroup import controller_hierarchy, cpu_quota


@dataclass(frozen=True)
class CpuCapacity:
    affinity_slots: int | None
    quota_slots: float | None
    effective_slots: float | None
    status: str


def observe_cpu_capacity():
    """Intersect process affinity and all visible v1/v2 bandwidth quotas.

    A fractional quota remains fractional. Unreadable allocation metadata does
    not grant the host CPU count to a container.
    """
    try:
        if hasattr(os, "sched_getaffinity"):
            affinity = len(os.sched_getaffinity(0))
        elif sys.platform == "linux":
            raise ValueError("Linux process affinity unavailable")
        else:
            affinity = os.cpu_count()
        if type(affinity) is not int or affinity <= 0:
            raise ValueError("CPU affinity/count unavailable")
    except (OSError, ValueError):
        return CpuCapacity(None, None, None, "unavailable:CPU_affinity")
    try:
        version, directories = controller_hierarchy("cpu")
        quota = cpu_quota(version, directories)
        if quota is not None and (not math.isfinite(quota) or quota <= 0):
            raise ValueError("CPU quota is not finite and positive")
    except (OSError, ValueError, TypeError, OverflowError):
        return CpuCapacity(affinity, None, None, "unavailable:cgroup_CPU_quota")
    return CpuCapacity(affinity, quota, min(affinity, quota) if quota is not None else affinity,
                       "observed_cgroup_quota" if quota is not None else "affinity_no_visible_quota")


def checked_local_cpu_slots(required):
    """Validate an unchanged Ray launch declaration, without shrinking actors."""
    if type(required) is not int or required <= 0:
        raise ValueError("declared local CPU slots must be a positive integer")
    capacity = observe_cpu_capacity()
    if capacity.effective_slots is None:
        raise RuntimeError("local CPU allocation is unknown; refusing an unchecked Ray CPU declaration")
    if required > capacity.effective_slots:
        raise ValueError(f"local CPU budget requires {required} slots but effective allocation is {capacity.effective_slots}")
    return required
