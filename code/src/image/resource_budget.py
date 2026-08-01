"""Fail-closed CPU resource budgets for staged image execution graphs."""

from __future__ import annotations

import os
from dataclasses import dataclass


def available_cpu_slots() -> int:
    """Return CPUs available to this process, respecting affinity when possible."""
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        return os.cpu_count() or 1


@dataclass(frozen=True)
class RayCpuBudget:
    """Explicit CPU slots held by one Ray-backed image execution graph."""

    cluster_slots: int
    host_slots: int
    source_slots: int | None
    preprocess_slots: int | None
    model_slots: int | None
    semantics: str

    @property
    def declared_total_slots(self) -> int:
        return sum(
            value
            for value in (self.source_slots, self.preprocess_slots, self.model_slots)
            if value is not None
        )

    def validate(self) -> None:
        if self.cluster_slots != self.declared_total_slots:
            raise ValueError(
                "Ray CPU cluster size must equal the sum of explicitly declared slots"
            )
        if self.cluster_slots > self.host_slots:
            raise ValueError(
                f"Ray CPU budget requires {self.cluster_slots} slots but the process "
                f"can use only {self.host_slots}; refusing CPU oversubscription"
            )


def build_ray_cpu_budget(
    *,
    arm: str,
    source_shards: int,
    preprocess_workers: int,
    gpu_workers: int,
    model_workers: int,
    host_slots: int | None = None,
) -> RayCpuBudget:
    """Build an exact source + stage + model CPU budget for one runner arm."""
    values = (source_shards, preprocess_workers, gpu_workers, model_workers)
    if min(values) <= 0:
        raise ValueError("resource counts must be positive")
    detected_host_slots = available_cpu_slots() if host_slots is None else host_slots
    if detected_host_slots <= 0:
        raise ValueError("host CPU slots must be positive")

    if arm == "daft_ray":
        parts = (source_shards, None, model_workers)
        semantics = "ray_reserved_slots_includes_daft_sql_readers_and_fused_model_actors"
    elif arm == "daft_staged":
        parts = (source_shards, preprocess_workers, model_workers)
        semantics = "ray_reserved_slots_includes_daft_sql_readers_and_both_actor_stages"
    elif arm == "ray_data_staged":
        parts = (source_shards, preprocess_workers, gpu_workers)
        semantics = "ray_reserved_slots_includes_sql_readers_and_both_actor_stages"
    elif arm == "project_ray":
        parts = (None, preprocess_workers, gpu_workers)
        semantics = "ray_reserved_actor_slots_source_executes_outside_ray_cluster"
    elif arm == "daft_native":
        parts = (None, None, None)
        semantics = "no_local_ray_cluster"
    else:
        raise ValueError(f"unsupported image arm: {arm}")

    budget = RayCpuBudget(
        cluster_slots=sum(value for value in parts if value is not None),
        host_slots=detected_host_slots,
        source_slots=parts[0],
        preprocess_slots=parts[1],
        model_slots=parts[2],
        semantics=semantics,
    )
    if arm != "daft_native":
        budget.validate()
    return budget
