"""Explicit Ray resource contracts for external HTTP model workers."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class RayWorkerOptions:
    """Ray options for workers that call an external HTTP model service."""

    num_cpus: float
    actor_max_concurrency: int = 1

    def __post_init__(self) -> None:
        if not math.isfinite(self.num_cpus) or self.num_cpus <= 0:
            raise ValueError("num_cpus must be positive and finite")
        if self.actor_max_concurrency <= 0:
            raise ValueError("actor_max_concurrency must be positive")

    def task_options(self) -> dict[str, object]:
        return {
            "num_cpus": self.num_cpus,
            "num_gpus": 0,
            "max_retries": 0,
        }

    def actor_options(self) -> dict[str, object]:
        return {
            "num_cpus": self.num_cpus,
            "num_gpus": 0,
            "max_concurrency": self.actor_max_concurrency,
            "max_restarts": 0,
            "max_task_retries": 0,
        }
