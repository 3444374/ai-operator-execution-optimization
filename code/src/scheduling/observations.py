"""Cached service observations for adaptive scheduling."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass

from .models import AdmissionObservation


@dataclass(frozen=True)
class ServiceMetricsSnapshot:
    running: int | None
    waiting: int | None
    kv_usage: float | None

    def __post_init__(self) -> None:
        # Reuse the observation schema's validation without inventing another
        # set of engine-facing range rules.
        AdmissionObservation(
            observed_at_s=0.0,
            fresh=True,
            inflight=0,
            running=self.running,
            waiting=self.waiting,
            kv_usage=self.kv_usage,
        )


@dataclass(frozen=True)
class AdmissionTraceEvent:
    observed_at_s: float
    fresh: bool
    inflight: int
    window: int
    running: int | None
    waiting: int | None
    kv_usage: float | None
    controller_action: str
    reason: str
    allowed: bool


class CachedMetricsObservationProvider:
    def __init__(
        self,
        sampler: Callable[[], ServiceMetricsSnapshot | None],
        *,
        min_sample_interval_s: float = 0.25,
        clock: Callable[[], float] = time.monotonic,
    ):
        if min_sample_interval_s < 0:
            raise ValueError("min_sample_interval_s must be non-negative")
        self.sampler = sampler
        self.min_sample_interval_s = min_sample_interval_s
        self.clock = clock
        self._last_sample_s: float | None = None
        self._cached: ServiceMetricsSnapshot | None = None

    def latest(self, inflight: int) -> AdmissionObservation:
        now = self.clock()
        sample_due = (
            self._last_sample_s is None
            or now - self._last_sample_s >= self.min_sample_interval_s
        )
        if sample_due:
            self._cached = self.sampler()
            self._last_sample_s = now
        snapshot = self._cached
        return AdmissionObservation(
            observed_at_s=now,
            fresh=sample_due,
            inflight=inflight,
            running=snapshot.running if snapshot is not None else None,
            waiting=snapshot.waiting if snapshot is not None else None,
            kv_usage=snapshot.kv_usage if snapshot is not None else None,
        )
