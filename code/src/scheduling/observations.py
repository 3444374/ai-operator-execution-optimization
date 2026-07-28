"""Cached service observations for adaptive scheduling."""

from __future__ import annotations

import time
import threading
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
    sample_age_s: float | None = None
    hol_age_s: float | None = None


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

    def latest(
        self, inflight: int, *, hol_age_s: float | None = None
    ) -> AdmissionObservation:
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
            sample_age_s=(
                now - self._last_sample_s
                if self._last_sample_s is not None
                else None
            ),
            hol_age_s=hol_age_s,
        )

    def close(self) -> None:
        """Match the observation-provider lifecycle without owning resources."""


class NonBlockingMetricsObservationProvider:
    """Sample service metrics off the policy-decision path.

    The sampler may perform blocking network I/O. ``latest`` only reads the
    most recent snapshot under a lock, so replay and flush deadlines never
    wait for a metrics request.
    """

    def __init__(
        self,
        sampler: Callable[[], ServiceMetricsSnapshot | None],
        *,
        poll_interval_s: float = 0.25,
        stale_after_s: float = 0.5,
        clock: Callable[[], float] = time.monotonic,
        close_timeout_s: float | None = None,
    ):
        if poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be positive")
        if stale_after_s < 0:
            raise ValueError("stale_after_s must be non-negative")
        if close_timeout_s is not None and close_timeout_s < 0:
            raise ValueError("close_timeout_s must be non-negative")
        self.sampler = sampler
        self.poll_interval_s = poll_interval_s
        self.stale_after_s = stale_after_s
        self.clock = clock
        self.close_timeout_s = close_timeout_s
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._sampled = threading.Event()
        self._snapshot: ServiceMetricsSnapshot | None = None
        self._sampled_at_s: float | None = None
        self._sample_generation = 0
        self._last_delivered_generation = 0
        self._thread = threading.Thread(
            target=self._sample_loop,
            name="service-metrics-sampler",
            daemon=True,
        )
        self._thread.start()

    @property
    def is_running(self) -> bool:
        return self._thread.is_alive()

    def wait_until_sampled(self, timeout_s: float | None = None) -> bool:
        return self._sampled.wait(timeout=timeout_s)

    def latest(
        self, inflight: int, *, hol_age_s: float | None = None
    ) -> AdmissionObservation:
        now = self.clock()
        with self._lock:
            snapshot = self._snapshot
            sampled_at_s = self._sampled_at_s
            sample_generation = self._sample_generation
            fresh = (
                snapshot is not None
                and sampled_at_s is not None
                and now - sampled_at_s <= self.stale_after_s
                and sample_generation != self._last_delivered_generation
            )
            if fresh:
                self._last_delivered_generation = sample_generation
        return AdmissionObservation(
            observed_at_s=now,
            fresh=fresh,
            inflight=inflight,
            running=snapshot.running if snapshot is not None else None,
            waiting=snapshot.waiting if snapshot is not None else None,
            kv_usage=snapshot.kv_usage if snapshot is not None else None,
            sample_age_s=(
                now - sampled_at_s
                if sampled_at_s is not None
                else None
            ),
            hol_age_s=hol_age_s,
        )

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self.close_timeout_s)

    def _sample_loop(self) -> None:
        while not self._stop.is_set():
            try:
                snapshot = self.sampler()
            except Exception:
                snapshot = None
            sampled_at_s = self.clock()
            with self._lock:
                self._snapshot = snapshot
                self._sampled_at_s = sampled_at_s
                self._sample_generation += 1
            self._sampled.set()
            if self._stop.wait(self.poll_interval_s):
                return
