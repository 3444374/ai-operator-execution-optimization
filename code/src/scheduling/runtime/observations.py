"""Cached service observations for adaptive scheduling."""

from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from ..core.models import AdmissionObservation

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ServiceMetricsSnapshot:
    running: int | None
    waiting: int | None
    kv_usage: float | None
    completed_tokens_total: float | None = None
    endpoint_count: int = 1
    source_sequence: int | None = None
    source_observed_at_s: float | None = None
    completed_items_total: int | None = None
    pending_items: int | None = None
    preemptions_total: int | None = None
    memory_pressure: bool | None = None
    transition_pending: int = 0
    consumer_backpressure: bool = False
    completed_input_work_total: int | None = None
    completed_output_work_total: int | None = None
    completed_computed_input_work_total: int | None = None
    completed_cached_input_work_total: int | None = None
    source_uid: str | None = None
    source_session_id: int | None = None
    received_at_s: float | None = None
    control_request_index: int | None = None
    source_clock_domain: str | None = None
    received_clock_domain: str | None = None

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
            completed_items_total=self.completed_items_total, pending_items=self.pending_items,
            preemptions_total=self.preemptions_total, memory_pressure=self.memory_pressure,
            transition_pending=self.transition_pending, consumer_backpressure=self.consumer_backpressure,
            completed_input_work_total=self.completed_input_work_total,
            completed_output_work_total=self.completed_output_work_total,
            completed_computed_input_work_total=self.completed_computed_input_work_total,
            completed_cached_input_work_total=self.completed_cached_input_work_total,
        )
        if self.completed_tokens_total is not None and (
            not math.isfinite(self.completed_tokens_total)
            or self.completed_tokens_total < 0
        ):
            raise ValueError(
                "completed_tokens_total must be finite and non-negative"
            )
        if self.source_sequence is not None and (type(self.source_sequence) is not int or self.source_sequence < 0):
            raise ValueError("source_sequence must be a non-negative integer")
        if self.source_observed_at_s is not None and (not math.isfinite(self.source_observed_at_s) or self.source_observed_at_s < 0):
            raise ValueError("source timestamp must be finite and non-negative")
        if self.endpoint_count <= 0:
            raise ValueError("endpoint_count must be positive")


class _ServiceRateTracker:
    def __init__(self, alpha: float) -> None:
        if not math.isfinite(alpha) or not 0 < alpha <= 1:
            raise ValueError("rate_ewma_alpha must be in (0, 1]")
        self.alpha = alpha
        self._previous_total: float | None = None
        self._previous_sample_s: float | None = None
        self.rate_tokens_s_per_endpoint: float | None = None

    def update(
        self,
        snapshot: ServiceMetricsSnapshot | None,
        sampled_at_s: float,
    ) -> float | None:
        if snapshot is None or snapshot.completed_tokens_total is None:
            return self.rate_tokens_s_per_endpoint
        total = snapshot.completed_tokens_total
        if (
            self._previous_total is not None
            and self._previous_sample_s is not None
            and sampled_at_s > self._previous_sample_s
            and total >= self._previous_total
        ):
            instantaneous = (
                (total - self._previous_total)
                / (sampled_at_s - self._previous_sample_s)
                / snapshot.endpoint_count
            )
            self.rate_tokens_s_per_endpoint = (
                instantaneous
                if self.rate_tokens_s_per_endpoint is None
                else self.alpha * instantaneous
                + (1 - self.alpha) * self.rate_tokens_s_per_endpoint
            )
        self._previous_total = total
        self._previous_sample_s = sampled_at_s
        return self.rate_tokens_s_per_endpoint


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
    endpoint_id: str | None = None


class CachedMetricsObservationProvider:
    def __init__(
        self,
        sampler: Callable[[], ServiceMetricsSnapshot | None],
        *,
        min_sample_interval_s: float = 0.25,
        rate_ewma_alpha: float = 0.3,
        clock: Callable[[], float] = time.monotonic,
    ):
        if min_sample_interval_s < 0:
            raise ValueError("min_sample_interval_s must be non-negative")
        self.sampler = sampler
        self.min_sample_interval_s = min_sample_interval_s
        self.clock = clock
        self._last_sample_s: float | None = None
        self._cached: ServiceMetricsSnapshot | None = None
        self._rate_tracker = _ServiceRateTracker(rate_ewma_alpha)

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
            self._rate_tracker.update(self._cached, now)
        snapshot = self._cached
        return AdmissionObservation(
            observed_at_s=now,
            fresh=sample_due,
            inflight=inflight,
            running=snapshot.running if snapshot is not None else None,
            waiting=snapshot.waiting if snapshot is not None else None,
            kv_usage=snapshot.kv_usage if snapshot is not None else None,
            service_rate_tokens_s_per_endpoint=(
                self._rate_tracker.rate_tokens_s_per_endpoint
            ),
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
        rate_ewma_alpha: float = 0.3,
        clock: Callable[[], float] = time.monotonic,
        close_timeout_s: float | None = None,
        observer=None,
        ready_event: threading.Event | None = None,
        collect_ready: Callable[[], ServiceMetricsSnapshot | None] | None = None,
    ):
        if poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be positive")
        if stale_after_s < 0:
            raise ValueError("stale_after_s must be non-negative")
        if close_timeout_s is not None and close_timeout_s < 0:
            raise ValueError("close_timeout_s must be non-negative")
        if (ready_event is None) != (collect_ready is None):
            raise ValueError("ready_event and collect_ready must be supplied together")
        self.sampler = sampler
        self.poll_interval_s = poll_interval_s
        self.stale_after_s = stale_after_s
        self.clock = clock
        self.close_timeout_s = close_timeout_s
        self.observer = observer
        self._ready_event, self._collect_ready = ready_event, collect_ready
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._sampled = threading.Event()
        self._snapshot: ServiceMetricsSnapshot | None = None
        self._sampled_at_s: float | None = None
        self._sample_generation = 0
        self._source_sequence = None
        self._last_delivered_generation = 0
        self._service_rate_tokens_s_per_endpoint: float | None = None
        self._rate_tracker = _ServiceRateTracker(rate_ewma_alpha)
        self._epoch = 0
        self._enabled = True
        self._rate_alpha = rate_ewma_alpha
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
                and 0 <= now - sampled_at_s <= self.stale_after_s
                and sample_generation != self._last_delivered_generation
            )
            if fresh:
                self._last_delivered_generation = sample_generation
            reason = ("fresh_for_consumer" if fresh else "missing_snapshot" if snapshot is None
                      else "stale_snapshot" if sampled_at_s is None or not 0 <= now-sampled_at_s <= self.stale_after_s
                      else "already_delivered_generation")
        self._observe(snapshot, stage="consumer", accepted=fresh, reason=reason, at_s=now)
        return AdmissionObservation(
            observed_at_s=now,
            fresh=fresh,
            inflight=inflight,
            running=snapshot.running if snapshot is not None else None,
            waiting=snapshot.waiting if snapshot is not None else None,
            kv_usage=snapshot.kv_usage if snapshot is not None else None,
            service_rate_tokens_s_per_endpoint=(
                self._service_rate_tokens_s_per_endpoint
                if sampled_at_s is not None
                and now - sampled_at_s <= self.stale_after_s
                else None
            ),
            sample_age_s=(
                now - sampled_at_s
                if sampled_at_s is not None
                else None
            ),
            hol_age_s=hol_age_s,
            completed_items_total=None if snapshot is None else snapshot.completed_items_total,
            pending_items=None if snapshot is None else snapshot.pending_items,
            preemptions_total=None if snapshot is None else snapshot.preemptions_total,
            memory_pressure=None if snapshot is None else snapshot.memory_pressure,
            transition_pending=0 if snapshot is None else snapshot.transition_pending,
            consumer_backpressure=False if snapshot is None else snapshot.consumer_backpressure,
            completed_input_work_total=None if snapshot is None else snapshot.completed_input_work_total,
            completed_output_work_total=None if snapshot is None else snapshot.completed_output_work_total,
            completed_computed_input_work_total=None if snapshot is None else snapshot.completed_computed_input_work_total,
            completed_cached_input_work_total=None if snapshot is None else snapshot.completed_cached_input_work_total,
        )

    def close(self) -> None:
        self._stop.set()
        if self._ready_event is not None:
            self._ready_event.set()
        self._thread.join(timeout=self.close_timeout_s)

    def _observe(self, snapshot, *, stage, accepted, reason, at_s):
        if self.observer is None:
            return
        try:
            self.observer(dict(stage=stage, accepted=accepted, reason=reason, decision_at_s=at_s,
                decision_clock_domain='provider.clock seconds',
                engine_uid=None if snapshot is None else snapshot.source_uid,
                session_id=None if snapshot is None else snapshot.source_session_id,
                source_sequence=None if snapshot is None else snapshot.source_sequence,
                source_observed_at_s=None if snapshot is None else snapshot.source_observed_at_s,
                received_at_s=None if snapshot is None else snapshot.received_at_s,
                control_request_index=None if snapshot is None else snapshot.control_request_index,
                source_clock_domain=None if snapshot is None else snapshot.source_clock_domain,
                received_clock_domain=None if snapshot is None else snapshot.received_clock_domain))
        except Exception:
            pass  # Optional observation cannot change freshness, results or ownership.

    def observe_native(self, snapshot, *, session_id, received_at_s, accepted, reason,
                       control_request_index=None, source_clock_domain=None, received_clock_domain=None):
        """Optional RPC-validation boundary; not a policy admission decision."""
        if self.observer is None:
            return
        snapshot = snapshot or {}
        try:
            self.observer(dict(stage="RPC_sample", accepted=accepted, reason=reason,
                control_request_index=control_request_index,
                source_clock_domain=source_clock_domain, received_clock_domain=received_clock_domain,
                decision_clock_domain=received_clock_domain,
                engine_uid=snapshot.get("engine_uid"), session_id=session_id,
                source_sequence=snapshot.get("sequence"), source_observed_at_s=snapshot.get("observed_at_s"),
                received_at_s=received_at_s, decision_at_s=received_at_s))
        except Exception:
            pass

    def _sample_once(self, sampler, *, ready=False):
        with self._lock:
            epoch, enabled = self._epoch, self._enabled
        snapshot = None
        if enabled:
            try:
                snapshot = sampler()
            except Exception:
                _logger.warning("metrics sampler failed in background thread", exc_info=True)
        # An old notification or a pending/cancelled RPC has no new sample.
        # Regular polls retain their original missing-snapshot behavior.
        if ready and snapshot is None:
            return
        now = self.clock()
        observed_snapshot = snapshot
        reason = "sampler_unavailable" if snapshot is None else "sample_updated"
        accepted = False
        with self._lock:
            if epoch == self._epoch and not self._stop.is_set():
                if (snapshot is not None and snapshot.source_observed_at_s is not None
                        and snapshot.source_observed_at_s > now):
                    snapshot = None
                    reason = "source_clock_in_future"
                sequence = None if snapshot is None else snapshot.source_sequence
                duplicate = (sequence is not None and self._source_sequence is not None
                             and sequence <= self._source_sequence)
                if not duplicate:
                    sampled_at = (snapshot.source_observed_at_s if snapshot is not None
                                  and snapshot.source_observed_at_s is not None else now)
                    self._snapshot, self._sampled_at_s = snapshot, sampled_at
                    self._service_rate_tokens_s_per_endpoint = self._rate_tracker.update(snapshot, sampled_at)
                    self._sample_generation += 1
                    if sequence is not None:
                        self._source_sequence = sequence
                    accepted = snapshot is not None
                else:
                    reason = "duplicate_or_older_source_sequence"
            else:
                reason = "sampling_generation_invalidated"
        self._observe(observed_snapshot, stage="sample", accepted=accepted, reason=reason, at_s=now)
        self._sampled.set()

    def _sample_loop(self) -> None:
        while not self._stop.is_set():
            self._sample_once(self.sampler)
            if self._ready_event is None:
                if self._stop.wait(self.poll_interval_s):
                    return
                continue
            # Completion can interrupt this wait, but cannot advance its regular
            # dispatch deadline. Scheduling uses wall monotonic, as Event.wait did.
            deadline = time.monotonic()+self.poll_interval_s
            while not self._stop.is_set():
                remaining = deadline-time.monotonic()
                if remaining <= 0 or not self._ready_event.wait(remaining):
                    break
                self._ready_event.clear()
                if self._stop.is_set():
                    return
                self._sample_once(self._collect_ready, ready=True)


    def _set_enabled(self, enabled):
        with self._lock:
            self._epoch += 1
            self._enabled = enabled
            self._snapshot = self._sampled_at_s = self._source_sequence = None
            self._service_rate_tokens_s_per_endpoint = None
            self._rate_tracker = _ServiceRateTracker(self._rate_alpha)
            self._sample_generation += 1


    def pause(self):
        self._set_enabled(False)


    def resume(self):
        self._set_enabled(True)
