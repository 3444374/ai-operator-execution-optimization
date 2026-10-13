"""Bounded completion-rate estimates; service metrics providers remain caller-owned."""

from dataclasses import dataclass, field, replace
import threading
import time

from ...scheduling.endpoint_routing.replica_adaptation import ReplicaFeedback
from .full_response import decode_full_response
from ..completion import CompletionAdapterError


@dataclass
class _Support:
    started_at: float | None = None
    busy_s: float = 0.0
    active_s: float = 0.0
    peak: int = 0
    work: int = 0
    completed: int = 0

    def plus(self, other):
        return _Support(self.started_at if self.started_at is not None else other.started_at, self.busy_s + other.busy_s,
            self.active_s + other.active_s, max(self.peak, other.peak),
            self.work + other.work, self.completed + other.completed)


@dataclass
class _Rate:
    active: int = 0
    last_at: float = 0.0
    pending: _Support = field(default_factory=_Support)
    segment: _Support = field(default_factory=_Support)
    last_window: _Support | None = None
    filter_work: float = 0.0
    filter_busy_s: float = 0.0
    before_window_work: float = 0.0
    before_window_busy_s: float = 0.0
    sample: ReplicaFeedback = field(default_factory=ReplicaFeedback)
    published: int = 0
    tail_corrections: int = 0
    total_successful_work: int = 0
    total_successful_requests: int = 0
    total_busy_s: float = 0.0
    last_segment: _Support | None = None
    low_support_holds: int = 0
    carried: _Support = field(default_factory=_Support)
    carried_at: float | None = None


class ReplicaCompletionFeedback:
    """Estimated completed work / busy transport time, including client waits.

    This is an owner-local completion signal, not GPU utilization, actual token
    throughput or service capacity. Failed/unknown calls do not add completed
    work. Only Core can retire their outstanding request/work reservations.
    Windows carry work and time together. A short final window corrects the
    previous window from the same continuous busy segment; it is never assigned
    a separate tiny denominator. Completed segments are diagnostic totals, not
    a second contribution to the online filter. Support describes the latest
    window's concurrency and request mix, not an invariant capacity. Storage is
    bounded per endpoint; no per-request history.
    """

    def __init__(self, endpoint_ids, config, *, observations=None, clock=time.monotonic, prior=None):
        self._rates = {k: _Rate() for k in endpoint_ids}
        self.config, self.clock = config, clock
        self.observations = observations or {}
        if set(self.observations) - set(self._rates):
            raise ValueError("metrics provider refers to an unknown replica")
        self._lock = threading.Lock()
        for endpoint_id, sample in (prior or {}).items():
            if endpoint_id not in self._rates or type(sample) is not ReplicaFeedback:
                raise ValueError("invalid replica completion prior")
            if not self.config.supports_rate(sample):
                raise ValueError("completion prior requires qualified measured support")
            state = self._rates[endpoint_id]
            state.sample = sample
            state.filter_work = sample.service_rate_work_units_s * sample.rate_support_s
            state.filter_busy_s = sample.rate_support_s

    def _advance(self, state, now):
        if now < state.last_at:
            raise ValueError("completion clock moved backwards")
        if state.active:
            elapsed = now - state.last_at
            for support in (state.pending, state.segment):
                support.busy_s += elapsed
                support.active_s += elapsed * state.active
            state.total_busy_s += elapsed
        state.last_at = now

    def started(self, endpoint_id):
        with self._lock:
            state = self._rates[endpoint_id]
            self._advance(state, self.clock())
            if not state.active:
                if state.carried_at is not None and state.last_at - state.carried_at > self.config.stale_after_s:
                    state.carried = _Support()
                state.pending = _Support(started_at=state.last_at)
                state.segment = _Support(started_at=state.last_at)
                state.last_window = None
            state.active += 1
            state.pending.peak = max(state.pending.peak, state.active)
            state.segment.peak = max(state.segment.peak, state.active)

    def _snapshot(self, support, now, rate):
        return ReplicaFeedback(rate_observed_at_s=now, service_rate_work_units_s=rate,
            rate_support_s=support.busy_s, rate_completed_requests=support.completed,
            rate_completed_work=support.work,
            rate_mean_active_requests=support.active_s / support.busy_s,
            rate_peak_active_requests=support.peak,
            rate_sample_started_at_s=support.started_at, rate_provenance="transport")

    def _publish(self, state, support, now, *, correction=False):
        qualified = (support.busy_s >= self.config.min_rate_support_s
                     and support.completed >= self.config.min_rate_completed_requests)
        previous_qualified = self.config.supports_rate(state.sample)
        if not qualified:
            if previous_qualified:
                # Hold the measured prior without refreshing its age.
                state.low_support_holds += 1
            else:
                state.sample = self._snapshot(support, now, support.work / support.busy_s)
            # Short serial segments accumulate separately. Publishing a weak
            # cold observation must not count it again in the online filter.
            return False
        if not correction:
            state.before_window_work = state.filter_work
            state.before_window_busy_s = state.filter_busy_s
        alpha = self.config.rate_alpha
        state.filter_work = (1 - alpha) * state.before_window_work + alpha * support.work
        state.filter_busy_s = (1 - alpha) * state.before_window_busy_s + alpha * support.busy_s
        state.sample = self._snapshot(support, now, state.filter_work / state.filter_busy_s)
        state.last_window = replace(support)
        state.published += 1
        state.tail_corrections += int(correction)
        state.pending = _Support(started_at=now, peak=state.active)
        return True

    def finished(self, endpoint_id, work, *, successful):
        with self._lock:
            state = self._rates[endpoint_id]
            if state.active < 1:
                raise ValueError("completion has no matching active call")
            if type(work) is not int or work < 1:
                raise ValueError("completion work must be a positive integer")
            now = self.clock()
            self._advance(state, now)
            state.active -= 1
            if successful:
                for support in (state.pending, state.segment):
                    support.work += work
                    support.completed += 1
                state.total_successful_work += work
                state.total_successful_requests += 1
            pending = state.pending
            if pending.work > 0 or not state.active:
                if (pending.busy_s >= self.config.sample_interval_s
                        and state.carried.busy_s + pending.busy_s >= self.config.min_rate_support_s
                        and state.carried.completed + pending.completed >= self.config.min_rate_completed_requests):
                    if self._publish(state, state.carried.plus(pending), now):
                        state.carried = _Support()
                elif not state.active:
                    correction = state.last_window is not None
                    if correction:
                        support = state.last_window.plus(pending)
                        self._publish(state, support, now, correction=correction)
                    else:
                        state.carried = state.carried.plus(pending)
                        state.carried_at = now
                        support = state.carried
                        if support.busy_s > 0 and support.work > 0:
                            self._publish(state, support, now)
                            if (support.busy_s >= self.config.min_rate_support_s
                                    and support.completed >= self.config.min_rate_completed_requests):
                                state.carried = _Support()
            if not state.active:
                state.last_segment = replace(state.segment)

    def latest(self, endpoint_id):
        with self._lock:
            state = self._rates[endpoint_id]
            sample, active = state.sample, state.active
        pressure_at = waiting = kv = None
        provider = self.observations.get(endpoint_id)
        if provider is not None:
            observation = provider.latest(active)
            if observation.sample_age_s is not None and observation.has_service_metrics:
                pressure_at = observation.observed_at_s - observation.sample_age_s
                waiting, kv = observation.waiting, observation.kv_usage
        return replace(sample, pressure_observed_at_s=pressure_at, waiting=waiting, kv_usage=kv)

    def diagnostics(self, endpoint_id):
        with self._lock:
            state = self._rates[endpoint_id]
            segment = state.last_segment
            return dict(active=state.active, rate_updates=state.published,
                tail_corrections=state.tail_corrections,
                low_support_holds=state.low_support_holds,
                successful_work=state.total_successful_work,
                successful_requests=state.total_successful_requests, busy_s=state.total_busy_s,
                last_segment_work=None if segment is None else segment.work,
                last_segment_completed=None if segment is None else segment.completed,
                last_segment_busy_s=None if segment is None else segment.busy_s,
                last_segment_rate=None if segment is None or not segment.busy_s else segment.work / segment.busy_s)


def successful_response(payload, response_mode):
    if type(payload) is not bytes:
        return False
    if response_mode == "full":
        try:
            return 200 <= decode_full_response(payload).status_code < 300
        except (ValueError, CompletionAdapterError):
            return False
    return not payload.startswith(b'{"bridge_error":')
