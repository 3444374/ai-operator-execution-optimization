"""Optional single-owner replica supply and routing, within declared hard limits."""

from dataclasses import dataclass, replace
import math
import time

from ..core.errors import EndpointCapacityUnavailable
from ..core.models import AdmissionDecision, TopologySnapshot
from .policies import LeastWorkEndpointRouter


@dataclass(frozen=True)
class ReplicaAdaptationConfig:
    target_drain_s: float | None = None
    sample_interval_s: float = 0.25
    stale_after_s: float = 2.0
    rate_alpha: float = 0.3
    congestion_kv_usage: float = 0.85
    min_rate_support_s: float = 0.25
    min_rate_completed_requests: int = 2
    adapt_supply: bool = False

    def __post_init__(self):
        for name in ("sample_interval_s", "stale_after_s", "min_rate_support_s"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.target_drain_s is not None and (
                not math.isfinite(self.target_drain_s) or self.target_drain_s <= 0):
            raise ValueError("target_drain_s must be finite and positive")
        for name in ("rate_alpha", "congestion_kv_usage"):
            if not math.isfinite(getattr(self, name)) or not 0 < getattr(self, name) <= 1:
                raise ValueError(f"{name} must be in (0, 1]")
        if type(self.min_rate_completed_requests) is not int or self.min_rate_completed_requests < 1:
            raise ValueError("min_rate_completed_requests must be a positive integer")
        if type(self.adapt_supply) is not bool:
            raise ValueError("adapt_supply must be a boolean")

    def supports_rate(self, sample):
        return (sample.rate_provenance != "unqualified"
                and sample.service_rate_work_units_s is not None
                and sample.rate_support_s is not None
                and sample.rate_support_s >= self.min_rate_support_s
                and sample.rate_completed_requests is not None
                and sample.rate_completed_requests >= self.min_rate_completed_requests
                and sample.rate_completed_work is not None
                and sample.rate_mean_active_requests is not None
                and sample.rate_peak_active_requests is not None
                and sample.rate_sample_started_at_s is not None)


@dataclass(frozen=True)
class ReplicaFeedback:
    """Rates use the task descriptor's unit; queue/KV are service observations.

    A service running count can include this owner's calls. It is deliberately
    absent: adding it to Core outstanding counts would charge calls twice.
    """

    rate_observed_at_s: float | None = None
    service_rate_work_units_s: float | None = None
    pressure_observed_at_s: float | None = None
    waiting: int | None = None
    kv_usage: float | None = None
    rate_support_s: float | None = None
    rate_completed_requests: int | None = None
    rate_completed_work: int | None = None
    rate_mean_active_requests: float | None = None
    rate_peak_active_requests: int | None = None
    rate_sample_started_at_s: float | None = None
    rate_provenance: str = "unqualified"

    def __post_init__(self):
        for name in ("rate_observed_at_s", "pressure_observed_at_s", "rate_sample_started_at_s"):
            value = getattr(self, name)
            if value is not None and (not math.isfinite(value) or value < 0):
                raise ValueError("invalid feedback timestamp")
        if self.service_rate_work_units_s is not None and (
                not math.isfinite(self.service_rate_work_units_s)
                or self.service_rate_work_units_s <= 0 or self.rate_observed_at_s is None):
            raise ValueError("invalid completion work rate")
        if self.waiting is not None and (type(self.waiting) is not int or self.waiting < 0):
            raise ValueError("invalid service queue count")
        if self.kv_usage is not None and (
                not math.isfinite(self.kv_usage) or not 0 <= self.kv_usage <= 1):
            raise ValueError("invalid service KV usage")
        for name in ("rate_support_s", "rate_mean_active_requests"):
            value = getattr(self, name)
            if value is not None and (not math.isfinite(value) or value <= 0):
                raise ValueError(f"invalid {name}")
        for name in ("rate_completed_requests", "rate_completed_work", "rate_peak_active_requests"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError(f"invalid {name}")
        if self.rate_provenance not in ("unqualified", "transport", "calibration"):
            raise ValueError("invalid rate provenance")
        if (self.rate_sample_started_at_s is not None and self.rate_observed_at_s is not None
                and self.rate_sample_started_at_s > self.rate_observed_at_s):
            raise ValueError("rate support starts after observation")
        if (self.rate_mean_active_requests is not None and self.rate_peak_active_requests is not None
                and self.rate_mean_active_requests > self.rate_peak_active_requests + 1e-9):
            raise ValueError("mean active requests exceeds peak")


@dataclass(frozen=True)
class ReplicaCapacity:
    endpoint_id: str
    requests: int
    work: int

    def __post_init__(self):
        if not self.endpoint_id or any(type(v) is not int or v < 1 for v in (self.requests, self.work)):
            raise ValueError("invalid replica capacity")


class ReplicaAdaptivePolicy:
    """Reuse least-work routing; optional diagnostic pressure supply stays bounded.

    Feedback cannot settle Core reservations. Missing/stale pressure restores
    declared supply. Rates compare qualified eligible candidates; unknown idle
    candidates receive original tasks, and all-unknown candidates use work.
    The optional drain target is a soft work ceiling, with one legal oversized
    task allowed on an idle endpoint so estimation errors cannot stall it.
    """

    def __init__(self, capacities, max_requests, *, config, feedback, clock=time.monotonic):
        self.capacities = {c.endpoint_id: c for c in capacities}
        if not self.capacities or len(self.capacities) != len(capacities):
            raise ValueError("replica capacities must have unique identities")
        if type(max_requests) is not int or not 1 <= max_requests <= sum(c.requests for c in capacities):
            raise ValueError("invalid global replica request capacity")
        self.maximum = max_requests
        self.config, self.feedback, self.clock = config, feedback, clock
        self._limits = {k: c.requests for k, c in self.capacities.items()}
        self._last_pressure = {}
        self._samples = {}
        self._states = {k: "fixed" for k in self.capacities}
        self._last_applied = {}
        self._counts = {k: dict(down=0, recovery=0, hold=0, fallback=0) for k in self.capacities}
        self._probe_counts = {k: 0 for k in self.capacities}
        self._least_work = LeastWorkEndpointRouter()
        self.limit = max_requests

    def _fresh(self, timestamp, now):
        return timestamp is not None and 0 <= now - timestamp <= self.config.stale_after_s

    def _usable_rate(self, sample, now):
        return self.config.supports_rate(sample) and self._fresh(sample.rate_observed_at_s, now)

    def _state(self, endpoint_id, reason, action=None):
        # Count transitions, not how often Core polls an unchanged observation.
        if action is not None or self._states[endpoint_id] != reason:
            self._counts[endpoint_id][action or "hold"] += 1
        self._states[endpoint_id] = reason

    def _refresh(self):
        for endpoint_id, capacity in self.capacities.items():
            sample = self.feedback(endpoint_id)
            now = self.clock()  # A provider may have just published a newer sample.
            if type(sample) is not ReplicaFeedback:
                raise ValueError("replica feedback must be typed")
            self._samples[endpoint_id] = sample
            if not self.config.adapt_supply:
                self._states[endpoint_id] = "fixed"
                continue
            complete = sample.waiting is not None and sample.kv_usage is not None
            if not complete or not self._fresh(sample.pressure_observed_at_s, now):
                self._limits[endpoint_id] = capacity.requests
                reason = "missing_pressure" if not complete else "stale_pressure"
                self._state(endpoint_id, reason, "fallback" if self._states[endpoint_id] != reason else None)
            elif self._last_pressure.get(endpoint_id, -1) < sample.pressure_observed_at_s:
                self._last_pressure[endpoint_id] = sample.pressure_observed_at_s
                if sample.pressure_observed_at_s - self._last_applied.get(endpoint_id, -math.inf) < self.config.sample_interval_s:
                    self._state(endpoint_id, "cadence_hold")
                    continue
                congested = sample.waiting > 0 or sample.kv_usage >= self.config.congestion_kv_usage
                previous = self._limits[endpoint_id]
                self._limits[endpoint_id] = min(capacity.requests, max(1,
                    self._limits[endpoint_id] + (-1 if congested else 1)))
                self._last_applied[endpoint_id] = sample.pressure_observed_at_s
                action = "down" if self._limits[endpoint_id] < previous else "recovery" if self._limits[endpoint_id] > previous else None
                self._state(endpoint_id, action or "bound_hold", action)
            else:
                self._state(endpoint_id, "unchanged_pressure")
        self.limit = min(self.maximum, sum(self._limits.values()))
        return self.clock()

    def decide(self, inflight, *, hol_age_s=None):
        del hol_age_s
        if type(inflight) is not int or inflight < 0:
            raise ValueError("inflight must be a non-negative integer")
        self._refresh()
        allowed = inflight < self.limit
        return AdmissionDecision(allowed, self.limit, "admit" if allowed else "wait", "replica_supply")

    def route(self, request, topology, pool_id):
        now = self._refresh()
        if {e.endpoint_id for e in topology.endpoints} != set(self.capacities):
            raise ValueError("replica topology differs from declared capacities")
        if request.estimated_work_units > max(c.work for c in self.capacities.values()):
            raise ValueError("request exceeds every replica's work capacity")
        eligible = []
        for endpoint in topology.endpoints:
            c = self.capacities[endpoint.endpoint_id]
            occupied = endpoint.running + endpoint.waiting
            work = endpoint.estimated_active_work + request.estimated_work_units
            if (endpoint.pool_id != pool_id or not endpoint.healthy or not endpoint.available
                    or occupied >= self._limits[endpoint.endpoint_id] or work > c.work):
                continue
            sample = self._samples[endpoint.endpoint_id]
            rate = sample.service_rate_work_units_s if self._usable_rate(sample, now) else None
            if (rate is not None and self.config.target_drain_s is not None and occupied > 0
                    and work > rate * self.config.target_drain_s):
                continue
            eligible.append(replace(endpoint, service_rate_tokens_s=None,
                service_rate_work_units_s=rate))
        if not eligible:
            raise EndpointCapacityUnavailable("replica supply or hard capacity is occupied")
        known = [e for e in eligible if e.service_rate_work_units_s is not None]
        unknown = [e for e in eligible if e.service_rate_work_units_s is None]
        # Probe with this original task only. An occupied unknown endpoint keeps
        # its existing Core responsibility and does not disable known rates.
        minimum_work = min(e.estimated_active_work for e in eligible)
        idle_unknown = [e for e in unknown if e.running + e.waiting == 0
                        and e.estimated_active_work == minimum_work]
        from ..core.models import RoutingDecision
        if known and idle_unknown:
            endpoint = min(idle_unknown, key=lambda e: (self._probe_counts[e.endpoint_id], e.endpoint_id))
            self._probe_counts[endpoint.endpoint_id] += 1
            return RoutingDecision(endpoint.endpoint_id, pool_id, "replica_adaptive_probe_original")
        if known:
            # All entries in this comparison use work / qualified work rate.
            decision = self._least_work.route(request, TopologySnapshot(tuple(known), now), pool_id)
            reason = "replica_adaptive_partial_rates" if unknown else "replica_adaptive_least_work"
            return replace(decision, reason=reason)
        endpoint = min(eligible, key=lambda e: (e.estimated_active_work,
            self._probe_counts[e.endpoint_id], e.endpoint_id))
        self._probe_counts[endpoint.endpoint_id] += 1
        return RoutingDecision(endpoint.endpoint_id, pool_id, "replica_adaptive_neutral_work")

    def supply(self):
        """Owner-thread diagnostic; reading it does not advance the controller."""
        return dict(self._limits)

    def diagnostics(self):
        return {k: dict(state=self._states[k], **self._counts[k]) for k in self.capacities}
