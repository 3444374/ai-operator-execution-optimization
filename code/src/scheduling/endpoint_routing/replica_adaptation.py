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

    def __post_init__(self):
        for name in ("sample_interval_s", "stale_after_s"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.target_drain_s is not None and (
                not math.isfinite(self.target_drain_s) or self.target_drain_s <= 0):
            raise ValueError("target_drain_s must be finite and positive")
        for name in ("rate_alpha", "congestion_kv_usage"):
            if not math.isfinite(getattr(self, name)) or not 0 < getattr(self, name) <= 1:
                raise ValueError(f"{name} must be in (0, 1]")


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

    def __post_init__(self):
        for name in ("rate_observed_at_s", "pressure_observed_at_s"):
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


@dataclass(frozen=True)
class ReplicaCapacity:
    endpoint_id: str
    requests: int
    work: int

    def __post_init__(self):
        if not self.endpoint_id or any(type(v) is not int or v < 1 for v in (self.requests, self.work)):
            raise ValueError("invalid replica capacity")


class ReplicaAdaptivePolicy:
    """Reuse least-work routing; queue pressure changes supply one slot per sample.

    Feedback cannot settle Core reservations. Missing/stale pressure restores
    declared supply; missing/stale rates use a common neutral-work score.
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
        self._least_work = LeastWorkEndpointRouter()
        self.limit = max_requests

    def _fresh(self, timestamp, now):
        return timestamp is not None and 0 <= now - timestamp <= self.config.stale_after_s

    def _refresh(self):
        for endpoint_id, capacity in self.capacities.items():
            sample = self.feedback(endpoint_id)
            now = self.clock()  # A provider may have just published a newer sample.
            if type(sample) is not ReplicaFeedback:
                raise ValueError("replica feedback must be typed")
            self._samples[endpoint_id] = sample
            complete = sample.waiting is not None and sample.kv_usage is not None
            if not complete or not self._fresh(sample.pressure_observed_at_s, now):
                self._limits[endpoint_id] = capacity.requests
            elif self._last_pressure.get(endpoint_id, -1) < sample.pressure_observed_at_s:
                congested = sample.waiting > 0 or sample.kv_usage >= self.config.congestion_kv_usage
                self._limits[endpoint_id] = min(capacity.requests, max(1,
                    self._limits[endpoint_id] + (-1 if congested else 1)))
                self._last_pressure[endpoint_id] = sample.pressure_observed_at_s
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
        rates = {k: s.service_rate_work_units_s for k, s in self._samples.items()
                 if self._fresh(s.rate_observed_at_s, now) and s.service_rate_work_units_s is not None}
        # Do not compare seconds on one endpoint against work units on another.
        comparable_rates = len(rates) == len(self.capacities)
        eligible = []
        for endpoint in topology.endpoints:
            c = self.capacities[endpoint.endpoint_id]
            occupied = endpoint.running + endpoint.waiting
            work = endpoint.estimated_active_work + request.estimated_work_units
            if (endpoint.pool_id != pool_id or not endpoint.healthy or not endpoint.available
                    or occupied >= self._limits[endpoint.endpoint_id] or work > c.work):
                continue
            rate = rates.get(endpoint.endpoint_id)
            if (rate is not None and self.config.target_drain_s is not None and occupied > 0
                    and work > rate * self.config.target_drain_s):
                continue
            eligible.append(replace(endpoint, service_rate_tokens_s=None,
                service_rate_work_units_s=rate if comparable_rates else None))
        if not eligible:
            raise EndpointCapacityUnavailable("replica supply or hard capacity is occupied")
        if not comparable_rates:
            # Probe an unmeasured idle replica before repeatedly assigning an
            # equal-work tie to the same identity (important with global K=1).
            endpoint = min(eligible, key=lambda e: (e.estimated_active_work,
                e.endpoint_id in rates, e.endpoint_id))
            from ..core.models import RoutingDecision
            return RoutingDecision(endpoint.endpoint_id, pool_id, "replica_adaptive_neutral_work")
        decision = self._least_work.route(request, TopologySnapshot(tuple(eligible), now), pool_id)
        return replace(decision, reason="replica_adaptive_least_work" if comparable_rates
                       else "replica_adaptive_neutral_work")

    def supply(self):
        """Owner-thread diagnostic; reading it does not advance the controller."""
        return dict(self._limits)
