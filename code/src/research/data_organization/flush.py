"""Independent flush timing policies for pending upstream batches."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class FlushObservation:
    now_s: float
    oldest_arrival_s: float
    pending_rows: int
    pending_cost: int
    budget_reached: bool
    metrics_fresh: bool
    running: int | None
    waiting: int | None
    kv_usage: float | None
    token_budget: int = 0
    arrival_rate_tokens_s: float | None = None
    service_rate_tokens_s_per_endpoint: float | None = None

    def __post_init__(self) -> None:
        if self.pending_rows <= 0:
            raise ValueError("pending_rows must be positive")
        if self.pending_cost < 0:
            raise ValueError("pending_cost must be non-negative")
        if self.now_s < self.oldest_arrival_s:
            raise ValueError("oldest_arrival_s must not be after now_s")
        if (
            self.running is not None
            and self.running < 0
            or self.waiting is not None
            and self.waiting < 0
        ):
            raise ValueError("running and waiting must be non-negative when present")
        if self.kv_usage is not None and not 0.0 <= self.kv_usage <= 1.0:
            raise ValueError("kv_usage must be between 0 and 1 when present")
        if (
            not isinstance(self.token_budget, int)
            or isinstance(self.token_budget, bool)
            or self.token_budget < 0
        ):
            raise ValueError("token_budget must be a non-negative integer")
        for name, value in (
            ("arrival_rate_tokens_s", self.arrival_rate_tokens_s),
            (
                "service_rate_tokens_s_per_endpoint",
                self.service_rate_tokens_s_per_endpoint,
            ),
        ):
            if value is not None and (
                isinstance(value, bool)
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError(f"{name} must be finite and non-negative")

    @property
    def age_s(self) -> float:
        return self.now_s - self.oldest_arrival_s

    @property
    def has_service_metrics(self) -> bool:
        return (
            self.running is not None
            and self.waiting is not None
            and self.kv_usage is not None
        )


@dataclass(frozen=True)
class FlushDecision:
    flush: bool
    action: str
    reason: str
    pending_age_s: float


@dataclass(frozen=True)
class FlushWindow:
    wait_s: float
    reason: str

    def __post_init__(self) -> None:
        if self.wait_s < 0:
            raise ValueError("wait_s must be non-negative")
        if not self.reason:
            raise ValueError("reason must be non-empty")


class ImmediateFlush:
    def select_window(self, observation: FlushObservation) -> FlushWindow:
        return FlushWindow(0.0, "immediate")

    def decide(self, observation: FlushObservation) -> FlushDecision:
        return FlushDecision(True, "flush", "immediate", observation.age_s)


class FixedTimeoutFlush:
    def __init__(self, timeout_s: float = 0.025):
        if timeout_s < 0:
            raise ValueError("timeout_s must be non-negative")
        self.timeout_s = timeout_s

    def select_window(self, observation: FlushObservation) -> FlushWindow:
        return FlushWindow(self.timeout_s, "fixed_timeout")

    def decide(self, observation: FlushObservation) -> FlushDecision:
        if observation.budget_reached:
            return FlushDecision(
                True, "flush", "budget_reached", observation.age_s
            )
        if observation.age_s >= self.timeout_s:
            return FlushDecision(
                True, "flush", "fixed_timeout", observation.age_s
            )
        return FlushDecision(
            False, "wait", "fixed_timeout_wait", observation.age_s
        )


class QueueAdaptiveFlush:
    def __init__(
        self,
        *,
        min_wait_s: float = 0.025,
        max_wait_s: float = 0.050,
        pressure_running: int = 8,
        congestion_kv_usage: float = 0.85,
    ):
        if min_wait_s <= 0:
            raise ValueError("min_wait_s must be positive")
        if max_wait_s < min_wait_s:
            raise ValueError("max_wait_s must be at least min_wait_s")
        if pressure_running <= 0:
            raise ValueError("pressure_running must be positive")
        if not 0.0 <= congestion_kv_usage <= 1.0:
            raise ValueError("congestion_kv_usage must be between 0 and 1")
        self.min_wait_s = min_wait_s
        self.max_wait_s = max_wait_s
        self.pressure_running = pressure_running
        self.congestion_kv_usage = congestion_kv_usage

    def select_window(self, observation: FlushObservation) -> FlushWindow:
        if not observation.metrics_fresh:
            return FlushWindow(self.min_wait_s, "fixed_fallback")
        if not observation.has_service_metrics:
            return FlushWindow(self.min_wait_s, "fixed_fallback")
        if observation.waiting > 0:
            return FlushWindow(self.max_wait_s, "queue_pressure")
        if observation.kv_usage >= self.congestion_kv_usage:
            return FlushWindow(self.max_wait_s, "kv_pressure")
        if observation.running >= self.pressure_running:
            return FlushWindow(self.max_wait_s, "running_pressure")
        return FlushWindow(self.min_wait_s, "underloaded_base_window")

    def decide(self, observation: FlushObservation) -> FlushDecision:
        if observation.budget_reached:
            return self._flush("budget_reached", observation)
        window = self.select_window(observation)
        if observation.age_s >= window.wait_s:
            return self._flush(window.reason, observation)
        return self._wait(
            f"{window.reason}_wait",
            observation,
        )

    @staticmethod
    def _flush(
        reason: str,
        observation: FlushObservation,
    ) -> FlushDecision:
        return FlushDecision(True, "flush", reason, observation.age_s)

    @staticmethod
    def _wait(
        reason: str,
        observation: FlushObservation,
    ) -> FlushDecision:
        return FlushDecision(False, "wait", reason, observation.age_s)


def _require_finite_range(
    name: str,
    value: float,
    *,
    lower: float,
    upper: float | None = None,
    lower_inclusive: bool = False,
) -> None:
    lower_ok = value >= lower if lower_inclusive else value > lower
    if isinstance(value, bool) or not math.isfinite(value) or not lower_ok or (
        upper is not None and value > upper
    ):
        bracket = "[" if lower_inclusive else "("
        ceiling = f", {upper}]" if upper is not None else ", infinity)"
        raise ValueError(f"{name} must be in {bracket}{lower}{ceiling}")


def _ewma(previous: float | None, sample: float, alpha: float) -> float:
    return sample if previous is None else alpha * sample + (1 - alpha) * previous


def _load_window(
    min_wait_s: float,
    max_wait_s: float,
    load_ratio: float,
    transition_ratio: float,
) -> float:
    if transition_ratio == 0:
        fraction = float(load_ratio >= 1)
    else:
        fraction = min(
            1.0,
            max(
                0.0,
                (load_ratio - (1 - transition_ratio))
                / (2 * transition_ratio),
            ),
        )
    return min_wait_s + (max_wait_s - min_wait_s) * fraction


def _validate_slo_flush_config(
    min_wait_s: float,
    max_wait_s: float,
    request_slo_s: float,
    ewma_alpha: float,
    deadband_ratio: float,
    endpoint_count: int,
    service_capacity_tokens_s_per_endpoint: float | None,
) -> None:
    _require_finite_range("min_wait_s", min_wait_s, lower=0)
    _require_finite_range("max_wait_s", max_wait_s, lower=0)
    _require_finite_range("request_slo_s", request_slo_s, lower=0)
    _require_finite_range("ewma_alpha", ewma_alpha, lower=0, upper=1)
    _require_finite_range(
        "deadband_ratio", deadband_ratio, lower=0, upper=1, lower_inclusive=True
    )
    if max_wait_s < min_wait_s:
        raise ValueError("max_wait_s must be finite and at least min_wait_s")
    if (
        not isinstance(endpoint_count, int)
        or isinstance(endpoint_count, bool)
        or endpoint_count <= 0
    ):
        raise ValueError("endpoint_count must be a positive integer")
    if service_capacity_tokens_s_per_endpoint is not None:
        _require_finite_range(
            "service_capacity_tokens_s_per_endpoint",
            service_capacity_tokens_s_per_endpoint,
            lower=0,
        )


class SloAwareEwmaFlush:
    """Control upstream batching delay from EWMA fill time and SLO slack.

    The design follows Clipper's delayed batching and Clockwork's deadline
    slack control; it does not change vLLM's internal scheduler.
    """

    def __init__(
        self,
        *,
        min_wait_s: float = 0.025,
        max_wait_s: float = 0.050,
        request_slo_s: float,
        ewma_alpha: float = 0.3,
        deadband_ratio: float = 0.1,
        endpoint_count: int = 1,
        service_capacity_tokens_s_per_endpoint: float | None = None,
    ) -> None:
        _validate_slo_flush_config(
            min_wait_s,
            max_wait_s,
            request_slo_s,
            ewma_alpha,
            deadband_ratio,
            endpoint_count,
            service_capacity_tokens_s_per_endpoint,
        )
        self.min_wait_s = min_wait_s
        self.max_wait_s = max_wait_s
        self.request_slo_s = request_slo_s
        self.ewma_alpha = ewma_alpha
        self.deadband_ratio = deadband_ratio
        self.endpoint_count = endpoint_count
        self._capacity_floor = service_capacity_tokens_s_per_endpoint
        self._arrival_rate: float | None = None
        self._service_rate: float | None = None
        self._last_wait_s: float | None = None

    def select_window(self, observation: FlushObservation) -> FlushWindow:
        if observation.budget_reached:
            return FlushWindow(0.0, "budget_reached")
        if not (
            observation.metrics_fresh
            and observation.has_service_metrics
            and (observation.arrival_rate_tokens_s or 0) > 0
            and (observation.service_rate_tokens_s_per_endpoint or 0) > 0
        ):
            return FlushWindow(self.max_wait_s, "fixed_fallback")
        self._arrival_rate = _ewma(
            self._arrival_rate,
            observation.arrival_rate_tokens_s,
            self.ewma_alpha,
        )
        self._service_rate = _ewma(
            self._service_rate,
            observation.service_rate_tokens_s_per_endpoint,
            self.ewma_alpha,
        )
        effective_service_rate = max(self._service_rate, self._capacity_floor or 0)
        aggregate_service_rate = effective_service_rate * self.endpoint_count
        service_s = observation.pending_cost / aggregate_service_rate
        oldest_age_limit_s = self.request_slo_s - service_s
        if observation.age_s >= oldest_age_limit_s:
            self._last_wait_s = 0.0
            return FlushWindow(0.0, "slo_deadline")
        hard_wait_s = min(self.max_wait_s, oldest_age_limit_s)
        if observation.running == 0 and observation.waiting == 0:
            target_s = min(self.min_wait_s, hard_wait_s)
            reason = "service_idle"
        else:
            load_ratio = self._arrival_rate / aggregate_service_rate
            target_s = min(
                hard_wait_s,
                _load_window(
                    self.min_wait_s,
                    self.max_wait_s,
                    load_ratio,
                    self.deadband_ratio,
                ),
            )
            reason = "busy_load_ewma"
        deadband_s = (self.max_wait_s - self.min_wait_s) * self.deadband_ratio
        if (
            self._last_wait_s is not None
            and self._last_wait_s <= hard_wait_s
            and abs(target_s - self._last_wait_s) <= deadband_s
        ):
            target_s = self._last_wait_s
            reason = f"{reason}_hysteresis"
        self._last_wait_s = target_s
        return FlushWindow(target_s, reason)

    def decide(self, observation: FlushObservation) -> FlushDecision:
        window = self.select_window(observation)
        flush = observation.age_s >= window.wait_s
        return FlushDecision(
            flush,
            "flush" if flush else "wait",
            window.reason if flush else f"{window.reason}_wait",
            observation.age_s,
        )
