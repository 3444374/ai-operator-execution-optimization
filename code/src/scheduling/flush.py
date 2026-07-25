"""Independent flush timing policies for pending upstream batches."""

from __future__ import annotations

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


class ImmediateFlush:
    def decide(self, observation: FlushObservation) -> FlushDecision:
        return FlushDecision(True, "flush", "immediate", observation.age_s)


class FixedTimeoutFlush:
    def __init__(self, timeout_s: float = 0.025):
        if timeout_s < 0:
            raise ValueError("timeout_s must be non-negative")
        self.timeout_s = timeout_s

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
        max_wait_s: float = 0.050,
        low_load_running: int = 64,
        congestion_kv_usage: float = 0.85,
    ):
        if max_wait_s <= 0:
            raise ValueError("max_wait_s must be positive")
        if low_load_running <= 0:
            raise ValueError("low_load_running must be positive")
        if not 0.0 <= congestion_kv_usage <= 1.0:
            raise ValueError("congestion_kv_usage must be between 0 and 1")
        self.max_wait_s = max_wait_s
        self.low_load_running = low_load_running
        self.congestion_kv_usage = congestion_kv_usage

    def decide(self, observation: FlushObservation) -> FlushDecision:
        if observation.budget_reached:
            return self._flush("budget_reached", observation)
        if observation.age_s >= self.max_wait_s:
            return self._flush("hard_max_wait", observation)
        if not observation.metrics_fresh:
            return self._wait("stale_metrics_wait", observation)
        if not observation.has_service_metrics:
            return self._wait("missing_metrics_wait", observation)

        congested = (
            observation.waiting > 0
            or observation.kv_usage >= self.congestion_kv_usage
        )
        if congested:
            return self._wait("service_congested", observation)
        underloaded = (
            observation.waiting == 0
            and observation.running < self.low_load_running
        )
        if underloaded:
            return self._flush("service_underloaded", observation)
        return self._wait("service_deadband_wait", observation)

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
