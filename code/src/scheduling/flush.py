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
