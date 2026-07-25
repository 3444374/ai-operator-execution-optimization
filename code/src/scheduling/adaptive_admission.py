"""Engine-independent AIMD admission controllers.

Ref: Clipper (NSDI 2017) provides the AIMD pattern. The asymmetric +2/x0.5
defaults follow the CONCUR-style candidate recorded in the project experiment
plan; they are hypotheses to test, not universal constants.
"""

from __future__ import annotations

from dataclasses import dataclass

from .models import (
    AdmissionObservation,
    ControlDiagnostics,
    WindowDecision,
)


@dataclass(frozen=True)
class AimdConfig:
    min_window: int = 4
    max_window: int = 16
    additive_increase: int = 2
    multiplicative_decrease: float = 0.5
    congestion_kv_usage: float = 0.85
    low_load_kv_usage: float = 0.50
    low_load_running: int = 64

    def __post_init__(self) -> None:
        if self.min_window <= 0 or self.max_window < self.min_window:
            raise ValueError("window bounds must satisfy 0 < min_window <= max_window")
        if self.additive_increase <= 0:
            raise ValueError("additive_increase must be positive")
        if not 0.0 < self.multiplicative_decrease < 1.0:
            raise ValueError("multiplicative_decrease must be between 0 and 1")
        if not 0.0 <= self.low_load_kv_usage < self.congestion_kv_usage <= 1.0:
            raise ValueError("KV thresholds must define a valid deadband")
        if self.low_load_running <= 0:
            raise ValueError("low_load_running must be positive")


class AimdAdmissionController:
    def __init__(
        self,
        config: AimdConfig | None = None,
        initial_window: int | None = None,
    ):
        self.config = config or AimdConfig()
        initial = self.config.min_window if initial_window is None else initial_window
        if not self.config.min_window <= initial <= self.config.max_window:
            raise ValueError("initial_window must be within configured bounds")
        self.current_window = initial

    def update(self, observation: AdmissionObservation) -> WindowDecision:
        if not observation.fresh:
            return self._hold("stale_observation")
        if not observation.has_service_metrics:
            return self._hold("missing_metrics")
        return self._apply_signals(
            running=float(observation.running),
            waiting_congested=observation.waiting > 0,
            waiting_clear=observation.waiting == 0,
            kv_usage=float(observation.kv_usage),
        )

    def _apply_signals(
        self,
        *,
        running: float,
        waiting_congested: bool,
        waiting_clear: bool,
        kv_usage: float,
        diagnostics: ControlDiagnostics | None = None,
    ) -> WindowDecision:
        if waiting_congested or kv_usage >= self.config.congestion_kv_usage:
            reason = "queue_congestion" if waiting_congested else "kv_congestion"
            updated = max(
                self.config.min_window,
                int(self.current_window * self.config.multiplicative_decrease),
            )
            if updated == self.current_window:
                return self._hold("at_minimum", diagnostics)
            self.current_window = updated
            return WindowDecision(updated, "decrease", reason, diagnostics or ControlDiagnostics())

        low_load = (
            waiting_clear
            and kv_usage <= self.config.low_load_kv_usage
            and running < self.config.low_load_running
        )
        if low_load:
            updated = min(
                self.config.max_window,
                self.current_window + self.config.additive_increase,
            )
            if updated == self.current_window:
                return self._hold("at_maximum", diagnostics)
            self.current_window = updated
            return WindowDecision(updated, "increase", "low_load", diagnostics or ControlDiagnostics())
        return self._hold("deadband", diagnostics)

    def _hold(
        self,
        reason: str,
        diagnostics: ControlDiagnostics | None = None,
    ) -> WindowDecision:
        return WindowDecision(
            self.current_window,
            "hold",
            reason,
            diagnostics or ControlDiagnostics(),
        )


class EwmaAimdAdmissionController(AimdAdmissionController):
    def __init__(
        self,
        config: AimdConfig | None = None,
        initial_window: int | None = None,
        *,
        alpha: float = 0.3,
        smoothed_waiting_threshold: float = 0.5,
    ):
        super().__init__(config, initial_window)
        if not 0.0 < alpha <= 1.0:
            raise ValueError("alpha must be between 0 and 1")
        if smoothed_waiting_threshold <= 0:
            raise ValueError("smoothed_waiting_threshold must be positive")
        self.alpha = alpha
        self.smoothed_waiting_threshold = smoothed_waiting_threshold
        self._running: float | None = None
        self._waiting: float | None = None
        self._kv_usage: float | None = None

    def update(self, observation: AdmissionObservation) -> WindowDecision:
        if not observation.fresh:
            return self._hold("stale_observation", self._diagnostics())
        if not observation.has_service_metrics:
            return self._hold("missing_metrics", self._diagnostics())

        self._running = self._smooth(self._running, float(observation.running))
        self._waiting = self._smooth(self._waiting, float(observation.waiting))
        self._kv_usage = self._smooth(self._kv_usage, float(observation.kv_usage))
        diagnostics = self._diagnostics()
        return self._apply_signals(
            running=self._running,
            waiting_congested=self._waiting >= self.smoothed_waiting_threshold,
            waiting_clear=self._waiting < self.smoothed_waiting_threshold,
            kv_usage=self._kv_usage,
            diagnostics=diagnostics,
        )

    def _smooth(self, previous: float | None, sample: float) -> float:
        if previous is None:
            return sample
        return self.alpha * sample + (1.0 - self.alpha) * previous

    def _diagnostics(self) -> ControlDiagnostics:
        return ControlDiagnostics(
            smoothed_running=self._running,
            smoothed_waiting=self._waiting,
            smoothed_kv_usage=self._kv_usage,
        )
