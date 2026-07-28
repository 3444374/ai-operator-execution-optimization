"""Bounded PID admission control for vLLM waiting depth."""

from __future__ import annotations

import math
from dataclasses import dataclass

from ..models import AdmissionObservation, ControlDiagnostics, WindowDecision


@dataclass(frozen=True)
class PidConfig:
    min_window: int = 2
    max_window: int = 16
    target_waiting: float = 1.0
    proportional_gain: float = 0.5
    integral_gain: float = 0.1
    derivative_gain: float = 0.05
    integral_limit: float = 20.0

    def __post_init__(self) -> None:
        if self.min_window <= 0 or self.max_window < self.min_window:
            raise ValueError("window bounds must satisfy 0 < min_window <= max_window")
        values = (
            self.target_waiting,
            self.proportional_gain,
            self.integral_gain,
            self.derivative_gain,
            self.integral_limit,
        )
        if any(not math.isfinite(value) for value in values):
            raise ValueError("PID parameters must be finite")
        if self.target_waiting < 0:
            raise ValueError("target_waiting must be non-negative")
        if min(
            self.proportional_gain,
            self.integral_gain,
            self.derivative_gain,
        ) < 0:
            raise ValueError("PID gains must be non-negative")
        if self.integral_limit <= 0:
            raise ValueError("integral_limit must be positive")


class PidAdmissionController:
    def __init__(
        self,
        config: PidConfig | None = None,
        initial_window: int | None = None,
    ):
        self.config = config or PidConfig()
        initial = self.config.min_window if initial_window is None else initial_window
        if not self.config.min_window <= initial <= self.config.max_window:
            raise ValueError("initial_window must be within configured bounds")
        self.current_window = initial
        self._window_value = float(initial)
        self._integral_error = 0.0
        self._previous_error: float | None = None
        self._previous_observed_at_s: float | None = None

    def update(self, observation: AdmissionObservation) -> WindowDecision:
        if not observation.fresh:
            return self._hold("stale_observation")
        if observation.waiting is None:
            return self._hold("missing_queue_metric")
        if (
            self._previous_observed_at_s is not None
            and observation.observed_at_s <= self._previous_observed_at_s
        ):
            return self._hold("non_monotonic_observation")

        error = self.config.target_waiting - float(observation.waiting)
        elapsed_s = (
            0.0
            if self._previous_observed_at_s is None
            else observation.observed_at_s - self._previous_observed_at_s
        )
        if elapsed_s > 0:
            self._integral_error = self._clamp(
                self._integral_error + error * elapsed_s,
                -self.config.integral_limit,
                self.config.integral_limit,
            )
        derivative_error = (
            0.0
            if self._previous_error is None or elapsed_s == 0
            else (error - self._previous_error) / elapsed_s
        )
        adjustment = (
            self.config.proportional_gain * error
            + self.config.integral_gain * self._integral_error
            + self.config.derivative_gain * derivative_error
        )
        unclamped_value = self._window_value + adjustment
        self._window_value = self._clamp(
            unclamped_value,
            float(self.config.min_window),
            float(self.config.max_window),
        )
        previous_window = self.current_window
        self.current_window = int(round(self._window_value))
        self.current_window = max(
            self.config.min_window,
            min(self.config.max_window, self.current_window),
        )
        self._previous_error = error
        self._previous_observed_at_s = observation.observed_at_s

        diagnostics = ControlDiagnostics(
            error=error,
            integral_error=self._integral_error,
            derivative_error=derivative_error,
        )
        if self.current_window > previous_window:
            return WindowDecision(
                self.current_window, "increase", "pid_adjustment", diagnostics
            )
        if self.current_window < previous_window:
            return WindowDecision(
                self.current_window, "decrease", "pid_adjustment", diagnostics
            )
        if error == 0:
            reason = "target_met"
        elif unclamped_value != self._window_value:
            reason = "output_clamped"
        else:
            reason = "pid_adjustment_below_resolution"
        return WindowDecision(self.current_window, "hold", reason, diagnostics)

    def _hold(self, reason: str) -> WindowDecision:
        return WindowDecision(
            self.current_window,
            "hold",
            reason,
            ControlDiagnostics(
                integral_error=self._integral_error,
                error=self._previous_error,
            ),
        )

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))
