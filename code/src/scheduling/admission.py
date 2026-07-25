"""Admission-control policies."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from .models import (
    AdmissionDecision,
    AdmissionObservation,
    WindowDecision,
)
from .observations import AdmissionTraceEvent


class WindowController(Protocol):
    current_window: int

    def update(self, observation: AdmissionObservation) -> WindowDecision:
        ...


class ObservationProvider(Protocol):
    def latest(self, inflight: int) -> AdmissionObservation:
        ...


class StaticAdmissionController:
    def __init__(self, limit: int):
        if limit <= 0:
            raise ValueError("limit must be positive")
        self.limit = limit

    def decide(self, inflight: int) -> AdmissionDecision:
        if inflight < 0:
            raise ValueError("inflight must be non-negative")
        allowed = inflight < self.limit
        return AdmissionDecision(
            allowed=allowed,
            limit=self.limit,
            action="admit" if allowed else "wait",
            reason="below_static_limit" if allowed else "at_static_limit",
        )


class DynamicAdmissionGate:
    def __init__(
        self,
        controller: WindowController,
        observation_provider: ObservationProvider,
        *,
        trace_sink: Callable[[AdmissionTraceEvent], None] | None = None,
    ):
        if controller.current_window <= 0:
            raise ValueError("controller current_window must be positive")
        self.controller = controller
        self.observation_provider = observation_provider
        self.trace_sink = trace_sink
        self.limit = controller.current_window

    def decide(self, inflight: int) -> AdmissionDecision:
        if inflight < 0:
            raise ValueError("inflight must be non-negative")
        observation = self.observation_provider.latest(inflight)
        window_decision = self.controller.update(observation)
        self.limit = window_decision.window
        allowed = inflight < self.limit
        if self.trace_sink is not None:
            self.trace_sink(
                AdmissionTraceEvent(
                    observed_at_s=observation.observed_at_s,
                    fresh=observation.fresh,
                    inflight=inflight,
                    window=self.limit,
                    running=observation.running,
                    waiting=observation.waiting,
                    kv_usage=observation.kv_usage,
                    controller_action=window_decision.action,
                    reason=window_decision.reason,
                    allowed=allowed,
                    sample_age_s=observation.sample_age_s,
                )
            )
        return AdmissionDecision(
            allowed=allowed,
            limit=self.limit,
            action="admit" if allowed else "wait",
            reason=window_decision.reason,
        )
