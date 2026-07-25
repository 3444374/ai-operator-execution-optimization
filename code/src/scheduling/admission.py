"""Admission-control policies."""

from __future__ import annotations

from .models import AdmissionDecision


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
