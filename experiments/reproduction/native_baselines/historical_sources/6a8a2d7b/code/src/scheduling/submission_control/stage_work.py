"""Bounded stage-work control candidate with fail-safe static fallback.

This is an engine-independent policy candidate, not a performance claim. It
only moves inside an offline-calibrated action set and never treats GPU
utilization or a single service metric as sufficient evidence.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from ...planning.work import RuntimeStateSnapshot


@dataclass(frozen=True)
class WorkCreditDecision:
    work_limit: int
    action: str
    reason: str


class BoundedStageWorkController:
    """Adjust one stage's work credit by at most one calibrated step."""

    def __init__(
        self,
        candidates: tuple[int, ...],
        *,
        fallback_work: int,
        controlled_stage: str,
        upstream_stage: str,
        low_active_fraction: float = 0.8,
        congestion_queue_fraction: float = 0.1,
        congestion_age_s: float = 1.0,
    ) -> None:
        ordered = tuple(sorted(set(candidates)))
        if not ordered or any(item <= 0 for item in ordered):
            raise ValueError("candidates must contain positive work limits")
        if fallback_work not in ordered:
            raise ValueError("fallback_work must be one of candidates")
        if not controlled_stage or not upstream_stage:
            raise ValueError("stage names must be non-empty")
        if not 0 < low_active_fraction < 1:
            raise ValueError("low_active_fraction must be in (0, 1)")
        if not 0 <= congestion_queue_fraction < 1:
            raise ValueError("congestion_queue_fraction must be in [0, 1)")
        if not math.isfinite(congestion_age_s) or congestion_age_s <= 0:
            raise ValueError("congestion_age_s must be finite and positive")
        self.candidates = ordered
        self.fallback_work = fallback_work
        self.controlled_stage = controlled_stage
        self.upstream_stage = upstream_stage
        self.low_active_fraction = low_active_fraction
        self.congestion_queue_fraction = congestion_queue_fraction
        self.congestion_age_s = congestion_age_s
        self._index = ordered.index(fallback_work)

    @property
    def current_work(self) -> int:
        return self.candidates[self._index]

    def select(
        self,
        snapshot: RuntimeStateSnapshot,
        *,
        now_s: float,
        max_age_s: float,
        calibration_signature: str,
    ) -> WorkCreditDecision:
        if (
            snapshot.calibration_signature != calibration_signature
            or not snapshot.is_fresh(now_s=now_s, max_age_s=max_age_s)
        ):
            return self._fallback("stale_or_signature_mismatch")
        controlled = snapshot.for_stage(self.controlled_stage)
        upstream = snapshot.for_stage(self.upstream_stage)
        if controlled is None or upstream is None or controlled.capacity_work is None:
            return self._fallback("missing_stage_or_capacity")

        congested = (
            controlled.queued_work
            >= controlled.capacity_work * self.congestion_queue_fraction
            and controlled.oldest_queue_age_s >= self.congestion_age_s
        )
        if congested:
            return self._move(-1, "service_queue_congestion")

        underfilled = (
            upstream.queued_work > 0
            and controlled.queued_work == 0
            and controlled.active_work
            < controlled.capacity_work * self.low_active_fraction
        )
        if underfilled:
            return self._move(1, "upstream_work_available")
        return WorkCreditDecision(self.current_work, "hold", "deadband")

    def _fallback(self, reason: str) -> WorkCreditDecision:
        self._index = self.candidates.index(self.fallback_work)
        return WorkCreditDecision(self.current_work, "fallback", reason)

    def _move(self, delta: int, reason: str) -> WorkCreditDecision:
        updated = min(max(self._index + delta, 0), len(self.candidates) - 1)
        action = "hold" if updated == self._index else (
            "increase" if delta > 0 else "decrease"
        )
        self._index = updated
        return WorkCreditDecision(self.current_work, action, reason)
