"""Bounded request/work capacity control over offline-calibrated arms."""

from __future__ import annotations

import math
from dataclasses import dataclass

from ...planning.work import RuntimeStateSnapshot


@dataclass(frozen=True, order=True)
class CapacityArm:
    request_limit: int
    work_limit: int

    def __post_init__(self) -> None:
        if self.request_limit <= 0 or self.work_limit <= 0:
            raise ValueError("capacity limits must be positive")


@dataclass(frozen=True)
class CapacityDecision:
    arm: CapacityArm
    action: str
    reason: str
    service_rate_tokens_s: float | None


class BoundedCapacityController:
    """Move one step inside a calibrated capacity set with fail-safe fallback.

    The controller never creates demand. It increases only while upstream work
    is queued, the current request credit is substantially occupied, the model
    queue is clear, and measured service rate is below the calibrated target.
    Congestion must persist for multiple samples before a downshift.
    """

    def __init__(
        self,
        candidates: tuple[CapacityArm, ...],
        *,
        fallback: CapacityArm,
        initial: CapacityArm | None = None,
        controlled_stage: str = "model",
        upstream_stage: str = "organizer",
        target_service_rate_tokens_s: float,
        target_fraction: float = 0.95,
        occupied_fraction: float = 0.8,
        congestion_kv_usage: float = 0.85,
        consecutive_samples: int = 4,
        cooldown_samples: int = 4,
    ) -> None:
        ordered = tuple(sorted(set(candidates)))
        resolved_initial = fallback if initial is None else initial
        if (
            not ordered
            or fallback not in ordered
            or resolved_initial not in ordered
        ):
            raise ValueError("fallback must belong to a non-empty candidate set")
        if (
            not math.isfinite(target_service_rate_tokens_s)
            or target_service_rate_tokens_s <= 0
        ):
            raise ValueError("target service rate must be finite and positive")
        if not 0 < target_fraction <= 1 or not 0 < occupied_fraction <= 1:
            raise ValueError("controller fractions must be in (0, 1]")
        if not 0 < congestion_kv_usage <= 1:
            raise ValueError("congestion_kv_usage must be in (0, 1]")
        if consecutive_samples <= 0 or cooldown_samples < 0:
            raise ValueError("sample hysteresis values are invalid")
        self.candidates = ordered
        self.fallback = fallback
        self.controlled_stage = controlled_stage
        self.upstream_stage = upstream_stage
        self.target_service_rate_tokens_s = target_service_rate_tokens_s
        self.target_fraction = target_fraction
        self.occupied_fraction = occupied_fraction
        self.congestion_kv_usage = congestion_kv_usage
        self.consecutive_samples = consecutive_samples
        self.cooldown_samples = cooldown_samples
        self._index = ordered.index(resolved_initial)
        self._increase_streak = 0
        self._decrease_streak = 0
        self._cooldown = 0

    @property
    def current_arm(self) -> CapacityArm:
        return self.candidates[self._index]

    def select(
        self,
        snapshot: RuntimeStateSnapshot,
        *,
        active_requests: int,
        service_waiting_requests: int,
        service_rate_tokens_s: float | None,
        kv_usage: float | None = None,
        now_s: float,
        max_age_s: float,
        calibration_signature: str,
    ) -> CapacityDecision:
        if active_requests < 0 or service_waiting_requests < 0:
            raise ValueError("request counts must be non-negative")
        if (
            snapshot.calibration_signature != calibration_signature
            or not snapshot.is_fresh(now_s=now_s, max_age_s=max_age_s)
        ):
            return self._fallback("stale_or_signature_mismatch", service_rate_tokens_s)
        model = snapshot.for_stage(self.controlled_stage)
        upstream = snapshot.for_stage(self.upstream_stage)
        if model is None or upstream is None:
            return self._fallback("missing_stage", service_rate_tokens_s)
        if service_rate_tokens_s is None or not math.isfinite(service_rate_tokens_s):
            self._clear_streaks()
            return self._decision("hold", "missing_service_rate", None)
        if kv_usage is None or not math.isfinite(kv_usage) or not 0 <= kv_usage <= 1:
            return self._fallback(
                "missing_or_invalid_kv_usage",
                service_rate_tokens_s,
            )
        if self._cooldown:
            self._cooldown -= 1
            self._clear_streaks()
            return self._decision("hold", "cooldown", service_rate_tokens_s)

        has_backlog = upstream.queued_work > 0
        congested = has_backlog and (
            service_waiting_requests > 0
            or kv_usage >= self.congestion_kv_usage
        )
        occupied = (
            active_requests
            >= self.current_arm.request_limit * self.occupied_fraction
        )
        below_target = (
            service_rate_tokens_s
            < self.target_service_rate_tokens_s * self.target_fraction
        )
        feed_limited = (
            has_backlog
            and occupied
            and service_waiting_requests == 0
            and below_target
        )
        self._decrease_streak = self._decrease_streak + 1 if congested else 0
        self._increase_streak = self._increase_streak + 1 if feed_limited else 0
        if self._decrease_streak >= self.consecutive_samples:
            return self._move(-1, "persistent_service_queue", service_rate_tokens_s)
        if self._increase_streak >= self.consecutive_samples:
            return self._move(1, "ready_backlog_below_target", service_rate_tokens_s)
        return self._decision("hold", "hysteresis_or_deadband", service_rate_tokens_s)

    def _fallback(
        self,
        reason: str,
        service_rate_tokens_s: float | None,
    ) -> CapacityDecision:
        self._index = self.candidates.index(self.fallback)
        self._clear_streaks()
        self._cooldown = self.cooldown_samples
        return self._decision("fallback", reason, service_rate_tokens_s)

    def _move(
        self,
        delta: int,
        reason: str,
        service_rate_tokens_s: float,
    ) -> CapacityDecision:
        updated = min(max(self._index + delta, 0), len(self.candidates) - 1)
        action = "hold" if updated == self._index else (
            "increase" if delta > 0 else "decrease"
        )
        self._index = updated
        self._clear_streaks()
        self._cooldown = self.cooldown_samples if action != "hold" else 0
        return self._decision(action, reason, service_rate_tokens_s)

    def _clear_streaks(self) -> None:
        self._increase_streak = 0
        self._decrease_streak = 0

    def _decision(
        self,
        action: str,
        reason: str,
        service_rate_tokens_s: float | None,
    ) -> CapacityDecision:
        return CapacityDecision(
            self.current_arm,
            action,
            reason,
            service_rate_tokens_s,
        )
