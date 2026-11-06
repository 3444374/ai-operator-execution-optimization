"""Token-budget policies for upstream request organization."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class TokenBudgetObservation:
    """Signals available before opening the next upstream micro-batch."""

    arrival_rate_tokens_s: float | None
    service_rate_tokens_s_per_endpoint: float | None

    def __post_init__(self) -> None:
        for name, value in (
            ("arrival_rate_tokens_s", self.arrival_rate_tokens_s),
            (
                "service_rate_tokens_s_per_endpoint",
                self.service_rate_tokens_s_per_endpoint,
            ),
        ):
            if value is not None and (
                not math.isfinite(value) or value < 0
            ):
                raise ValueError(f"{name} must be finite and non-negative")


@dataclass(frozen=True)
class TokenBudgetDecision:
    token_budget: int
    reason: str
    raw_target_tokens: float | None = None

    def __post_init__(self) -> None:
        if self.token_budget < 0:
            raise ValueError("token_budget must be non-negative")
        if not self.reason:
            raise ValueError("reason must be non-empty")
        if self.raw_target_tokens is not None and (
            not math.isfinite(self.raw_target_tokens)
            or self.raw_target_tokens < 0
        ):
            raise ValueError(
                "raw_target_tokens must be finite and non-negative"
            )


class StaticTokenBudgetController:
    def __init__(self, token_budget: int) -> None:
        if token_budget <= 0:
            raise ValueError("token_budget must be positive")
        self.token_budget = token_budget

    def select(
        self,
        observation: TokenBudgetObservation,
    ) -> TokenBudgetDecision:
        del observation
        return TokenBudgetDecision(self.token_budget, "static")


class ServiceQuantumTokenBudgetController:
    """Choose a safe discrete budget from arrival and service-rate feedback.

    The controller deliberately operates only inside a capacity curve that was
    calibrated offline. It never invents a budget beyond the configured action
    set and moves by at most one action per decision to avoid oscillation.
    """

    def __init__(
        self,
        candidates: tuple[int, ...],
        *,
        fallback_budget: int,
        target_service_s: float,
        max_fill_wait_s: float,
    ) -> None:
        if not candidates:
            raise ValueError("candidates must not be empty")
        if any(
            not isinstance(candidate, int)
            or isinstance(candidate, bool)
            or candidate <= 0
            for candidate in candidates
        ):
            raise ValueError("candidates must contain positive integers")
        ordered = tuple(sorted(set(candidates)))
        if fallback_budget not in ordered:
            raise ValueError("fallback_budget must be one of candidates")
        if not math.isfinite(target_service_s) or target_service_s <= 0:
            raise ValueError("target_service_s must be finite and positive")
        if not math.isfinite(max_fill_wait_s) or max_fill_wait_s <= 0:
            raise ValueError("max_fill_wait_s must be finite and positive")
        self.candidates = ordered
        self.fallback_budget = fallback_budget
        self.target_service_s = target_service_s
        self.max_fill_wait_s = max_fill_wait_s
        self._current_index = ordered.index(fallback_budget)

    @property
    def current_budget(self) -> int:
        return self.candidates[self._current_index]

    def select(
        self,
        observation: TokenBudgetObservation,
    ) -> TokenBudgetDecision:
        arrival_rate = observation.arrival_rate_tokens_s
        service_rate = observation.service_rate_tokens_s_per_endpoint
        if arrival_rate is None or service_rate is None:
            return TokenBudgetDecision(
                self.current_budget,
                "feedback_unavailable_hold",
            )

        service_quantum = service_rate * self.target_service_s
        fillable_quantum = arrival_rate * self.max_fill_wait_s
        raw_target = min(service_quantum, fillable_quantum)
        target_index = self._floor_candidate_index(raw_target)
        if target_index > self._current_index:
            self._current_index += 1
            reason = "increase_one_step"
        elif target_index < self._current_index:
            self._current_index -= 1
            reason = "decrease_one_step"
        else:
            reason = "hold_nearest_safe_budget"
        return TokenBudgetDecision(
            self.current_budget,
            reason,
            raw_target_tokens=raw_target,
        )

    def _floor_candidate_index(self, raw_target: float) -> int:
        selected = 0
        for index, candidate in enumerate(self.candidates):
            if candidate > raw_target:
                break
            selected = index
        return selected


class ArrivalRateEwma:
    """Estimate effective replay token arrival rate without zero-gap spikes."""

    def __init__(self, alpha: float = 0.3) -> None:
        if not math.isfinite(alpha) or not 0 < alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")
        self.alpha = alpha
        self._last_arrival_s: float | None = None
        self._pending_tokens = 0
        self._rate: float | None = None

    @property
    def rate_tokens_s(self) -> float | None:
        return self._rate

    def observe(
        self,
        *,
        arrival_s: float,
        tokens: int,
        time_scale: float,
    ) -> float | None:
        if not math.isfinite(arrival_s) or arrival_s < 0:
            raise ValueError("arrival_s must be finite and non-negative")
        if not isinstance(tokens, int) or isinstance(tokens, bool) or tokens < 0:
            raise ValueError("tokens must be a non-negative integer")
        if not math.isfinite(time_scale) or time_scale <= 0:
            raise ValueError("time_scale must be finite and positive")
        self._pending_tokens += tokens
        if self._last_arrival_s is None:
            self._last_arrival_s = arrival_s
            return self._rate
        source_delta_s = arrival_s - self._last_arrival_s
        if source_delta_s < 0:
            raise ValueError("arrival_s values must be non-decreasing")
        if source_delta_s == 0:
            return self._rate
        instantaneous = self._pending_tokens / (source_delta_s * time_scale)
        self._rate = (
            instantaneous
            if self._rate is None
            else self.alpha * instantaneous + (1 - self.alpha) * self._rate
        )
        self._last_arrival_s = arrival_s
        self._pending_tokens = 0
        return self._rate
