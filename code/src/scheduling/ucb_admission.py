"""Finite-action UCB1 admission controller and SLO-aware reward."""

from __future__ import annotations

import math
from dataclasses import dataclass

from .models import ControlDiagnostics, WindowDecision


@dataclass(frozen=True)
class UcbConfig:
    arms: tuple[int, ...] = (4, 8, 16)
    exploration_coefficient: float = math.sqrt(2.0)

    def __post_init__(self) -> None:
        if (
            not self.arms
            or any(arm <= 0 for arm in self.arms)
            or len(set(self.arms)) != len(self.arms)
        ):
            raise ValueError("arms must contain unique positive windows")
        if tuple(sorted(self.arms)) != self.arms:
            raise ValueError("arms must be sorted for deterministic tie-breaking")
        if (
            not math.isfinite(self.exploration_coefficient)
            or self.exploration_coefficient < 0
        ):
            raise ValueError("exploration_coefficient must be finite and non-negative")


@dataclass(frozen=True)
class SloRewardInput:
    epoch_tokens_per_s: float
    baseline_tokens_per_s: float
    service_p99_s: float
    slo_s: float

    def __post_init__(self) -> None:
        values = (
            self.epoch_tokens_per_s,
            self.baseline_tokens_per_s,
            self.service_p99_s,
            self.slo_s,
        )
        if any(not math.isfinite(value) for value in values):
            raise ValueError("reward inputs must be finite")
        if self.epoch_tokens_per_s < 0 or self.service_p99_s < 0:
            raise ValueError("throughput and service_p99_s must be non-negative")
        if self.baseline_tokens_per_s <= 0 or self.slo_s <= 0:
            raise ValueError("baseline throughput and slo_s must be positive")


def slo_constrained_reward(inputs: SloRewardInput) -> float:
    throughput_ratio = min(
        2.0,
        inputs.epoch_tokens_per_s / inputs.baseline_tokens_per_s,
    )
    if inputs.service_p99_s <= inputs.slo_s:
        return throughput_ratio
    penalty = (inputs.slo_s / inputs.service_p99_s) ** 2
    return throughput_ratio * penalty


class UcbAdmissionController:
    def __init__(self, config: UcbConfig | None = None):
        self.config = config or UcbConfig()
        self._counts = {arm: 0 for arm in self.config.arms}
        self._reward_sums = {arm: 0.0 for arm in self.config.arms}
        self._selected_arm: int | None = None

    def select(self) -> WindowDecision:
        if self._selected_arm is not None:
            raise RuntimeError("selected arm must receive a reward before selecting again")

        untried = [arm for arm in self.config.arms if self._counts[arm] == 0]
        if untried:
            selected = untried[0]
            scores: tuple[tuple[int, float], ...] = ()
            action = "explore"
            reason = "untried_arm"
        else:
            total_pulls = sum(self._counts.values())
            scores = tuple(
                (
                    arm,
                    self._mean_reward(arm)
                    + self.config.exploration_coefficient
                    * math.sqrt(math.log(total_pulls) / self._counts[arm]),
                )
                for arm in self.config.arms
            )
            selected = max(scores, key=lambda item: (item[1], -item[0]))[0]
            action = "exploit"
            reason = "highest_ucb_score"

        self._selected_arm = selected
        return WindowDecision(
            window=selected,
            action=action,
            reason=reason,
            diagnostics=ControlDiagnostics(
                selected_arm=selected,
                arm_scores=scores,
            ),
        )

    def update_reward(self, arm: int, reward: float) -> None:
        if arm != self._selected_arm:
            raise ValueError("reward must update the currently selected arm")
        if not math.isfinite(reward) or reward < 0:
            raise ValueError("reward must be finite and non-negative")
        self._counts[arm] += 1
        self._reward_sums[arm] += reward
        self._selected_arm = None

    def arm_statistics(self) -> tuple[tuple[int, int, float], ...]:
        return tuple(
            (arm, self._counts[arm], self._mean_reward(arm))
            for arm in self.config.arms
        )

    def _mean_reward(self, arm: int) -> float:
        count = self._counts[arm]
        return self._reward_sums[arm] / count if count else 0.0
