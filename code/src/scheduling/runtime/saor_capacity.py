"""Engine-neutral runtime adapter for the SAOR capacity-only ablation."""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import Mapping

from ...planning.work import RuntimeStateSnapshot
from ..core.control import CapacityArm
from ..submission_control.saor import (
    SaorAction,
    SaorControlState,
    SaorJobState,
    SaorPolicy,
)


@dataclass(frozen=True)
class LinearCostFeature:
    field: str
    scale: float
    weight: float

    def __post_init__(self) -> None:
        if not self.field:
            raise ValueError("cost feature field must be non-empty")
        if not math.isfinite(self.scale) or self.scale <= 0:
            raise ValueError("cost feature scale must be finite and positive")
        if not math.isfinite(self.weight) or self.weight < 0:
            raise ValueError("cost feature weight must be finite and non-negative")


@dataclass(frozen=True)
class SaorObservationModel:
    """Map explicit runtime fields to dimensionless SAOR observations."""

    goodput_field: str
    goodput_scale: float
    tail_features: tuple[LinearCostFeature, ...]
    energy_features: tuple[LinearCostFeature, ...] = ()

    def __post_init__(self) -> None:
        if not self.goodput_field:
            raise ValueError("goodput_field must be non-empty")
        if not math.isfinite(self.goodput_scale) or self.goodput_scale <= 0:
            raise ValueError("goodput_scale must be finite and positive")
        feature_names = tuple(
            feature.field for feature in (*self.tail_features, *self.energy_features)
        )
        if not self.tail_features or len(feature_names) != len(set(feature_names)):
            raise ValueError("cost feature fields must be unique with a tail feature")

    def evaluate(self, row: Mapping[str, object]) -> tuple[float, float, float]:
        goodput = _nonnegative_field(row, self.goodput_field) / self.goodput_scale
        tail_risk = sum(
            feature.weight
            * _nonnegative_field(row, feature.field)
            / feature.scale
            for feature in self.tail_features
        )
        energy = sum(
            feature.weight
            * _nonnegative_field(row, feature.field)
            / feature.scale
            for feature in self.energy_features
        )
        return goodput, tail_risk, energy


@dataclass(frozen=True)
class SaorArmEstimate:
    name: str
    arm: CapacityArm
    goodput: float
    tail_risk: float
    energy: float

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("SAOR arm name must be non-empty")
        if any(
            not math.isfinite(value) or value < 0
            for value in (self.goodput, self.tail_risk, self.energy)
        ):
            raise ValueError("SAOR arm estimates must be finite and non-negative")


@dataclass(frozen=True)
class SaorCapacityDecision:
    arm_name: str
    arm: CapacityArm
    action: str
    reason: str
    action_scores: tuple[tuple[str, float], ...]


class SaorCapacityController:
    """Learn current-arm costs and choose an adjacent calibrated capacity arm."""

    def __init__(
        self,
        *,
        arms: tuple[SaorArmEstimate, ...],
        initial_arm: str,
        fallback_arm: str,
        ewma_alpha: float,
        queue_work_scale: int,
        min_dwell_samples: int,
        v: float,
        tail_weight: float,
        energy_weight: float,
        switch_weight: float,
    ) -> None:
        names = tuple(item.name for item in arms)
        if not names or len(names) != len(set(names)):
            raise ValueError("SAOR capacity arms must be unique and non-empty")
        ordered = tuple(sorted(arms, key=lambda item: item.arm))
        if tuple(item.name for item in ordered) != names:
            raise ValueError("SAOR capacity arms must be ordered by capacity")
        if initial_arm not in names or fallback_arm not in names:
            raise ValueError("initial and fallback arms must be configured")
        if not math.isfinite(ewma_alpha) or not 0 < ewma_alpha <= 1:
            raise ValueError("ewma_alpha must be in (0, 1]")
        if queue_work_scale <= 0:
            raise ValueError("queue_work_scale must be positive")
        if min_dwell_samples < 0:
            raise ValueError("min_dwell_samples must be non-negative")
        self._estimates = {item.name: item for item in arms}
        self._names = names
        self._current_name = initial_arm
        self._fallback_name = fallback_arm
        self._ewma_alpha = ewma_alpha
        self._queue_work_scale = queue_work_scale
        self._min_dwell_samples = min_dwell_samples
        self._dwell_remaining = 0
        self._policy = SaorPolicy(
            v=v,
            eta_f=0.0,
            tail_weight=tail_weight,
            energy_weight=energy_weight,
            switch_weight=switch_weight,
        )

    @property
    def current_arm(self) -> SaorArmEstimate:
        return self._estimates[self._current_name]

    def select(
        self,
        snapshot: RuntimeStateSnapshot,
        *,
        observed_goodput: float,
        observed_tail_risk: float,
        observed_energy: float,
        now_s: float,
        max_age_s: float,
        calibration_signature: str,
    ) -> SaorCapacityDecision:
        observed = (observed_goodput, observed_tail_risk, observed_energy)
        if any(not math.isfinite(value) or value < 0 for value in observed):
            return self._fallback("invalid_observation")
        signature_matches = snapshot.calibration_signature == calibration_signature
        fresh = signature_matches and snapshot.is_fresh(
            now_s=now_s,
            max_age_s=max_age_s,
        )
        if not fresh:
            return self._fallback(
                "calibration_signature_mismatch"
                if not signature_matches
                else "stale_observation"
            )

        current = self.current_arm
        alpha = self._ewma_alpha
        self._estimates[self._current_name] = replace(
            current,
            goodput=alpha * observed_goodput + (1 - alpha) * current.goodput,
            tail_risk=alpha * observed_tail_risk + (1 - alpha) * current.tail_risk,
            energy=alpha * observed_energy + (1 - alpha) * current.energy,
        )
        current = self.current_arm
        if self._dwell_remaining:
            self._dwell_remaining -= 1
            return SaorCapacityDecision(
                current.name,
                current.arm,
                "hold",
                "minimum_dwell",
                ((current.name, 0.0),),
            )

        current_index = self._names.index(self._current_name)
        eligible_names = self._names[
            max(0, current_index - 1) : min(len(self._names), current_index + 2)
        ]
        upstream = snapshot.for_stage("organizer")
        queued_work = upstream.queued_work if upstream is not None else 0
        queue_units = (
            math.ceil(queued_work / self._queue_work_scale)
            if queued_work > 0
            else 0
        )
        jobs = (
            (SaorJobState("aggregate-ready-work", 1.0, queue_units),)
            if queue_units > 0
            else ()
        )
        actions = tuple(
            self._action(
                self._estimates[name],
                current,
                queue_units=queue_units,
            )
            for name in eligible_names
        )
        state = SaorControlState(
            jobs=jobs,
            actions=actions,
            fallback_action=SaorAction(
                self._fallback_name,
                "capacity",
                self._estimates[self._fallback_name].arm,
            ),
            current_arm=current.arm,
            observed_at_s=snapshot.observed_at_s,
            calibration_signature=snapshot.calibration_signature,
        )
        decision = self._policy.select(
            state,
            now_s=now_s,
            max_age_s=max_age_s,
            calibration_signature=calibration_signature,
        )
        previous_index = current_index
        self._current_name = decision.action.action_id
        updated_index = self._names.index(self._current_name)
        action = (
            "increase" if updated_index > previous_index
            else "decrease" if updated_index < previous_index
            else "hold"
        )
        if action != "hold":
            self._dwell_remaining = self._min_dwell_samples
        return SaorCapacityDecision(
            self._current_name,
            decision.action.arm,
            action,
            decision.reason,
            decision.action_scores,
        )

    @staticmethod
    def _action(
        candidate: SaorArmEstimate,
        current: SaorArmEstimate,
        *,
        queue_units: int,
    ) -> SaorAction:
        return SaorAction(
            candidate.name,
            "capacity",
            candidate.arm,
            predicted_service_by_job=(
                (("aggregate-ready-work", min(candidate.goodput, queue_units)),)
                if queue_units > 0
                else ()
            ),
            predicted_goodput_delta=candidate.goodput - current.goodput,
            tail_risk_delta=candidate.tail_risk - current.tail_risk,
            energy_delta=candidate.energy - current.energy,
            switch_cost=float(candidate.name != current.name),
        )

    def _fallback(self, reason: str) -> SaorCapacityDecision:
        previous = self._current_name
        self._current_name = self._fallback_name
        self._dwell_remaining = self._min_dwell_samples
        fallback = self.current_arm
        return SaorCapacityDecision(
            fallback.name,
            fallback.arm,
            "fallback" if previous != fallback.name or reason else "hold",
            reason,
            (),
        )


def _nonnegative_field(row: Mapping[str, object], field: str) -> float:
    try:
        value = float(row[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"runtime observation field {field} is missing or invalid") from exc
    if not math.isfinite(value) or value < 0:
        raise ValueError(
            f"runtime observation field {field} must be finite and non-negative"
        )
    return value
