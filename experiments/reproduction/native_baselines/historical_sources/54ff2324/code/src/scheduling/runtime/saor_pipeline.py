"""Engine-neutral SAOR controller for a bounded two-stage pipeline.

The controller deliberately does not create workers.  A Ray/Daft adapter owns
prestarted CPU and GPU pools and applies the returned admission limits.  This
keeps expensive actor-pool topology changes on a slower, offline-calibrated
timescale while the online loop only changes bounded flow between stages.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace

from ...planning.work import RuntimeStateSnapshot


@dataclass(frozen=True)
class SaorPipelineArmEstimate:
    """One safe flow arm and its service/cost estimate for one control slot.

    ``prepare_service_quanta`` and ``model_service_quanta`` must use the same
    normalized queue units as the respective queue scales supplied to the
    controller.  The arm limits are enforcement values consumed by the runtime
    adapter; they are not inferred from the service estimates.
    """

    name: str
    prepare_inflight_limit: int
    ready_buffer_work_limit: int
    model_inflight_limit: int
    prepare_service_quanta: float
    model_service_quanta: float
    tail_risk: float = 0.0
    memory_cost: float = 0.0

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("pipeline arm name must be non-empty")
        if min(
            self.prepare_inflight_limit,
            self.ready_buffer_work_limit,
            self.model_inflight_limit,
        ) <= 0:
            raise ValueError("pipeline arm limits must be positive")
        if any(
            not math.isfinite(value) or value < 0
            for value in (
                self.prepare_service_quanta,
                self.model_service_quanta,
                self.tail_risk,
                self.memory_cost,
            )
        ):
            raise ValueError("pipeline arm service and cost must be finite and non-negative")


@dataclass(frozen=True)
class SaorPipelineDecision:
    arm: SaorPipelineArmEstimate
    action: str
    reason: str
    action_scores: tuple[tuple[str, float], ...]
    prepare_backpressure: float
    model_backlog: float


class SaorPipelineController:
    """Select a finite safe arm using two-stage differential backpressure.

    For normalized prepare queue ``q_p`` and ready-model queue ``q_m``, the
    action-dependent drift surrogate is::

        -(q_p - q_m) * mu_p(a) - q_m * mu_m(a) + V * cost(a)

    This is the tandem-queue MaxWeight term.  A large ready-model queue reduces
    permission to create more tensors while increasing pressure to drain the
    model stage.  Device utilization is intentionally absent from the trigger.
    """

    def __init__(
        self,
        *,
        arms: tuple[SaorPipelineArmEstimate, ...],
        initial_arm: str,
        fallback_arm: str,
        prepare_queue_work_scale: int,
        model_queue_work_scale: int,
        ewma_alpha: float,
        min_dwell_samples: int,
        v: float,
        tail_weight: float,
        memory_weight: float,
        switch_weight: float,
    ) -> None:
        names = tuple(arm.name for arm in arms)
        if not names or len(names) != len(set(names)):
            raise ValueError("pipeline arms must be unique and non-empty")
        if initial_arm not in names or fallback_arm not in names:
            raise ValueError("initial and fallback pipeline arms must be configured")
        if min(prepare_queue_work_scale, model_queue_work_scale) <= 0:
            raise ValueError("pipeline queue work scales must be positive")
        if not math.isfinite(ewma_alpha) or not 0 < ewma_alpha <= 1:
            raise ValueError("ewma_alpha must be in (0, 1]")
        if min_dwell_samples < 0:
            raise ValueError("min_dwell_samples must be non-negative")
        weights = (v, tail_weight, memory_weight, switch_weight)
        if any(not math.isfinite(value) or value < 0 for value in weights):
            raise ValueError("pipeline controller weights must be finite and non-negative")
        self._arms = {arm.name: arm for arm in arms}
        self._names = names
        self._current_name = initial_arm
        self._fallback_name = fallback_arm
        self._prepare_scale = prepare_queue_work_scale
        self._model_scale = model_queue_work_scale
        self._ewma_alpha = ewma_alpha
        self._min_dwell_samples = min_dwell_samples
        self._dwell_remaining = 0
        self._v = v
        self._tail_weight = tail_weight
        self._memory_weight = memory_weight
        self._switch_weight = switch_weight

    @property
    def current_arm(self) -> SaorPipelineArmEstimate:
        return self._arms[self._current_name]

    def select(
        self,
        snapshot: RuntimeStateSnapshot,
        *,
        observed_prepare_service_quanta: float,
        observed_model_service_quanta: float,
        now_s: float,
        max_age_s: float,
        calibration_signature: str,
    ) -> SaorPipelineDecision:
        observations = (
            observed_prepare_service_quanta,
            observed_model_service_quanta,
        )
        if any(not math.isfinite(value) or value < 0 for value in observations):
            return self._fallback("invalid_observation")
        signature_matches = snapshot.calibration_signature == calibration_signature
        if not signature_matches:
            return self._fallback("calibration_signature_mismatch")
        if not snapshot.is_fresh(now_s=now_s, max_age_s=max_age_s):
            return self._fallback("stale_observation")
        prepare = snapshot.for_stage("prepare")
        model = snapshot.for_stage("model")
        if prepare is None or model is None:
            return self._fallback("missing_stage_observation")

        current = self.current_arm
        alpha = self._ewma_alpha
        self._arms[self._current_name] = replace(
            current,
            prepare_service_quanta=(
                alpha * observed_prepare_service_quanta
                + (1 - alpha) * current.prepare_service_quanta
            ),
            model_service_quanta=(
                alpha * observed_model_service_quanta
                + (1 - alpha) * current.model_service_quanta
            ),
        )
        current = self.current_arm
        q_prepare = prepare.queued_work / self._prepare_scale
        q_model = model.queued_work / self._model_scale
        prepare_backpressure = q_prepare - q_model

        if self._dwell_remaining:
            self._dwell_remaining -= 1
            return SaorPipelineDecision(
                current,
                "hold",
                "minimum_dwell",
                ((current.name, 0.0),),
                prepare_backpressure,
                q_model,
            )

        scores = tuple(
            (
                name,
                self._score(
                    self._arms[name],
                    current_name=current.name,
                    prepare_backpressure=prepare_backpressure,
                    model_backlog=q_model,
                ),
            )
            for name in self._names
        )
        selected_name, _score = min(scores, key=lambda item: (item[1], item[0]))
        previous = current
        self._current_name = selected_name
        selected = self.current_arm
        action = "hold" if selected.name == previous.name else "switch"
        if action == "switch":
            self._dwell_remaining = self._min_dwell_samples
        return SaorPipelineDecision(
            selected,
            action,
            "minimum_two_stage_drift_plus_penalty",
            scores,
            prepare_backpressure,
            q_model,
        )

    def _score(
        self,
        arm: SaorPipelineArmEstimate,
        *,
        current_name: str,
        prepare_backpressure: float,
        model_backlog: float,
    ) -> float:
        drift = (
            -prepare_backpressure * arm.prepare_service_quanta
            - model_backlog * arm.model_service_quanta
        )
        cost = (
            self._tail_weight * arm.tail_risk
            + self._memory_weight * arm.memory_cost
            + self._switch_weight * float(arm.name != current_name)
        )
        return drift + self._v * cost

    def _fallback(self, reason: str) -> SaorPipelineDecision:
        previous = self._current_name
        self._current_name = self._fallback_name
        self._dwell_remaining = self._min_dwell_samples
        fallback = self.current_arm
        return SaorPipelineDecision(
            fallback,
            "fallback" if previous != fallback.name or reason else "hold",
            reason,
            (),
            0.0,
            0.0,
        )
