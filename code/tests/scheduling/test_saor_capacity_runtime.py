from __future__ import annotations

import unittest

from src.planning.work import RuntimeStateSnapshot, StageStateSnapshot
from src.scheduling.core.control import CapacityArm
from src.scheduling.runtime.saor_capacity import (
    LinearCostFeature,
    SaorArmEstimate,
    SaorCapacityController,
    SaorObservationModel,
)


def _snapshot(*, observed_at_s: float = 1.0, signature: str = "sig") -> RuntimeStateSnapshot:
    return RuntimeStateSnapshot(
        stages=(
            StageStateSnapshot("organizer", 0, 100, None, 1.0, observed_at_s),
            StageStateSnapshot("model", 10, 0, 10.0, 0.0, observed_at_s, 20),
        ),
        observed_at_s=observed_at_s,
        calibration_signature=signature,
    )


class SaorCapacityRuntimeTest(unittest.TestCase):
    def _controller(self) -> SaorCapacityController:
        return SaorCapacityController(
            arms=(
                SaorArmEstimate("lower", CapacityArm(2, 20), 0.7, 0.1, 0.5),
                SaorArmEstimate("upper", CapacityArm(4, 40), 1.0, 0.2, 0.6),
            ),
            initial_arm="lower",
            fallback_arm="lower",
            ewma_alpha=1.0,
            queue_work_scale=100,
            min_dwell_samples=0,
            v=1.0,
            tail_weight=1.0,
            energy_weight=0.0,
            switch_weight=0.0,
        )

    def test_uses_priors_then_updates_current_arm_from_observation(self) -> None:
        controller = self._controller()

        up = controller.select(
            _snapshot(),
            observed_goodput=0.7,
            observed_tail_risk=0.1,
            observed_energy=0.5,
            now_s=1.0,
            max_age_s=0.0,
            calibration_signature="sig",
        )
        down = controller.select(
            _snapshot(observed_at_s=2.0),
            observed_goodput=0.6,
            observed_tail_risk=1.0,
            observed_energy=0.6,
            now_s=2.0,
            max_age_s=0.0,
            calibration_signature="sig",
        )

        self.assertEqual(up.arm_name, "upper")
        self.assertEqual(up.action, "increase")
        self.assertEqual(down.arm_name, "lower")
        self.assertEqual(down.action, "decrease")

    def test_stale_state_falls_back_without_learning(self) -> None:
        controller = self._controller()
        decision = controller.select(
            _snapshot(),
            observed_goodput=1.0,
            observed_tail_risk=0.0,
            observed_energy=0.0,
            now_s=3.0,
            max_age_s=1.0,
            calibration_signature="sig",
        )
        self.assertEqual(decision.arm_name, "lower")
        self.assertEqual(decision.action, "fallback")
        self.assertEqual(decision.reason, "stale_observation")

    def test_observation_model_requires_every_configured_feature(self) -> None:
        model = SaorObservationModel(
            goodput_field="service_rate",
            goodput_scale=10.0,
            tail_features=(LinearCostFeature("queue_age", 5.0, 2.0),),
            energy_features=(LinearCostFeature("power", 100.0, 1.0),),
        )
        observation = model.evaluate(
            {"service_rate": 5.0, "queue_age": 2.5, "power": 50.0}
        )
        self.assertEqual(observation, (0.5, 1.0, 0.5))
        with self.assertRaisesRegex(ValueError, "queue_age"):
            model.evaluate({"service_rate": 5.0, "power": 50.0})


if __name__ == "__main__":
    unittest.main()
