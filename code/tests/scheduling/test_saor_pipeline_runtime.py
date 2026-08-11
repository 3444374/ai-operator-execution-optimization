from __future__ import annotations

import unittest

from src.planning.work import RuntimeStateSnapshot, StageStateSnapshot
from src.scheduling.runtime.saor_pipeline import (
    SaorPipelineArmEstimate,
    SaorPipelineController,
)


def _snapshot(
    *,
    prepare_queued: int,
    model_queued: int,
    observed_at_s: float = 1.0,
    signature: str = "image-sig",
) -> RuntimeStateSnapshot:
    return RuntimeStateSnapshot(
        stages=(
            StageStateSnapshot(
                "prepare", 0, prepare_queued, None, 0.0, observed_at_s, 100
            ),
            StageStateSnapshot(
                "model", 0, model_queued, None, 0.0, observed_at_s, 100
            ),
        ),
        observed_at_s=observed_at_s,
        calibration_signature=signature,
    )


class SaorPipelineRuntimeTest(unittest.TestCase):
    def _controller(self) -> SaorPipelineController:
        return SaorPipelineController(
            arms=(
                SaorPipelineArmEstimate("balanced", 2, 100, 1, 1.0, 1.0),
                SaorPipelineArmEstimate(
                    "prepare_boost", 4, 100, 1, 2.0, 1.0, memory_cost=0.1
                ),
                SaorPipelineArmEstimate(
                    "model_boost", 2, 100, 2, 1.0, 2.0, memory_cost=0.1
                ),
            ),
            initial_arm="balanced",
            fallback_arm="balanced",
            prepare_queue_work_scale=10,
            model_queue_work_scale=10,
            ewma_alpha=1.0,
            min_dwell_samples=0,
            v=1.0,
            tail_weight=0.0,
            memory_weight=1.0,
            switch_weight=0.0,
        )

    def test_upstream_backlog_selects_prepare_capacity(self) -> None:
        decision = self._controller().select(
            _snapshot(prepare_queued=100, model_queued=0),
            observed_prepare_service_quanta=1.0,
            observed_model_service_quanta=1.0,
            now_s=1.0,
            max_age_s=0.0,
            calibration_signature="image-sig",
        )
        self.assertEqual(decision.arm.name, "prepare_boost")
        self.assertGreater(decision.prepare_backpressure, 0)

    def test_ready_tensor_backlog_stops_extra_prepare_and_drains_model(self) -> None:
        decision = self._controller().select(
            _snapshot(prepare_queued=0, model_queued=100),
            observed_prepare_service_quanta=1.0,
            observed_model_service_quanta=1.0,
            now_s=1.0,
            max_age_s=0.0,
            calibration_signature="image-sig",
        )
        self.assertEqual(decision.arm.name, "model_boost")
        self.assertLess(decision.prepare_backpressure, 0)

    def test_stale_snapshot_falls_back(self) -> None:
        controller = self._controller()
        controller.select(
            _snapshot(prepare_queued=100, model_queued=0),
            observed_prepare_service_quanta=1.0,
            observed_model_service_quanta=1.0,
            now_s=1.0,
            max_age_s=0.0,
            calibration_signature="image-sig",
        )
        decision = controller.select(
            _snapshot(prepare_queued=100, model_queued=0),
            observed_prepare_service_quanta=1.0,
            observed_model_service_quanta=1.0,
            now_s=2.0,
            max_age_s=0.5,
            calibration_signature="image-sig",
        )
        self.assertEqual(decision.arm.name, "balanced")
        self.assertEqual(decision.reason, "stale_observation")


if __name__ == "__main__":
    unittest.main()
