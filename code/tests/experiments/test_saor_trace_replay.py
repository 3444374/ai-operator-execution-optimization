from __future__ import annotations

import unittest

from src.experiments.saor.trace_replay import (
    PairedCapacityReplayConfig,
    replay_paired_capacity_trace,
)
from src.scheduling.core.control import CapacityArm


class SaorTraceReplayTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = PairedCapacityReplayConfig(
            arms=(
                ("lower", CapacityArm(2, 20)),
                ("upper", CapacityArm(4, 40)),
            ),
            initial_arm="lower",
            service_field="service_rate",
            risk_proxy_weights=(("queue_risk", 1.0),),
            v=1.0,
            tail_weight=0.5,
            switch_weight=0.1,
            prediction_lag_samples=1,
            calibration_signature="test-signature",
        )

    def test_uses_only_prior_sample_and_reports_heldout_regret(self) -> None:
        rows = [
            {"endpoint": "e0", "phase": "0", "arm": "lower", "service_rate": "10", "queue_risk": "0.1"},
            {"endpoint": "e0", "phase": "0", "arm": "upper", "service_rate": "14", "queue_risk": "0.1"},
            {"endpoint": "e0", "phase": "1", "arm": "lower", "service_rate": "10", "queue_risk": "0.1"},
            {"endpoint": "e0", "phase": "1", "arm": "upper", "service_rate": "11", "queue_risk": "1.0"},
            {"endpoint": "e0", "phase": "2", "arm": "lower", "service_rate": "10", "queue_risk": "0.1"},
            {"endpoint": "e0", "phase": "2", "arm": "upper", "service_rate": "11", "queue_risk": "1.0"},
        ]

        decisions = replay_paired_capacity_trace(rows, self.config)

        self.assertEqual(
            [row.selected_arm for row in decisions],
            ["lower", "upper", "lower"],
        )
        self.assertFalse(decisions[0].regret_eligible)
        self.assertTrue(decisions[1].regret_eligible)
        self.assertGreater(decisions[1].regret, 0.0)
        self.assertEqual(decisions[2].regret, 0.0)

    def test_requires_every_arm_in_each_paired_sample(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing arms"):
            replay_paired_capacity_trace(
                [
                    {"endpoint": "e0", "phase": "0", "arm": "lower", "service_rate": "10", "queue_risk": "0"},
                ],
                self.config,
            )


if __name__ == "__main__":
    unittest.main()
