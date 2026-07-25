from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.models import (  # noqa: E402
    AdmissionObservation,
    ControlDiagnostics,
    WindowDecision,
)


class AdaptiveAdmissionModelTests(unittest.TestCase):
    def test_observation_accepts_complete_fresh_metrics(self) -> None:
        observation = AdmissionObservation(
            observed_at_s=1.0,
            fresh=True,
            inflight=3,
            running=4,
            waiting=1,
            kv_usage=0.5,
        )

        self.assertEqual(observation.inflight, 3)
        self.assertEqual(observation.waiting, 1)

    def test_observation_allows_missing_service_metrics(self) -> None:
        observation = AdmissionObservation(
            observed_at_s=1.0,
            fresh=False,
            inflight=0,
            running=None,
            waiting=None,
            kv_usage=None,
        )

        self.assertIsNone(observation.running)
        self.assertFalse(observation.fresh)

    def test_observation_rejects_invalid_counts_and_kv_usage(self) -> None:
        with self.assertRaisesRegex(ValueError, "inflight"):
            AdmissionObservation(1.0, True, -1, 0, 0, 0.0)
        with self.assertRaisesRegex(ValueError, "running and waiting"):
            AdmissionObservation(1.0, True, 0, -1, 0, 0.0)
        with self.assertRaisesRegex(ValueError, "kv_usage"):
            AdmissionObservation(1.0, True, 0, 0, 0, 1.1)

    def test_window_decision_carries_typed_diagnostics(self) -> None:
        diagnostics = ControlDiagnostics(
            smoothed_waiting=0.5,
            error=0.5,
            selected_arm=8,
            arm_scores=((4, 0.2), (8, 0.4)),
        )
        decision = WindowDecision(8, "hold", "deadband", diagnostics)

        self.assertEqual(decision.window, 8)
        self.assertEqual(decision.diagnostics.selected_arm, 8)
        self.assertEqual(decision.diagnostics.arm_scores[1], (8, 0.4))

    def test_window_decision_rejects_invalid_public_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "window"):
            WindowDecision(0, "hold", "reason")
        with self.assertRaisesRegex(ValueError, "action and reason"):
            WindowDecision(1, "", "reason")


if __name__ == "__main__":
    unittest.main()
