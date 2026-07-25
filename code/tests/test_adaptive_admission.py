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
from src.scheduling.adaptive_admission import (  # noqa: E402
    AimdAdmissionController,
    AimdConfig,
    EwmaAimdAdmissionController,
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


def observation(
    *,
    observed_at_s: float = 1.0,
    fresh: bool = True,
    inflight: int = 0,
    running: int | None = 10,
    waiting: int | None = 0,
    kv_usage: float | None = 0.2,
) -> AdmissionObservation:
    return AdmissionObservation(
        observed_at_s,
        fresh,
        inflight,
        running,
        waiting,
        kv_usage,
    )


class AimdAdmissionControllerTests(unittest.TestCase):
    def test_low_load_additively_increases_and_respects_maximum(self) -> None:
        controller = AimdAdmissionController(
            AimdConfig(min_window=4, max_window=8),
            initial_window=6,
        )

        first = controller.update(observation())
        second = controller.update(observation(observed_at_s=2.0))

        self.assertEqual(first.window, 8)
        self.assertEqual(first.action, "increase")
        self.assertEqual(second.window, 8)
        self.assertEqual(second.action, "hold")
        self.assertEqual(second.reason, "at_maximum")

    def test_queue_or_kv_congestion_multiplicatively_decreases(self) -> None:
        queue_controller = AimdAdmissionController(initial_window=15)
        kv_controller = AimdAdmissionController(initial_window=5)

        queue_decision = queue_controller.update(observation(waiting=1))
        kv_decision = kv_controller.update(observation(kv_usage=0.9))

        self.assertEqual(queue_decision.window, 7)
        self.assertEqual(queue_decision.reason, "queue_congestion")
        self.assertEqual(kv_decision.window, 4)
        self.assertEqual(kv_decision.reason, "kv_congestion")

    def test_deadband_missing_and_stale_observations_hold(self) -> None:
        controller = AimdAdmissionController(initial_window=8)

        deadband = controller.update(observation(kv_usage=0.7))
        missing = controller.update(
            observation(observed_at_s=2.0, waiting=None)
        )
        stale = controller.update(
            observation(observed_at_s=3.0, fresh=False, waiting=2)
        )

        self.assertEqual(
            [(item.window, item.action) for item in (deadband, missing, stale)],
            [(8, "hold"), (8, "hold"), (8, "hold")],
        )
        self.assertEqual(missing.reason, "missing_metrics")
        self.assertEqual(stale.reason, "stale_observation")

    def test_configuration_rejects_invalid_bounds_and_factors(self) -> None:
        with self.assertRaisesRegex(ValueError, "window bounds"):
            AimdConfig(min_window=8, max_window=4)
        with self.assertRaisesRegex(ValueError, "multiplicative_decrease"):
            AimdConfig(multiplicative_decrease=1.0)


class EwmaAimdAdmissionControllerTests(unittest.TestCase):
    def test_ewma_smooths_each_fresh_sample_once(self) -> None:
        controller = EwmaAimdAdmissionController(
            AimdConfig(min_window=4, max_window=16),
            initial_window=8,
            alpha=0.5,
            smoothed_waiting_threshold=0.5,
        )

        low = controller.update(observation(waiting=0))
        congested = controller.update(
            observation(observed_at_s=2.0, waiting=1)
        )

        self.assertEqual(low.window, 10)
        self.assertEqual(congested.window, 5)
        self.assertEqual(congested.action, "decrease")
        self.assertEqual(congested.diagnostics.smoothed_waiting, 0.5)

    def test_stale_sample_does_not_change_ewma_state(self) -> None:
        controller = EwmaAimdAdmissionController(initial_window=8, alpha=0.5)
        controller.update(observation(waiting=0, running=10, kv_usage=0.2))

        stale = controller.update(
            observation(
                observed_at_s=2.0,
                fresh=False,
                waiting=10,
                running=100,
                kv_usage=1.0,
            )
        )
        fresh = controller.update(
            observation(
                observed_at_s=3.0,
                waiting=0,
                running=10,
                kv_usage=0.2,
            )
        )

        self.assertEqual(stale.reason, "stale_observation")
        self.assertEqual(fresh.diagnostics.smoothed_waiting, 0.0)
        self.assertEqual(fresh.diagnostics.smoothed_running, 10.0)

    def test_ewma_rejects_invalid_alpha(self) -> None:
        with self.assertRaisesRegex(ValueError, "alpha"):
            EwmaAimdAdmissionController(alpha=0.0)


if __name__ == "__main__":
    unittest.main()
