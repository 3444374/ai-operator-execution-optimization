from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.core.models import (  # noqa: E402
    AdmissionObservation,
    ControlDiagnostics,
    WindowDecision,
)
from src.scheduling.submission_control.adaptive import (  # noqa: E402
    AimdAdmissionController,
    AimdConfig,
    EwmaAimdAdmissionController,
    HolAgeAimdAdmissionController,
    HolAgeAimdConfig,
)
from src.scheduling.submission_control.pid import (  # noqa: E402
    PidAdmissionController,
    PidConfig,
)
from src.scheduling.submission_control.ucb import (  # noqa: E402
    SloRewardInput,
    UcbAdmissionController,
    UcbConfig,
    slo_constrained_reward,
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

    def test_observation_accepts_and_rejects_hol_age(self) -> None:
        valid = AdmissionObservation(
            1.0, True, 0, 0, 0, 0.0, hol_age_s=1.5
        )
        self.assertEqual(valid.hol_age_s, 1.5)
        with self.assertRaisesRegex(ValueError, "hol_age_s"):
            AdmissionObservation(1.0, True, 0, 0, 0, 0.0, hol_age_s=-0.1)

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
    hol_age_s: float | None = None,
) -> AdmissionObservation:
    return AdmissionObservation(
        observed_at_s,
        fresh,
        inflight,
        running,
        waiting,
        kv_usage,
        hol_age_s=hol_age_s,
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


class HolAgeAimdAdmissionControllerTests(unittest.TestCase):
    def test_low_hol_age_additively_increases(self) -> None:
        controller = HolAgeAimdAdmissionController(
            HolAgeAimdConfig(min_window=4, max_window=8),
            initial_window=4,
        )

        decision = controller.update(observation(hol_age_s=0.2))

        self.assertEqual(decision.window, 6)
        self.assertEqual(decision.action, "increase")
        self.assertEqual(decision.reason, "hol_age_low_load")

    def test_congested_hol_age_multiplicatively_decreases(self) -> None:
        controller = HolAgeAimdAdmissionController(
            HolAgeAimdConfig(min_window=2, max_window=16),
            initial_window=6,
        )

        decision = controller.update(observation(hol_age_s=2.5))

        self.assertEqual(decision.window, 3)
        self.assertEqual(decision.action, "decrease")
        self.assertEqual(decision.reason, "hol_age_congestion")

    def test_deadband_and_missing_hol_age_hold(self) -> None:
        controller = HolAgeAimdAdmissionController(initial_window=8)

        deadband = controller.update(observation(hol_age_s=1.0))
        missing = controller.update(observation(hol_age_s=None))

        self.assertEqual(
            [(item.window, item.action) for item in (deadband, missing)],
            [(8, "hold"), (8, "hold")],
        )
        self.assertEqual(deadband.reason, "deadband")
        self.assertEqual(missing.reason, "missing_hol_age")

    def test_stale_observation_does_not_reapply_hol_decision(self) -> None:
        controller = HolAgeAimdAdmissionController(
            HolAgeAimdConfig(min_window=4, max_window=12),
            initial_window=4,
        )

        first = controller.update(observation(hol_age_s=0.2))
        repeated = controller.update(
            observation(fresh=False, hol_age_s=0.2)
        )

        self.assertEqual(first.window, 6)
        self.assertEqual(repeated.window, 6)
        self.assertEqual(repeated.reason, "stale_observation")

    def test_config_rejects_invalid_thresholds(self) -> None:
        invalid_pairs = [
            (3.0, 2.0),
            (float("inf"), float("inf")),
            (0.5, float("inf")),
            (float("nan"), 2.0),
        ]
        for low_load, congestion in invalid_pairs:
            with self.subTest(
                low_load=low_load,
                congestion=congestion,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "HOL-age thresholds",
                ):
                    HolAgeAimdConfig(
                        low_load_hol_age_s=low_load,
                        congestion_hol_age_s=congestion,
                    )


class PidAdmissionControllerTests(unittest.TestCase):
    def test_queue_above_target_decreases_window(self) -> None:
        controller = PidAdmissionController(initial_window=8)

        decision = controller.update(observation(waiting=5))

        self.assertEqual(decision.window, 6)
        self.assertEqual(decision.action, "decrease")
        self.assertEqual(decision.diagnostics.error, -4.0)

    def test_queue_at_target_holds_window(self) -> None:
        decision = PidAdmissionController(initial_window=8).update(
            observation(waiting=1)
        )

        self.assertEqual(decision.window, 8)
        self.assertEqual(decision.action, "hold")

    def test_repeated_low_queue_increases_window_deterministically(self) -> None:
        controller = PidAdmissionController(initial_window=8)

        first = controller.update(observation(observed_at_s=1.0, waiting=0))
        second = controller.update(observation(observed_at_s=2.0, waiting=0))

        self.assertEqual(first.window, 8)
        self.assertEqual(second.window, 9)
        self.assertEqual(second.action, "increase")

    def test_integral_is_clamped_to_prevent_windup(self) -> None:
        controller = PidAdmissionController(
            PidConfig(
                min_window=2,
                max_window=16,
                proportional_gain=0.0,
                integral_gain=1.0,
                derivative_gain=0.0,
                integral_limit=2.0,
            ),
            initial_window=8,
        )

        decision = None
        for timestamp in range(1, 20):
            decision = controller.update(
                observation(observed_at_s=float(timestamp), waiting=0)
            )

        self.assertIsNotNone(decision)
        self.assertEqual(decision.window, 16)
        self.assertEqual(decision.diagnostics.integral_error, 2.0)

    def test_missing_stale_and_non_monotonic_samples_hold(self) -> None:
        controller = PidAdmissionController(initial_window=8)
        controller.update(observation(observed_at_s=2.0, waiting=1))

        missing = controller.update(
            observation(observed_at_s=3.0, waiting=None)
        )
        stale = controller.update(
            observation(observed_at_s=4.0, fresh=False, waiting=10)
        )
        non_monotonic = controller.update(
            observation(observed_at_s=1.0, waiting=10)
        )

        self.assertEqual(missing.reason, "missing_queue_metric")
        self.assertEqual(stale.reason, "stale_observation")
        self.assertEqual(non_monotonic.reason, "non_monotonic_observation")
        self.assertTrue(all(item.window == 8 for item in (missing, stale, non_monotonic)))

    def test_pid_configuration_rejects_invalid_bounds(self) -> None:
        with self.assertRaisesRegex(ValueError, "window bounds"):
            PidConfig(min_window=8, max_window=4)


class UcbAdmissionControllerTests(unittest.TestCase):
    def test_ucb_visits_each_arm_before_exploitation(self) -> None:
        controller = UcbAdmissionController(UcbConfig(arms=(4, 8, 16)))

        selected = []
        for reward in (0.5, 1.5, 0.75):
            decision = controller.select()
            selected.append(decision.window)
            self.assertEqual(decision.action, "explore")
            controller.update_reward(decision.window, reward)

        exploitation = controller.select()

        self.assertEqual(selected, [4, 8, 16])
        self.assertEqual(exploitation.window, 8)
        self.assertEqual(exploitation.action, "exploit")
        self.assertEqual(exploitation.diagnostics.selected_arm, 8)

    def test_reward_update_changes_selected_arm_only(self) -> None:
        controller = UcbAdmissionController()
        selected = controller.select()

        with self.assertRaisesRegex(ValueError, "selected arm"):
            controller.update_reward(8, 1.0)
        controller.update_reward(selected.window, 1.0)

        self.assertEqual(
            controller.arm_statistics(),
            ((4, 1, 1.0), (8, 0, 0.0), (16, 0, 0.0)),
        )

    def test_ucb_tie_breaks_by_smallest_window(self) -> None:
        controller = UcbAdmissionController()
        for _ in range(3):
            decision = controller.select()
            controller.update_reward(decision.window, 1.0)

        self.assertEqual(controller.select().window, 4)

    def test_slo_reward_penalizes_tail_violation(self) -> None:
        within_slo = slo_constrained_reward(
            SloRewardInput(120.0, 100.0, 0.9, 1.0)
        )
        violated = slo_constrained_reward(
            SloRewardInput(120.0, 100.0, 2.0, 1.0)
        )

        self.assertAlmostEqual(within_slo, 1.2)
        self.assertAlmostEqual(violated, 0.3)

    def test_ucb_rejects_invalid_configuration_and_reward(self) -> None:
        with self.assertRaisesRegex(ValueError, "unique positive"):
            UcbConfig(arms=(4, 4))
        controller = UcbAdmissionController()
        selected = controller.select()
        with self.assertRaisesRegex(ValueError, "finite and non-negative"):
            controller.update_reward(selected.window, float("nan"))


if __name__ == "__main__":
    unittest.main()
