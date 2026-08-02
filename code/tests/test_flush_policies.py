from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.submission_control.flush import (  # noqa: E402
    FixedTimeoutFlush,
    FlushObservation,
    ImmediateFlush,
    QueueAdaptiveFlush,
    SloAwareEwmaFlush,
)


def observation(
    *,
    now_s: float = 1.0,
    oldest_arrival_s: float = 0.98,
    budget_reached: bool = False,
    fresh: bool = True,
    running: int | None = 10,
    waiting: int | None = 0,
    kv_usage: float | None = 0.2,
    pending_cost: int = 100,
    token_budget: int = 1000,
    arrival_rate_tokens_s: float | None = 10_000.0,
    service_rate_tokens_s_per_endpoint: float | None = 2_000.0,
) -> FlushObservation:
    return FlushObservation(
        now_s=now_s,
        oldest_arrival_s=oldest_arrival_s,
        pending_rows=2,
        pending_cost=pending_cost,
        budget_reached=budget_reached,
        metrics_fresh=fresh,
        running=running,
        waiting=waiting,
        kv_usage=kv_usage,
        token_budget=token_budget,
        arrival_rate_tokens_s=arrival_rate_tokens_s,
        service_rate_tokens_s_per_endpoint=(
            service_rate_tokens_s_per_endpoint
        ),
    )


class FlushPolicyTests(unittest.TestCase):
    def test_immediate_flush_closes_every_nonempty_pending_batch(self) -> None:
        decision = ImmediateFlush().decide(observation())

        self.assertTrue(decision.flush)
        self.assertEqual(decision.reason, "immediate")

    def test_fixed_timeout_waits_then_flushes_at_bound(self) -> None:
        policy = FixedTimeoutFlush(timeout_s=0.05)

        waiting = policy.decide(observation(now_s=1.0, oldest_arrival_s=0.98))
        timed_out = policy.decide(
            observation(now_s=1.04, oldest_arrival_s=0.98)
        )

        self.assertFalse(waiting.flush)
        self.assertEqual(waiting.reason, "fixed_timeout_wait")
        self.assertTrue(timed_out.flush)
        self.assertEqual(timed_out.reason, "fixed_timeout")

    def test_all_policies_flush_immediately_at_budget(self) -> None:
        item = observation(budget_reached=True)

        self.assertTrue(FixedTimeoutFlush(1.0).decide(item).flush)
        self.assertTrue(QueueAdaptiveFlush().decide(item).flush)

    def test_queue_adaptive_selects_base_window_under_low_load(self) -> None:
        policy = QueueAdaptiveFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            pressure_running=8,
        )

        window = policy.select_window(observation(running=2))
        waiting = policy.decide(observation(running=2))
        elapsed = policy.decide(
            observation(
                now_s=1.01,
                oldest_arrival_s=0.98,
                running=2,
            )
        )

        self.assertEqual(window.wait_s, 0.025)
        self.assertEqual(window.reason, "underloaded_base_window")
        self.assertFalse(waiting.flush)
        self.assertEqual(waiting.reason, "underloaded_base_window_wait")
        self.assertTrue(elapsed.flush)
        self.assertEqual(elapsed.reason, "underloaded_base_window")

    def test_queue_adaptive_selects_fixed_fallback_for_unknown_metrics(
        self,
    ) -> None:
        policy = QueueAdaptiveFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            pressure_running=8,
        )

        stale = policy.select_window(observation(fresh=False))
        missing = policy.select_window(observation(waiting=None))

        self.assertEqual(stale.wait_s, 0.025)
        self.assertEqual(stale.reason, "fixed_fallback")
        self.assertEqual(missing.reason, "fixed_fallback")

    def test_queue_adaptive_selects_max_window_for_each_pressure_signal(
        self,
    ) -> None:
        policy = QueueAdaptiveFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            pressure_running=8,
        )

        queue = policy.select_window(observation(waiting=1))
        running = policy.select_window(observation(running=8))
        kv = policy.select_window(observation(kv_usage=0.9))

        self.assertEqual((queue.wait_s, queue.reason), (0.050, "queue_pressure"))
        self.assertEqual(running.reason, "running_pressure")
        self.assertEqual(kv.reason, "kv_pressure")

    def test_queue_adaptive_flushes_when_selected_pressure_window_elapses(
        self,
    ) -> None:
        policy = QueueAdaptiveFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            pressure_running=8,
        )

        waiting = policy.decide(
            observation(
                now_s=1.04,
                oldest_arrival_s=1.0,
                waiting=1,
            )
        )
        elapsed = policy.decide(
            observation(
                now_s=1.05,
                oldest_arrival_s=1.0,
                waiting=1,
            )
        )

        self.assertFalse(waiting.flush)
        self.assertTrue(elapsed.flush)
        self.assertEqual(elapsed.reason, "queue_pressure")

    def test_queue_adaptive_rejects_invalid_window_configuration(self) -> None:
        invalid = (
            {"min_wait_s": 0.0},
            {"min_wait_s": 0.05, "max_wait_s": 0.025},
            {"pressure_running": 0},
            {"congestion_kv_usage": 1.1},
        )

        for parameters in invalid:
            with self.subTest(parameters=parameters):
                with self.assertRaises(ValueError):
                    QueueAdaptiveFlush(**parameters)

    def test_flush_observation_rejects_negative_age(self) -> None:
        with self.assertRaisesRegex(ValueError, "oldest_arrival_s"):
            observation(now_s=0.5, oldest_arrival_s=1.0)

    def test_slo_ewma_uses_fixed_max_fallback_for_missing_feedback(self) -> None:
        policy = SloAwareEwmaFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            request_slo_s=1.0,
        )

        stale = policy.select_window(observation(fresh=False))
        missing = policy.select_window(
            observation(arrival_rate_tokens_s=None)
        )

        self.assertEqual((stale.wait_s, stale.reason), (0.050, "fixed_fallback"))
        self.assertEqual(missing.reason, "fixed_fallback")

    def test_slo_ewma_flushes_at_budget_or_exhausted_slack(self) -> None:
        policy = SloAwareEwmaFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            request_slo_s=0.1,
        )

        budget = policy.select_window(observation(budget_reached=True))
        deadline = policy.select_window(
            observation(
                now_s=1.08,
                oldest_arrival_s=1.0,
                pending_cost=100,
                service_rate_tokens_s_per_endpoint=1_000.0,
            )
        )

        self.assertEqual((budget.wait_s, budget.reason), (0.0, "budget_reached"))
        self.assertEqual((deadline.wait_s, deadline.reason), (0.0, "slo_deadline"))

    def test_slo_ewma_converts_remaining_slack_to_oldest_age_limit(self) -> None:
        policy = SloAwareEwmaFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            request_slo_s=0.1,
        )

        window = policy.select_window(
            observation(
                now_s=1.04,
                oldest_arrival_s=1.0,
                pending_cost=20,
                token_budget=520,
                arrival_rate_tokens_s=10_000.0,
                service_rate_tokens_s_per_endpoint=1_000.0,
                running=4,
            )
        )

        self.assertEqual(window.wait_s, 0.050)
        self.assertEqual(window.reason, "busy_load_ewma")

    def test_slo_ewma_uses_idle_minimum_and_busy_load_ratio(self) -> None:
        idle_policy = SloAwareEwmaFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            request_slo_s=1.0,
            endpoint_count=2,
        )
        busy_policy = SloAwareEwmaFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            request_slo_s=1.0,
            endpoint_count=2,
        )

        idle = idle_policy.select_window(observation(running=0))
        busy = busy_policy.select_window(
            observation(
                running=4,
                pending_cost=500,
                token_budget=1000,
                arrival_rate_tokens_s=5_000.0,
                service_rate_tokens_s_per_endpoint=2_000.0,
            )
        )

        self.assertEqual((idle.wait_s, idle.reason), (0.025, "service_idle"))
        self.assertAlmostEqual(busy.wait_s, 0.050)
        self.assertEqual(busy.reason, "busy_load_ewma")

    def test_slo_ewma_uses_minimum_when_arrivals_trail_service(self) -> None:
        policy = SloAwareEwmaFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            request_slo_s=1.0,
            endpoint_count=2,
        )

        window = policy.select_window(
            observation(
                running=4,
                arrival_rate_tokens_s=2_000.0,
                service_rate_tokens_s_per_endpoint=2_000.0,
            )
        )

        self.assertEqual((window.wait_s, window.reason), (0.025, "busy_load_ewma"))

    def test_slo_ewma_uses_calibrated_capacity_floor_for_load(self) -> None:
        policy = SloAwareEwmaFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            request_slo_s=1.0,
            endpoint_count=2,
            service_capacity_tokens_s_per_endpoint=4_000.0,
        )

        window = policy.select_window(
            observation(
                running=4,
                arrival_rate_tokens_s=4_000.0,
                service_rate_tokens_s_per_endpoint=500.0,
            )
        )

        self.assertEqual((window.wait_s, window.reason), (0.025, "busy_load_ewma"))

    def test_slo_ewma_deadband_holds_small_window_change(self) -> None:
        policy = SloAwareEwmaFlush(
            min_wait_s=0.025,
            max_wait_s=0.050,
            request_slo_s=1.0,
            ewma_alpha=1.0,
            deadband_ratio=0.2,
            endpoint_count=2,
        )

        first = policy.select_window(
            observation(
                running=4,
                pending_cost=500,
                token_budget=1000,
                arrival_rate_tokens_s=4_000.0,
                service_rate_tokens_s_per_endpoint=2_000.0,
            )
        )
        second = policy.select_window(
            observation(
                running=4,
                pending_cost=500,
                token_budget=1000,
                arrival_rate_tokens_s=4_080.0,
                service_rate_tokens_s_per_endpoint=2_000.0,
            )
        )

        self.assertAlmostEqual(first.wait_s, 0.0375)
        self.assertAlmostEqual(second.wait_s, first.wait_s)
        self.assertEqual(second.reason, "busy_load_ewma_hysteresis")


if __name__ == "__main__":
    unittest.main()
