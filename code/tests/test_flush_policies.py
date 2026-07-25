from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.flush import (  # noqa: E402
    FixedTimeoutFlush,
    FlushObservation,
    ImmediateFlush,
    QueueAdaptiveFlush,
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
) -> FlushObservation:
    return FlushObservation(
        now_s=now_s,
        oldest_arrival_s=oldest_arrival_s,
        pending_rows=2,
        pending_cost=100,
        budget_reached=budget_reached,
        metrics_fresh=fresh,
        running=running,
        waiting=waiting,
        kv_usage=kv_usage,
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


if __name__ == "__main__":
    unittest.main()
