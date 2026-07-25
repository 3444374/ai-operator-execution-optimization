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

    def test_queue_adaptive_flushes_partial_batch_under_low_load(self) -> None:
        decision = QueueAdaptiveFlush().decide(observation())

        self.assertTrue(decision.flush)
        self.assertEqual(decision.reason, "service_underloaded")

    def test_queue_adaptive_waits_during_congestion(self) -> None:
        queue_congestion = QueueAdaptiveFlush().decide(
            observation(waiting=1)
        )
        kv_congestion = QueueAdaptiveFlush().decide(
            observation(kv_usage=0.9)
        )

        self.assertFalse(queue_congestion.flush)
        self.assertEqual(queue_congestion.reason, "service_congested")
        self.assertFalse(kv_congestion.flush)

    def test_queue_adaptive_hard_timeout_overrides_congestion(self) -> None:
        decision = QueueAdaptiveFlush(max_wait_s=0.05).decide(
            observation(
                now_s=1.05,
                oldest_arrival_s=1.0,
                waiting=10,
                kv_usage=0.95,
            )
        )

        self.assertTrue(decision.flush)
        self.assertEqual(decision.reason, "hard_max_wait")

    def test_missing_or_stale_metrics_wait_only_until_hard_timeout(self) -> None:
        policy = QueueAdaptiveFlush(max_wait_s=0.05)

        missing = policy.decide(observation(waiting=None))
        stale = policy.decide(observation(fresh=False))

        self.assertFalse(missing.flush)
        self.assertEqual(missing.reason, "missing_metrics_wait")
        self.assertFalse(stale.flush)
        self.assertEqual(stale.reason, "stale_metrics_wait")

    def test_flush_observation_rejects_negative_age(self) -> None:
        with self.assertRaisesRegex(ValueError, "oldest_arrival_s"):
            observation(now_s=0.5, oldest_arrival_s=1.0)


if __name__ == "__main__":
    unittest.main()
