from __future__ import annotations

import unittest
import sys
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.token_budget import (  # noqa: E402
    ArrivalRateEwma,
    ServiceQuantumTokenBudgetController,
    StaticTokenBudgetController,
    TokenBudgetObservation,
)


class TokenBudgetControllerTests(unittest.TestCase):
    def test_static_budget_is_an_explicit_option(self) -> None:
        controller = StaticTokenBudgetController(8192)

        decision = controller.select(
            TokenBudgetObservation(
                arrival_rate_tokens_s=100.0,
                service_rate_tokens_s_per_endpoint=200.0,
            )
        )

        self.assertEqual(decision.token_budget, 8192)
        self.assertEqual(decision.reason, "static")

    def test_dynamic_budget_holds_when_feedback_is_unavailable(self) -> None:
        controller = ServiceQuantumTokenBudgetController(
            (2048, 4096, 8192),
            fallback_budget=4096,
            target_service_s=2.0,
            max_fill_wait_s=1.0,
        )

        decision = controller.select(
            TokenBudgetObservation(
                arrival_rate_tokens_s=None,
                service_rate_tokens_s_per_endpoint=3000.0,
            )
        )

        self.assertEqual(decision.token_budget, 4096)
        self.assertEqual(decision.reason, "feedback_unavailable_hold")

    def test_dynamic_budget_moves_one_safe_step_toward_service_quantum(self) -> None:
        controller = ServiceQuantumTokenBudgetController(
            (2048, 4096, 8192, 16384),
            fallback_budget=4096,
            target_service_s=2.0,
            max_fill_wait_s=2.0,
        )
        high_load = TokenBudgetObservation(
            arrival_rate_tokens_s=10000.0,
            service_rate_tokens_s_per_endpoint=5000.0,
        )

        first = controller.select(high_load)
        second = controller.select(high_load)

        self.assertEqual(first.token_budget, 8192)
        self.assertEqual(second.token_budget, 8192)
        self.assertEqual(second.reason, "hold_nearest_safe_budget")
        self.assertEqual(second.raw_target_tokens, 10000.0)

    def test_fill_rate_prevents_choosing_an_unfillable_large_budget(self) -> None:
        controller = ServiceQuantumTokenBudgetController(
            (1024, 2048, 4096, 8192),
            fallback_budget=4096,
            target_service_s=2.0,
            max_fill_wait_s=1.0,
        )

        decision = controller.select(
            TokenBudgetObservation(
                arrival_rate_tokens_s=1500.0,
                service_rate_tokens_s_per_endpoint=10000.0,
            )
        )

        self.assertEqual(decision.token_budget, 2048)
        self.assertEqual(decision.raw_target_tokens, 1500.0)

    def test_arrival_rate_ewma_coalesces_zero_gap_bursts(self) -> None:
        estimator = ArrivalRateEwma(alpha=0.5)

        self.assertIsNone(
            estimator.observe(arrival_s=0.0, tokens=100, time_scale=0.5)
        )
        self.assertIsNone(
            estimator.observe(arrival_s=0.0, tokens=200, time_scale=0.5)
        )
        rate = estimator.observe(arrival_s=2.0, tokens=300, time_scale=0.5)

        self.assertEqual(rate, 600.0)


if __name__ == "__main__":
    unittest.main()
