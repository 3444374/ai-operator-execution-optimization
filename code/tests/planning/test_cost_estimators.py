"""Tests for cost-model ranking and residual-interval metrics."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np

CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
sys.path.insert(0, str(CODE_ROOT))

from src.planning.costs.regression import (  # noqa: E402
    pairwise_accuracy,
    residual_interval_bounds,
    top_k_precision,
)


class MetricTests(unittest.TestCase):
    def test_pairwise_constant_predictor_is_random(self) -> None:
        actual = np.array([1.0, 5.0, 3.0, 9.0, 2.0])
        constant = np.full(5, 7.0)
        self.assertAlmostEqual(pairwise_accuracy(actual, constant), 0.5)

    def test_pairwise_perfect_predictor_is_one(self) -> None:
        actual = np.array([1.0, 5.0, 3.0, 9.0, 2.0])
        self.assertAlmostEqual(pairwise_accuracy(actual, actual), 1.0)

    def test_topk_perfect_predictor(self) -> None:
        actual = np.array([9.0, 4.0, 7.0, 1.0, 6.0, 8.0, 2.0, 5.0])
        self.assertAlmostEqual(top_k_precision(actual, actual, k=3), 1.0)

    def test_interval_widens_with_confidence(self) -> None:
        residuals = np.linspace(-10, 10, 41)
        lo90, hi90 = residual_interval_bounds(residuals, confidence=0.9)
        lo99, hi99 = residual_interval_bounds(residuals, confidence=0.98)
        self.assertLess(hi90 - lo90, hi99 - lo99)


if __name__ == "__main__":
    unittest.main()
