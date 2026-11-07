"""Tests for the existing historical CSV cost estimators."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np

CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
sys.path.insert(0, str(CODE_ROOT))

from src.experiments.operator_costs.estimators import (  # noqa: E402
    AnalyticalCostEstimator,
    HybridCostEstimator,
    LookupCostEstimator,
)


def _row(token_count: int, cap: int, total_rows: int, model="m", batching="b") -> dict:
    return {
        "token_count": str(token_count),
        "completion_max_tokens": str(cap),
        "total_rows": str(total_rows),
        "model_name": model,
        "batching_policy": batching,
    }


class EstimatorTests(unittest.TestCase):
    def test_analytical_recovers_linear_signal(self) -> None:
        rows = [_row(100 * i, 10, i) for i in range(1, 40)]
        targets = np.array([2.0 + 0.01 * (100 * i) + 0.001 * (i * 10) for i in range(1, 40)])
        pred = AnalyticalCostEstimator().fit(rows, targets).predict(rows)
        rel_err = np.abs(pred - targets) / targets
        self.assertLess(np.mean(rel_err), 0.05)

    def test_lookup_bucket_then_global_fallback(self) -> None:
        rows = [_row(100, 8, 8, "m1", "b1")] * 4 + [_row(200, 16, 16, "m2", "b2")] * 4
        targets = np.array([10, 11, 9, 10, 30, 31, 29, 30], dtype=float)
        estimator = LookupCostEstimator(min_group=3).fit(rows, targets)
        # exact bucket hit -> bucket median
        self.assertAlmostEqual(estimator.predict([_row(100, 8, 8, "m1", "b1")])[0], 10.0)
        # unseen config -> graceful global-median fallback (not crash, not zero)
        fallback = estimator.predict([_row(999, 999, 999, "unknown", "zzz")])[0]
        self.assertGreater(fallback, 0.0)

    def test_hybrid_handles_signed_residual(self) -> None:
        rows = [_row(100 * i, 10, i) for i in range(1, 30)]
        targets = np.array([5.0 + 0.5 * i for i in range(1, 30)])
        features = np.arange(29 * 3, dtype=float).reshape(29, 3)
        # must not raise despite residuals going negative
        estimator = HybridCostEstimator(alpha=1.0).fit(rows, features, targets)
        pred = estimator.predict(rows, features)
        self.assertEqual(len(pred), len(targets))
        self.assertTrue(np.all(np.isfinite(pred)))


if __name__ == "__main__":
    unittest.main()
