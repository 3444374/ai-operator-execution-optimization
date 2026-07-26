from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.cost_estimation import (  # noqa: E402
    FEATURE_NAMES,
    RidgeCostEstimator,
    grouped_train_test_split,
    regression_metrics,
)


class CostEstimationTests(unittest.TestCase):
    def test_grouped_split_is_deterministic_and_has_no_group_leakage(self) -> None:
        groups = ["a", "a", "b", "b", "c", "c", "d", "d"]

        first = grouped_train_test_split(groups, test_fraction=0.25, seed=7)
        second = grouped_train_test_split(groups, test_fraction=0.25, seed=7)

        self.assertEqual(first, second)
        train_groups = {groups[index] for index in first.train_indices}
        test_groups = {groups[index] for index in first.test_indices}
        self.assertFalse(train_groups & test_groups)
        self.assertTrue(train_groups)
        self.assertTrue(test_groups)

    def test_ridge_estimator_learns_monotonic_cost(self) -> None:
        x = np.asarray([[1.0], [2.0], [3.0], [4.0], [5.0]])
        y = np.expm1(np.asarray([0.2, 0.4, 0.6, 0.8, 1.0]))

        estimator = RidgeCostEstimator(alpha=1e-6).fit(x, y)
        predicted = estimator.predict(np.asarray([[6.0], [7.0]]))

        self.assertGreater(predicted[1], predicted[0])
        self.assertAlmostEqual(predicted[0], np.expm1(1.2), delta=0.1)

    def test_regression_metrics_handle_zero_targets(self) -> None:
        metrics = regression_metrics(
            np.asarray([0.0, 2.0]),
            np.asarray([1.0, 2.0]),
        )

        self.assertEqual(metrics.count, 2)
        self.assertAlmostEqual(metrics.mae, 0.5)
        self.assertTrue(np.isfinite(metrics.mape_pct))

    def test_feature_schema_excludes_post_execution_measurements(self) -> None:
        forbidden = ("actual", "e2e", "service_s", "vllm", "energy", "mfu")

        for feature in FEATURE_NAMES:
            self.assertFalse(
                any(marker in feature for marker in forbidden),
                feature,
            )


if __name__ == "__main__":
    unittest.main()
