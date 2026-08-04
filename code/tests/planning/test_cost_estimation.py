from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.planning.costs.regression import (  # noqa: E402
    FEATURE_NAMES,
    RidgeCostEstimator,
    grouped_train_test_split,
    regression_metrics,
    selection_metrics,
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
        self.assertTrue(np.isfinite(metrics.q_error_p99))

    def test_selection_metrics_reports_pick_rate_and_regret(self) -> None:
        metrics = selection_metrics(
            np.asarray([10.0, 20.0, 30.0, 15.0]),
            np.asarray([11.0, 9.0, 29.0, 16.0]),
            ["workload-a", "workload-a", "workload-b", "workload-b"],
            ["plan-1", "plan-2", "plan-1", "plan-2"],
        )

        self.assertEqual(metrics["decision_contexts_evaluated"], 2)
        self.assertEqual(metrics["pick_rate"], 0.5)
        self.assertEqual(metrics["performance_regression_count"], 1)
        self.assertGreater(metrics["decision_regret_pct"], 0.0)

    def test_selection_ties_are_independent_of_input_row_order(self) -> None:
        first = selection_metrics(
            np.asarray([20.0, 10.0]),
            np.asarray([5.0, 5.0]),
            ["workload", "workload"],
            ["candidate-b", "candidate-a"],
        )
        second = selection_metrics(
            np.asarray([10.0, 20.0]),
            np.asarray([5.0, 5.0]),
            ["workload", "workload"],
            ["candidate-a", "candidate-b"],
        )

        self.assertEqual(first["decision_regret_pct"], 0.0)
        self.assertEqual(first["decision_regret_pct"], second["decision_regret_pct"])
        self.assertEqual(first["predicted_best_tie_contexts"], 1)
        self.assertIn("candidate_id", str(first["tie_policy"]))

    def test_feature_schema_excludes_post_execution_measurements(self) -> None:
        forbidden = ("actual", "e2e", "service_s", "vllm", "energy", "mfu")

        for feature in FEATURE_NAMES:
            self.assertFalse(
                any(marker in feature for marker in forbidden),
                feature,
            )


if __name__ == "__main__":
    unittest.main()
