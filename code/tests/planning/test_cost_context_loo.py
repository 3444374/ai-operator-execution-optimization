from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

import numpy as np


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
SCRIPT = CODE_ROOT / "scripts" / "analysis" / "compare_cost_estimators_contextloo.py"
SPEC = importlib.util.spec_from_file_location("compare_cost_estimators_contextloo", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class CostContextLooTests(unittest.TestCase):
    def test_candidate_repeats_are_averaged_before_ranking(self) -> None:
        aggregated = MODULE.aggregate_candidate_repeats(
            np.asarray([10.0, 12.0, 20.0, 22.0]),
            np.asarray([9.0, 13.0, 18.0, 24.0]),
            ["a", "a", "b", "b"],
        )

        self.assertEqual([item["candidate_id"] for item in aggregated], ["a", "b"])
        self.assertEqual(aggregated[0]["repeat_count"], 2)
        self.assertEqual(aggregated[0]["actual_mean_s"], 11.0)
        self.assertEqual(aggregated[1]["predicted_mean_s"], 21.0)

    def test_summary_reports_mean_median_and_range(self) -> None:
        summary = MODULE.summarize([0.0, 2.0, 10.0])

        self.assertEqual(summary["count"], 3)
        self.assertEqual(summary["mean"], 4.0)
        self.assertEqual(summary["median"], 2.0)
        self.assertEqual(summary["min"], 0.0)
        self.assertEqual(summary["max"], 10.0)

    def test_pooling_fold_selection_preserves_fold_choices(self) -> None:
        folds = [
            {
                "selection": {
                    "decision_contexts_evaluated": 1,
                    "pick_rate": 1.0,
                    "selected_runtime": 10.0,
                    "oracle_runtime": 10.0,
                    "performance_regression_count": 0,
                    "selected_plan_rank_mean": 1.0,
                    "surpassed_plans": 0,
                }
            },
            {
                "selection": {
                    "decision_contexts_evaluated": 1,
                    "pick_rate": 0.0,
                    "selected_runtime": 30.0,
                    "oracle_runtime": 20.0,
                    "performance_regression_count": 1,
                    "selected_plan_rank_mean": 2.0,
                    "surpassed_plans": 1,
                }
            },
        ]

        pooled = MODULE.pool_fold_selection(folds)

        self.assertEqual(pooled["pick_rate"], 0.5)
        self.assertAlmostEqual(pooled["decision_regret_pct"], 100.0 / 3.0)
        self.assertEqual(pooled["performance_regression_count"], 1)


if __name__ == "__main__":
    unittest.main()
