from __future__ import annotations

import csv
import hashlib
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import numpy as np


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.analysis import compare_cost_estimators_contextloo as MODULE  # noqa: E402
from tests.planning.test_estimate_operator_cost import profile_row  # noqa: E402

SCRIPT = CODE_ROOT / "scripts" / "analysis" / "compare_cost_estimators_contextloo.py"


class CostContextLooTests(unittest.TestCase):
    def test_comparison_scripts_require_one_explicit_source(self) -> None:
        from scripts.analysis import compare_cost_estimators_full

        for module in (MODULE, compare_cost_estimators_full):
            output = ["--output", "selected-output.json"] if module is MODULE else []
            for arguments in ([], ["--data-csv", "chosen.csv", "--ref-json", "chosen.json"]):
                with self.subTest(script=module.__file__, arguments=arguments):
                    with patch.object(sys, "argv", [module.__file__, *output, *arguments]), redirect_stderr(io.StringIO()):
                        with self.assertRaises(SystemExit) as error:
                            module.parse_args()
                    self.assertEqual(error.exception.code, 2)
        with patch.object(sys, "argv", [str(SCRIPT), "--data-csv", "chosen.csv"]), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                MODULE.parse_args()
        self.assertEqual(error.exception.code, 2)

    def test_external_csv_writes_json_with_actual_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "runs.csv"
            rows = []
            for total_rows in ("512", "1024"):
                for token_budget, cost in (("6144", "10"), ("8192", "12")):
                    row = profile_row()
                    row.update(
                        status="ok", phase="formal", total_rows=total_rows,
                        token_budget=token_budget, e2e_s=cost,
                    )
                    rows.append(row)
            with csv_path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
            output = Path(directory) / "result.json"
            arguments = [str(SCRIPT), "--data-csv", str(csv_path), "--output", str(output)]
            with patch.object(sys, "argv", arguments), redirect_stdout(io.StringIO()):
                with (
                    patch.object(MODULE, "ESTIMATORS", ("CE0_mean",)),
                    patch.object(MODULE.platform, "platform", return_value="cpu-test-fixture"),
                ):
                    MODULE.main()
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(payload["source_reference"], str(csv_path.resolve()))
            self.assertEqual(payload["source_csvs"], [{
                "path": str(csv_path.resolve()),
                "sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
            }])
            self.assertEqual(payload["dataset_coverage"]["row_count"], 4)
            self.assertEqual(payload["estimators"]["CE0_mean"]["summary"]["fold_count"], 2)
            sources = {
                "context_loo_script_sha256": SCRIPT,
                "profiles_module_sha256": CODE_ROOT / "src/experiments/operator_costs/profiles.py",
                "comparison_module_sha256": CODE_ROOT / "src/experiments/operator_costs/comparison.py",
                "profile_estimator_module_sha256": CODE_ROOT / "src/experiments/operator_costs/estimators.py",
                "estimator_module_sha256": CODE_ROOT / "src/planning/costs/estimators.py",
                "metric_module_sha256": CODE_ROOT / "src/planning/costs/regression.py",
            }
            for key, path in sources.items():
                self.assertEqual(payload["code_evidence"][key], hashlib.sha256(path.read_bytes()).hexdigest(), key)

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

    def test_promotion_contract_passes_only_when_all_four_gates_hold(self) -> None:
        # plan §6: candidate pairwise >= 0.75 AND median<=5 AND macro-mean<=5 AND max<=15.
        contract = MODULE._promotion_contract(
            {"decision_regret_pct": [0.0, 3.0, 4.0], "candidate_pairwise_accuracy": [0.8, 0.8, 0.8]}
        )
        self.assertTrue(contract["regret_median_pass"])
        self.assertTrue(contract["regret_macro_mean_pass"])
        self.assertTrue(contract["regret_max_pass"])
        self.assertTrue(contract["pairwise_pass"])
        self.assertTrue(contract["passed"])

    def test_promotion_contract_fails_on_macro_mean_regret_even_if_median_passes(self) -> None:
        # The audit F1 bug: median 5% passes but macro-mean 5.25% fails -> contract must fail,
        # even though max (6%) is under 15%. (regret_pass needs median AND macro AND max.)
        contract = MODULE._promotion_contract(
            {"decision_regret_pct": [5.0, 5.0, 5.0, 6.0], "candidate_pairwise_accuracy": [0.9, 0.9, 0.9, 0.9]}
        )
        self.assertTrue(contract["regret_median_pass"])
        self.assertFalse(contract["regret_macro_mean_pass"])
        self.assertTrue(contract["regret_max_pass"])
        self.assertFalse(contract["regret_pass"])
        self.assertFalse(contract["passed"])

    def test_promotion_contract_fails_on_max_regret(self) -> None:
        contract = MODULE._promotion_contract(
            {"decision_regret_pct": [0.0, 2.0, 39.77], "candidate_pairwise_accuracy": [0.76, 0.76, 0.76]}
        )
        self.assertFalse(contract["regret_max_pass"])
        self.assertFalse(contract["passed"])

    def test_promotion_contract_uses_candidate_not_row_pairwise(self) -> None:
        # audit F1: candidate pairwise (>=0.75) is the gate; row-level is NOT. candidate 0.758 passes.
        contract = MODULE._promotion_contract(
            {"decision_regret_pct": [0.0, 0.0], "candidate_pairwise_accuracy": [0.758, 0.758]}
        )
        self.assertTrue(contract["pairwise_pass"])
        self.assertTrue(contract["passed"])

    def test_repo_relpath_handles_out_of_repo_inputs(self) -> None:
        # audit F3/F6: an absolute --data-csv outside REPO_ROOT must not crash relative_to.
        in_repo = MODULE.REPO_ROOT / "code" / "scripts" / "x.py"
        expected_in = str(in_repo.resolve().relative_to(MODULE.REPO_ROOT))
        self.assertEqual(MODULE._repo_relpath(in_repo), expected_in)
        outside = MODULE.REPO_ROOT.parent.parent / "external_cost.csv"
        self.assertEqual(MODULE._repo_relpath(outside), str(outside.resolve()))


if __name__ == "__main__":
    unittest.main()
