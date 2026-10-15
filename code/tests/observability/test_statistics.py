from __future__ import annotations

import json
import math
import statistics
import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.analysis.summarize_formal_repeats import summarize  # noqa: E402
from src.observability.metrics import repeat_summary  # noqa: E402
from src.observability.metrics.statistics import _student_t_critical_95  # noqa: E402


class StudentTCriticalTests(unittest.TestCase):
    def test_critical_values_cover_reference_quantiles_at_bucket_boundaries(self) -> None:
        # t.ppf(0.975, df), independently checked with 45-digit incomplete-beta
        # inversion. No numerical package is required to run these regressions.
        references = {
            1: 12.706204736174705,
            2: 4.302652729749464,
            3: 3.1824463052837095,
            4: 2.7764451051977943,
            5: 2.5705818356363155,
            6: 2.44691185114497,
            7: 2.3646242515927853,
            8: 2.3060041352041667,
            9: 2.2621571627982053,
            10: 2.228138851986275,
            11: 2.2009851600916397,
            12: 2.178812829667229,
            13: 2.1603686564627926,
            14: 2.144786687917804,
            15: 2.1314495455597755,
            16: 2.1199052992212546,
            17: 2.109815577833317,
            18: 2.1009220402410387,
            19: 2.0930240544083096,
            20: 2.085963447265865,
            21: 2.0796138447276804,
            22: 2.0738730679040263,
            23: 2.0686576104190486,
            24: 2.063898561628026,
            25: 2.0595385527532977,
            26: 2.055529438642873,
            27: 2.0518305164802855,
            28: 2.048407141795245,
            29: 2.0452296421327043,
            30: 2.042272456301238,
            31: 2.0395134463964086,
            39: 2.0226909200367613,
            40: 2.0210753903062733,
            41: 2.0195409704413763,
            59: 2.000995378088268,
            60: 2.0002978220142604,
            61: 1.9996235849949398,
            119: 1.98009987645694,
            120: 1.979930405082441,
            121: 1.979763762505387,
            999: 1.96234146113345,
            1000: 1.9623390808264085,
            1001: 1.96233670528088,
            9999: 1.9602012636213577,
            10000: 1.9602012398906263,
            10001: 1.9602012161646412,
            999999: 1.9599663568164793,
            1000000: 1.959966356814107,
            1000001: 1.9599663568117347,
        }
        for df, reference in references.items():
            with self.subTest(df=df):
                critical = _student_t_critical_95(df)
                self.assertGreaterEqual(critical, reference)
                self.assertLess(critical, reference * 1.011)
                if df <= 30:
                    self.assertLess(critical - reference, 1e-6)

    def test_critical_values_decrease_to_a_conservative_finite_limit(self) -> None:
        dfs = [*range(1, 1001), 10000, 1000000, 10**100]
        criticals = [_student_t_critical_95(df) for df in dfs]
        self.assertTrue(all(math.isfinite(value) for value in criticals))
        self.assertTrue(all(a >= b for a, b in zip(criticals, criticals[1:])))
        self.assertGreater(criticals[-1], statistics.NormalDist().inv_cdf(0.975))
        self.assertLess(criticals[-1], 1.959968)

    def test_zero_df_is_the_single_repeat_sentinel(self) -> None:
        self.assertEqual(_student_t_critical_95(0), 0.0)

    def test_negative_and_non_integer_df_are_rejected(self) -> None:
        for df in (-1, -100):
            with self.subTest(df=df), self.assertRaises(ValueError):
                _student_t_critical_95(df)
        for df in (True, False, 1.0, 1.5, math.nan, math.inf, "11", None):
            with self.subTest(df=df), self.assertRaises(TypeError):
                _student_t_critical_95(df)


class RepeatSummaryTests(unittest.TestCase):
    def test_mean_intervals_cover_small_and_large_repeat_counts(self) -> None:
        for n, reference in (
            (2, 12.706204736174705),
            (3, 4.302652729749464),
            (12, 2.2009851600916397),
            (32, 2.0395134463964086),
            (10001, 1.9602012398906263),
        ):
            with self.subTest(n=n):
                values = [float(index) for index in range(n)]
                summary = repeat_summary(values)
                expected_mean = (n - 1) / 2
                # Analytic sample variance of the integers 0, ..., n-1.
                expected_sd = math.sqrt(n * (n + 1) / 12)
                reference_margin = reference * expected_sd / math.sqrt(n)
                actual_margin = float(summary["ci95_upper"]) - expected_mean
                self.assertEqual(summary["status"], "ok")
                self.assertEqual(summary["count"], n)
                self.assertEqual(summary["mean"], expected_mean)
                self.assertAlmostEqual(summary["standard_deviation"], expected_sd)
                self.assertGreaterEqual(actual_margin, reference_margin)
                self.assertLess(actual_margin, reference_margin * 1.002)
                self.assertAlmostEqual(summary["ci95_lower"], expected_mean - actual_margin)

    def test_empty_single_and_constant_repeats_retain_their_status(self) -> None:
        self.assertEqual(repeat_summary([])["status"], "unavailable:no_formal_repeats")
        single = repeat_summary([7.0])
        self.assertEqual(single["status"], "single_repeat:no_interval")
        self.assertEqual(single["ci95_lower"], 7.0)
        self.assertEqual(single["ci95_upper"], 7.0)
        constant = repeat_summary([7.0] * 12)
        self.assertEqual(constant["standard_deviation"], 0.0)
        self.assertEqual(constant["ci95_lower"], constant["ci95_upper"])

    def test_large_finite_observations_keep_representable_intervals_serializable(self) -> None:
        values = [0.0] * 6 + [1.6e308] * 6
        rows = [
            {"phase": "formal", "scenario_id": "large", "e2e_s": str(value)}
            for value in values
        ]
        result = summarize(
            rows,
            group_fields=("scenario_id",),
            metrics=("e2e_s",),
            baseline_scenario_id="",
            regression_tolerance_pct=0.0,
        )
        summary = result["repeat_summaries"][0]["metrics"]["e2e_s"]
        json.dumps(result, allow_nan=False)
        # For six zeros and six values a, standard error is a/sqrt(44).
        exact_margin = 2.2009851600916397 * (1.6e308 / math.sqrt(44))
        margin = summary["ci95_upper"] - summary["mean"]
        self.assertEqual(summary["mean"], 8e307)
        self.assertGreaterEqual(margin, exact_margin)
        self.assertLess(margin / exact_margin, 1.000001)
        self.assertGreater(summary["ci95_lower"], 0.0)

    def test_subnormal_observations_keep_a_representable_nonzero_interval(self) -> None:
        smallest = math.ulp(0.0)
        summary = repeat_summary([0.0] * 6 + [2 * smallest] * 6)
        self.assertEqual(summary["standard_deviation"], smallest)
        self.assertEqual(summary["ci95_lower"], 0.0)
        self.assertEqual(summary["ci95_upper"], 2 * smallest)

    def test_non_finite_repeats_are_rejected(self) -> None:
        for invalid in (math.nan, math.inf, -math.inf):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                repeat_summary([1.0, invalid])

    def test_formal_csv_caller_excludes_warmup_and_groups_actual_metric_counts(self) -> None:
        rows = [{"phase": "warmup", "scenario_id": "baseline", "e2e_s": "999999"}]
        rows.extend(
            {
                "phase": "formal",
                "scenario_id": scenario,
                "repeat_index": str(index),
                "e2e_s": str(index + offset),
                "tokens_per_s": str(index) if index <= 12 else "",
            }
            for scenario, offset in (("baseline", 0), ("candidate", 1))
            for index in range(1, 33)
        )
        result = summarize(
            rows,
            group_fields=("scenario_id",),
            metrics=("e2e_s", "tokens_per_s", "unavailable"),
            baseline_scenario_id="baseline",
            regression_tolerance_pct=0.0,
        )
        self.assertEqual(result["formal_rows"], 64)
        baseline, candidate = result["repeat_summaries"]
        self.assertEqual(baseline["metrics"]["e2e_s"], repeat_summary(list(range(1, 33))))
        self.assertEqual(candidate["metrics"]["e2e_s"]["mean"], 17.5)
        for summary in (baseline, candidate):
            self.assertEqual(summary["metrics"]["tokens_per_s"]["count"], 12)
            self.assertEqual(
                summary["metrics"]["unavailable"]["status"], "unavailable:no_formal_repeats"
            )
        e2e_regression = next(
            item for item in result["paired_regressions"] if item["metric"] == "e2e_s"
        )
        self.assertEqual(e2e_regression["paired_repeats"], 32)
        self.assertEqual(e2e_regression["regression_count"], 32)


if __name__ == "__main__":
    unittest.main()
