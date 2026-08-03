from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.analysis import summarize_output_aware_bfd as summary  # noqa: E402


class OutputAwareSummaryTests(unittest.TestCase):
    def test_default_metrics_cover_performance_slo_packing_and_resources(
        self,
    ) -> None:
        required = {
            "rows_per_s",
            "tokens_per_s",
            "e2e_s",
            "request_e2e_s_p95",
            "request_slo_violation_ratio",
            "request_slo_goodput_per_s",
            "request_actual_output_tokens_p95",
            "request_finish_reason_stop_ratio",
            "request_finish_reason_length_ratio",
            "operator_invocations",
            "packing_batch_count",
            "packing_budget_utilization_mean",
            "batch_rows_mean",
            "batch_tokens_p95",
            "gpu_utilization_pct_mean",
            "gpu_memory_used_mib_max",
            "gpu_power_w_mean",
            "gpu_energy_j",
            "energy_j_per_1k_observed_tokens",
            "vllm_running_mean",
            "vllm_waiting_mean",
            "vllm_kv_cache_usage_mean",
            "mfu_estimate",
        }

        self.assertTrue(required.issubset(summary.DEFAULT_METRICS))

    def test_summarize_rows_uses_only_successful_formal_runs(self) -> None:
        rows = [
            {
                "status": "ok",
                "phase": "formal",
                "scenario_id": "adaptive",
                "tokens_per_s": "100",
                "gpu_utilization_pct_mean": "50",
                "mfu_estimate": "0.2",
            },
            {
                "status": "ok",
                "phase": "formal",
                "scenario_id": "adaptive",
                "tokens_per_s": "120",
                "gpu_utilization_pct_mean": "70",
                "mfu_estimate": "0.4",
            },
            {
                "status": "ok",
                "phase": "warmup",
                "scenario_id": "adaptive",
                "tokens_per_s": "999",
                "gpu_utilization_pct_mean": "99",
                "mfu_estimate": "0.9",
            },
            {
                "status": "failed",
                "phase": "formal",
                "scenario_id": "adaptive",
                "tokens_per_s": "999",
                "gpu_utilization_pct_mean": "99",
                "mfu_estimate": "0.9",
            },
            {
                "status": "ok",
                "phase": "formal",
                "scenario_id": "baseline",
                "tokens_per_s": "80",
                "gpu_utilization_pct_mean": "",
                "mfu_estimate": "",
            },
        ]

        result = summary.summarize_rows(
            rows,
            metric_names=(
                "tokens_per_s",
                "gpu_utilization_pct_mean",
                "mfu_estimate",
            ),
        )

        adaptive_tokens = next(
            item
            for item in result
            if item["scenario_id"] == "adaptive"
            and item["metric"] == "tokens_per_s"
        )
        self.assertEqual(adaptive_tokens["n"], 2)
        self.assertEqual(adaptive_tokens["mean"], 110.0)
        self.assertAlmostEqual(
            adaptive_tokens["sample_std"],
            14.1421356237,
        )
        self.assertEqual(adaptive_tokens["p50"], 100.0)
        self.assertEqual(adaptive_tokens["min"], 100.0)
        self.assertEqual(adaptive_tokens["max"], 120.0)

        baseline_gpu = next(
            item
            for item in result
            if item["scenario_id"] == "baseline"
            and item["metric"] == "gpu_utilization_pct_mean"
        )
        self.assertEqual(baseline_gpu["n"], 0)
        self.assertEqual(baseline_gpu["mean"], "")

    def test_main_writes_long_form_csv(self) -> None:
        test_tmp_root = CODE_ROOT.parent / "tmp"
        test_tmp_root.mkdir(exist_ok=True)
        runs_path = test_tmp_root / "output_aware_runs.csv"
        output_path = test_tmp_root / "output_aware_summary.csv"
        runs_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
        try:
            with runs_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "status",
                        "phase",
                        "scenario_id",
                        *summary.DEFAULT_METRICS,
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "status": "ok",
                        "phase": "formal",
                        "scenario_id": "baseline",
                        **{
                            metric: "1"
                            for metric in summary.DEFAULT_METRICS
                        },
                    }
                )

            exit_code = summary.main(
                [
                    "--runs",
                    str(runs_path),
                    "--output",
                    str(output_path),
                ]
            )

            self.assertEqual(exit_code, 0)
            with output_path.open(
                newline="",
                encoding="utf-8",
            ) as handle:
                written = list(csv.DictReader(handle))
            self.assertEqual(len(written), len(summary.DEFAULT_METRICS))
            self.assertEqual(
                list(written[0]),
                [
                    "scenario_id",
                    "metric",
                    "n",
                    "mean",
                    "sample_std",
                    "p50",
                    "min",
                    "max",
                ],
            )
        finally:
            runs_path.unlink(missing_ok=True)
            output_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
