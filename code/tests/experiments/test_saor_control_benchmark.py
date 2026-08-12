from __future__ import annotations

import unittest

from src.experiments.saor.control_benchmark import run_control_benchmark


class SaorControlBenchmarkTest(unittest.TestCase):
    def test_reports_all_control_paths_for_each_job_count(self) -> None:
        rows = run_control_benchmark(
            job_counts=(1, 4),
            iterations=8,
            warmup_iterations=2,
            repeats=2,
        )

        self.assertEqual(len(rows), 14)
        self.assertEqual(
            {row.policy for row in rows},
            {
                "static",
                "legacy_threshold",
                "shared_drr",
                "shared_fifo",
                "external_vtc",
                "saor_release",
                "saor_dpp_oracle",
            },
        )
        self.assertEqual({row.job_count for row in rows}, {1, 4})
        for row in rows:
            self.assertEqual(row.iterations, 16)
            self.assertGreaterEqual(row.latency_us_p50, 0.0)
            self.assertGreaterEqual(row.latency_us_p95, row.latency_us_p50)
            self.assertGreater(row.operations_per_s, 0.0)
            self.assertEqual(row.claim_scope, "cpu_control_path_only")

    def test_rejects_invalid_benchmark_shape(self) -> None:
        with self.assertRaisesRegex(ValueError, "job_counts"):
            run_control_benchmark(
                job_counts=(),
                iterations=1,
                warmup_iterations=0,
                repeats=1,
            )


if __name__ == "__main__":
    unittest.main()
