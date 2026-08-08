#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.analysis.summarize_text_native_multijob import summarize


class NativeMultiJobSummaryTests(unittest.TestCase):
    def _matrix(self, root: Path, *, bad_successes: bool = False) -> Path:
        runs = []
        ordinal = 0
        for phase, repeats in (("warmup", 1), ("formal", 3)):
            for repeat in range(1, repeats + 1):
                for arm in ("daft_native", "daft_ray", "ray_data_http"):
                    ordinal += 1
                    run_id = f"{ordinal:03d}_{phase}_{repeat:02d}_{arm}"
                    run_root = root / "runs" / run_id
                    run_root.mkdir(parents=True)
                    (run_root / "service_counters.json").write_text(
                        json.dumps(
                            {
                                "delta": {
                                    "0": {"prompt_tokens": 300000, "generation_tokens": 100000},
                                    "1": {"prompt_tokens": 300000, "generation_tokens": 100000},
                                }
                            }
                        )
                    )
                    success = 511 if bad_successes and ordinal == 1 else 512
                    jobs = []
                    for job_id, start, end, sha in (
                        ("short", 100.0, 110.0, "short-sha"),
                        ("long", 115.0, 170.0, "long-sha"),
                    ):
                        jobs.append(
                            {
                                "job_id": job_id,
                                "status": "passed",
                                "exactly_once": True,
                                "completed_count": 512,
                                "actual_launch_epoch_s": start,
                                "ended_epoch_s": end,
                                "job_barrier_jct_s": end - start,
                                "manifest_sha256": sha,
                            }
                        )
                    runs.append(
                        {
                            "run_id": run_id,
                            "phase": phase,
                            "repeat": repeat,
                            "arm_id": arm,
                            "adapter": arm,
                            "status": "passed",
                            "exactly_once": True,
                            "comparison_eligible": phase == "formal",
                            "arm_barrier_jct_s": 70.0,
                            "jobs": jobs,
                            "gpu_summary": {
                                "gpu0_util_mean": 90.0,
                                "gpu1_util_mean": 92.0,
                                "gpu0_power_mean": 300.0,
                                "gpu1_power_mean": 310.0,
                            },
                            "gauge_summary": {
                                "vllm_running_mean": 100.0,
                                "vllm_running_max": 200.0,
                                "vllm_waiting_mean": 20.0,
                                "vllm_waiting_max": 50.0,
                                "vllm_kv_cache_usage_mean": 0.5,
                                "vllm_kv_cache_usage_max": 0.9,
                            },
                            "vllm_latency_deltas": {
                                "0": {
                                    "vllm_estimated_flops_per_gpu_delta": 1e15,
                                    "vllm_request_success_delta": success,
                                },
                                "1": {
                                    "vllm_estimated_flops_per_gpu_delta": 1e15,
                                    "vllm_request_success_delta": 512,
                                },
                            },
                        }
                    )
        (root / "matrix_index.json").write_text(
            json.dumps(
                {
                    "status": "passed",
                    "comparison_admission": "admissible",
                    "repository_commit": "abc123",
                    "arms": [{"id": arm} for arm in ("daft_native", "daft_ray", "ray_data_http")],
                    "runs": runs,
                }
            )
        )
        return root

    def test_uses_service_counters_and_writes_formal_summary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self._matrix(Path(directory) / "matrix")
            output = Path(directory) / "summary"
            self.assertTrue(summarize(root, output, gpu_peak_tflops=165.0))
            audit = json.loads((output / "audit.json").read_text())
            self.assertEqual(audit["status"], "passed")
            with (output / "all_runs.csv").open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 12)
            self.assertAlmostEqual(float(rows[0]["service_tokens_per_s"]), 800000 / 70)
            self.assertEqual(float(rows[0]["job_overlap_s"]), 0.0)
            with (output / "formal_summary.csv").open(newline="") as handle:
                summaries = list(csv.DictReader(handle))
            self.assertEqual(len(summaries), 3)
            self.assertTrue(all(int(row["formal_repeats"]) == 3 for row in summaries))

    def test_fails_closed_on_service_success_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self._matrix(Path(directory) / "matrix", bad_successes=True)
            output = Path(directory) / "summary"
            self.assertFalse(summarize(root, output, gpu_peak_tflops=165.0))
            audit = json.loads((output / "audit.json").read_text())
            self.assertEqual(audit["status"], "failed")
            self.assertTrue(any("service success delta" in item for item in audit["errors"]))


if __name__ == "__main__":
    unittest.main()
