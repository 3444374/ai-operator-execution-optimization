from __future__ import annotations

import csv
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.summarize_static_credit_workload_surface import (  # noqa: E402
    summarize,
)


class StaticCreditWorkloadSurfaceTests(unittest.TestCase):
    def test_stops_when_one_work_limit_is_robust(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            short = root / "short.csv"
            long = root / "long.csv"
            values = {
                "w32768": (90.0, 9.0, 32768.0, 4.0),
                "w65536": (100.0, 10.0, 65536.0, 3.0),
                "w98304": (98.0, 9.7, 98304.0, 3.5),
            }
            self._write(short, values)
            self._write(long, values)

            result = summarize({"short": short, "long": long})

            self.assertEqual(result["status"], "not_justified")
            self.assertEqual(
                result["decision"], "stop_adaptive_formal_ranking"
            )

    def test_passes_when_work_optima_move_with_cross_regret(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            short = root / "short.csv"
            long = root / "long.csv"
            self._write(
                short,
                {
                    "w32768": (90.0, 8.0, 32768.0, 4.0),
                    "w65536": (98.0, 9.0, 65536.0, 3.0),
                    "w98304": (100.0, 10.0, 98304.0, 3.0),
                },
            )
            self._write(
                long,
                {
                    "w32768": (90.0, 8.0, 32768.0, 4.0),
                    "w65536": (100.0, 10.0, 65536.0, 3.0),
                    "w98304": (85.0, 7.0, 98304.0, 4.5),
                },
            )

            result = summarize({"short": short, "long": long})

            self.assertEqual(result["status"], "passed")
            self.assertEqual(
                result["decision"], "continue_adaptive_experiments"
            )

    def test_stops_when_acceptable_static_regions_overlap(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            short = root / "short.csv"
            long = root / "long.csv"
            self._write(
                short,
                {
                    "w32768": (90.0, 8.0, 32768.0, 4.0),
                    "w65536": (98.0, 9.8, 65536.0, 3.0),
                    "w98304": (100.0, 10.0, 98304.0, 3.0),
                },
            )
            self._write(
                long,
                {
                    "w32768": (90.0, 8.0, 32768.0, 4.0),
                    "w65536": (100.0, 10.0, 65536.0, 3.0),
                    "w98304": (96.0, 9.8, 98304.0, 3.0),
                },
            )

            result = summarize({"short": short, "long": long})

            self.assertEqual(result["status"], "not_justified")
            self.assertFalse(result["pairs"][0]["acceptable_sets_disjoint"])

    def test_rejects_unstable_equivalent_no_pressure_arms(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            short = root / "short.csv"
            long = root / "long.csv"
            self._write(
                short,
                {
                    "k256": (60.0, 6.0, 512.0, 0.0),
                    "w32768": (70.0, 7.0, 32768.0, 4.0),
                    "w65536": (100.0, 10.0, 49000.0, 0.0),
                    "w98304": (80.0, 8.0, 49000.0, 0.0),
                },
                total_rows=512,
            )
            self._write(
                long,
                {
                    "k256": (90.0, 9.0, 325.0, 0.0),
                    "w32768": (90.0, 8.0, 32768.0, 4.0),
                    "w65536": (100.0, 10.0, 65536.0, 3.0),
                    "w98304": (85.0, 7.0, 98304.0, 4.5),
                },
                total_rows=325,
            )

            result = summarize({"short": short, "long": long})

            self.assertEqual(result["status"], "inconclusive")
            self.assertTrue(
                any(
                    item["kind"] == "equivalent_arm_instability"
                    for item in result["audit_failures"]
                )
            )

    @staticmethod
    def _write(
        path: Path,
        values: dict[str, tuple[float, float, float, float]],
        *,
        total_rows: int = 2048,
    ) -> None:
        fields = [
            "status",
            "phase",
            "repeat_index",
            "scenario_id",
            "actor_worker_failures",
            "server_version",
            "pgvector_version",
            "endpoint_count",
            "total_rows",
            "model_request_tokens_per_s",
            "request_slo_goodput_per_s",
            "request_slo_violation_ratio",
            "e2e_s",
            "request_e2e_s_p95",
            "request_e2e_s_p99",
            "vllm_running_mean",
            "vllm_waiting_mean",
            "vllm_kv_cache_usage_mean",
            "mfu_estimate",
            "max_inflight_seen",
            "max_active_work_per_endpoint_seen",
            "bounded_wait_s",
            "request_actual_output_tokens_observed",
        ]
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=fields, lineterminator="\n"
            )
            writer.writeheader()
            for scenario_id, metrics in values.items():
                control = scenario_id[0]
                limit = int(scenario_id[1:])
                for repeat in range(1, 4):
                    writer.writerow(
                        {
                            "status": "ok",
                            "phase": "formal",
                            "repeat_index": repeat,
                            "scenario_id": scenario_id,
                            "actor_worker_failures": "0;0",
                            "server_version": "18.4",
                            "pgvector_version": "0.8.0",
                            "endpoint_count": 2,
                            "total_rows": total_rows,
                            "model_request_tokens_per_s": metrics[0],
                            "request_slo_goodput_per_s": metrics[1],
                            "request_slo_violation_ratio": 0,
                            "e2e_s": 10,
                            "request_e2e_s_p95": 10,
                            "request_e2e_s_p99": 10,
                            "vllm_running_mean": 10,
                            "vllm_waiting_mean": 0,
                            "vllm_kv_cache_usage_mean": 0.1,
                            "mfu_estimate": 0.2,
                            "max_inflight_seen": (
                                metrics[2]
                                if control == "k"
                                else 128
                            ),
                            "max_active_work_per_endpoint_seen": (
                                metrics[2]
                                if control == "w"
                                else limit * 100
                            ),
                            "bounded_wait_s": metrics[3],
                            "request_actual_output_tokens_observed": (
                                total_rows
                            ),
                        }
                    )


if __name__ == "__main__":
    unittest.main()
