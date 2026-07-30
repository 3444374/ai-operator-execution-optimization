from __future__ import annotations

import csv
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from scripts.select_strategy_calibration import build_selection  # noqa: E402
from src.calibration import load_calibration_contract  # noqa: E402


class StrategyCalibrationTests(unittest.TestCase):
    def test_selects_measured_32k_point_and_freezes_capacity(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            feeding = root / "feeding.csv"
            budget = root / "budget.csv"
            direct = root / "direct"
            self._write_feeding(feeding, throughput=970.0)
            self._write_budget(budget)
            self._write_direct(direct, throughput=1000.0)

            selection = build_selection(
                feeding_runs=feeding,
                feeding_scenario="fixed16_c16",
                direct_baseline_root=direct,
                direct_cell="bounded_fixed16_c16",
                token_budget_runs=budget,
                minimum_repeats=3,
                minimum_feeding_ratio=0.95,
            )

            self.assertEqual(
                selection["selection"],
                {
                    "best_token_budget": 32768,
                    "project_static_k_per_endpoint": 256,
                    "project_active_work_per_endpoint": 65536,
                    "project_actor_workers_per_endpoint": 1,
                    "project_ray_actor_max_concurrency": 256,
                },
            )

    def test_rejects_underfed_project_path(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            feeding = root / "feeding.csv"
            budget = root / "budget.csv"
            direct = root / "direct"
            self._write_feeding(feeding, throughput=900.0)
            self._write_budget(budget)
            self._write_direct(direct, throughput=1000.0)

            with self.assertRaisesRegex(ValueError, "feeding ratio"):
                build_selection(
                    feeding_runs=feeding,
                    feeding_scenario="fixed16_c16",
                    direct_baseline_root=direct,
                    direct_cell="bounded_fixed16_c16",
                    token_budget_runs=budget,
                    minimum_repeats=3,
                    minimum_feeding_ratio=0.95,
                )

    def test_contract_rejects_changed_runtime_value(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "selection.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "ready",
                        "selection": {"best_token_budget": 32768},
                        "evidence": {
                            "feeding": {"status": "passed"},
                            "token_budget": {"status": "passed"},
                        },
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "selection mismatch"):
                load_calibration_contract(
                    path,
                    {"best_token_budget": 8192},
                )

    @staticmethod
    def _write_feeding(path: Path, *, throughput: float) -> None:
        fields = [
            "status",
            "phase",
            "repeat_index",
            "scenario_id",
            "actor_worker_failures",
            "model_request_tokens_per_s",
        ]
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for repeat in range(1, 4):
                writer.writerow(
                    {
                        "status": "ok",
                        "phase": "formal",
                        "repeat_index": repeat,
                        "scenario_id": "fixed16_c16",
                        "actor_worker_failures": "0;0",
                        "model_request_tokens_per_s": throughput,
                    }
                )

    @staticmethod
    def _write_budget(path: Path) -> None:
        fields = [
            "status",
            "phase",
            "repeat_index",
            "scenario_id",
            "actor_worker_failures",
            "token_budget",
            "model_request_tokens_per_s",
            "per_endpoint_inflight_limit",
            "max_active_work_per_endpoint",
            "actor_workers_per_endpoint",
            "ray_actor_max_concurrency",
        ]
        throughputs = {
            2048: 900.0,
            8192: 950.0,
            32768: 1000.0,
            49152: 940.0,
        }
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for budget, throughput in throughputs.items():
                for repeat in range(1, 4):
                    writer.writerow(
                        {
                            "status": "ok",
                            "phase": "formal",
                            "repeat_index": repeat,
                            "scenario_id": f"tb{budget}",
                            "actor_worker_failures": "0;0",
                            "token_budget": budget,
                            "model_request_tokens_per_s": throughput,
                            "per_endpoint_inflight_limit": 256,
                            "max_active_work_per_endpoint": 65536,
                            "actor_workers_per_endpoint": 1,
                            "ray_actor_max_concurrency": 256,
                        }
                    )

    @staticmethod
    def _write_direct(root: Path, *, throughput: float) -> None:
        cell = root / "bounded_fixed16_c16"
        (cell / "shard_0").mkdir(parents=True)
        (cell / "shard_1").mkdir()
        (cell / "gate.json").write_text(
            json.dumps({"passed": True}),
            encoding="utf-8",
        )
        total_tokens = 10000
        jct_s = total_tokens * 2 / throughput
        for endpoint in (0, 1):
            (cell / f"shard_{endpoint}" / "summary.json").write_text(
                json.dumps(
                    {
                        "status": "completed",
                        "failed_count": 0,
                        "worker_failures": 0,
                        "exactly_once": True,
                        "jct_s": jct_s,
                        "total_tokens": total_tokens,
                    }
                ),
                encoding="utf-8",
            )


if __name__ == "__main__":
    unittest.main()
