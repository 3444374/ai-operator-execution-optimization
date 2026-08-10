from __future__ import annotations

import csv
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
SCRIPT = CODE_ROOT / "scripts/analysis/summarize_vtc_compatible.py"
SPEC = importlib.util.spec_from_file_location("summarize_vtc_compatible", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
summarize = MODULE.summarize


class SummarizeVtcCompatibleTests(unittest.TestCase):
    @staticmethod
    def _row(scenario: str, phase: str, repeat: int) -> dict[str, object]:
        count = 1 if scenario.startswith("solo_") else 2
        values = [1000.0 + index * 100.0 for index in range(count)]
        rows = [10.0 + index for index in range(count)]
        return {
            "scenario_id": scenario,
            "phase": phase,
            "repeat_index": repeat,
            "policy": "independent_full" if count == 1 else scenario,
            "tokens_per_s": 100.0,
            "job_actual_work": json.dumps(values),
            "job_jct_s": json.dumps(rows),
            "job_p99_s": json.dumps([2.0] * count),
            "job_slo_violation_ratio": json.dumps([0.0] * count),
            "job_failed_rows": json.dumps([0] * count),
            "job_arrived_rows": json.dumps([8] * count),
            "job_completed_rows": json.dumps([8] * count),
            "vllm_time_to_first_token_p99_s": 0.5,
            "max_overlap_normalized_service_disparity_ratio": 0.1,
            "credit_endpoint_idle_sample_fraction": 0.1,
            "credit_borrowed_work_mean": 50.0,
            "gpu_utilization_pct_mean": 90.0,
            "vllm_running_mean": 8.0,
            "vllm_waiting_mean": 1.0,
            "vllm_kv_usage_mean": 0.5,
        }

    def test_complete_on_off_matrix_passes(self) -> None:
        scenarios = (
            "solo_client_0_full_pool",
            "solo_client_1_full_pool",
            "on_off_static_partition",
            "on_off_shared_fcfs_control",
            "on_off_shared_work",
        )
        rows = []
        for scenario in scenarios:
            rows.append(self._row(scenario, "warmup", 0))
            rows.extend(self._row(scenario, "formal", index) for index in range(3))
        with TemporaryDirectory() as temporary:
            root = Path(temporary) / "matrix"
            output = Path(temporary) / "summary"
            root.mkdir()
            (root / "manifest.json").write_text(
                json.dumps({"status": "completed", "incidents": []}),
                encoding="utf-8",
            )
            with (root / "group_runs.csv").open(
                "w", newline="", encoding="utf-8"
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            summarize(root, output, "on_off_overload")

            validation = json.loads(
                (output / "validation.json").read_text(encoding="utf-8")
            )
            with (output / "summary.csv").open(
                newline="", encoding="utf-8"
            ) as handle:
                summary_rows = list(csv.DictReader(handle))
        self.assertEqual(validation["status"], "passed")
        self.assertEqual(len(summary_rows), 3)


if __name__ == "__main__":
    unittest.main()
