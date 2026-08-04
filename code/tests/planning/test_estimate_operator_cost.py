from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
SCRIPTS_ROOT = CODE_ROOT / "scripts"
for path in (CODE_ROOT, SCRIPTS_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from scripts.analysis.estimate_operator_cost import (  # noqa: E402
    decision_context_payload,
    feature_vector,
    scenario_group,
)


def profile_row() -> dict[str, str]:
    return {
        "model_id": "qwen2.5-1.5b",
        "source_workload_name": "sharegpt",
        "batching_policy": "token_budget",
        "flush_policy": "fixed_timeout",
        "total_rows": "512",
        "token_count": "10000",
        "completion_max_tokens": "16",
        "token_budget": "6144",
        "packing_batch_count": "12",
        "batch_estimated_cost_units_p50": "500",
        "batch_estimated_cost_units_p95": "600",
        "batch_estimated_cost_units_max": "700",
        "max_inflight_limit": "8",
        "flush_timeout_ms": "50",
        "flush_max_wait_ms": "50",
        "arrival_time_scale": "0.0005",
        "arrival_replay": "true",
        "server_version": "18.4",
        "pgvector_version": "0.8.2",
        "model_backend": "compatible_http",
        "model_endpoint_url": "http://localhost:8000/v1/completions",
        "gpu_name": "NVIDIA GeForce RTX 5070",
        "gpu_memory_total_mib": "12227",
        "gpu_peak_tflops": "61.7",
        "endpoint_gpu_ids": "0",
    }


class EstimateOperatorCostTests(unittest.TestCase):
    def test_feature_vector_has_stable_numeric_schema(self) -> None:
        values = feature_vector(profile_row())

        self.assertEqual(len(values), 23)
        self.assertEqual(values[0], 512.0)
        self.assertEqual(values[12:15], [1.0, 0.0, 0.0])
        self.assertEqual(values[-4:], [1.0, 0.0, 61.7, 12227.0])

    def test_scenario_group_ignores_run_identity(self) -> None:
        first = profile_row()
        second = profile_row()
        first.update({"experiment_id": "a", "repeat_index": "1"})
        second.update({"experiment_id": "b", "repeat_index": "9"})

        self.assertEqual(scenario_group(first), scenario_group(second))

    def test_context_separates_hardware_and_normalizes_duplicate_gpu_names(self) -> None:
        one_gpu = profile_row()
        two_gpu = profile_row()
        two_gpu.update(
            {
                "gpu_name": "NVIDIA GeForce RTX 5070;NVIDIA GeForce RTX 5070",
                "gpu_memory_total_mib": "24454",
                "endpoint_count": "2",
                "endpoint_gpu_ids": "0;1",
            }
        )
        different_gpu = profile_row()
        different_gpu["gpu_name"] = "NVIDIA GeForce RTX 4090"

        self.assertEqual(
            decision_context_payload(one_gpu),
            decision_context_payload(two_gpu),
        )
        self.assertNotEqual(
            decision_context_payload(one_gpu),
            decision_context_payload(different_gpu),
        )

    def test_candidate_knobs_are_visible_to_feature_vector(self) -> None:
        row = profile_row()
        row.update(
            {
                "max_active_work_per_endpoint": "65536",
                "per_endpoint_inflight_limit": "16",
                "actor_workers_per_endpoint": "4",
                "ray_actor_max_concurrency": "64",
                "service_quantum_tokens": "2048",
            }
        )

        values = feature_vector(row)

        self.assertEqual(values[15:21], [65536.0, 16.0, 4.0, 64.0, 1.0, 2048.0])


if __name__ == "__main__":
    unittest.main()
