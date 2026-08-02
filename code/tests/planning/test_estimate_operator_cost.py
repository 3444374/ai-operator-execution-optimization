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

from scripts.analysis.estimate_operator_cost import feature_vector, scenario_group  # noqa: E402


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
    }


class EstimateOperatorCostTests(unittest.TestCase):
    def test_feature_vector_has_stable_numeric_schema(self) -> None:
        values = feature_vector(profile_row())

        self.assertEqual(len(values), 15)
        self.assertEqual(values[0], 512.0)
        self.assertEqual(values[-3:], [1.0, 0.0, 0.0])

    def test_scenario_group_ignores_run_identity(self) -> None:
        first = profile_row()
        second = profile_row()
        first.update({"experiment_id": "a", "repeat_index": "1"})
        second.update({"experiment_id": "b", "repeat_index": "9"})

        self.assertEqual(scenario_group(first), scenario_group(second))


if __name__ == "__main__":
    unittest.main()
