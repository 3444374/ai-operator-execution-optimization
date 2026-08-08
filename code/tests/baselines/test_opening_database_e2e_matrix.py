from __future__ import annotations

import importlib.util
import csv
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

SCRIPT = CODE_ROOT / "scripts" / "baselines" / "opening_database_e2e_matrix.py"
spec = importlib.util.spec_from_file_location("opening_database_e2e_matrix", SCRIPT)
matrix = importlib.util.module_from_spec(spec)
sys.modules["opening_database_e2e_matrix"] = matrix
spec.loader.exec_module(matrix)


class OpeningMatrixPureTests(unittest.TestCase):
    def _calibrated_config(self, directory: str) -> tuple[matrix.Config, matrix.Workload, Path]:
        root = Path(directory)
        manifest = root / "manifest.jsonl"
        manifest.write_text("{}\n", encoding="utf-8")
        contract_path = root / "calibration.json"
        observations = [
            {
                "token_budget": 6144,
                "max_active_work_per_endpoint": 65536,
                "actor_workers_per_endpoint": 8,
                "ray_actor_max_concurrency": 32,
                "per_endpoint_inflight_limit": 128,
            }
            for _ in range(3)
        ]
        contract = {
            "status": "selected",
            "audit": {"passed": True, "errors": []},
            "rows": 2,
            "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
            "selected_k_per_endpoint": 128,
            "thresholds": {
                "expected_repeats": 3,
                "feeding_ratio_to_direct_median_min": 0.95,
                "ratio_to_tested_project_peak_min": 0.97,
            },
            "project_candidates": {
                "128": {
                    "passes_feeding_floor": True,
                    "passes_project_peak_floor": True,
                    "observations": observations,
                }
            },
        }
        contract_path.write_text(json.dumps(contract), encoding="utf-8")
        workload = matrix.Workload(
            label="w",
            name="workload",
            rows=2,
            max_tokens=64,
            manifest=manifest,
            quality="completion_validity",
            project_max_inflight_per_endpoint=128,
            project_active_work_per_endpoint=65536,
            project_actor_workers_per_endpoint=8,
            project_actor_concurrency=32,
            project_calibration_contract=contract_path,
        )
        config = matrix.Config(
            experiment_id="test",
            database_url="postgresql://postgres:postgres@localhost/db",
            endpoint_urls=(
                "http://127.0.0.1:8000/v1/chat/completions",
                "http://127.0.0.1:8001/v1/chat/completions",
            ),
            model="qwen2.5-7b",
            tokenizer="tokenizer",
            output_root=root / "out",
            project_python="python3",
            workloads=(workload,),
        )
        return config, workload, contract_path

    def test_arm_order_is_deterministic_and_complete(self) -> None:
        workload = matrix.Workload(
            label="w",
            name="workload",
            rows=2,
            max_tokens=64,
            manifest=Path("unused"),
            quality="completion_validity",
        )
        first = matrix._arm_order(20260807, workload, "formal", 1)
        second = matrix._arm_order(20260807, workload, "formal", 1)
        self.assertEqual(first, second)
        self.assertEqual(set(first), set(matrix.ARMS))

    def test_pair_digest_is_order_independent_and_content_sensitive(self) -> None:
        self.assertEqual(
            matrix._pairs_digest([(2, "b"), (1, "a")]),
            matrix._pairs_digest([(1, "a"), (2, "b")]),
        )
        self.assertNotEqual(
            matrix._pairs_digest([(1, "a")]),
            matrix._pairs_digest([(1, "changed")]),
        )

    def test_percentile(self) -> None:
        self.assertEqual(matrix._percentile([], 0.5), 0.0)
        self.assertEqual(matrix._percentile([1.0, 3.0], 0.5), 2.0)

    def test_request_csv_preserves_endpoint_zero(self) -> None:
        result = matrix.BaselineRequestResult(
            doc_id=1,
            endpoint_index=0,
            status="completed",
            error=None,
            submitted_at_s=1.0,
            started_at_s=1.1,
            completed_at_s=1.2,
            input_tokens=3,
            output_tokens=1,
            output_text="x",
            finish_reason="stop",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.csv"
            matrix._write_requests(path, (result,))
            with path.open(encoding="utf-8", newline="") as handle:
                row = next(csv.DictReader(handle))
        self.assertEqual(row["endpoint_index"], "0")

    def test_calibrated_project_contract_accepts_matching_frozen_parameters(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config, workload, _ = self._calibrated_config(directory)
            matrix._validate_project_calibration_contract(config, workload)
            frozen = matrix._project_runtime_contract(config, workload)
        self.assertEqual(frozen["max_inflight_per_endpoint"], 128)
        self.assertEqual(frozen["actor_concurrency"], 32)
        self.assertIsNotNone(frozen["calibration_contract_sha256"])

    def test_calibrated_project_contract_rejects_configured_k_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config, workload, contract_path = self._calibrated_config(directory)
            contract = json.loads(contract_path.read_text(encoding="utf-8"))
            contract["selected_k_per_endpoint"] = 256
            contract_path.write_text(json.dumps(contract), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "configured K"):
                matrix._validate_project_calibration_contract(config, workload)


if __name__ == "__main__":
    unittest.main()
