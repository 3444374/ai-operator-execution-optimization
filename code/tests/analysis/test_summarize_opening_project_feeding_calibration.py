"""Tests for the opening project feeding calibration audit and selector."""

from __future__ import annotations

import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


CODE_ROOT = next(p for p in Path(__file__).resolve().parents if (p / "src").is_dir())
SCRIPT = CODE_ROOT / "scripts" / "analysis" / "summarize_opening_project_feeding_calibration.py"
SPEC = importlib.util.spec_from_file_location("summarize_opening_project_feeding_calibration", SCRIPT)
mod = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class FeedingCalibrationSummaryTests(unittest.TestCase):
    ROWS = 100
    SHA = "a" * 64

    def _direct(self, scale: Path, rep: int, rate: float) -> dict:
        cell = scale / f"bounded_http_c32_rep{rep}"
        gate_dir = cell / "gate_output" / "bounded_http_c32"
        total_tokens = 10_000
        jct = total_tokens / rate
        for shard_id in (0, 1):
            shard_dir = gate_dir / f"shard_{shard_id}"
            shard_dir.mkdir(parents=True)
            summary = {
                "status": "completed",
                "exactly_once": True,
                "failed_count": 0,
                "worker_failures": 0,
                "vllm_num_requests_running_final": 0,
                "vllm_num_requests_waiting_final": 0,
                "service_total_tokens_delta": total_tokens // 2,
                "jct_s": jct,
                "completed_count": self.ROWS // 2,
            }
            (shard_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
            (shard_dir / "manifest_metadata.json").write_text(
                json.dumps({"row_count": self.ROWS, "sha256": self.SHA}), encoding="utf-8"
            )
        (gate_dir / "gate.json").write_text(
            json.dumps({"status": "passed", "passed": True, "metrics": {"group_service_total_tokens_per_s": rate}}),
            encoding="utf-8",
        )
        return {"arm": "bounded_http", "concurrency": 32, "rep": rep, "status": "passed", "cell": str(cell)}

    def _project(self, scale: Path, k: int, rep: int, rate: float, *, manifest_sha: str | None = None) -> dict:
        cell = scale / f"project_static_K{k}_rep{rep}"
        cell.mkdir(parents=True)
        row = {
            "status": "ok",
            "request_manifest_validation_status": "ok",
            "resource_metrics_status": "ok",
            "total_rows": self.ROWS,
            "written_rows": self.ROWS,
            "object_count": self.ROWS,
            "request_manifest_rows": self.ROWS,
            "request_manifest_validated_rows": self.ROWS,
            "vllm_request_success_delta": self.ROWS,
            "endpoint_count": 2,
            "vllm_num_requests_running_after": 0,
            "vllm_num_requests_waiting_after": 0,
            "actor_worker_failures": "0;0",
            "model_request_tokens_per_s": rate,
            "request_manifest_sha256": manifest_sha or self.SHA,
            "model_name": "qwen2.5-7b",
            "completion_protocol": "chat_completions",
            "service_prefix_caching": "enabled",
            "token_budget": 6144,
            "gpu_utilization_pct_mean": 90,
            "vllm_running_mean": k,
            "vllm_running_p95": k,
            "vllm_running_max": k,
            "vllm_waiting_mean": 0,
            "vllm_waiting_max": 0,
            "vllm_kv_cache_usage_mean": 0.1,
            "vllm_kv_cache_usage_max": 0.2,
            "max_active_work_per_endpoint": 65536,
            "max_active_work_per_endpoint_seen": 65500,
            "actor_workers_per_endpoint": 8,
            "ray_actor_max_concurrency": 32,
            "per_endpoint_inflight_limit": k,
        }
        with (cell / "project_static_summary.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row))
            writer.writeheader()
            writer.writerow(row)
        return {"arm": "project_static", "concurrency": k, "rep": rep, "status": "passed", "cell": str(cell)}

    def _tree(self, root: Path, *, bad_sha: bool = False) -> None:
        scale = root / f"scale_{self.ROWS}"
        records = [self._direct(scale, rep, rate) for rep, rate in enumerate((1000, 1010, 990), 1)]
        rates = {32: (850, 860, 855), 64: (940, 945, 950), 128: (980, 985, 990), 256: (1000, 1010, 1005)}
        for k, values in rates.items():
            for rep, rate in enumerate(values, 1):
                sha = "b" * 64 if bad_sha and k == 256 and rep == 3 else None
                records.append(self._project(scale, k, rep, rate, manifest_sha=sha))
        (root / "ramp_run.json").write_text(
            json.dumps({"experiment_id": "test", "records": records, "n_passed": 15, "n_failed": 0}),
            encoding="utf-8",
        )

    def test_selects_smallest_k_meeting_direct_and_tested_peak_floors(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._tree(root)
            result = mod.summarize(root, rows=self.ROWS)
        self.assertEqual(result["status"], "selected")
        self.assertEqual(result["selected_k_per_endpoint"], 128)
        self.assertTrue(result["audit"]["passed"])
        self.assertAlmostEqual(result["direct_control"]["median_tokens_per_s"], 1000.0)

    def test_manifest_mismatch_fails_closed_without_selection(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._tree(root, bad_sha=True)
            result = mod.summarize(root, rows=self.ROWS)
        self.assertEqual(result["status"], "audit_failed")
        self.assertIsNone(result["selected_k_per_endpoint"])
        self.assertTrue(any("manifest SHA mismatch" in error for error in result["audit"]["errors"]))


if __name__ == "__main__":
    unittest.main()
