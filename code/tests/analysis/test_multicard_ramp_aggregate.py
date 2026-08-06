"""Tests for multicard_ramp_aggregate -- the scale-ramp aggregator.

Covers the three codex-audit fixes: (1) status comes from run_status.json /
run_error.json / ramp_run.json (not shard-file presence -> a failed cell with
leftover summaries is NOT mislabelled passed); (2) rows_per_s = completed
request rows / wall (not output_tokens/wall); (3) multiple reps aggregate to
mean/CV (not silently overwritten).
"""

from __future__ import annotations

import csv
import importlib.util
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

_AGG_PATH = CODE_ROOT / "scripts" / "analysis" / "multicard_ramp_aggregate.py"
_spec = importlib.util.spec_from_file_location("multicard_ramp_aggregate", _AGG_PATH)
agg = importlib.util.module_from_spec(_spec)
sys.modules["multicard_ramp_aggregate"] = agg
_spec.loader.exec_module(agg)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _gate_cell(root: Path, scale: int, arm: str, *, total_tokens: int, jct: float,
               ttft_p50: float, prefix_hit: float, completed: int = 10, rep: int = 1,
               status: str = "passed") -> None:
    cell = root / f"scale_{scale}" / f"{arm}_c32_rep{rep}"
    shard_dir = cell / "gate_output" / f"{arm}_c32"
    for i in (0, 1):
        _write(shard_dir / f"shard_{i}" / "summary.json", json.dumps({
            "service_total_tokens_delta": total_tokens // 2, "jct_s": jct,
            "latency_p50_s": 2.0, "latency_p95_s": 4.0, "latency_p99_s": 5.0,
        }))
        # requests.csv with `completed` completed rows + 1 failed row
        with (shard_dir / f"shard_{i}" / "requests.csv").open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["doc_id", "status"])
            w.writeheader()
            for d in range(completed):
                w.writerow({"doc_id": d, "status": "completed"})
            w.writerow({"doc_id": 999, "status": "failed"})
    _write(cell / "gate_output" / "run_status.json", json.dumps({"status": status}))
    _write(cell / "ttft_metrics.json", json.dumps({
        "0": {"vllm_time_to_first_token_p50_s": ttft_p50, "vllm_prefix_cache_hit_rate": prefix_hit},
        "1": {"vllm_time_to_first_token_p50_s": ttft_p50, "vllm_prefix_cache_hit_rate": prefix_hit},
    }))
    with (cell / "gpu_resource.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["sample_index", "gpu_index", "gpu_utilization_pct", "gpu_power_w"])
        w.writeheader()
        w.writerows([{"sample_index": 0, "gpu_index": "0", "gpu_utilization_pct": 70.0, "gpu_power_w": 250.0},
                     {"sample_index": 0, "gpu_index": "1", "gpu_utilization_pct": 65.0, "gpu_power_w": 240.0}])


class StatusAndRowsTests(unittest.TestCase):
    def test_failed_cell_marked_failed_not_passed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            # gate FAILED: run_status.json status=failed (summaries still present)
            _gate_cell(root, 4096, "duckdb_ai", total_tokens=800000, jct=16.0,
                       ttft_p50=0.05, prefix_hit=0.77, status="failed")
            result = agg.aggregate(root)
        m = result["scale_4096"]["arms"]["duckdb_ai"]
        self.assertEqual(m["status"], "failed")
        self.assertEqual(m["n_passed"], 0)

    def test_rows_per_s_uses_completed_rows_not_output_tokens(self) -> None:
        # 10 completed rows/shard x 2 shards = 20 completed; jct=4.0 -> rows/s=5.0
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _gate_cell(root, 4096, "bounded_http", total_tokens=800000, jct=4.0,
                       ttft_p50=0.05, prefix_hit=0.77, completed=10)
            result = agg.aggregate(root)
        m = result["scale_4096"]["arms"]["bounded_http"]
        self.assertEqual(m["status"], "passed")
        self.assertEqual(m["completed_rows"], 20)
        self.assertAlmostEqual(m["rows_per_s_mean"], 5.0, places=2)
        self.assertAlmostEqual(m["service_tokens_per_s_mean"], 200000.0, delta=1)  # 800000/4.0

    def test_ramp_run_json_overrides_status(self) -> None:
        # The driver's run log is authoritative even if shard files mislead.
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _gate_cell(root, 8192, "duckdb_ai", total_tokens=900000, jct=18.0,
                       ttft_p50=0.05, prefix_hit=0.8, status="passed")  # shard says passed
            _write(root / "ramp_run.json", json.dumps({"records": [
                {"scale": 8192, "arm": "duckdb_ai", "rep": 1, "status": "failed", "error": "cap=64"}]}))
            result = agg.aggregate(root)
        self.assertEqual(result["scale_8192"]["arms"]["duckdb_ai"]["status"], "failed")


class RepeatAggregationTests(unittest.TestCase):
    def test_three_reps_produce_mean_and_cv(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            for rep, tps in enumerate([50000.0, 60000.0, 70000.0], start=1):
                _gate_cell(root, 10570, "bounded_http",
                           total_tokens=int(tps * 10), jct=10.0,
                           ttft_p50=0.05, prefix_hit=0.77, rep=rep)
            result = agg.aggregate(root)
        m = result["scale_10570"]["arms"]["bounded_http"]
        self.assertEqual(m["n_reps"], 3)
        self.assertEqual(m["n_passed"], 3)
        self.assertEqual(m["status"], "passed")
        # mean of 50k/60k/70k = 60k
        self.assertAlmostEqual(m["service_tokens_per_s_mean"], 60000.0, delta=1)
        self.assertGreater(m["service_tokens_per_s_cv_pct"], 0.0)  # nonzero spread
        self.assertEqual(len(m["reps"]), 3)

    def test_partial_reps_marked_partial(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _gate_cell(root, 4096, "bounded_http", total_tokens=500000, jct=5.0,
                       ttft_p50=0.05, prefix_hit=0.77, rep=1, status="passed")
            _gate_cell(root, 4096, "bounded_http", total_tokens=500000, jct=5.0,
                       ttft_p50=0.05, prefix_hit=0.77, rep=2, status="failed")
            result = agg.aggregate(root)
        m = result["scale_4096"]["arms"]["bounded_http"]
        self.assertEqual(m["status"], "partial")
        self.assertEqual(m["n_passed"], 1)
        self.assertEqual(m["n_reps"], 2)


if __name__ == "__main__":
    unittest.main()
