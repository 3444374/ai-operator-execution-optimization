"""Tests for multicard_ramp_aggregate -- the scale-ramp performance aggregator.

Builds synthetic ramp-layout cells (gate arm with ttft_metrics.json + gpu_resource.csv
+ shard summaries; project arm with project_static_* files) and asserts the
aggregate extracts tokens/s, TTFT, GPU, and prefix-hit correctly. Catches parsing
regressions before the (expensive) server ramp output is processed.
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


def _build_gate_cell(root: Path, scale: int, arm: str, *, total_tokens: int,
                     jct: float, ttft_p50: float, prefix_hit: float,
                     gpu_rows: list[dict]) -> None:
    cell = root / f"scale_{scale}" / f"{arm}_c32_rep1"
    shard_dir = cell / "gate_output" / f"{arm}_c32"
    for i in (0, 1):
        summary = {
            "service_total_tokens_delta": total_tokens // 2,
            "jct_s": jct,
            "latency_p50_s": 2.0, "latency_p95_s": 4.0, "latency_p99_s": 5.0,
            "output_tokens": total_tokens // 4,
        }
        _write(shard_dir / f"shard_{i}" / "summary.json", json.dumps(summary))
    _write(shard_dir / "run_status.json", json.dumps({"status": "passed"}))
    _write(cell / "ttft_metrics.json", json.dumps({
        "0": {"vllm_time_to_first_token_p50_s": ttft_p50, "vllm_time_to_first_token_p95_s": ttft_p50 * 1.5,
              "vllm_time_to_first_token_p99_s": ttft_p50 * 2.0, "vllm_prefix_cache_hit_rate": prefix_hit},
        "1": {"vllm_time_to_first_token_p50_s": ttft_p50, "vllm_time_to_first_token_p95_s": ttft_p50 * 1.5,
              "vllm_time_to_first_token_p99_s": ttft_p50 * 2.0, "vllm_prefix_cache_hit_rate": prefix_hit},
    }))
    with (cell / "gpu_resource.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["sample_index", "gpu_index", "gpu_utilization_pct", "gpu_power_w"])
        w.writeheader()
        w.writerows(gpu_rows)


def _build_project_cell(root: Path, scale: int, *, tps: float, ttft_p50: float,
                        overhead: float, gpu_util: float) -> None:
    cell = root / f"scale_{scale}" / "project_static_K32_rep1"
    cell.mkdir(parents=True, exist_ok=True)
    prof = {
        "model_request_tokens_per_s": tps,
        "vllm_prompt_tokens_delta": 400000, "vllm_generation_tokens_delta": 10000,
        "model_request_wall_s": 5.5, "rows_per_s": 300.0,
        "request_e2e_s_p50": 3.5, "request_e2e_s_p95": 6.0, "request_e2e_s_p99": 6.5,
        "vllm_time_to_first_token_p50_s": ttft_p50, "vllm_time_to_first_token_p95_s": ttft_p50 * 1.4,
        "vllm_time_to_first_token_p99_s": ttft_p50 * 1.5,
        "vllm_prefix_cache_hit_rate": 0.77,
        "scheduling_control_overhead_pct": overhead, "submit_s": 1.9,
    }
    fields = list(prof)
    with (cell / "project_static_summary.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerow(prof)
    with (cell / "project_static_resource.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["sample_index", "gpu_index", "gpu_utilization_pct", "gpu_power_w"])
        w.writeheader()
        for i in range(5):
            w.writerow({"sample_index": i, "gpu_index": "", "gpu_utilization_pct": gpu_util, "gpu_power_w": 250.0})
    with (cell / "project_static_completion_evidence.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["doc_id", "output_text"])
        w.writeheader()
        for d in range(10):
            w.writerow({"doc_id": d, "output_text": "ans"})


class RampAggregateTests(unittest.TestCase):
    def test_gate_cell_extracts_unified_tps_ttft_gpu(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _build_gate_cell(root, 4096, "bounded_http", total_tokens=800000, jct=16.0,
                             ttft_p50=0.05, prefix_hit=0.77,
                             gpu_rows=[{"sample_index": i, "gpu_index": str(g),
                                        "gpu_utilization_pct": 60.0 + g * 10, "gpu_power_w": 200.0 + g * 50}
                                       for i in range(3) for g in (0, 1)])
            result = agg.aggregate(root)
        m = result["scale_4096"]["arms"]["bounded_http"]
        self.assertEqual(m["status"], "passed")
        # 800000 / 16.0 = 50000
        self.assertAlmostEqual(m["service_tokens_per_s"], 50000.0, delta=1)
        self.assertAlmostEqual(m["ttft_s_p50"], 0.05, places=4)
        self.assertAlmostEqual(m["prefix_cache_hit_rate"], 0.77, places=3)
        self.assertEqual(m["gpu"]["gpu0_util_mean"], 60.0)
        self.assertEqual(m["gpu"]["gpu1_util_mean"], 70.0)
        self.assertEqual(m["gpu"]["n_samples"], 6)
        self.assertAlmostEqual(m["request_e2e_s_p50"], 2.0, places=3)

    def test_project_cell_extracts_tps_overhead_aggregated_gpu(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            _build_project_cell(root, 8192, tps=78000.0, ttft_p50=0.052, overhead=36.0, gpu_util=65.0)
            result = agg.aggregate(root)
        m = result["scale_8192"]["arms"]["project_static"]
        self.assertEqual(m["status"], "passed")
        self.assertAlmostEqual(m["service_tokens_per_s"], 78000.0, delta=1)
        self.assertAlmostEqual(m["ttft_s_p50"], 0.052, places=4)
        self.assertAlmostEqual(m["scheduling_overhead_pct"], 36.0, places=2)
        # project resource CSV has no gpu_index -> aggregated bucket
        self.assertEqual(m["gpu"]["gpu_aggregated_util_mean"], 65.0)
        self.assertEqual(m["n_evidence_rows"], 10)

    def test_three_scales_three_arms_table_shape(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            for scale in (4096, 8192, 10570):
                _build_gate_cell(root, scale, "bounded_http", total_tokens=800000, jct=16.0,
                                 ttft_p50=0.05, prefix_hit=0.77,
                                 gpu_rows=[{"sample_index": 0, "gpu_index": "0",
                                            "gpu_utilization_pct": 70.0, "gpu_power_w": 250.0}])
                _build_project_cell(root, scale, tps=78000.0, ttft_p50=0.052, overhead=36.0, gpu_util=63.0)
            result = agg.aggregate(root)
            md = agg._md(result)
        self.assertEqual(len(result), 3)
        for scale in (4096, 8192, 10570):
            self.assertIn(f"scale_{scale}", result)
            self.assertIn("bounded_http", result[f"scale_{scale}"]["arms"])
        self.assertIn("service tok/s", md)


if __name__ == "__main__":
    unittest.main()
