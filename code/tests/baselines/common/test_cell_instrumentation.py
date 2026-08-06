"""Tests for cell_instrumentation -- the TTFT + per-GPU sweep wrapper.

Uses injected fakes (no real vLLM /metrics, no real nvidia-smi) so the
histogram-delta math, per-GPU CSV shape, and context-manager bracketing are
verified without a server.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

_CI_PATH = CODE_ROOT / "src" / "baselines" / "common" / "cell_instrumentation.py"
_spec = importlib.util.spec_from_file_location("cell_instrumentation", _CI_PATH)
ci = importlib.util.module_from_spec(_spec)
sys.modules["cell_instrumentation"] = ci
_spec.loader.exec_module(ci)


def _histogram_metrics(p50_bucket_low: float, counts: dict[str, int], sum_total: float, count: int) -> dict:
    """Build a parsed-/metrics-style dict with a TTFT histogram + counters."""
    metrics: dict[str, float] = {}
    for le, cumulative in counts.items():
        metrics[f'vllm:time_to_first_token_seconds_bucket{{le="{le}"}}'] = float(cumulative)
    metrics["vllm:time_to_first_token_seconds_count"] = float(count)
    metrics["vllm:time_to_first_token_seconds_sum"] = float(sum_total)
    metrics["vllm:prompt_tokens_total"] = 100.0
    metrics["vllm:generation_tokens_total"] = 10.0
    return metrics


class EndpointLatencyDeltaTests(unittest.TestCase):
    def test_two_endpoint_delta_computes_per_endpoint_ttft(self) -> None:
        # before: 30 reqs, after: 60 reqs -> delta of 30 reqs with the bucket shape.
        before_counts = {"0.05": 10, "0.1": 20, "+Inf": 30}
        after_counts = {"0.05": 20, "0.1": 50, "+Inf": 60}
        before = {0: _histogram_metrics(0, before_counts, 2.0, 30),
                  1: _histogram_metrics(0, before_counts, 2.0, 30)}
        after = {0: _histogram_metrics(0, after_counts, 5.0, 60),
                 1: _histogram_metrics(0, after_counts, 5.0, 60)}
        deltas = ci.endpoint_latency_deltas(before, after)
        self.assertEqual(set(deltas), {0, 1})
        for ep in (0, 1):
            self.assertEqual(deltas[ep]["vllm_metrics_status"], "ok")
            self.assertEqual(deltas[ep]["vllm_prompt_tokens_delta"], 0.0)  # 100->100 no change here
            # delta count = 60 - 30 = 30
            self.assertEqual(deltas[ep]["vllm_ttft_histogram_status"], "ok")
            # p50: rank=0.5*30=15; falls in (0.05,0.1] bucket: prev=10, bucket=30, frac=0.25
            self.assertAlmostEqual(deltas[ep]["vllm_time_to_first_token_p50_s"], 0.0625, places=4)

    def test_mismatched_endpoint_sets_raise(self) -> None:
        with self.assertRaises(ValueError):
            ci.endpoint_latency_deltas({0: {}}, {1: {}})


class GpuResourceSamplerTests(unittest.TestCase):
    def test_samples_per_gpu_and_writes_csv_plus_summary(self) -> None:
        call = {"n": 0}

        def fake_snapshotter() -> list[dict]:
            # two GPUs each call, util rises with the call index
            call["n"] += 1
            u = 50.0 * call["n"]
            return [
                {"gpu_index": 0, "gpu_name": "RTX4090", "gpu_utilization_pct": u,
                 "gpu_memory_used_mib": 1000.0, "gpu_memory_total_mib": 24564.0, "gpu_power_w": 100.0 + u},
                {"gpu_index": 1, "gpu_name": "RTX4090", "gpu_utilization_pct": u + 10,
                 "gpu_memory_used_mib": 2000.0, "gpu_memory_total_mib": 24564.0, "gpu_power_w": 110.0 + u},
            ]

        sampler = ci.GpuResourceSampler(
            interval_s=0.001,
            snapshotter=fake_snapshotter,
            sleep=lambda _: None,  # don't actually sleep
            monotonic=lambda: 0.0,
        )
        sampler.start(base_epoch_s=0.0)
        # take 3 samples manually (sleep is a no-op, so _loop spins fast)
        import time as _t
        _t.sleep(0.05)
        sampler.stop()
        self.assertGreaterEqual(len(sampler.samples), 2)
        gpu0_samples = [s for s in sampler.samples if int(s["gpu_index"]) == 0]
        gpu1_samples = [s for s in sampler.samples if int(s["gpu_index"]) == 1]
        self.assertGreaterEqual(len(gpu0_samples), 1)
        self.assertGreaterEqual(len(gpu1_samples), 1)
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / "gpu_resource.csv"
            sampler.write_csv(csv_path)
            with csv_path.open(encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        self.assertTrue(all("gpu_utilization_pct" in r for r in rows))
        self.assertTrue(any(int(r["gpu_index"]) == 1 for r in rows))  # gpu1 recorded separately
        summary = sampler.summary()
        self.assertIn("gpu0_util_mean", summary)
        self.assertIn("gpu1_util_mean", summary)
        self.assertGreater(summary["gpu1_util_mean"], summary["gpu0_util_mean"])  # gpu1 was +10

    def test_empty_nvidia_smi_yields_no_samples(self) -> None:
        sampler = ci.GpuResourceSampler(
            interval_s=0.001, snapshotter=lambda: [], sleep=lambda _: None,
        )
        sampler.start(base_epoch_s=0.0)
        sampler.stop()
        self.assertEqual(sampler.samples, [])
        self.assertEqual(sampler.summary(), {"n_samples": 0.0})


class InstrumentedCellTests(unittest.TestCase):
    def test_brackets_cell_and_populates_ttft_on_exit(self) -> None:
        # Snapshotter returns "before" on the first 2 calls (eps 0,1) and "after"
        # on the next 2 (eps 0,1 again), simulating cumulative counters advancing.
        before_counts = {"0.05": 0, "0.1": 0, "+Inf": 0}
        after_counts = {"0.05": 10, "0.1": 30, "+Inf": 30}
        state = {"calls": 0, "n_endpoints": 2}

        def fake_metrics(url: str) -> dict:
            state["calls"] += 1
            # first n_endpoints calls = before, rest = after
            stage = "before" if state["calls"] <= state["n_endpoints"] else "after"
            counts = before_counts if stage == "before" else after_counts
            sum_v = 0.0 if stage == "before" else 3.0
            count_v = 0 if stage == "before" else 30
            return _histogram_metrics(0, counts, sum_v, count_v)

        with tempfile.TemporaryDirectory() as td:
            gpu_csv = Path(td) / "cell" / "gpu_resource.csv"
            with ci.instrumented_cell(
                ("http://e0/metrics", "http://e1/metrics"),
                gpu_csv,
                metrics_snapshotter=fake_metrics,
                gpu_snapshotter=lambda: [{"gpu_index": 0, "gpu_name": "G0",
                                          "gpu_utilization_pct": 42.0, "gpu_memory_used_mib": 1.0,
                                          "gpu_memory_total_mib": 2.0, "gpu_power_w": 100.0}],
                interval_s=0.001,
            ) as instr:
                # Inside the with-block: ttft not yet computed; gpu sampler running.
                self.assertIsNone(instr.ttft_deltas)
                # emulate the cell doing work
                import time as _t
                _t.sleep(0.03)
            # After exit: ttft populated for both endpoints; gpu csv written.
            self.assertIsNotNone(instr.ttft_deltas)
            self.assertEqual(set(instr.ttft_deltas), {0, 1})
            self.assertAlmostEqual(instr.ttft_deltas[0]["vllm_time_to_first_token_p50_s"], 0.0625, places=4)
            self.assertTrue(gpu_csv.is_file())
            self.assertIn("gpu0_util_mean", instr.gpu_summary)


if __name__ == "__main__":
    unittest.main()
