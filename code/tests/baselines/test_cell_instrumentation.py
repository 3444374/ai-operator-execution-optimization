"""Tests for cell_instrumentation -- the per-cell /metrics + GPU instrumentation wrapper.

Covers the §7.5C(1) feeding-saturation sampler (VllmGaugeSampler) added in the audit-followup
ramp-metric 补齐: during-cell vLLM gauge mean/max (running/waiting/KV), since the before/after
/metrics snapshots are both idle (gauges return to 0) and cannot evidence GPU saturation.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.common.cell_instrumentation import VllmGaugeSampler  # noqa: E402


def _sample(running: float, waiting: float, kv: float, t: float = 0.0, idx: int = 0) -> dict:
    return {
        "sample_epoch_s": t,
        "sample_index": idx,
        "running_sum": running,
        "waiting_sum": waiting,
        "kv_mean": kv,
        "n_endpoints": 2,
    }


class VllmGaugeSamplerSummaryTests(unittest.TestCase):
    """summary() computes mean/max of total-running/waiting + KV across during-cell samples."""

    def test_summary_mean_and_max_of_running_total(self) -> None:
        s = VllmGaugeSampler(metrics_urls=("u0", "u1"))
        s.samples = [_sample(50, 0, 0.3, 0.0, 0), _sample(120, 2, 0.6, 0.5, 1), _sample(80, 0, 0.5, 1.0, 2)]
        out = s.summary()
        # running total: mean (50+120+80)/3 = 83.33, max 120
        self.assertAlmostEqual(out["vllm_running_mean"], 83.33, places=1)
        self.assertEqual(out["vllm_running_max"], 120.0)
        # waiting: mean (0+2+0)/3 = 0.667, max 2
        self.assertAlmostEqual(out["vllm_waiting_mean"], 0.667, places=2)
        self.assertEqual(out["vllm_waiting_max"], 2.0)
        # KV (fraction): mean (0.3+0.6+0.5)/3 = 0.467, max 0.6
        self.assertAlmostEqual(out["vllm_kv_cache_usage_mean"], 0.467, places=2)
        self.assertAlmostEqual(out["vllm_kv_cache_usage_max"], 0.6, places=2)
        self.assertEqual(out["n_gauge_samples"], 3.0)

    def test_summary_empty_when_no_samples(self) -> None:
        s = VllmGaugeSampler(metrics_urls=("u0",))
        out = s.summary()
        self.assertEqual(out, {"n_gauge_samples": 0.0})


class VllmGaugeSamplerThreadTests(unittest.TestCase):
    """The background thread polls the injected snapshotter and accumulates samples."""

    def test_thread_collects_gauge_samples_from_snapshotter(self) -> None:
        import time

        # Fake snapshotter returns increasing running counts per endpoint per call.
        calls = {"n": 0}

        def fake_snap(url: str) -> dict[str, float]:
            calls["n"] += 1
            r = 40.0 + (calls["n"] % 5) * 20.0  # 40..120 cycling
            return {"vllm:num_requests_running": r, "vllm:num_requests_waiting": 0.0,
                    "vllm:kv_cache_usage_perc": 0.4}

        s = VllmGaugeSampler(
            interval_s=0.05, metrics_urls=("u0", "u1"), snapshotter=fake_snap,
        )
        s.start(base_epoch_s=time.monotonic())
        time.sleep(0.25)  # ~5 polls
        s.stop()
        self.assertGreaterEqual(len(s.samples), 2)
        out = s.summary()
        self.assertIn("vllm_running_mean", out)
        self.assertGreater(out["vllm_running_mean"], 0.0)
        self.assertGreaterEqual(out["vllm_running_max"], out["vllm_running_mean"])
        # each sample summed across 2 endpoints -> running_total >= single-endpoint value
        self.assertGreaterEqual(out["vllm_running_max"], 40.0)


if __name__ == "__main__":
    unittest.main()
