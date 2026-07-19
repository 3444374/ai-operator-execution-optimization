from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.metrics import batch_result_stats, parse_prometheus_metrics, vllm_metric_delta_stats  # noqa: E402


class MetricsTests(unittest.TestCase):
    def test_batch_result_stats_summarizes_latency_and_tokens(self) -> None:
        stats = batch_result_stats(
            [
                {"rows": 2, "token_count": 20, "service_s": 0.20},
                {"rows": 4, "token_count": 80, "service_s": 0.80},
                {"rows": 1, "token_count": 10, "service_s": 0.10},
            ]
        )

        self.assertEqual(stats["batch_rows_min"], 1)
        self.assertEqual(stats["batch_rows_max"], 4)
        self.assertEqual(stats["batch_tokens_min"], 10)
        self.assertEqual(stats["batch_tokens_max"], 80)
        self.assertAlmostEqual(stats["batch_tokens_mean"], 36.666667, places=5)
        self.assertAlmostEqual(stats["batch_tokens_p50"], 20.0)
        self.assertAlmostEqual(stats["batch_tokens_p95"], 80.0)
        self.assertAlmostEqual(stats["batch_service_s_p50"], 0.20)
        self.assertAlmostEqual(stats["batch_service_s_p95"], 0.80)
        self.assertAlmostEqual(stats["batch_service_s_p99"], 0.80)

    def test_batch_result_stats_handles_empty_results(self) -> None:
        stats = batch_result_stats([])

        self.assertEqual(stats["batch_rows_min"], 0)
        self.assertEqual(stats["batch_tokens_max"], 0)
        self.assertEqual(stats["batch_service_s_p99"], 0.0)

    def test_vllm_metric_delta_stats_extracts_counter_and_latency_deltas(self) -> None:
        before = parse_prometheus_metrics(
            """
vllm:prompt_tokens_total{model_name="qwen2.5-1.5b"} 100
vllm:generation_tokens_total{model_name="qwen2.5-1.5b"} 20
vllm:request_success_total{finished_reason="length",model_name="qwen2.5-1.5b"} 4
vllm:e2e_request_latency_seconds_count{model_name="qwen2.5-1.5b"} 4
vllm:e2e_request_latency_seconds_sum{model_name="qwen2.5-1.5b"} 2.0
vllm:request_queue_time_seconds_count{model_name="qwen2.5-1.5b"} 4
vllm:request_queue_time_seconds_sum{model_name="qwen2.5-1.5b"} 0.2
vllm:num_requests_waiting{model_name="qwen2.5-1.5b"} 0
"""
        )
        after = parse_prometheus_metrics(
            """
vllm:prompt_tokens_total{model_name="qwen2.5-1.5b"} 180
vllm:generation_tokens_total{model_name="qwen2.5-1.5b"} 52
vllm:request_success_total{finished_reason="length",model_name="qwen2.5-1.5b"} 8
vllm:e2e_request_latency_seconds_count{model_name="qwen2.5-1.5b"} 8
vllm:e2e_request_latency_seconds_sum{model_name="qwen2.5-1.5b"} 5.0
vllm:request_queue_time_seconds_count{model_name="qwen2.5-1.5b"} 8
vllm:request_queue_time_seconds_sum{model_name="qwen2.5-1.5b"} 0.6
vllm:num_requests_waiting{model_name="qwen2.5-1.5b"} 1
"""
        )

        stats = vllm_metric_delta_stats(before, after)

        self.assertEqual(stats["vllm_metrics_status"], "ok")
        self.assertEqual(stats["vllm_prompt_tokens_delta"], 80)
        self.assertEqual(stats["vllm_generation_tokens_delta"], 32)
        self.assertEqual(stats["vllm_request_success_delta"], 4)
        self.assertAlmostEqual(stats["vllm_e2e_request_latency_mean_s"], 0.75)
        self.assertAlmostEqual(stats["vllm_request_queue_time_mean_s"], 0.1)
        self.assertEqual(stats["vllm_num_requests_waiting_after"], 1)

    def test_vllm_metric_delta_stats_marks_missing_snapshots(self) -> None:
        stats = vllm_metric_delta_stats({}, {})

        self.assertEqual(stats["vllm_metrics_status"], "unavailable")
        self.assertEqual(stats["vllm_request_success_delta"], 0)


if __name__ == "__main__":
    unittest.main()
