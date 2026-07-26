from __future__ import annotations

import csv
import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.metrics import (  # noqa: E402
    PeriodicSampler,
    append_metrics,
    batch_result_stats,
    estimate_mfu,
    gpu_metadata,
    parse_prometheus_metrics,
    preflight_metrics_schema,
    resource_sample_stats,
    vllm_metric_delta_stats,
)


class MetricsTests(unittest.TestCase):
    def _metrics_path(self, name: str) -> Path:
        path = CODE_ROOT.parent / ".test-tmp" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.unlink(missing_ok=True)
        self.addCleanup(path.unlink, missing_ok=True)
        return path

    def test_append_metrics_writes_header_for_existing_empty_file(self) -> None:
        path = self._metrics_path("metrics_existing_empty.csv")
        path.touch()

        append_metrics(path, {"status": "ok", "rows": 1})

        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(rows, [{"status": "ok", "rows": "1"}])

    def test_append_metrics_appends_when_schema_matches_exactly(self) -> None:
        path = self._metrics_path("metrics_matching_schema.csv")

        append_metrics(path, {"status": "ok", "rows": 1})
        append_metrics(path, {"status": "ok", "rows": 2})

        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(
            rows,
            [
                {"status": "ok", "rows": "1"},
                {"status": "ok", "rows": "2"},
            ],
        )

    def test_append_metrics_rejects_existing_incompatible_schema(self) -> None:
        path = self._metrics_path("metrics_incompatible_schema.csv")
        path.write_text("status,legacy_field\nok,1\n", encoding="utf-8")
        original = path.read_text(encoding="utf-8")

        with self.assertRaisesRegex(
            ValueError,
            "CSV schema mismatch",
        ):
            append_metrics(path, {"status": "ok", "rows": 2})

        self.assertEqual(path.read_text(encoding="utf-8"), original)

    def test_preflight_metrics_schema_rejects_without_writing(self) -> None:
        path = self._metrics_path("metrics_preflight_incompatible.csv")
        path.write_text("status,legacy_field\nok,1\n", encoding="utf-8")
        original = path.read_text(encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "CSV schema mismatch"):
            preflight_metrics_schema(path, ["status", "rows"])

        self.assertEqual(path.read_text(encoding="utf-8"), original)

    def test_preflight_accepts_formal_schema_containing_dry_run_keys(self) -> None:
        path = self._metrics_path("metrics_preflight_formal_schema.csv")
        path.write_text(
            "status,job_id,rows\nok,1,2\n",
            encoding="utf-8",
        )

        preflight_metrics_schema(
            path,
            ["status", "rows"],
            allow_additional_fields=True,
        )

    def test_preflight_rejects_dry_run_keys_in_a_different_order(self) -> None:
        path = self._metrics_path("metrics_preflight_wrong_order.csv")
        path.write_text(
            "rows,job_id,status\n2,1,ok\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "CSV schema mismatch"):
            preflight_metrics_schema(
                path,
                ["status", "rows"],
                allow_additional_fields=True,
            )

    def test_periodic_sampler_collects_and_stops(self) -> None:
        sampled_twice = threading.Event()
        calls = 0

        def sample() -> dict[str, int]:
            nonlocal calls
            calls += 1
            if calls >= 2:
                sampled_twice.set()
            return {"value": calls}

        sampler = PeriodicSampler(sample, interval_s=0.01)
        self.assertTrue(sampled_twice.wait(timeout=1.0))
        sampler.close()
        count_after_close = calls
        self.assertGreaterEqual(len(sampler.samples), 2)
        self.assertEqual(sampler.samples[0]["sample_index"], 0)
        self.assertEqual(sampler.samples[-1]["value"], count_after_close)
        self.assertFalse(sampler.is_running)
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
vllm:estimated_flops_per_gpu_total{model_name="qwen2.5-1.5b"} 1000000000000
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
vllm:estimated_flops_per_gpu_total{model_name="qwen2.5-1.5b"} 4000000000000
"""
        )

        stats = vllm_metric_delta_stats(before, after)

        self.assertEqual(stats["vllm_metrics_status"], "ok")
        self.assertEqual(stats["vllm_prompt_tokens_delta"], 80)
        self.assertEqual(stats["vllm_generation_tokens_delta"], 32)
        self.assertEqual(stats["vllm_request_success_delta"], 4)
        self.assertEqual(
            stats["vllm_estimated_flops_per_gpu_delta"],
            3_000_000_000_000.0,
        )
        self.assertAlmostEqual(stats["vllm_e2e_request_latency_mean_s"], 0.75)
        self.assertAlmostEqual(stats["vllm_request_queue_time_mean_s"], 0.1)
        self.assertEqual(stats["vllm_num_requests_waiting_after"], 1)

    def test_vllm_metric_delta_stats_marks_missing_snapshots(self) -> None:
        stats = vllm_metric_delta_stats({}, {})

        self.assertEqual(stats["vllm_metrics_status"], "unavailable")
        self.assertEqual(stats["vllm_request_success_delta"], 0)

    def test_resource_sample_stats_summarizes_gpu_vllm_and_energy(self) -> None:
        stats = resource_sample_stats(
            [
                {
                    "sample_epoch_s": 0.0,
                    "gpu_utilization_pct": "0",
                    "gpu_memory_used_mib": "100",
                    "gpu_memory_total_mib": "400",
                    "gpu_power_w": "100",
                    "vllm_num_requests_running": 0,
                    "vllm_num_requests_waiting": 0,
                    "vllm_kv_cache_usage_perc": 0.1,
                },
                {
                    "sample_epoch_s": 1.0,
                    "gpu_utilization_pct": "50",
                    "gpu_memory_used_mib": "200",
                    "gpu_memory_total_mib": "400",
                    "gpu_power_w": "200",
                    "vllm_num_requests_running": 2,
                    "vllm_num_requests_waiting": 1,
                    "vllm_kv_cache_usage_perc": 0.5,
                },
                {
                    "sample_epoch_s": 2.0,
                    "gpu_utilization_pct": "100",
                    "gpu_memory_used_mib": "300",
                    "gpu_memory_total_mib": "400",
                    "gpu_power_w": "300",
                    "vllm_num_requests_running": 4,
                    "vllm_num_requests_waiting": 3,
                    "vllm_kv_cache_usage_perc": 0.9,
                },
            ],
            observed_tokens=2000,
        )

        self.assertEqual(stats["resource_metrics_status"], "ok")
        self.assertEqual(stats["gpu_utilization_pct_mean"], 50.0)
        self.assertEqual(stats["gpu_utilization_pct_p50"], 50.0)
        self.assertEqual(stats["gpu_utilization_pct_p95"], 100.0)
        self.assertEqual(stats["gpu_utilization_pct_max"], 100.0)
        self.assertAlmostEqual(
            stats["gpu_utilization_below_10pct_ratio"],
            1 / 3,
        )
        self.assertEqual(stats["gpu_memory_used_mib_mean"], 200.0)
        self.assertEqual(stats["gpu_memory_used_mib_max"], 300.0)
        self.assertEqual(stats["gpu_memory_utilization_pct_mean"], 50.0)
        self.assertEqual(stats["gpu_memory_utilization_pct_max"], 75.0)
        self.assertEqual(stats["gpu_power_w_mean"], 200.0)
        self.assertEqual(stats["gpu_power_w_max"], 300.0)
        self.assertEqual(stats["gpu_energy_j"], 400.0)
        self.assertEqual(stats["energy_j_per_1k_observed_tokens"], 200.0)
        self.assertEqual(stats["vllm_running_mean"], 2.0)
        self.assertEqual(stats["vllm_running_p95"], 4.0)
        self.assertEqual(stats["vllm_running_max"], 4.0)
        self.assertAlmostEqual(stats["vllm_waiting_mean"], 4 / 3)
        self.assertEqual(stats["vllm_waiting_p95"], 3.0)
        self.assertEqual(stats["vllm_kv_cache_usage_mean"], 0.5)
        self.assertEqual(stats["vllm_kv_cache_usage_p95"], 0.9)

    def test_resource_sample_stats_marks_unavailable_measurements(self) -> None:
        stats = resource_sample_stats([], observed_tokens=0)

        self.assertEqual(stats["resource_metrics_status"], "unavailable")
        self.assertEqual(stats["gpu_utilization_pct_mean"], "")
        self.assertEqual(stats["gpu_energy_j"], "")
        self.assertEqual(stats["energy_j_per_1k_observed_tokens"], "")

    def test_mfu_requires_explicit_reproducible_inputs(self) -> None:
        stats = estimate_mfu(
            estimated_flops=0.0,
            observed_tokens=1000,
            operator_wall_s=2.0,
            model_flops_per_token=2_000_000_000.0,
            gpu_peak_tflops=1.0,
            precision="bf16",
        )

        self.assertEqual(stats["mfu_status"], "ok")
        self.assertEqual(stats["mfu_estimate"], 1.0)
        self.assertEqual(
            stats["mfu_estimation_method"],
            "configured_flops_per_observed_token",
        )
        self.assertEqual(stats["mfu_time_basis"], "operator_wall_s")
        self.assertEqual(stats["mfu_precision"], "bf16")

        missing = estimate_mfu(
            estimated_flops=0.0,
            observed_tokens=1000,
            operator_wall_s=2.0,
            model_flops_per_token=0.0,
            gpu_peak_tflops=1.0,
            precision="bf16",
        )
        self.assertEqual(
            missing["mfu_status"],
            "unavailable:missing_model_flops_per_token",
        )
        self.assertEqual(missing["mfu_estimate"], "")

    def test_mfu_prefers_vllm_estimated_flops_counter(self) -> None:
        stats = estimate_mfu(
            estimated_flops=50_000_000_000_000.0,
            observed_tokens=1000,
            operator_wall_s=2.0,
            model_flops_per_token=0.0,
            gpu_peak_tflops=100.0,
            precision="bf16",
        )

        self.assertEqual(stats["mfu_status"], "ok")
        self.assertEqual(stats["mfu_estimate"], 0.25)
        self.assertEqual(
            stats["mfu_estimation_method"],
            "vllm_estimated_flops_per_gpu_delta",
        )

    def test_gpu_metadata_collects_power_when_supported(self) -> None:
        with patch("src.metrics.subprocess.run") as run:
            run.return_value = SimpleNamespace(
                stdout="NVIDIA Test, 75, 1024, 8192, 125.5\n"
            )

            snapshot = gpu_metadata()

        self.assertEqual(snapshot["gpu_metrics_status"], "snapshot")
        self.assertEqual(snapshot["gpu_utilization_pct"], "75")
        self.assertEqual(snapshot["gpu_power_w"], "125.5")
        self.assertIn(
            "power.draw",
            run.call_args.args[0][1],
        )

    def test_gpu_metadata_leaves_unsupported_power_empty(self) -> None:
        with patch("src.metrics.subprocess.run") as run:
            run.return_value = SimpleNamespace(
                stdout="NVIDIA Test, 75, 1024, 8192, [N/A]\n"
            )

            snapshot = gpu_metadata()

        self.assertEqual(snapshot["gpu_metrics_status"], "snapshot")
        self.assertEqual(snapshot["gpu_power_w"], "")


if __name__ == "__main__":
    unittest.main()
