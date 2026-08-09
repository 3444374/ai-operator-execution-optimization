import sys
import unittest
from pathlib import Path


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.image.resource_sampling import (  # noqa: E402
    GpuResourceSample,
    summarize_cpu_samples,
    summarize_gpu_samples,
    summarize_ray_resource_samples,
)


class ImageResourceSamplingTest(unittest.TestCase):
    def test_gpu_summary_excludes_idle_visible_device_from_active_mean(self):
        summary = summarize_gpu_samples(
            [
                GpuResourceSample(1.0, 0, 20.0, 930.0, 100.0, 2000.0, 10000.0, 4, 16, 4, 16),
                GpuResourceSample(1.0, 1, 0.0, 4.0, 10.0, 200.0, 1000.0, 4, 16, 4, 16),
                GpuResourceSample(2.0, 0, 40.0, 930.0, 120.0, 2200.0, 11000.0, 4, 16, 4, 16),
                GpuResourceSample(2.0, 1, 0.0, 4.0, 10.0, 200.0, 1000.0, 4, 16, 4, 16),
            ],
            active_device_count=1,
            sample_window_s=2.0,
        )

        self.assertEqual(summary["gpu_util_mean_pct"], 15.0)
        self.assertEqual(summary["gpu_active_util_mean_pct"], 30.0)
        self.assertEqual(summary["gpu_active_devices_json"], "[0]")
        self.assertEqual(summary["gpu_active_power_mean_w"], 110.0)
        self.assertEqual(summary["gpu_energy_estimate_j"], 220.0)
        self.assertEqual(summary["gpu_active_pcie_generation"], 4)
        self.assertEqual(summary["gpu_active_pcie_width_max"], 16)

    def test_cpu_summary_reports_equivalent_busy_cores(self):
        summary = summarize_cpu_samples(
            [[100.0, 0.0, 50.0, 50.0], [50.0, 50.0, 50.0, 50.0]]
        )

        self.assertEqual(summary["cpu_system_mean_pct"], 50.0)
        self.assertEqual(summary["cpu_busy_cores_mean"], 2.0)
        self.assertEqual(summary["cpu_logical_count"], 4)

    def test_ray_summary_preserves_capacity_minimum_and_shm_peak(self):
        summary = summarize_ray_resource_samples(
            [
                {
                    "timestamp_s": 1.0,
                    "ray_available_cpu": 12.0,
                    "ray_available_gpu": 1.0,
                    "shm_used_bytes": 100.0,
                    "shm_total_bytes": 1000.0,
                },
                {
                    "timestamp_s": 2.0,
                    "ray_available_cpu": 4.0,
                    "ray_available_gpu": 0.0,
                    "shm_used_bytes": 300.0,
                    "shm_total_bytes": 1000.0,
                },
            ]
        )

        self.assertEqual(summary["ray_available_cpu_min"], 4.0)
        self.assertEqual(summary["ray_available_gpu_min"], 0.0)
        self.assertEqual(summary["shm_used_bytes_mean"], 200.0)
        self.assertEqual(summary["shm_used_bytes_peak"], 300.0)


if __name__ == "__main__":
    unittest.main()
