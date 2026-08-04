import sys
import unittest
import importlib.util
from pathlib import Path


CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.image.metrics import (  # noqa: E402
    IMAGE_METRIC_DEFINITIONS,
    image_run_derived_metrics,
)


AUGMENT_PATH = CODE_ROOT / "scripts" / "analysis" / "augment_image_observability.py"
AUGMENT_SPEC = importlib.util.spec_from_file_location("augment_image_observability", AUGMENT_PATH)
assert AUGMENT_SPEC is not None and AUGMENT_SPEC.loader is not None
AUGMENT_MODULE = importlib.util.module_from_spec(AUGMENT_SPEC)
AUGMENT_SPEC.loader.exec_module(AUGMENT_MODULE)


class ImageRunDerivedMetricsTest(unittest.TestCase):
    def test_metric_catalog_exposes_audit_contract(self):
        required = {
            "meaning_zh",
            "unit",
            "measurement_kind",
            "formula",
            "source",
            "comparison_scope",
            "limitations",
        }

        for name in (
            "operator_e2e_s",
            "first_output_s",
            "images_per_s",
            "gpu_energy_estimate_j",
            "logical_h2d_effective_gbps",
            "estimated_e2e_mfu",
        ):
            self.assertTrue(required.issubset(IMAGE_METRIC_DEFINITIONS[name]))

    def test_reports_streaming_and_per_image_metrics(self):
        metrics = image_run_derived_metrics(
            rows=1000,
            operator_e2e_s=100.0,
            first_output_s=25.0,
            cpu_core_seconds=500.0,
            gpu_seconds=200.0,
            gpu_energy_j=4000.0,
            host_disk_read_bytes=2000,
            host_disk_write_bytes=1000,
            host_net_recv_bytes=3000,
            host_net_sent_bytes=500,
        )

        self.assertEqual(metrics["post_first_output_s"], 75.0)
        self.assertEqual(metrics["first_output_fraction_of_e2e"], 0.25)
        self.assertEqual(metrics["post_first_output_fraction_of_e2e"], 0.75)
        self.assertTrue(metrics["steady_state_duration_gate_met"])
        self.assertEqual(metrics["joules_per_1k_images"], 4000.0)
        self.assertEqual(metrics["gpu_seconds_per_image"], 0.2)
        self.assertEqual(metrics["images_per_cpu_core_second"], 2.0)
        self.assertEqual(metrics["host_net_recv_bytes_per_image"], 3.0)

    def test_short_run_fails_duration_gate_without_invalidating_rate(self):
        metrics = image_run_derived_metrics(
            rows=100,
            operator_e2e_s=10.0,
            first_output_s=8.0,
            cpu_core_seconds=0.0,
            gpu_seconds=20.0,
            gpu_energy_j=50.0,
            host_disk_read_bytes=0,
            host_disk_write_bytes=0,
            host_net_recv_bytes=0,
            host_net_sent_bytes=0,
        )

        self.assertFalse(metrics["steady_state_duration_gate_met"])
        self.assertEqual(metrics["images_per_cpu_core_second"], "")

    def test_rejects_first_output_after_completion(self):
        with self.assertRaisesRegex(ValueError, "first_output_s"):
            image_run_derived_metrics(
                rows=1,
                operator_e2e_s=1.0,
                first_output_s=2.0,
                cpu_core_seconds=0.0,
                gpu_seconds=1.0,
                gpu_energy_j=0.0,
                host_disk_read_bytes=0,
                host_disk_write_bytes=0,
                host_net_recv_bytes=0,
                host_net_sent_bytes=0,
            )


class HistoricalImageCsvAugmentationTest(unittest.TestCase):
    def test_derives_metrics_from_schema_v11_row(self):
        row = {
            "arm": "project_ray",
            "rows": "1000",
            "operator_e2e_s": "100",
            "first_output_s": "25",
            "cpu_core_seconds_estimate": "500",
            "gpu_seconds": "200",
            "gpu_energy_estimate_j": "4000",
            "host_disk_read_bytes": "2000",
            "host_disk_write_bytes": "1000",
            "host_net_recv_bytes": "3000",
            "host_net_sent_bytes": "500",
        }

        augmented = AUGMENT_MODULE.augment_row(row)

        self.assertEqual(augmented["arm"], "project_ray")
        self.assertEqual(augmented["image_derived_metrics_status"], "available")
        self.assertEqual(augmented["first_output_fraction_of_e2e"], 0.25)

    def test_marks_missing_historical_fields_unavailable(self):
        augmented = AUGMENT_MODULE.augment_row({"rows": "100"})

        self.assertTrue(
            str(augmented["image_derived_metrics_status"]).startswith("unavailable:")
        )


if __name__ == "__main__":
    unittest.main()
