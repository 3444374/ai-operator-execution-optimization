import sys
import unittest
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.image.resource_budget import build_ray_cpu_budget  # noqa: E402


class ImageResourceBudgetTest(unittest.TestCase):
    def test_ray_data_reserves_source_and_both_actor_stages(self):
        budget = build_ray_cpu_budget(
            arm="ray_data_staged",
            source_shards=4,
            preprocess_workers=4,
            gpu_workers=2,
            model_workers=2,
            host_slots=32,
        )

        self.assertEqual(budget.cluster_slots, 10)
        self.assertEqual(budget.external_slots, 0)
        self.assertEqual(budget.source_slots, 4)
        self.assertEqual(budget.preprocess_slots, 4)
        self.assertEqual(budget.model_slots, 2)

    def test_daft_staged_also_reserves_sql_reader_slots(self):
        budget = build_ray_cpu_budget(
            arm="daft_staged",
            source_shards=4,
            preprocess_workers=4,
            gpu_workers=2,
            model_workers=2,
            host_slots=32,
        )

        self.assertEqual(budget.cluster_slots, 10)
        self.assertIn("daft_sql_readers", budget.semantics)

    def test_project_source_is_outside_ray_cluster(self):
        budget = build_ray_cpu_budget(
            arm="project_ray",
            source_shards=4,
            preprocess_workers=4,
            gpu_workers=2,
            model_workers=2,
            host_slots=32,
        )

        self.assertEqual(budget.cluster_slots, 6)
        self.assertIsNone(budget.source_slots)
        self.assertEqual(budget.external_slots, 4)
        self.assertEqual(budget.declared_total_slots, 10)

    def test_project_refuses_external_source_plus_actor_oversubscription(self):
        with self.assertRaisesRegex(ValueError, "refusing CPU oversubscription"):
            build_ray_cpu_budget(
                arm="project_ray",
                source_shards=6,
                preprocess_workers=4,
                gpu_workers=2,
                model_workers=2,
                host_slots=8,
            )

    def test_refuses_virtual_cpu_oversubscription(self):
        with self.assertRaisesRegex(ValueError, "refusing CPU oversubscription"):
            build_ray_cpu_budget(
                arm="ray_data_staged",
                source_shards=4,
                preprocess_workers=4,
                gpu_workers=2,
                model_workers=2,
                host_slots=8,
            )


if __name__ == "__main__":
    unittest.main()
