import sys
import unittest
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.image.provenance import (  # noqa: E402
    image_arm_provenance,
    require_formal_arm_allowed,
)


class ImageBaselineContractTest(unittest.TestCase):
    def test_vendor_builtin_has_no_project_scheduler(self):
        provenance = image_arm_provenance("daft_builtin_embed")
        self.assertTrue(provenance.formal_baseline_eligible)
        self.assertFalse(provenance.custom_scheduling_code)
        self.assertEqual(provenance.scheduler_owner, "daft")

    def test_ray_data_owns_native_graph_scheduling(self):
        provenance = image_arm_provenance("ray_data_staged")
        self.assertTrue(provenance.formal_baseline_eligible)
        self.assertFalse(provenance.custom_scheduling_code)
        self.assertEqual(provenance.scheduler_owner, "ray_data")

    def test_project_authored_daft_udf_is_not_formal_baseline(self):
        with self.assertRaisesRegex(ValueError, "not a native formal baseline"):
            require_formal_arm_allowed(
                "daft_staged",
                phase="formal",
                allow_non_native_diagnostic=False,
            )

    def test_override_does_not_change_provenance(self):
        require_formal_arm_allowed(
            "daft_staged",
            phase="formal",
            allow_non_native_diagnostic=True,
        )
        self.assertFalse(
            image_arm_provenance("daft_staged").formal_baseline_eligible
        )


if __name__ == "__main__":
    unittest.main()
