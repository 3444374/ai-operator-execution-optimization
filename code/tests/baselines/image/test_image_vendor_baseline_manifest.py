import json
import unittest
from pathlib import Path


REPOSITORY_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "code" / "src").is_dir()
)
MANIFEST_PATH = REPOSITORY_ROOT / "code/configs/image_vendor_baselines.json"


class ImageVendorBaselineManifestTest(unittest.TestCase):
    def test_official_daft_image_baseline_is_commit_and_file_pinned(self):
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        baseline = manifest["baselines"]["daft_image_classification_2024"]

        self.assertRegex(baseline["commit"], r"^[0-9a-f]{40}$")
        self.assertEqual(
            set(baseline["upstream_files"]),
            {"README.md", "daft_main.py", "ray_data_main.py"},
        )
        for digest in baseline["upstream_files"].values():
            self.assertRegex(digest, r"^[0-9a-f]{64}$")

    def test_adapter_policy_forbids_project_scheduling(self):
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        policy = manifest["baselines"]["daft_image_classification_2024"][
            "formal_adapter_policy"
        ]

        self.assertTrue(policy["adapter_diff_required"])
        forbidden = " ".join(policy["forbidden"])
        self.assertIn("project actor pools", forbidden)
        self.assertIn("project credit", forbidden)
        self.assertIn("custom backpressure", forbidden)


if __name__ == "__main__":
    unittest.main()
