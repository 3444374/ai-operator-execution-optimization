"""Optional native Q11 checks in the caller-selected existing SDK environment."""

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest

from src.research.data_organization.ecomm_media_profile import main


CHECKOUT = os.environ.get("SEMLOOM_NATIVE_TEST_CHECKOUT")
HAS_SDK = all(importlib.util.find_spec(name) is not None for name in ("lotus", "pandas", "PIL"))


@unittest.skipUnless(CHECKOUT and HAS_SDK, "needs an existing pinned SemBench checkout and native LOTUS SDK")
class NativeEcommMediaTests(unittest.TestCase):
    def test_original_join_suffix_and_image_requests_equal_with_duplicates_and_mixed_outputs(self):
        import pandas as pd
        from PIL import Image

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "images").mkdir()
            Image.new("RGB", (3, 2), "black").save(root / "images" / "a.jpg")
            Image.new("RGB", (2, 4), "blue").save(root / "images" / "b.jpg")
            # Duplicate business identity and a repeated asset remain separate
            # merged source occurrences. One product is ineligible for price.
            pd.DataFrame(dict(id=[1, 1, 2], price=[100, 100, 600],
                productDisplayName=["shoe", "shoe", "shirt"],
                productDescriptors=["brand A", "brand A", "brand B"])).to_csv(root / "styles.csv", index=False)
            pd.DataFrame(dict(id=[1, 2], filename=["a.jpg", "b.jpg"])).to_csv(root / "mapping.csv", index=False)
            for pattern in ("all_true", "mixed"):
                output = root / pattern
                main(["--sembench-checkout", CHECKOUT, "--data-dir", str(root),
                      "--styles-file", str(root / "styles.csv"), "--mapping-file", str(root / "mapping.csv"),
                      "--image-rows", "2", "--tile-assets", "1", "--max-logical-requests", "128",
                      "--model", "openai/CPU-vision-fixture", "--max-tokens", "8", "--pattern", pattern,
                      "--warmup", "0", "--repeats", "1", "--output-dir", str(output)])
                report = json.loads((output / "profile.json").read_text())
                self.assertEqual(report["actual_model_calls"], 0)
                self.assertEqual(report["source"]["workload"]["merged_occurrences"], 3)
                runs = {r["case"]: r for r in report["runs"]}
                self.assertTrue(all(r["original_join_result_equal"] for r in runs.values()))
                self.assertEqual(len({r["request_audit"]["request_set_sha256"] for r in runs.values()}), 1)
                for run in runs.values():
                    self.assertEqual(run["request_audit"]["stages"]["filter_0"], 3)
                native = runs["branch_native"]["media"]["initial_filters"]
                self.assertGreaterEqual(native["native_index_cache_hits"], 6)
                shared = runs["tile_shared"]["media"]["initial_filters"]
                self.assertGreater(shared["tile_shared_hits"], 0)


if __name__ == "__main__":
    unittest.main()
