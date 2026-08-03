from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
SCRIPTS_ROOT = CODE_ROOT / "scripts"
for path in (CODE_ROOT, SCRIPTS_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from scripts.profiling.profile_image_clip_preprocess_variants import (  # noqa: E402
    _parity,
    _payload_work_units,
)


class ImageClipPreprocessVariantTests(unittest.TestCase):
    def test_payload_work_uses_rows_and_spatial_shape(self) -> None:
        payload = np.zeros((2, 3, 224, 224), dtype=np.float32)

        self.assertEqual(_payload_work_units(payload, 2), 2 * 224 * 224)

    def test_embedding_parity_reports_cosine_and_max_error(self) -> None:
        # Identical vectors need cosine=1 even when their norms are not exactly 1.
        reference = np.asarray([[0.5, 0.0], [0.0, 0.75]], dtype=np.float32)
        candidate = reference.copy()

        mean_cosine, min_cosine, max_abs = _parity(candidate, reference)

        self.assertEqual(mean_cosine, 1.0)
        self.assertEqual(min_cosine, 1.0)
        self.assertEqual(max_abs, 0.0)


if __name__ == "__main__":
    unittest.main()
