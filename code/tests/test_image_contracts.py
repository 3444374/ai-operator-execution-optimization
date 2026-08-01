from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

import numpy as np

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))
SCRIPTS_ROOT = CODE_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from src.image.clip import extract_clip_image_features  # noqa: E402
from src.image.contracts import (  # noqa: E402
    EmbeddingSemantics,
    ImageBatchTelemetry,
    ImageEmbeddingBatch,
    ImageEmbeddingResult,
)
from src.image.source import (  # noqa: E402
    ImageSourceConfig,
    image_documents_query,
    split_image_source_config,
)
from import_coco_images import coco_doc_id  # noqa: E402


class ImageContractTests(unittest.TestCase):
    def test_batch_telemetry_rejects_negative_measurements(self) -> None:
        with self.assertRaisesRegex(ValueError, "byte counts"):
            ImageBatchTelemetry(encoded_bytes=-1)

    def test_embedding_result_validates_rows_dimension_and_finite_values(self) -> None:
        semantics = EmbeddingSemantics("clip-rev", "processor-rev", 2)
        result = ImageEmbeddingResult(
            doc_ids=("1", "2"),
            embeddings=np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
            semantics=semantics,
            service_s=0.1,
        )

        self.assertEqual(result.embeddings.shape, (2, 2))

    def test_image_batch_requires_positive_modality_work(self) -> None:
        with self.assertRaisesRegex(ValueError, "work_units"):
            ImageEmbeddingBatch(
                doc_ids=("1",),
                payload=np.zeros((1, 3, 224, 224), dtype=np.float32),
                input_kind="preprocessed_tensor",
                work_units=0,
                work_unit="pixels",
            )

    def test_clip_v5_pooler_output_is_selected(self) -> None:
        projected = np.ones((2, 512), dtype=np.float32)
        output = SimpleNamespace(pooler_output=projected)

        self.assertIs(extract_clip_image_features(output), projected)

    def test_image_source_query_escapes_workload_without_driver_collect(self) -> None:
        query = image_documents_query(
            ImageSourceConfig("coco_'heldout", limit=32, offset=4)
        )

        self.assertIn("coco_''heldout", query)
        self.assertIn("LIMIT 32 OFFSET 4", query)
        self.assertNotIn("to_arrow", query)

    def test_image_source_shards_cover_range_without_overlap(self) -> None:
        shards = split_image_source_config(
            ImageSourceConfig("coco", limit=10, offset=7),
            shards=3,
        )

        self.assertEqual(
            [(item.offset, item.limit) for item in shards],
            [(7, 4), (11, 3), (14, 3)],
        )

    def test_coco_import_preserves_numeric_source_id(self) -> None:
        self.assertEqual(coco_doc_id(Path("000000123456.jpg")), 123456)


if __name__ == "__main__":
    unittest.main()
