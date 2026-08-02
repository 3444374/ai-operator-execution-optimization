from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import numpy as np

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))
SCRIPTS_ROOT = CODE_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from src.modalities.image.clip import (  # noqa: E402
    configure_torch_thread_pools,
    extract_clip_image_features,
)
from src.modalities.image.contracts import (  # noqa: E402
    EmbeddingSemantics,
    ImageBatchTelemetry,
    ImageEmbeddingBatch,
    ImageEmbeddingResult,
)
from src.modalities.image.source import (  # noqa: E402
    ImageSourceConfig,
    image_documents_query,
    split_image_source_config,
)
from scripts.data.import_coco_images import (  # noqa: E402
    coco_doc_id,
    list_zip_images,
    primary_key_columns,
)


class ImageContractTests(unittest.TestCase):
    def test_torch_thread_contract_rejects_non_positive_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            configure_torch_thread_pools(0, 1)

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

    def test_image_source_query_can_repeat_unique_images_with_execution_ids(self) -> None:
        query = image_documents_query(
            ImageSourceConfig("coco", limit=32, offset=4, dataset_passes=2)
        )

        self.assertIn("generate_series(1, 2)", query)
        self.assertIn("'#pass='", query)
        self.assertIn("LIMIT 32 OFFSET 4", query)

    def test_image_source_shards_cover_range_without_overlap(self) -> None:
        shards = split_image_source_config(
            ImageSourceConfig("coco", limit=10, offset=7),
            shards=3,
        )

        self.assertEqual(
            [(item.offset, item.limit, item.dataset_passes) for item in shards],
            [(7, 4, 1), (11, 3, 1), (14, 3, 1)],
        )

    def test_image_source_shards_preserve_dataset_passes(self) -> None:
        shards = split_image_source_config(
            ImageSourceConfig("coco", limit=10, dataset_passes=3),
            shards=2,
        )

        self.assertEqual([item.dataset_passes for item in shards], [3, 3])

    def test_coco_import_preserves_numeric_source_id(self) -> None:
        self.assertEqual(coco_doc_id(Path("000000123456.jpg")), 123456)

    def test_coco_zip_import_is_sorted_limited_and_not_extracted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            archive_path = Path(directory) / "images.zip"
            with ZipFile(archive_path, "w") as archive:
                archive.writestr("train2017/000000000003.jpg", b"three")
                archive.writestr("train2017/000000000001.jpg", b"one")
                archive.writestr("train2017/README.txt", b"ignored")

            with ZipFile(archive_path) as archive:
                paths = list_zip_images(archive, "*.jpg", limit=1)
                payload = archive.read(str(paths[0]))

            self.assertEqual(str(paths[0]), "train2017/000000000001.jpg")
            self.assertEqual(coco_doc_id(paths[0]), 1)
            self.assertEqual(payload, b"one")
            self.assertFalse((Path(directory) / "train2017").exists())

    def test_coco_import_reads_ordered_workload_scoped_primary_key(self) -> None:
        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def execute(self, _query, parameters):
                self.parameters = parameters

            def fetchall(self):
                return [("workload_name",), ("doc_id",)]

        class Connection:
            def __init__(self):
                self.current = Cursor()

            def cursor(self):
                return self.current

        connection = Connection()
        columns = primary_key_columns(connection, "image_documents")

        self.assertEqual(columns, ("workload_name", "doc_id"))
        self.assertEqual(connection.current.parameters, ("image_documents",))


if __name__ == "__main__":
    unittest.main()
