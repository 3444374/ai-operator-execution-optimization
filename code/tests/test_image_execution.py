import unittest
import sys
from pathlib import Path

import numpy as np


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.image.execution import EmbeddingAudit, ExecutionResult
from src.image.ray_data_baseline import build_ray_data_clip_pipeline


class EmbeddingAuditTest(unittest.TestCase):
    def test_accepts_streamed_exactly_once_unit_embeddings(self):
        audit = EmbeddingAudit(frozenset({"1", "2"}), dimension=2)
        audit.add(("1",), np.asarray([[1.0, 0.0]], dtype=np.float32))
        audit.add(("2",), np.asarray([[0.0, 1.0]], dtype=np.float32))

        summary = audit.finish()

        self.assertEqual(summary["output_rows"], 2)
        self.assertTrue(summary["exactly_once"])
        self.assertEqual(summary["max_norm_error"], 0.0)
        self.assertEqual(summary["embedding_sum_all"], 2.0)
        self.assertEqual(len(summary["embedding_digest_xor_rounded5"]), 32)

    def test_rejects_duplicate_doc_id(self):
        audit = EmbeddingAudit(frozenset({"1"}), dimension=2)
        audit.add(("1",), np.asarray([[1.0, 0.0]], dtype=np.float32))

        with self.assertRaisesRegex(ValueError, "duplicate"):
            audit.add(("1",), np.asarray([[1.0, 0.0]], dtype=np.float32))

    def test_rejects_missing_doc_id(self):
        audit = EmbeddingAudit(frozenset({"1", "2"}), dimension=2)
        audit.add(("1",), np.asarray([[1.0, 0.0]], dtype=np.float32))

        with self.assertRaisesRegex(ValueError, "exactly-once"):
            audit.finish()


class ExecutionResultTest(unittest.TestCase):
    def test_rejects_first_output_after_total(self):
        with self.assertRaisesRegex(ValueError, "first_output"):
            ExecutionResult(total_s=1.0, first_output_s=2.0, audit={})

    def test_legacy_batch_service_alias_has_explicit_completion_semantics(self):
        result = ExecutionResult(
            total_s=2.0,
            first_output_s=1.0,
            audit={},
            batch_completion_wall_s=(0.2, 0.4),
            batch_actor_service_s=(0.1, 0.1),
            submitted_batches=2,
            pending_batches_peak=2,
        )

        self.assertEqual(result.batch_service_s, (0.2, 0.4))

    def test_rejects_negative_byte_counts(self):
        with self.assertRaisesRegex(ValueError, "byte counts"):
            ExecutionResult(
                total_s=1.0,
                first_output_s=1.0,
                audit={},
                encoded_bytes=-1,
            )

    def test_rejects_pending_peak_above_submitted_batches(self):
        with self.assertRaisesRegex(ValueError, "pending batch peak"):
            ExecutionResult(
                total_s=1.0,
                first_output_s=1.0,
                audit={},
                submitted_batches=1,
                pending_batches_peak=2,
            )


class RayDataBaselineValidationTest(unittest.TestCase):
    def test_rejects_active_batch_limit_below_gpu_count(self):
        from src.image.source import ImageSourceConfig

        with self.assertRaisesRegex(ValueError, "at least gpu_workers"):
            build_ray_data_clip_pipeline(
                database_url="postgresql://example",
                source_config=ImageSourceConfig("coco", limit=8),
                source_shards=2,
                processor_revision="processor",
                model_revision="model",
                dtype="float16",
                batch_size=4,
                cpu_workers=2,
                gpu_workers=2,
                max_active_batches=1,
            )

if __name__ == "__main__":
    unittest.main()
