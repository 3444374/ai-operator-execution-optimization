import unittest
import sys
from pathlib import Path

import numpy as np


CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.image.execution import (
    EmbeddingAudit,
    EmbeddingCapture,
    ExecutionResult,
)
from src.baselines.image.frameworks.ray_data import build_ray_data_clip_pipeline


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

    def test_optional_capture_retains_validated_chunks(self):
        capture = EmbeddingCapture()
        audit = EmbeddingAudit(
            frozenset({"1", "2"}),
            dimension=2,
            capture=capture,
        )
        first = np.asarray([[1.0, 0.0]], dtype=np.float32)
        audit.add(("1",), first)
        first[0, 0] = 9.0
        audit.add(("2",), np.asarray([[0.0, 1.0]], dtype=np.float32))

        audit.finish()
        doc_ids, embeddings = capture.finish()

        self.assertEqual(doc_ids, ("1", "2"))
        np.testing.assert_array_equal(
            embeddings,
            np.asarray([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
        )

    def test_default_audit_does_not_retain_embeddings(self):
        audit = EmbeddingAudit(frozenset({"1"}), dimension=2)
        audit.add(("1",), np.asarray([[1.0, 0.0]], dtype=np.float32))

        audit.finish()

        self.assertIsNone(audit.capture)


class ExecutionResultTest(unittest.TestCase):
    def test_accepts_driver_stage_timings(self):
        result = ExecutionResult(
            total_s=1.0,
            first_output_s=0.5,
            audit={},
            batch_source_next_s=(0.1,),
            batch_driver_materialize_s=(0.02,),
            batch_submit_s=(0.03,),
        )

        self.assertEqual(result.batch_source_next_s, (0.1,))

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
    def test_native_ray_data_graph_has_no_project_active_batch_knob(self):
        import inspect

        parameters = inspect.signature(build_ray_data_clip_pipeline).parameters

        self.assertNotIn("max_active_batches", parameters)

    def test_native_ray_data_graph_contains_no_project_task_scheduler(self):
        import inspect

        source = inspect.getsource(build_ray_data_clip_pipeline)

        for forbidden in (
            "max_tasks_in_flight_per_actor",
            "ray.wait",
            "ray.get",
            "@ray.remote",
        ):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
