import unittest
import sys
import importlib.util
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
from src.modalities.image.clip import l2_normalize_numpy_embeddings
from src.baselines.image.frameworks.ray_data import build_ray_data_clip_pipeline


RUNNER_PATH = CODE_ROOT / "scripts" / "experiments" / "run_image_clip_e2e.py"
RUNNER_SPEC = importlib.util.spec_from_file_location("image_clip_e2e_runner", RUNNER_PATH)
assert RUNNER_SPEC is not None and RUNNER_SPEC.loader is not None
RUNNER_MODULE = importlib.util.module_from_spec(RUNNER_SPEC)
RUNNER_SPEC.loader.exec_module(RUNNER_MODULE)


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


class EmbeddingNormalizationTest(unittest.TestCase):
    def test_numpy_normalization_preserves_rows_and_uses_float32(self):
        matrix = np.asarray([[3.0, 4.0], [0.0, 2.0]], dtype=np.float64)

        normalized = l2_normalize_numpy_embeddings(matrix)

        self.assertEqual(normalized.dtype, np.float32)
        np.testing.assert_allclose(
            normalized,
            np.asarray([[0.6, 0.8], [0.0, 1.0]], dtype=np.float32),
            rtol=1e-6,
        )

    def test_numpy_normalization_rejects_non_matrix(self):
        with self.assertRaisesRegex(ValueError, "two-dimensional"):
            l2_normalize_numpy_embeddings(np.asarray([1.0, 2.0]))


class ImageRunnerSchemaTest(unittest.TestCase):
    def test_output_contract_fields_are_declared_in_csv_schema(self):
        expected = {
            "embedding_output_contract_requested",
            "embedding_output_contract_effective",
            "embedding_normalization_in_timed_boundary",
            "embedding_normalization_owner",
            "first_output_fraction_of_e2e",
            "post_first_output_s",
            "steady_state_duration_gate_met",
            "joules_per_1k_images",
            "gpu_seconds_per_image",
            "images_per_cpu_core_second",
            "ray_address_mode",
            "formal_start_epoch_s_planned",
            "formal_start_epoch_s_actual",
            "formal_start_lateness_s",
            "ray_data_actor_pool_mode",
            "source_doc_ids_sha256",
            "source_manifest_match",
            "project_execution_mode",
            "hse_ready_bytes_limit",
            "hse_ready_bytes_peak",
            "batch_ready_residence_p95_s",
        }

        self.assertTrue(expected.issubset(set(RUNNER_MODULE.CSV_FIELDS)))

    def test_output_contract_metadata_matches_effective_arm_output(self):
        self.assertEqual(
            RUNNER_MODULE.embedding_output_contract_metadata(
                "daft_builtin_embed",
                "arm_default",
            ),
            {
                "embedding_output_contract_requested": "arm_default",
                "embedding_output_contract_effective": "provider_raw",
                "embedding_normalization_in_timed_boundary": False,
                "embedding_normalization_owner": "provider_native_output",
            },
        )
        self.assertEqual(
            RUNNER_MODULE.embedding_output_contract_metadata(
                "daft_builtin_embed",
                "l2_normalized",
            )["embedding_normalization_owner"],
            "baseline_adapter",
        )
        self.assertEqual(
            RUNNER_MODULE.embedding_output_contract_metadata(
                "project_ray",
                "arm_default",
            )["embedding_output_contract_effective"],
            "l2_normalized",
        )


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

    def test_native_ray_data_graph_can_use_framework_autoscaling_actor_pools(self):
        import inspect

        source = inspect.getsource(build_ray_data_clip_pipeline)

        self.assertIn("ActorPoolStrategy(min_size=1, max_size=cpu_workers)", source)
        self.assertIn("ActorPoolStrategy(min_size=1, max_size=gpu_workers)", source)


if __name__ == "__main__":
    unittest.main()
