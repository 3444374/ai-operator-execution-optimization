import unittest
import sys
import importlib.util
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, call, patch

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
from src.modalities.image import execution as image_execution
from src.modalities.image import staged_execution as staged_image_execution
from src.modalities.image.clip import l2_normalize_numpy_embeddings
from src.modalities.image.source import ImageSourceConfig
from src.modalities.image import clip_batch_workers
from src.baselines.image.frameworks.ray_data import build_ray_data_clip_pipeline
from src.experiments.image_clip import reporting as image_report
from src.experiments.image_clip import execution as clip_execution


RUNNER_PATH = CODE_ROOT / "scripts" / "experiments" / "run_image_clip_e2e.py"
RUNNER_SPEC = importlib.util.spec_from_file_location("image_clip_e2e_runner", RUNNER_PATH)
assert RUNNER_SPEC is not None and RUNNER_SPEC.loader is not None
RUNNER_MODULE = importlib.util.module_from_spec(RUNNER_SPEC)
RUNNER_SPEC.loader.exec_module(RUNNER_MODULE)


class SemLoomImageNamingContractTest(unittest.TestCase):
    def test_semloom_names_are_behavior_preserving_aliases(self):
        self.assertIs(
            image_execution.SemLoomRayWorkerPool,
            image_execution.ProjectRayWorkerPool,
        )
        self.assertIs(
            image_execution.build_semloom_ray_worker_pool,
            image_execution.build_project_ray_worker_pool,
        )
        self.assertIs(
            image_execution.run_semloom_ray_pipeline,
            image_execution.run_project_ray_pipeline,
        )
        self.assertIs(
            image_execution.stop_semloom_ray_worker_pool,
            image_execution.stop_project_ray_worker_pool,
        )
        self.assertIs(
            staged_image_execution.run_semloom_ray_hse_pipeline,
            staged_image_execution.run_project_ray_hse_pipeline,
        )
        self.assertIn("project_ray", RUNNER_MODULE.ARMS)
        self.assertNotIn("semloom_ray", RUNNER_MODULE.ARMS)


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

        self.assertTrue(expected.issubset(set(image_report.CSV_FIELDS)))

    def test_output_contract_metadata_matches_effective_arm_output(self):
        self.assertEqual(
            image_report.embedding_output_contract_metadata(
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
            image_report.embedding_output_contract_metadata(
                "daft_builtin_embed",
                "l2_normalized",
            )["embedding_normalization_owner"],
            "baseline_adapter",
        )
        self.assertEqual(
            image_report.embedding_output_contract_metadata(
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
    def test_native_graph_uses_framework_pools_for_cpu_and_gpu_stages(self):
        for autoscaling in (False, True):
            with self.subTest(autoscaling=autoscaling):
                dataset = Mock()
                dataset.map_batches.return_value = dataset
                ray, data = ModuleType("ray"), ModuleType("ray.data")
                ray.data = data
                data.read_sql = Mock(return_value=dataset)
                data.ActorPoolStrategy = Mock(side_effect=lambda **kwargs: kwargs)
                psycopg = ModuleType("psycopg")
                psycopg.connect = Mock(side_effect=AssertionError("unexpected SQL execution"))
                with patch.dict(sys.modules, {"ray": ray, "ray.data": data, "psycopg": psycopg}):
                    graph = build_ray_data_clip_pipeline(
                        database_url="postgresql://localhost/fixture",
                        source_config=ImageSourceConfig("fixture", 8),
                        source_shards=1, processor_revision="fixture", model_revision="fixture",
                        dtype="float32", batch_size=4, cpu_workers=2, gpu_workers=3,
                        autoscaling_actor_pools=autoscaling,
                    )
                pools = [{"min_size": 1, "max_size": n} if autoscaling else {"size": n}
                         for n in (2, 3)]
                self.assertIs(graph, dataset)
                self.assertEqual(data.ActorPoolStrategy.call_args_list, [call(**pool) for pool in pools])
                data.read_sql.assert_called_once()
                maps = dataset.map_batches.call_args_list
                self.assertEqual([item.args[0] for item in maps], [
                    clip_batch_workers.RayDataClipPreprocessor, clip_batch_workers.RayDataClipPredictor,
                ])
                for item, pool in zip(maps, pools, strict=True):
                    self.assertEqual(item.kwargs["compute"], pool)
                    self.assertEqual(item.kwargs["batch_size"], 4)
                    self.assertEqual(item.kwargs["num_cpus"], 1)
                self.assertNotIn("num_gpus", maps[0].kwargs)
                self.assertEqual(maps[1].kwargs["num_gpus"], 1)


class ImageRunCleanupTests(unittest.TestCase):
    def test_formal_failure_stops_started_samplers_and_workers(self):
        failure = RuntimeError("formal execution failed")
        execution = Mock()
        execution.execute.side_effect = [None, failure]
        gpu, cpu = Mock(), Mock()
        args = SimpleNamespace(
            gpu_sample_interval_s=0.5, cpu_sample_interval_s=0.25,
            gpu_workers=1, save_embeddings="", limit=2, dataset_passes=1,
        )
        setup = SimpleNamespace(warmup_count=1, warmup_ids=frozenset({"w"}),
                                formal_ids=frozenset({"a", "b"}))
        with patch.multiple(clip_execution, NvidiaSmiSampler=Mock(return_value=gpu),
                            SystemCpuSampler=Mock(return_value=cpu),
                            wait_for_formal_start=Mock(return_value=(0.0, 1.0))):
            with self.assertRaises(RuntimeError) as raised:
                clip_execution.run_observed_image_execution(
                    args, setup, execution, daft_module=None, ray_module=None,
                    torch_module=None, transformers_module=None,
                )
        self.assertIs(raised.exception, failure)
        gpu.stop.assert_called_once_with()
        cpu.stop.assert_called_once_with()
        self.assertEqual(execution.stop_project_workers.call_count, 2)

    def test_startup_capture_and_interrupt_cleanup_keep_normal_order(self):
        for failed_stage in (None, "warmup", "barrier", "gpu_start", "cpu_start",
                             "worker_start", "capture", "formal"):
            with self.subTest(failed_stage=failed_stage):
                events = []
                failure = (KeyboardInterrupt("interrupted") if failed_stage == "formal"
                           else RuntimeError("fixture failure"))

                def action(stage, value=None):
                    def invoke(*args, **kwargs):
                        events.append(stage)
                        if stage == failed_stage:
                            raise failure
                        return value
                    return invoke

                result = object()
                execution = Mock()

                def execute_phase(limit, *args, **kwargs):
                    return action("warmup" if limit == 1 else "formal", result)(*args, **kwargs)

                execution.execute.side_effect = execute_phase
                execution.stop_project_workers.side_effect = action("workers_stop")
                execution.start_formal_workers.side_effect = action("worker_start", 0.125)
                gpu, cpu = Mock(), Mock()
                gpu.start.side_effect = action("gpu_start")
                cpu.start.side_effect = action("cpu_start")
                gpu_metrics, cpu_metrics = {"gpu": 1}, {"cpu": 2}
                gpu.stop.side_effect = action("gpu_stop", gpu_metrics)
                cpu.stop.side_effect = action("cpu_stop", cpu_metrics)
                args = SimpleNamespace(
                    arm="project_ray", embedding_output_contract="arm_default",
                    gpu_sample_interval_s=0.5, cpu_sample_interval_s=0.25,
                    gpu_workers=1, save_embeddings="fixture.npz", limit=2, dataset_passes=1,
                )
                setup = SimpleNamespace(warmup_count=1, warmup_ids=frozenset({"w"}),
                                        formal_ids=frozenset({"a", "b"}))
                metadata = {"output": "fixture"}
                with patch.multiple(
                    clip_execution, NvidiaSmiSampler=Mock(return_value=gpu),
                    SystemCpuSampler=Mock(return_value=cpu),
                    wait_for_formal_start=Mock(side_effect=action("barrier", (0.0, 1.0))),
                    embedding_output_contract_metadata=Mock(return_value=metadata),
                    write_embedding_capture=Mock(side_effect=action("capture")),
                ):
                    if failed_stage is None:
                        observed = clip_execution.run_observed_image_execution(
                            args, setup, execution, daft_module=None, ray_module=None,
                            torch_module=None, transformers_module=None,
                        )
                        self.assertIs(observed.result, result)
                        self.assertIs(observed.gpu_metrics, gpu_metrics)
                        self.assertIs(observed.cpu_metrics, cpu_metrics)
                        self.assertIs(observed.output_contract_metadata, metadata)
                        self.assertEqual(observed.worker_setup_s, 0.125)
                        self.assertEqual((observed.planned_start_epoch_s, observed.actual_start_epoch_s),
                                         (0.0, 1.0))
                        self.assertEqual(events, [
                            "warmup", "workers_stop", "barrier", "gpu_start", "cpu_start",
                            "worker_start", "formal", "capture", "gpu_stop", "cpu_stop", "workers_stop",
                        ])
                    else:
                        with self.assertRaises(type(failure)) as raised:
                            clip_execution.run_observed_image_execution(
                                args, setup, execution, daft_module=None, ray_module=None,
                                torch_module=None, transformers_module=None,
                            )
                        self.assertIs(raised.exception, failure)
                if failed_stage in ("warmup", "barrier", "gpu_start"):
                    gpu.stop.assert_not_called()
                    cpu.stop.assert_not_called()
                elif failed_stage == "cpu_start":
                    self.assertEqual(events[-1:], ["gpu_stop"])
                    cpu.stop.assert_not_called()
                elif failed_stage == "worker_start":
                    self.assertEqual(events[-2:], ["gpu_stop", "cpu_stop"])
                else:
                    self.assertEqual(events[-3:], ["gpu_stop", "cpu_stop", "workers_stop"])
                expected_worker_stops = 2 if failed_stage in (None, "capture", "formal") else 1
                self.assertEqual(execution.stop_project_workers.call_count, expected_worker_stops)

    def test_sampler_stop_error_keeps_other_cleanup_and_original_error(self):
        failure = RuntimeError("formal execution failed")
        cleanup_error = RuntimeError("GPU sampling stop failed")
        execution = Mock()
        execution.execute.side_effect = [None, failure]
        gpu, cpu = Mock(), Mock()
        gpu.stop.side_effect = cleanup_error
        args = SimpleNamespace(
            gpu_sample_interval_s=0.5, cpu_sample_interval_s=0.25,
            gpu_workers=1, save_embeddings="", limit=2, dataset_passes=1,
        )
        setup = SimpleNamespace(warmup_count=1, warmup_ids=frozenset({"w"}),
                                formal_ids=frozenset({"a", "b"}))
        with patch.multiple(clip_execution, NvidiaSmiSampler=Mock(return_value=gpu),
                            SystemCpuSampler=Mock(return_value=cpu),
                            wait_for_formal_start=Mock(return_value=(0.0, 1.0))):
            with self.assertRaises(RuntimeError) as raised:
                clip_execution.run_observed_image_execution(
                    args, setup, execution, daft_module=None, ray_module=None,
                    torch_module=None, transformers_module=None,
                )
        self.assertIs(raised.exception, cleanup_error)
        self.assertIs(raised.exception.__context__, failure)
        gpu.stop.assert_called_once_with()
        cpu.stop.assert_called_once_with()
        self.assertEqual(execution.stop_project_workers.call_count, 2)

    def test_cli_disconnects_after_initialization_execution_and_report_errors(self):
        for failed_stage in (None, "initialize", "execute", "row", "csv", "manifest"):
            with self.subTest(failed_stage=failed_stage):
                failure = RuntimeError("fixture failure")
                modules = {name: ModuleType(name) for name in ("daft", "ray", "torch", "transformers")}
                ray = modules["ray"]
                ray.is_initialized = Mock(return_value=True)
                ray.shutdown = Mock()
                args = SimpleNamespace(out_csv="fixture.csv", out_manifest="fixture.json")
                hooks = {
                    "parse_args": Mock(return_value=args),
                    "validate_image_run_args": Mock(return_value=(None, None, None, None)),
                    "load_image_run_setup": Mock(),
                    "ImageExecution": Mock(),
                    "run_observed_image_execution": Mock(),
                    "build_image_result_row": Mock(return_value={"row": "fixture"}),
                    "append_csv": Mock(),
                    "build_image_manifest": Mock(return_value={"manifest": "fixture"}),
                    "Path": Mock(),
                }
                failure_hook = {"initialize": "ImageExecution", "execute": "run_observed_image_execution",
                                "row": "build_image_result_row", "csv": "append_csv"}
                if failed_stage in failure_hook:
                    hooks[failure_hook[failed_stage]].side_effect = failure
                if failed_stage == "manifest":
                    hooks["Path"].return_value.write_text.side_effect = failure
                with patch.dict(sys.modules, modules), patch.multiple(RUNNER_MODULE, **hooks), patch("builtins.print"):
                    if failed_stage is None:
                        RUNNER_MODULE.main()
                    else:
                        with self.assertRaises(RuntimeError) as raised:
                            RUNNER_MODULE.main()
                        self.assertIs(raised.exception, failure)
                ray.shutdown.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
