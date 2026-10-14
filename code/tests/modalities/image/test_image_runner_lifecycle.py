"""CLI owner lifecycle checks with no SDK, model, sampler threads or GPU."""

import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

from src.modalities.image.execution import ExecutionResult, ProjectRayWorkerPool
from tests.modalities.image.test_image_file_source import RUNNER


class _RayClient:
    def __init__(self, initialized=False):
        self.initialized = initialized
        self.shutdowns = 0

    def is_initialized(self): return self.initialized
    def init(self, **kwargs): self.initialized = True
    def shutdown(self): self.shutdowns += 1; self.initialized = False


class ImageRunnerLifecycleTest(unittest.TestCase):
    def args(self):
        with patch.object(sys, "argv", ["runner", "--arm", "project_ray", "--model", "fixture",
                                        "--pg-dsn", "fixture", "--limit", "2", "--cpu-workers", "1",
                                        "--gpu-workers", "1", "--out-csv", "/tmp/fixture-runs.csv",
                                        "--out-manifest", "/tmp/fixture-run.json"]):
            return RUNNER.parse_args()

    def setup(self):
        return SimpleNamespace(processor="fixture", ray_cluster_num_cpus=2, worker_runtime_env={}, source_cpu_threads=1,
                               warmup_count=1, warmup_ids=frozenset({"1"}), formal_ids=frozenset({"1", "2"}))

    def execution(self, ray=None):
        ray = ray or _RayClient()
        pool = ProjectRayWorkerPool((object(),), (object(),))
        with patch.object(RUNNER, "build_semloom_ray_worker_pool", return_value=pool):
            execution = RUNNER._ImageExecution(self.args(), self.setup(),
                                               daft_module=SimpleNamespace(set_runner_native=Mock()), ray_module=ray)
        return execution, pool, ray

    def test_failed_hse_pool_cannot_switch_to_direct_or_restart_after_cleanup_failure(self):
        execution, pool, ray = self.execution()
        execution.args.project_execution_mode = "hse_static"
        error = RuntimeError("fixture HSE failure")
        retained = object()
        error.hse_unconfirmed_work = retained
        with patch.object(execution, "_execute_once", side_effect=error):
            with self.assertRaises(RuntimeError) as raised: execution.execute(2, frozenset({"1", "2"}))
        self.assertIs(raised.exception, error)
        self.assertIs(error.hse_unconfirmed_work, retained)
        self.assertIs(error.image_execution_owner, execution)
        execution.args.project_execution_mode = "direct_dependency"
        with self.assertRaisesRegex(RuntimeError, "must not reuse"):
            execution.execute(2, frozenset({"1", "2"}))
        with self.assertRaisesRegex(RuntimeError, "cannot start"):
            execution.start_formal_workers()
        with patch.object(RUNNER, "stop_semloom_ray_worker_pool", side_effect=RuntimeError("fixture stop failure")):
            with self.assertRaisesRegex(RuntimeError, "stop failure"): execution.close()
        self.assertIs(execution.worker_pool, pool)
        self.assertEqual(ray.shutdowns, 1)
        self.assertFalse(ray.initialized)

    def test_cold_formal_workers_require_disposing_warmup_pool_first(self):
        execution, pool, ray = self.execution()
        with self.assertRaisesRegex(RuntimeError, "dispose the warmup"):
            execution.start_formal_workers()
        formal = ProjectRayWorkerPool((object(),), (object(),))
        with patch.object(RUNNER, "stop_semloom_ray_worker_pool") as stop, \
             patch.object(execution, "_build_project_workers", return_value=formal):
            execution.stop_project_workers()
            self.assertIsNone(execution.worker_pool)
            execution.start_formal_workers()
            self.assertIs(execution.worker_pool, formal)
            execution.close()
        self.assertEqual(stop.call_args_list[0].args, (pool,))
        self.assertEqual(stop.call_args_list[1].args, (formal,))
        self.assertEqual(ray.shutdowns, 1)
        with self.assertRaisesRegex(RuntimeError, "cannot start"):
            execution.start_formal_workers()

    def test_initialization_failure_disconnects_owned_client_and_preserves_existing_client(self):
        ray = _RayClient()
        error = RuntimeError("fixture model-worker readiness failure")
        with patch.object(RUNNER, "build_semloom_ray_worker_pool", side_effect=error):
            with self.assertRaises(RuntimeError) as raised:
                RUNNER._ImageExecution(self.args(), self.setup(),
                                      daft_module=SimpleNamespace(set_runner_native=Mock()), ray_module=ray)
        self.assertIs(raised.exception, error)
        self.assertEqual(ray.shutdowns, 1)
        self.assertTrue(error.image_execution_owner._closed)
        shared = _RayClient(initialized=True)
        with self.assertRaisesRegex(RuntimeError, "uninitialized Ray client"):
            self.execution(shared)
        self.assertEqual(shared.shutdowns, 0)
        self.assertTrue(shared.initialized)

    def test_warmup_and_formal_errors_cleanup_samplers_and_preserve_original_error(self):
        for phase in ("warmup", "formal"):
            with self.subTest(phase=phase):
                error = RuntimeError(f"fixture {phase} failure")
                result = ExecutionResult(3, 1, {"exactly_once": True})
                execution = SimpleNamespace(execute=Mock(side_effect=[error] if phase == "warmup" else [result, error]),
                                            stop_project_workers=Mock(), start_formal_workers=Mock(return_value=0),
                                            source_open_s=0)
                gpu, cpu = Mock(), Mock()
                gpu.stop.side_effect = ValueError("fixture sampler stop failure")
                with patch.object(RUNNER, "NvidiaSmiSampler", return_value=gpu) as gpu_factory, \
                     patch.object(RUNNER, "SystemCpuSampler", return_value=cpu), \
                     patch.object(RUNNER, "_wait_for_formal_start", return_value=(0, 0)):
                    with self.assertRaises(RuntimeError) as raised:
                        RUNNER._run_observed_image_execution(self.args(), self.setup(), execution,
                                                            daft_module=None, ray_module=None,
                                                            torch_module=None, transformers_module=None)
                self.assertIs(raised.exception, error)
                self.assertEqual(execution.stop_project_workers.call_count, 1 if phase == "warmup" else 2)
                if phase == "warmup":
                    gpu_factory.assert_not_called()
                else:
                    gpu.stop.assert_called_once()
                    cpu.stop.assert_called_once()
                    self.assertTrue(any("gpu_sampler" in note for note in error.__notes__))

    def test_success_cleans_up_before_returning_metrics(self):
        result = ExecutionResult(3, 1, {"exactly_once": True})
        execution = SimpleNamespace(execute=Mock(return_value=result), stop_project_workers=Mock(),
                                    start_formal_workers=Mock(return_value=0.5), source_open_s=0.25)
        gpu, cpu = Mock(), Mock()
        gpu.stop.return_value = {"gpu_fixture": 1}
        cpu.stop.return_value = {"cpu_fixture": 1}
        with patch.object(RUNNER, "NvidiaSmiSampler", return_value=gpu), \
             patch.object(RUNNER, "SystemCpuSampler", return_value=cpu), \
             patch.object(RUNNER, "_wait_for_formal_start", return_value=(0, 0)):
            observed = RUNNER._run_observed_image_execution(self.args(), self.setup(), execution,
                                                          daft_module=None, ray_module=None,
                                                          torch_module=None, transformers_module=None)
        self.assertIs(observed.result, result)
        self.assertEqual(observed.worker_setup_s, 0.5)
        self.assertEqual(observed.gpu_metrics, {"gpu_fixture": 1})
        self.assertEqual(observed.cpu_metrics, {"cpu_fixture": 1})
        self.assertEqual(execution.stop_project_workers.call_count, 2)

    def test_main_closes_owner_on_row_failure_before_writing_results(self):
        modules = {name: ModuleType(name) for name in ("daft", "ray", "torch", "transformers")}
        execution = SimpleNamespace(close=Mock())
        error = RuntimeError("fixture result-row failure")
        with patch.dict(sys.modules, modules), \
             patch.object(RUNNER, "parse_args", return_value=self.args()), \
             patch.object(RUNNER, "_validate_image_run_args", return_value=("fixture", None, None, None)), \
             patch.object(RUNNER, "_load_image_run_setup", return_value=self.setup()), \
             patch.object(RUNNER, "_ImageExecution", return_value=execution), \
             patch.object(RUNNER, "_run_observed_image_execution", return_value=object()), \
             patch.object(RUNNER, "_build_image_result_row", side_effect=error), \
             patch.object(RUNNER, "append_csv") as append:
            with self.assertRaises(RuntimeError) as raised: RUNNER.main()
        self.assertIs(raised.exception, error)
        execution.close.assert_called_once()
        append.assert_not_called()


if __name__ == "__main__":
    unittest.main()
