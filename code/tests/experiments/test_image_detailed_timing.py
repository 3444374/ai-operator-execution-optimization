"""Image diagnostics preserve all six arm protocols without SDKs or models."""
from contextlib import ExitStack, contextmanager
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.experiments import execution_timing
from src.modalities.image.execution import ExecutionResult
from tests.modalities.image import test_image_runner_lifecycle as lifecycle

RUNNER = lifecycle.RUNNER


class _MemoryWriter:
    def __init__(self, _path): self.events = []
    def record(self, event): self.events.append(event)
    def close(self): pass


class ImageDetailedTimingTest(unittest.TestCase):
    def args(self, arm):
        args = lifecycle.ImageRunnerLifecycleTest().args()
        args.arm = arm
        return args

    def setup(self):
        setup = lifecycle.ImageRunnerLifecycleTest().setup()
        setup.file_manifest = None
        setup.source_shards = 2
        setup.model_workers = 1
        setup.gpus_per_model_worker = 1
        setup.database_metadata = {"max_encoded_bytes": 10}
        setup.hse_limits = object()
        return setup

    @contextmanager
    def fixture(self, arm, root, *, enabled=True, mode="direct_dependency"):
        result = ExecutionResult(3, 1, {"exactly_once": True})
        trace = execution_timing.Timing(root, writer_factory=_MemoryWriter)
        args, setup, ray = self.args(arm), self.setup(), lifecycle._RayClient()
        args.project_execution_mode = mode
        daft = SimpleNamespace(set_runner_native=Mock(), set_runner_ray=Mock())
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, {"SEMLOOM_DETAILED_TIMING_DIR": str(root) if enabled else ""}))
            stack.enter_context(patch.object(execution_timing, "_shared", trace))
            for name, value in (
                ("build_semloom_ray_worker_pool", object()), ("build_daft_clip_embedder", object()),
                ("build_daft_staged_clip_pipeline", (object(), object())), ("make_source", object()),
                ("build_ray_data_clip_pipeline", object()), ("stop_semloom_ray_worker_pool", None),
            ):
                stack.enter_context(patch.object(RUNNER, name, return_value=value))
            runs = {}
            for name in ("run_daft_builtin_image_embedding", "run_daft_clip_baseline",
                         "run_daft_staged_clip_baseline", "run_ray_data_clip_baseline",
                         "run_semloom_ray_pipeline", "run_semloom_ray_hse_pipeline"):
                runs[name] = stack.enter_context(patch.object(RUNNER, name, return_value=result))
            execution = RUNNER._ImageExecution(args, setup, daft_module=daft, ray_module=ray)
            try:
                yield SimpleNamespace(result=result, trace=trace, execution=execution,
                                      runs=runs, args=args, setup=setup, ray=ray)
            finally:
                execution.close()
                trace.close()

    def test_every_arm_has_optional_outer_boundaries_and_unchanged_execution_result(self):
        for arm in RUNNER.ARMS:
            modes = ("direct_dependency", "hse_static") if arm == "project_ray" else ("direct_dependency",)
            for mode in modes:
                for enabled in (False, True):
                    with self.subTest(arm=arm, mode=mode, enabled=enabled), TemporaryDirectory() as root:
                        with self.fixture(arm, root, enabled=enabled, mode=mode) as fixture:
                            result = fixture.execution.execute(2, frozenset({"1", "2"}))
                            self.assertIs(result, fixture.result)
                            self.assertEqual(result.total_s, 3)
                            self.assertEqual(result.first_output_s, 1)
                            self.assertEqual(sum(run.call_count for run in fixture.runs.values()), 1)
                            self.assertFalse(fixture.args.detailed_stage_timing)
                        events = fixture.trace.writer.events
                        self.assertEqual(fixture.ray.shutdowns, 0 if arm == "daft_native" else 1)
                        if not enabled:
                            self.assertEqual(events, [])
                            continue
                        operations = {event["operation"] for event in events}
                        self.assertTrue({"image.arm_initialize", "image.source_graph_build", "image.execute",
                                         "image.full_output_audit_returned", "image.execution_close"} <= operations)
                        unavailable = [event for event in events if event["operation"] == "image.boundary_unavailable"]
                        self.assertEqual({event["source_event"] for event in unavailable}, {"first_batch", "raw_eof"})
                        self.assertTrue(all(event["status"] == "unknown" for event in unavailable))
                        self.assertTrue(all(event["arm"] == arm for event in events))
                        self.assertFalse({"image.first_batch", "image.raw_eof"} & operations)

    def test_warmup_and_formal_use_separate_query_identity(self):
        with TemporaryDirectory() as root, self.fixture("project_ray", root) as fixture:
            gpu, cpu = Mock(), Mock()
            gpu.stop.return_value, cpu.stop.return_value = {}, {}
            with patch.object(RUNNER, "NvidiaSmiSampler", return_value=gpu), \
                    patch.object(RUNNER, "SystemCpuSampler", return_value=cpu), \
                    patch.object(RUNNER, "_wait_for_formal_start", return_value=(0, 0)):
                observed = RUNNER._run_observed_image_execution(
                    fixture.args, fixture.setup, fixture.execution, daft_module=None,
                    ray_module=None, torch_module=None, transformers_module=None)
            self.assertIs(observed.result, fixture.result)
            starts = [event for event in fixture.trace.writer.events
                      if event["operation"] == "image.execute" and event["event"] == "span_started"]
            self.assertEqual([event["phase"] for event in starts], ["warmup", "formal"])
            self.assertNotEqual(starts[0]["query_id"], starts[1]["query_id"])
            cleanup = [event for event in fixture.trace.writer.events
                       if event["operation"] == "image.project_worker_cleanup_call" and event["event"] == "span_started"]
            self.assertEqual([event["query_id"] for event in cleanup], [event["query_id"] for event in starts])

    def test_observation_failure_cannot_replace_the_original_execution_error(self):
        primary = ValueError("fixture original execution error")
        with TemporaryDirectory() as root, self.fixture("daft_native", root) as fixture:
            with patch.object(fixture.trace.writer, "record", side_effect=OSError("fixture timing failure")), \
                    patch.object(fixture.execution, "_execute_once", side_effect=primary):
                with self.assertRaises(ValueError) as raised:
                    fixture.execution.execute(2, frozenset({"1", "2"}))
            self.assertIs(raised.exception, primary)
            self.assertTrue(fixture.execution._failed)
            self.assertIs(primary.image_execution_owner, fixture.execution)
            self.assertTrue(any("timing" in note.lower() for note in primary.__notes__))

    def test_cli_finalizes_diagnostics_after_owner_cleanup_and_preserves_first_error(self):
        cases = ((None, False), (None, True), (ValueError("fixture row failure"), True))
        for primary, close_failure in cases:
            with self.subTest(primary=type(primary).__name__, close_failure=close_failure), TemporaryDirectory() as root:
                trace = execution_timing.Timing(root, writer_factory=_MemoryWriter)
                args = self.args("project_ray")
                args.out_manifest = str(Path(root) / "manifest.json")
                execution = SimpleNamespace(close=Mock())

                def close():
                    execution.close.assert_called_once()
                    if close_failure:
                        raise OSError("fixture timing close failure")

                modules = {name: ModuleType(name) for name in ("daft", "ray", "torch", "transformers")}
                with ExitStack() as stack:
                    stack.enter_context(patch.dict(os.environ, {"SEMLOOM_DETAILED_TIMING_DIR": root}))
                    stack.enter_context(patch.object(execution_timing, "_shared", trace))
                    stack.enter_context(patch.dict(sys.modules, modules))
                    stack.enter_context(patch.object(trace.writer, "close", side_effect=close))
                    for name, value in (
                        ("parse_args", args), ("_validate_image_run_args", ("fixture", None, None, None)),
                        ("_load_image_run_setup", self.setup()), ("_ImageExecution", execution),
                        ("_run_observed_image_execution", object()), ("_build_image_manifest", {}),
                        ("append_csv", None),
                    ):
                        stack.enter_context(patch.object(RUNNER, name, return_value=value))
                    stack.enter_context(patch("builtins.print"))
                    stack.enter_context(patch.object(RUNNER, "_build_image_result_row",
                                                    return_value={}, side_effect=primary))
                    if primary or close_failure:
                        with self.assertRaises(type(primary) if primary else OSError) as raised:
                            RUNNER.main()
                    else:
                        RUNNER.main()
                    self.assertTrue(trace.closed)
                    if primary:
                        self.assertIs(raised.exception, primary)
                        self.assertTrue(any("timing close" in note.lower() for note in primary.__notes__))
                    elif not close_failure:
                        self.assertTrue(trace.path.with_suffix(".closed.json").is_file())

    def test_init_end_write_failure_still_stops_the_returned_worker_pool(self):
        with TemporaryDirectory() as root:
            trace = execution_timing.Timing(root, writer_factory=_MemoryWriter)
            record = trace.writer.record

            def fail_factory_end(event):
                if event["operation"] == "image.project_worker_initialize" and event["event"] == "span_ended":
                    raise OSError("fixture factory timing write failed")
                record(event)

            ray, pool = lifecycle._RayClient(), object()
            daft = SimpleNamespace(set_runner_native=Mock())
            with patch.dict(os.environ, {"SEMLOOM_DETAILED_TIMING_DIR": root}), \
                    patch.object(execution_timing, "_shared", trace), \
                    patch.object(trace.writer, "record", side_effect=fail_factory_end), \
                    patch.object(RUNNER, "build_semloom_ray_worker_pool", return_value=pool), \
                    patch.object(RUNNER, "stop_semloom_ray_worker_pool") as stop:
                with self.assertRaisesRegex(OSError, "factory timing write failed") as caught:
                    RUNNER._ImageExecution(self.args("project_ray"), self.setup(),
                                           daft_module=daft, ray_module=ray)
                stop.assert_called_once_with(pool)
                self.assertEqual(ray.shutdowns, 1)
                owner = caught.exception.image_execution_owner
                self.assertIsNone(owner.worker_pool)
                self.assertTrue(owner._failed)
            trace.close()

    def test_formal_factory_end_write_failure_keeps_handles_for_close(self):
        with TemporaryDirectory() as root, self.fixture("project_ray", root) as fixture:
            fixture.execution.stop_project_workers()
            pool, record = object(), fixture.trace.writer.record

            def fail_factory_end(event):
                if event["operation"] == "image.project_worker_initialize" and event["event"] == "span_ended":
                    raise OSError("fixture formal timing write failed")
                record(event)

            with patch.object(fixture.trace.writer, "record", side_effect=fail_factory_end), \
                    patch.object(RUNNER, "build_semloom_ray_worker_pool", return_value=pool), \
                    patch.object(RUNNER, "stop_semloom_ray_worker_pool") as stop:
                with self.assertRaisesRegex(OSError, "formal timing write failed"):
                    fixture.execution.start_formal_workers()
                self.assertIs(fixture.execution.worker_pool, pool)
                self.assertTrue(fixture.execution._failed)
                fixture.execution.close()
                stop.assert_called_once_with(pool)

    def test_real_cleanup_failure_takes_priority_over_diagnostic_failure(self):
        for prior_timing_failure in (False, True):
            with self.subTest(prior_timing_failure=prior_timing_failure):
                observation = execution_timing._timing_error(OSError("fixture writer failed"))
                actual = RuntimeError("fixture worker termination failed")
                disconnect = Mock(return_value="disconnected")
                with self.assertRaises(RuntimeError) as caught:
                    RUNNER._cleanup_image_run([
                        ("diagnostic", Mock(side_effect=observation)),
                        ("worker_pool", Mock(side_effect=actual)),
                        ("owned_ray", disconnect),
                    ], error=observation if prior_timing_failure else None)
                self.assertIs(caught.exception, actual)
                disconnect.assert_called_once()
                self.assertTrue(any("OSError" in note for note in actual.__notes__))

    def test_execution_error_or_cancel_stays_primary_when_cleanup_also_fails(self):
        for primary in (ValueError("fixture execution failed"), KeyboardInterrupt("fixture cancel")):
            with self.subTest(primary=type(primary).__name__):
                observation = execution_timing._timing_error(OSError("fixture writer failed"))
                actual = RuntimeError("fixture worker termination failed")
                disconnect = Mock(return_value="disconnected")
                result = RUNNER._cleanup_image_run([
                    ("diagnostic", Mock(side_effect=observation)),
                    ("worker_pool", Mock(side_effect=actual)),
                    ("owned_ray", disconnect),
                ], error=primary)
                self.assertEqual(result, {"owned_ray": "disconnected"})
                disconnect.assert_called_once()
                self.assertTrue(any("OSError" in note for note in primary.__notes__))
                self.assertTrue(any("RuntimeError" in note for note in primary.__notes__))


if __name__ == "__main__":
    unittest.main()
