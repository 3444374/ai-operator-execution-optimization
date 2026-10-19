"""Async CLI configuration/dispatch contracts; the actor/Core seam is a fixture here."""

from contextlib import contextmanager, redirect_stdout
from copy import deepcopy
from dataclasses import asdict
import io
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.experiments import lotus_records_movie as runner
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources, identity
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


BACKEND = "semloom-local-model-core-async"


def configuration(**changes):
    value = dict(schema="semloom.movie_records_run.v1", backend=BACKEND, query_id="async-q",
        method=FilterMethod("openai/local-fixture").describe(),
        engine=dict(model_path="/tmp/fixture-model", model_python="/tmp/model-python",
                    driver_python="/tmp/driver-python", max_model_calls=2),
        resources=dict(prepare_batch_rows=1, model_batch_rows=1, max_active_batches=2))
    value.update(changes)
    return value


def terminal(position):
    return dict(identity("async-q", "row-" + str(position), position), status="succeeded",
        text="Answer: True", prompt_tokens=10 + position, completion_tokens=2,
        total_tokens=12 + position, finish_reason="stop", engine_uid="fixture-engine",
        engine_request_id="request-" + str(position), submission_confirmed=True,
        logprobs_json="null")


class AsyncConfigurationTests(unittest.TestCase):
    def read(self, value):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"config.json"
            path.write_text(json.dumps(value))
            return runner.read_config(path)

    def test_explicit_selector_keeps_five_values_and_inherited_generation(self):
        value = configuration(async_limits=dict(max_inflight=2), generation=dict(seed=7))
        result = self.read(value)
        self.assertEqual(len(result), 5)
        config, method, generation, resources, engine = result
        self.assertEqual(config, json.loads(json.dumps(value)))
        self.assertEqual(method, FilterMethod("openai/local-fixture"))
        self.assertEqual(generation, GenerationSpec(seed=7))
        self.assertEqual(resources.max_active_batches, 2)
        self.assertEqual(engine.max_model_calls, 2)
        self.assertEqual(runner._async_limits(config), AsyncEngineLimits(max_inflight=2))
        self.assertIsNone(runner._async_limits(self.read(configuration())[0]))

    def test_invalid_limits_fail_before_engine_profile_construction(self):
        bad = [None, [], True, dict(max_inflight=True), dict(max_inflight=0),
               dict(result_slots=-1), dict(input_bytes=float("inf")), dict(item_result_bytes=1),
               dict(input_bytes=1), dict(control_frame_bytes=1), dict(extra=1), {},
               dict(max_inflight=8), dict(max_inflight=2, result_slots=1)]
        for limits in bad:
            with self.subTest(limits=limits), patch.object(EngineSpec, "from_dict") as engine:
                with self.assertRaises((ValueError, TypeError)):
                    self.read(configuration(async_limits=limits))
                engine.assert_not_called()

    def test_async_limits_are_not_accepted_by_existing_backends(self):
        old = [name for name in runner.BACKENDS if name != BACKEND]
        self.assertEqual(len(old), 4)
        for backend in old:
            with self.subTest(backend=backend), patch.object(EngineSpec, "from_dict") as engine:
                with self.assertRaisesRegex(ValueError, "explicit async"):
                    self.read(configuration(backend=backend, async_limits={}))
                engine.assert_not_called()
            self.assertEqual(self.read(configuration(backend=backend))[0]["backend"], backend)

    def test_async_uses_existing_core_single_row_guard_before_engine(self):
        for resources in ({}, dict(prepare_batch_rows=2, model_batch_rows=1),
                          dict(prepare_batch_rows=1, model_batch_rows=2)):
            with self.subTest(resources=resources), patch.object(EngineSpec, "from_dict") as engine:
                with self.assertRaisesRegex(ValueError, "prepare_batch_rows=model_batch_rows=1"):
                    self.read(configuration(resources=resources))
                engine.assert_not_called()

    def test_check_and_run_without_execute_model_remain_preflight_only(self):
        config, method, generation, resources, engine = self.read(configuration())
        source = SimpleNamespace(manifest_path=Path("fixture-source.json"))
        for action in ("check", "run"):
            argv = [action, "--source-manifest", "fixture-source.json", "--sembench-checkout", "fixture-checkout",
                    "--config", "fixture-config.json"]
            if action == "run":
                argv.extend(["--output-dir", "unused"])
            output = io.StringIO()
            with self.subTest(action=action), redirect_stdout(output), \
                 patch.object(runner.MovieRecordSource, "from_manifest", return_value=source), \
                 patch.object(runner, "read_config", return_value=(config,method,generation,resources,engine)), \
                 patch.object(runner, "model_preflight", return_value=dict(status="preflight_only", inference_calls=0)) as preflight, \
                 patch.object(runner, "_model_worker") as worker, patch.object(runner, "new_private_directory") as directory:
                self.assertEqual(runner.main(argv), 0)
            preflight.assert_called_once_with(source, "fixture-checkout", method, generation, engine)
            worker.assert_not_called()
            directory.assert_not_called()
            report = json.loads(output.getvalue())
            self.assertIsNone(report["requested_async_limits"])
            self.assertNotIn("execution_metadata", report)

    def test_outer_supervisor_command_and_timeout_are_unchanged(self):
        config, method, generation, resources, engine = self.read(configuration())
        source = SimpleNamespace(manifest_path=Path("fixture-source.json"))
        with patch.object(runner.MovieRecordSource, "from_manifest", return_value=source), \
             patch.object(runner, "read_config", return_value=(config,method,generation,resources,engine)), \
             patch.object(runner, "model_preflight", return_value=dict(status="preflight_only")), \
             patch.object(runner, "new_private_directory"), patch.object(runner, "write_private_json"), \
             patch("src.experiments.lotus_records_supervisor.supervise") as supervise, \
             patch.object(runner, "_model_worker") as worker:
            self.assertEqual(runner.main(["run", "--source-manifest", "fixture-source.json",
                "--sembench-checkout", "fixture-checkout", "--config", "fixture-config.json",
                "--output-dir", "unused", "--execute-model"]), 0)
        worker.assert_not_called()
        supervise.assert_called_once()
        command, _ = supervise.call_args.args
        self.assertIn("--execute-model", command)
        self.assertIn("--worker", command)
        self.assertEqual(supervise.call_args.kwargs, dict(timeout_s=resources.startup_timeout_s + resources.query_timeout_s + 30))


class AsyncWorkerTests(unittest.TestCase):
    @contextmanager
    def wiring(self, *, close_report=None, core_error=None, metadata_error=None):
        self.config = configuration(async_limits=dict(max_inflight=2))
        self.method = FilterMethod("openai/local-fixture")
        self.engine = EngineSpec.from_dict(self.config["engine"])
        self.generation = GenerationSpec(seed=7)
        self.resources = RecordsResources(**self.config["resources"])
        self.expected = [identity("async-q", "row-" + str(i), i) for i in range(2)]
        self.source = SimpleNamespace(expected=Mock(return_value=self.expected))
        self.rows = iter(())
        self.metadata = dict(fixture_only=True, effective_async_limits=dict(max_inflight=2),
                             logical_bytes_are_not_rss=True)
        self.actor = SimpleNamespace(execution_metadata=Mock(return_value=self.metadata, side_effect=metadata_error),
            close=Mock(return_value=dict(status="closed", cleanup_errors=[])))
        self.owner = SimpleNamespace(evaluate=Mock(return_value=self.rows), production_core_connected=False,
            close=Mock(return_value=close_report if close_report is not None else dict(
                status="closed", cleanup_scope="fixture-scoped-api", cleanup_errors=[],
                gpu_exit_confirmed=None, descendants_exit_confirmed=None)), last_query_report=None)
        self.runtime = SimpleNamespace(init=Mock(), is_initialized=Mock(return_value=True), shutdown=Mock())
        self.recorded = []
        actor_module = ModuleType("src.execution_provider.adapters.lotus_records_async_actor")
        actor_module.AsyncRecordsActorOwner = self.actor_constructor = Mock(return_value=self.actor)
        with patch.dict(sys.modules, {"ray":self.runtime, actor_module.__name__:actor_module}), \
             patch("src.execution_provider.adapters.lotus_records_core.SemLoomRecordsCore",
                   return_value=self.owner, side_effect=core_error) as self.core_constructor, \
             patch("src.execution_provider.adapters.lotus_records_static.SemLoomRecordsStaticReference") as self.static_constructor, \
             patch.object(runner, "write_private_json", side_effect=lambda _, value:self.recorded.append(deepcopy(value))):
            yield

    def run_worker(self, query):
        with patch.object(runner, "run_movie_query", side_effect=query):
            runner._model_worker(self.config, self.source, "fixture-checkout", self.method, self.generation,
                                 self.resources, self.engine, Path("unused"))

    def test_lazy_injection_passes_exact_profiles_and_preserves_metadata_scope(self):
        with self.wiring():
            def query(*args, **kwargs):
                self.actor_constructor.assert_not_called()
                self.core_constructor.assert_not_called()
                self.assertIs(args[4](), self.rows)
                self.assertIs(args[3], self.generation)
                return dict(status="completed", production_core_used=False)
            self.run_worker(query)
            self.actor_constructor.assert_called_once_with(self.method, self.engine, self.generation, self.resources,
                limits=AsyncEngineLimits(max_inflight=2))
            self.core_constructor.assert_called_once_with(self.method, self.engine, self.generation, self.resources,
                actor_owner=self.actor)
            self.owner.close.assert_called_once_with(timeout=20)
            self.owner.evaluate.assert_called_once_with(self.source, query_id="async-q")
            self.static_constructor.assert_not_called()
            self.runtime.shutdown.assert_called_once()
            report = self.recorded[-1]
            self.assertEqual(report["execution_kind"], "optional async local-model qualification")
            self.assertEqual(report["generation"], asdict(self.generation))
            self.assertEqual(report["execution_metadata"], self.metadata)
            self.assertFalse(report["production_core_used"])
            self.assertFalse(report["performance_claim"])
            self.assertFalse(report["independent_gpu_process_exit_check"])
            self.assertIsNone(report["owner_close"]["gpu_exit_confirmed"])

    def test_failure_before_lazy_factory_does_not_construct_async_or_core_owner(self):
        with self.wiring():
            primary = ValueError("source preflight failed before execute")
            with self.assertRaises(ValueError) as failure:
                self.run_worker(Mock(side_effect=primary))
            self.assertIs(failure.exception, primary)
            self.actor_constructor.assert_not_called()
            self.core_constructor.assert_not_called()
            self.assertFalse(self.recorded[-1]["production_core_used"])

    def test_omitted_limits_preserve_actual_actor_core_bound_defaults(self):
        with self.wiring():
            self.config.pop("async_limits")
            def query(*args, **kwargs):
                args[4]()
                return dict(status="completed")
            self.run_worker(query)
            self.actor_constructor.assert_called_once_with(self.method, self.engine, self.generation,
                self.resources, limits=None)
            self.assertIsNone(self.recorded[-1]["requested_async_limits"])

    def test_unknown_scoped_close_with_empty_errors_cannot_complete_query(self):
        with self.wiring(close_report=dict(status="unknown", cleanup_errors=[], gpu_exit_confirmed=None)):
            def query(*args, **kwargs):
                args[4]()
                return dict(status="completed", calls=dict(known_prompt_tokens=7))
            with self.assertRaisesRegex(RuntimeError, "cleanup remains unconfirmed") as failure:
                self.run_worker(query)
            self.assertEqual(failure.exception.query_report["status"], "failed")
            self.assertEqual(failure.exception.query_report["calls"]["known_prompt_tokens"], 7)
            self.assertEqual(self.recorded[-1]["owner_close"]["status"], "unknown")

    def test_core_constructor_failure_still_closes_unadopted_actor_owner(self):
        primary = ValueError("injected owner validation failed")
        with self.wiring(core_error=primary):
            with self.assertRaises(ValueError) as failure:
                self.run_worker(lambda *args, **kwargs: args[4]())
            self.assertIs(failure.exception, primary)
            self.actor.close.assert_called_once_with()
            self.owner.close.assert_not_called()
            self.assertFalse(self.recorded[-1]["production_core_used"])

    def test_primary_and_late_known_usage_survive_metadata_and_close_failure(self):
        primary = RuntimeError("query suffix failed")
        with self.wiring(close_report=dict(status="unknown", cleanup_errors=[]), metadata_error=OSError("metadata failed")):
            ledger = CallLedger(self.expected, max_calls=2)
            ledger.record(terminal(0))
            ledger.mark_unconfirmed("RuntimeError")
            report = dict(status="failed", calls=ledger.report(), runner_unconfirmed_call_ids=[self.expected[1]["call_id"]])
            self.owner.last_query_report = dict(unconsumed_call_results=[terminal(1)])
            def query(*args, **kwargs):
                args[4]()
                primary.query_report = report
                raise primary
            with self.assertRaises(RuntimeError) as failure:
                self.run_worker(query)
            self.assertIs(failure.exception, primary)
            self.assertEqual(report["calls"]["counts"]["succeeded"], 2)
            self.assertEqual(report["calls"]["usage"]["prompt_tokens"], 21)
            self.assertEqual(report["execution_metadata_error_type"], "OSError")
            self.assertTrue(any("owner close" in note for note in primary.__notes__))
            self.assertTrue(any("execution metadata" in note for note in primary.__notes__))

    def test_backend_unknown_is_not_upgraded_by_async_owner_close_receipt(self):
        primary = RuntimeError("native work remained unknown")
        with self.wiring():
            ledger = CallLedger(self.expected, max_calls=2)
            ledger.record(terminal(0))
            ledger.record(dict(terminal(1), status="unknown", submission_confirmed=False,
                               prompt_tokens=None, completion_tokens=None, total_tokens=None))
            report = dict(status="failed", calls=ledger.report(), runner_unconfirmed_call_ids=[])
            self.owner.last_query_report = dict(unconsumed_call_results=[terminal(1)])
            def query(*args, **kwargs):
                args[4]()
                primary.query_report = report
                raise primary
            with self.assertRaises(RuntimeError) as failure:
                self.run_worker(query)
            self.assertIs(failure.exception, primary)
            self.assertEqual(report["calls"]["counts"]["unknown"], 1)
            self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 10)
            self.assertIsNone(report["calls"]["usage"]["prompt_tokens"])


if __name__ == "__main__":
    unittest.main()
