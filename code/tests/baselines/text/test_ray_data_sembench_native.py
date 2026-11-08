"""Native public API wiring with lazy CPU doubles, never a Ray/model start."""

import builtins
from copy import deepcopy
import os
from pathlib import Path
import socket
import subprocess
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.frameworks import ray_data_sembench_native as native


class CPUDataSet:
    """Only the public Dataset methods consumed by this binding."""

    def __init__(self, read, events):
        self.read, self.events = read, events

    def filter(self, predicate):
        self.events.append(("filter", predicate.__name__))
        return CPUDataSet(lambda: [row for row in self.read() if predicate(row)], self.events)

    def materialize(self):
        self.events.append(("materialize",))
        rows = self.read()
        return CPUDataSet(lambda: rows, self.events)

    def take_all(self):
        self.events.append(("take_all",))
        return list(self.read())

    def count(self):
        self.events.append(("count",))
        return len(self.read())


class NativeRayQ3Tests(unittest.TestCase):
    def setUp(self):
        self.events, self.requests, self.config_calls = [], [], []
        self.run_error = self.cleanup_error = self.build_error = None
        self.output_transform = lambda rows: rows
        self.mutate_sampling = False
        self.initialized = False
        self.ray = ModuleType("ray")
        self.ray.__path__ = []
        self.ray.__version__ = "2.56.1-CPU-double"
        self.ray.is_initialized = lambda: self.initialized
        self.ray.init = Mock(side_effect=AssertionError("real ray.init forbidden"))
        self.ray.shutdown = Mock(side_effect=self.shutdown)
        self.data = ModuleType("ray.data")
        self.data.__path__ = []
        self.data.from_items = Mock(side_effect=self.from_items)
        self.data.DataContext = SimpleNamespace(get_current=lambda: SimpleNamespace(
            actor_init_retry_on_errors=False, actor_init_max_retries=3,
            actor_task_retry_on_errors=False, retried_map_errors=False, max_map_retries=3,
            max_tasks_in_flight_per_actor=None))
        self.ray.data = self.data
        self.llm = ModuleType("ray.data.llm")
        self.llm.vLLMEngineProcessorConfig = self.config
        self.llm.build_processor = Mock(side_effect=self.build)
        self.data.llm = self.llm
        self.workload = ModuleType("src.baselines.text.sembench_movie_native")
        # The shared messages/parser are independently qualified by their owner.
        self.workload.q3_messages = Mock(side_effect=lambda text: [{"role": "user", "content": text}])
        self.workload.q3_decision = Mock(side_effect=lambda text: text != "negative")
        modules = {"ray": self.ray, "ray.data": self.data, "ray.data.llm": self.llm,
                   "src.baselines.text.sembench_movie_native": self.workload}
        original_import = builtins.__import__

        def guarded_import(name, *args, **kwargs):
            if name not in modules and any(name == root or name.startswith(root + ".") for root in (
                "ray", "daft", "vllm", "torch", "lotus", "src.semantic_methods", "src.serving",
                "src.execution_provider", "src.scheduling", "src.research",
            )):
                self.fail("native Q3 attempted a real framework/model/LOTUS/Core import: " + name)
            return original_import(name, *args, **kwargs)

        for scope in (
            patch.dict(sys.modules, modules), patch.object(builtins, "__import__", guarded_import),
            patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")),
            patch.object(socket.socket, "connect_ex", side_effect=AssertionError("network forbidden")),
        ):
            scope.start()
            self.addCleanup(scope.stop)
        self.rows = [
            dict(occurrence_id="outside", id="another_movie", reviewId=4, reviewText="excluded"),
            dict(occurrence_id="a", id="taken_3", reviewId=7, reviewText="same review"),
            dict(occurrence_id="b", id="taken_3", reviewId=7, reviewText="same review"),
            dict(occurrence_id="c", id="taken_3", reviewId=8, reviewText="bad review"),
        ]
        self.replies = dict(a="positive", b="invalid native reply", c="negative")
        self.sampling = dict(temperature=0, max_tokens=61, stop=["fixture-stop"], n=1)

    def shutdown(self):
        if self.cleanup_error is not None:
            raise self.cleanup_error
        self.initialized = False

    def from_items(self, rows):
        self.initialized = True  # Ray's native datasource owns automatic initialization.
        self.events.append(("from_items", [row["occurrence_id"] for row in rows]))
        return CPUDataSet(lambda: rows, self.events)

    def config(self, **kwargs):
        self.config_calls.append(deepcopy(kwargs))
        values = dict(batch_size=32, concurrency=1, max_concurrent_batches=8,
                      max_pending_requests=None, max_tasks_in_flight_per_actor=None,
                      should_continue_on_error=False)
        values.update(kwargs)
        # Avoid a second implementation of native validation/default resolution.
        return SimpleNamespace(values=values, model_dump=lambda **unused: deepcopy(values))

    def build(self, config, *, preprocess, postprocess):
        if self.build_error is not None:
            raise self.build_error
        owner = self
        # Native graph construction fills in applied engine defaults.
        config.values["engine_kwargs"].setdefault("distributed_executor_backend", "uni")

        class CPUProcessor:
            def list_stage_names(self):
                return ["ChatTemplateStage", "TokenizeStage", "vLLMEngineStage", "DetokenizeStage"]

            def __call__(self, selected):
                owner.events.append(("processor",))

                def read():
                    if owner.run_error is not None:
                        raise owner.run_error
                    outputs = []
                    for row in selected.read():
                        request = dict(row, **preprocess(row))
                        owner.requests.append(deepcopy(request))
                        if owner.mutate_sampling:
                            request["sampling_params"]["structured_outputs"].pop("regex")
                        outputs.append(postprocess(dict(request,
                            generated_text=owner.replies[row["occurrence_id"]],
                            num_input_tokens=17, num_generated_tokens=2,
                            request_id=len(owner.requests), batch_uuid="CPU-batch",
                            time_taken_llm=0.01, params="CPU native parameters",
                            metrics={"CPU_fixture": True})))
                    return owner.output_transform(outputs)

                return CPUDataSet(read, owner.events)

        return CPUProcessor()

    def execute(self, rows=None, **kwargs):
        return native.execute_q3(self.rows if rows is None else rows,
            model="/tmp/CPU-native-model", sampling_params=self.sampling, **kwargs)

    def test_complete_cohort_is_filtered_before_processor_and_count_reuses_materialization(self):
        result = self.execute()
        self.assertEqual(result["positive_review_cnt"], 2)
        self.assertEqual([row["occurrence_id"] for row in self.requests], ["a", "b", "c"])
        self.assertEqual(self.events, [
            ("from_items", ["outside", "a", "b", "c"]), ("filter", "_eligible"),
            ("processor",), ("materialize",), ("take_all",), ("filter", "_positive"), ("count",),
        ])
        self.assertEqual([row["reviewId"] for row in result["predictions"]], [7, 7, 8])
        self.assertEqual(result["predictions"][1]["generated_text"], "invalid native reply")
        self.assertTrue(result["predictions"][1]["decision"])
        self.assertEqual(result["predictions"][0]["native_usage"]["num_input_tokens"], 17)
        self.assertNotIn("reviewText", result["predictions"][0])
        self.ray.init.assert_not_called()
        self.ray.shutdown.assert_called_once_with()

    def test_native_defaults_are_omitted_from_constructor_and_reported_after_build(self):
        result = self.execute()
        self.assertEqual(self.config_calls, [dict(model_source="/tmp/CPU-native-model", engine_kwargs={})])
        metadata = result["metadata"]
        self.assertEqual(metadata["processor_config"]["batch_size"], 32)
        self.assertEqual(metadata["processor_config"]["engine_kwargs"], dict(distributed_executor_backend="uni"))
        self.assertEqual(metadata["logical_inference_rows"], 3)
        self.assertEqual(metadata["native_capacity"]["status"], "unobserved")
        self.assertEqual(metadata["retry_policy"]["adapter_overrides"], {})
        self.assertEqual(metadata["retry_policy"]["actor_pool_source_defaults"],
                         dict(max_restarts=-1, max_task_retries=-1))
        self.assertFalse(metadata["retry_policy"]["actor_task_retry_on_errors"])
        self.assertIsNone(metadata["retry_policy"]["physical_attempts"])
        self.assertEqual(metadata["lifecycle"]["runtime_owner"], "binding")
        self.assertTrue(metadata["lifecycle"]["ray_driver_disconnected"])
        self.assertIsNone(metadata["lifecycle"]["model_process_exit_confirmed"])

    def test_applied_options_and_generation_reach_public_callbacks_unchanged(self):
        options = dict(batch_size=48, concurrency=2, max_concurrent_batches=5,
                       chat_template_stage=dict(chat_template_kwargs=dict(enable_thinking=False)),
                       tokenize_stage=dict(concurrency=3, num_cpus=0.5),
                       detokenize_stage=dict(concurrency=2))
        engine = dict(max_num_seqs=37, max_model_len=7000)
        expected_options, expected_engine = deepcopy(options), deepcopy(engine)
        result = self.execute(processor_options=options, engine_kwargs=engine)
        self.assertEqual(self.config_calls[0], dict(model_source="/tmp/CPU-native-model",
                         engine_kwargs=expected_engine, **expected_options))
        self.assertTrue(all(row["sampling_params"] == self.sampling for row in self.requests))
        self.assertEqual(self.workload.q3_messages.call_args_list[0].args, ("same review",))
        self.assertEqual(options, expected_options)
        self.assertEqual(engine, expected_engine)
        self.assertEqual(result["metadata"]["processor_config"]["batch_size"], 48)

    def test_native_sampling_mutation_is_isolated_per_occurrence_and_from_caller(self):
        self.sampling["structured_outputs"] = dict(regex="True|False")
        original = deepcopy(self.sampling)
        self.mutate_sampling = True
        self.execute()
        self.assertEqual(self.sampling, original)
        self.assertTrue(all(row["sampling_params"] == original for row in self.requests))

    def test_completion_order_is_preserved_with_occurrence_identity(self):
        self.output_transform = lambda rows: list(reversed(rows))
        result = self.execute()
        self.assertEqual([row["occurrence_id"] for row in result["predictions"]], ["c", "b", "a"])
        self.assertEqual(result["positive_review_cnt"], 2)

    def test_missing_duplicate_or_unknown_predictions_fail_before_count(self):
        for transform in (lambda rows: rows[:-1], lambda rows: [rows[0]] * len(rows),
                          lambda rows: [dict(rows[0], occurrence_id="unknown"), *rows[1:]],
                          lambda rows: [dict(rows[0], reviewId=999), *rows[1:]]):
            with self.subTest(transform=transform):
                self.events.clear()
                self.output_transform = transform
                with self.assertRaisesRegex(ValueError, "missing, duplicate or unknown") as caught:
                    self.execute()
                self.assertNotIn(("count",), self.events)
                self.assertTrue(caught.exception.native_q3_report["predictions"])
                self.assertIsNone(caught.exception.native_q3_report["positive_review_cnt"])

    def test_unknown_and_ignored_options_fail_before_allocating_runtime(self):
        for options in (dict(runner_threads=1), dict(max_active_batches=3),
                        dict(tokenize_stage=dict(batch_size=8)),
                        dict(chat_template_stage=SimpleNamespace(batch_size=8))):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.execute(processor_options=options)
        self.data.from_items.assert_not_called()
        self.llm.build_processor.assert_not_called()

    def test_empty_source_has_no_native_allocation(self):
        result = self.execute([])
        self.assertEqual(result["predictions"], [])
        self.assertEqual(result["positive_review_cnt"], 0)
        self.assertEqual(result["metadata"]["lifecycle"]["runtime_owner"], "none")
        self.data.from_items.assert_not_called()
        self.ray.shutdown.assert_not_called()

    def test_empty_native_selection_counts_without_building_model_processor(self):
        result = self.execute(self.rows[:1])
        self.assertEqual(result["positive_review_cnt"], 0)
        self.assertEqual(self.events, [("from_items", ["outside"]), ("filter", "_eligible"), ("count",)])
        self.llm.build_processor.assert_not_called()
        self.assertEqual(self.requests, [])
        self.ray.shutdown.assert_called_once_with()

    def test_caller_runtime_is_retained_without_resident_model_claim(self):
        self.initialized = True
        result = self.execute()
        self.ray.shutdown.assert_not_called()
        self.assertTrue(self.initialized)
        self.assertEqual(result["metadata"]["lifecycle"]["runtime_owner"], "caller")
        self.assertIsNone(result["metadata"]["lifecycle"]["ray_driver_disconnected"])

    def test_native_build_or_inference_failure_is_preserved_and_owned_runtime_closes(self):
        for failure_stage in ("build_error", "run_error"):
            with self.subTest(stage=failure_stage):
                self.ray.shutdown.reset_mock()
                failure = RuntimeError("CPU original " + failure_stage)
                setattr(self, failure_stage, failure)
                with self.assertRaises(RuntimeError) as caught:
                    self.execute()
                self.assertIs(caught.exception, failure)
                self.assertTrue(failure.native_q3_report["metadata"]["lifecycle"]["ray_driver_disconnected"])
                self.assertIsNone(failure.native_q3_report["positive_review_cnt"])
                self.ray.shutdown.assert_called_once_with()
                setattr(self, failure_stage, None)

    def test_native_config_and_shared_parser_errors_are_preserved(self):
        for target, name in ((self.llm, "vLLMEngineProcessorConfig"),
                             (self.workload, "q3_decision")):
            with self.subTest(target=target.__name__):
                failure = ValueError("CPU original public boundary failure")
                with patch.object(target, name, side_effect=failure), self.assertRaises(ValueError) as caught:
                    self.execute()
                self.assertIs(caught.exception, failure)
                self.assertTrue(failure.native_q3_report["metadata"]["lifecycle"]["ray_driver_disconnected"])

    def test_missing_generated_text_is_an_error_without_shared_parser_fallback(self):
        with self.assertRaises(KeyError) as caught:
            native._postprocess(dict(occurrence_id="a", reviewId=7, id="taken_3"))
        self.assertEqual(caught.exception.args, ("generated_text",))
        self.workload.q3_decision.assert_not_called()

    def test_full_2000_row_budget_reaches_only_120_native_logical_requests(self):
        rows = [dict(occurrence_id=f"cohort-{i}", id="taken_3" if i < 120 else "outside",
                     reviewId=i, reviewText=f"CPU cohort review {i}") for i in range(2000)]
        self.replies = {row["occurrence_id"]: "positive" if i < 14 else "negative"
                        for i, row in enumerate(rows[:120])}
        result = self.execute(rows)
        self.assertEqual(result["metadata"]["input_rows"], 2000)
        self.assertEqual(result["metadata"]["logical_inference_rows"], 120)
        self.assertEqual(len(self.requests), 120)
        self.assertEqual(result["positive_review_cnt"], 14)
        self.assertEqual(self.events.count(("materialize",)), 1)
        self.assertIsNone(result["metadata"]["retry_policy"]["physical_attempts"])

    def test_cleanup_failure_does_not_replace_original_inference_failure(self):
        self.run_error = ValueError("CPU original model failure")
        self.cleanup_error = RuntimeError("CPU native shutdown failure")
        with self.assertRaises(ValueError) as caught:
            self.execute()
        self.assertIs(caught.exception, self.run_error)
        report = caught.exception.native_q3_report
        self.assertEqual(report["metadata"]["lifecycle"]["cleanup_error_type"], "RuntimeError")
        self.assertEqual(caught.exception.__notes__, ["native Ray runtime cleanup failed: RuntimeError"])

    def test_cleanup_failure_after_result_is_reported_with_predictions(self):
        self.cleanup_error = RuntimeError("CPU native shutdown failure")
        with self.assertRaises(RuntimeError) as caught:
            self.execute()
        self.assertIs(caught.exception, self.cleanup_error)
        self.assertEqual(len(caught.exception.native_q3_report["predictions"]), 3)
        self.assertEqual(caught.exception.native_q3_report["positive_review_cnt"], 2)


class NativeImportBoundaryTests(unittest.TestCase):
    def test_module_and_empty_source_import_without_frameworks_lotus_or_core(self):
        code = Path(__file__).resolve().parents[3]
        program = """
import importlib.abc, socket, sys
sys.path.insert(0, sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == p or fullname.startswith(p + '.') for p in
               ('ray', 'daft', 'vllm', 'torch', 'lotus', 'src.semantic_methods', 'src.serving',
                'src.execution_provider', 'src.scheduling', 'src.research')):
            raise AssertionError('forbidden dependency: ' + fullname)
sys.meta_path.insert(0, Guard())
def no_network(*args, **kwargs):
    raise AssertionError('network forbidden')
socket.socket.connect = socket.socket.connect_ex = no_network
from src.baselines.text.frameworks.ray_data_sembench_native import execute_q3
result = execute_q3([], model='/tmp/CPU-native-model', sampling_params={})
assert result['predictions'] == [] and result['positive_review_cnt'] == 0
"""
        result = subprocess.run([sys.executable, "-B", "-c", program, str(code)],
            capture_output=True, text=True, timeout=20, env=dict(os.environ, CUDA_VISIBLE_DEVICES=""))
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
