"""CPU-only observation tests; no torch, vLLM, models or service access."""
from types import SimpleNamespace
import ast
from enum import IntEnum
import hashlib
import os
from pathlib import Path
import unittest
from unittest.mock import Mock

from src.observability.native_gpu_decisions import cpu_counts, observe_decisions


class Runner:
    def __init__(self):
        self.input_batch = SimpleNamespace(req_ids=["a", "b"])
        self.cascade_attn_enabled = True
        self.parallel_config = SimpleNamespace(use_ubatching=False)
        self.cascade_result = [[16]]
        self.graph_result = (SimpleNamespace(name="PIECEWISE"), SimpleNamespace(num_tokens=8, num_reqs=2),
                             False, object(), object())
        self.calls = []

    def _compute_cascade_attn_prefix_lens(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.cascade_result

    def _determine_batch_execution_and_padding(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.graph_result


def schedule():
    return SimpleNamespace(num_common_prefix_blocks=[2], total_num_scheduled_tokens=5,
                           scheduled_encoder_inputs={"a": [0, 1]})


class DecisionTests(unittest.TestCase):
    def test_records_actual_decisions_preserving_arguments_results_and_attributes(self):
        runner = Runner()
        events = []
        original_dict = dict(vars(runner))
        def emit(event, **fields): events.append(dict(event=event, **fields))
        tokens = [2, 3]
        with observe_decisions(runner, schedule(), 7, emit):
            self.assertIs(runner._compute_cascade_attn_prefix_lens(tokens, [32, 16], [2]), runner.cascade_result)
            result = runner._determine_batch_execution_and_padding(
                5, 2, tokens, 3, True, num_encoder_reqs=1)
            self.assertIs(result, runner.graph_result)
        self.assertEqual(vars(runner), original_dict)
        self.assertIs(runner.calls[0][0][0], tokens)
        self.assertIs(runner.calls[1][0][2], tokens)
        self.assertEqual(events[0]["encoder_input_counts"], {"a": 2})
        self.assertEqual(events[1]["computed_tokens"], {"a": 32, "b": 16})
        self.assertEqual(events[2]["scheduled_tokens"], {"a": 2, "b": 3})
        self.assertEqual(events[2]["padded_tokens"], 8)
        self.assertEqual(events[2]["cudagraph_mode"], "PIECEWISE")
        self.assertTrue(all(event["observer_execute_id"] == 7 for event in events))

    def test_native_exception_is_same_object_and_instance_method_restored(self):
        runner = Runner()
        failure = ValueError("private content")
        original = Mock(side_effect=failure)
        runner._compute_cascade_attn_prefix_lens = original
        sink = Mock(side_effect=RuntimeError("sink failure"))
        with self.assertRaises(ValueError) as caught:
            with observe_decisions(runner, schedule(), 1, sink):
                runner._compute_cascade_attn_prefix_lens([], [], [])
        self.assertIs(caught.exception, failure)
        self.assertIs(runner._compute_cascade_attn_prefix_lens, original)
        original.assert_called_once()

    def test_unknown_extraction_does_not_change_return_or_leak_exception_text(self):
        runner = Runner()
        runner.graph_result = object()
        events = []
        with observe_decisions(runner, schedule(), 1, lambda event, **kw: events.append((event, kw))):
            self.assertIs(runner._determine_batch_execution_and_padding(), runner.graph_result)
        self.assertEqual(events[-1][0], "native_decision_unknown")
        self.assertNotIn("reason", events[-1][1])

    def test_device_values_are_never_read(self):
        class Device:
            device = SimpleNamespace(type="cuda")
            def tolist(self): raise AssertionError("must not transfer")
            def cpu(self): raise AssertionError("must not transfer")
        Device.__module__ = "torch.fixture"
        self.assertIsNone(cpu_counts(Device()))
        runner = Runner()
        events = []
        with observe_decisions(runner, schedule(), 1, lambda event, **kw: events.append(kw)):
            runner._compute_cascade_attn_prefix_lens(Device(), Device(), [2])
        self.assertIsNone(events[-1]["scheduled_tokens"])
        self.assertIsNone(events[-1]["computed_tokens"])

    def test_sink_failure_preserves_result_and_missing_methods_only_reduce_evidence(self):
        runner = SimpleNamespace()
        sink = Mock(side_effect=RuntimeError("sink"))
        with observe_decisions(runner, schedule(), 1, sink):
            runner.result = 4
        self.assertEqual(runner.result, 4)

    def test_none_cascade_is_not_a_missing_observation(self):
        runner = Runner()
        runner.cascade_result = None
        events = []
        with observe_decisions(runner, schedule(), 1, lambda event, **kw: events.append(kw)):
            runner._compute_cascade_attn_prefix_lens([2, 3], [32, 16], [2])
        self.assertIs(events[-1]["cascade_selected"], False)
        self.assertIsNone(events[-1]["cascade_prefix_lengths"])


class PinnedSourceTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get("SEMLOOM_VLLM_RUNNER_SOURCE"), "optional pinned upstream source")
    def test_actual_upstream_graph_choice_function_cpu_branches(self):
        source = Path(os.environ["SEMLOOM_VLLM_RUNNER_SOURCE"]).read_bytes()
        self.assertEqual(hashlib.sha256(source).hexdigest(),
                         "6c92ded8468f44d6df863a617ce588f132fa6df7031feecc0cc421702a41610e")
        tree = ast.parse(source)
        cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "GPUModelRunner")
        method = next(node for node in cls.body if isinstance(node, ast.FunctionDef)
                      and node.name == "_determine_batch_execution_and_padding")
        program = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), method], type_ignores=[])
        ast.fix_missing_locations(program)
        class Mode(IntEnum): NONE = 0; PIECEWISE = 1; FULL = 2
        namespace = {"CUDAGraphMode": Mode}
        exec(compile(program, "pinned_vllm_gpu_model_runner", "exec"), namespace)
        native = namespace[method.name]
        class SourceRunner(Runner):
            _determine_batch_execution_and_padding = native
            def _is_uniform_decode(self, **kw): return False
            def _pad_for_sequence_parallelism(self, tokens): return tokens
        for cascade, encoder, eager in ((False, False, False), (True, False, False),
                                        (False, True, False), (False, False, True)):
            with self.subTest(cascade=cascade, encoder=encoder, eager=eager):
                runner = SourceRunner()
                runner.model_config = SimpleNamespace(is_encoder_decoder=encoder)
                runner.input_batch.lora_id_to_lora_request = {}
                runner.uniform_decode_query_len = 1
                runner.compilation_config = SimpleNamespace(pass_config=SimpleNamespace(enable_sp=False))
                runner.vllm_config = SimpleNamespace(parallel_config=SimpleNamespace(data_parallel_size=1),
                                                     observability_config=SimpleNamespace(cudagraph_metrics=False))
                def dispatch(**kw):
                    mode = Mode.NONE if kw['valid_modes'] == {Mode.NONE} else (Mode.PIECEWISE if kw['invalid_modes'] else Mode.FULL)
                    return mode, SimpleNamespace(num_tokens=5 if mode == Mode.NONE else 8, num_reqs=2)
                runner.cudagraph_dispatcher = SimpleNamespace(dispatch=dispatch)
                events = []
                args = dict(num_tokens=5, num_reqs=2, num_scheduled_tokens_np=[2, 3], max_num_scheduled_tokens=3,
                            use_cascade_attn=cascade, num_encoder_reqs=int(encoder), force_eager=eager)
                expected = runner._determine_batch_execution_and_padding(**args)
                with observe_decisions(runner, schedule(), 1, lambda event, **kw: events.append(kw)):
                    actual = runner._determine_batch_execution_and_padding(**args)
                self.assertEqual(actual, expected)
                self.assertEqual(events[-1]['cudagraph_mode'], actual[0].name)
                self.assertEqual(events[-1]['padded_tokens'], actual[1].num_tokens)


if __name__ == "__main__":
    unittest.main()
