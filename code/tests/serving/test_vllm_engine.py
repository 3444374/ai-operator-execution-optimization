"""Standard-library CPU checks; all vLLM allocations and results are explicit fakes."""

from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import types
import unittest
from contextlib import ExitStack
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from unittest.mock import patch

CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents
                 if (parent / "src").is_dir())
MODULE_PATH = CODE_ROOT / "src/serving/backends/vllm_engine.py"
# Isolate this CPU-only test from the existing backend package's numpy/Arrow imports.
# The normal public package import has its own explicitly dependency-gated check below.
MODULE_NAME = "_vllm_engine_cpu_test_kernel"
SPEC = importlib.util.spec_from_file_location(MODULE_NAME, MODULE_PATH)
kernel = importlib.util.module_from_spec(SPEC)
sys.modules[MODULE_NAME] = kernel
SPEC.loader.exec_module(kernel)
EngineSpec, GenerationSpec, VllmEngine = (
    kernel.EngineSpec, kernel.GenerationSpec, kernel.VllmEngine,
)


def prepared(index=0):
    return dict(
        query_id="movie-q3", model_role="main", algorithm_stage="filter",
        row_id=f"row-{index}", source_position=index, call_index=0, call_id=f"call-{index}",
        messages_json=json.dumps([
            dict(role="system", content="Return True or False."),
            dict(role="user", content="Review: 非常好\n{{literal}}"),
        ], ensure_ascii=False),
    )


def output(request_id="request-0", **changes):
    choice = types.SimpleNamespace(
        index=0, text="True", token_ids=(5, 6), finish_reason="stop", logprobs=None,
    )
    value = types.SimpleNamespace(
        request_id=request_id, prompt_token_ids=[1, 2, 3], finished=True, outputs=[choice],
    )
    for name, item in changes.items():
        setattr(value, name, item)
    return value


@dataclass
class FakeLogprob:
    """The three fields of vLLM 0.25.1's genuine Logprob dataclass."""

    logprob: float
    rank: int | None = None
    decoded_token: str | None = None


def probability_output(request_id="request-0", token_ids=(5, 6), values=None):
    value = output(request_id)
    value.outputs[0].token_ids = token_ids
    value.outputs[0].logprobs = values if values is not None else [
        {5: FakeLogprob(-0.2, 1, "True"), 8: FakeLogprob(-1.8, 2, "False")},
        {6: FakeLogprob(-0.3, 1, "\n")},
    ]
    return value


class FakeCore:
    def __init__(self):
        self.calls = []
        self.errors = []

    def shutdown(self, timeout=None):
        self.calls.append(timeout)
        if self.errors:
            raise self.errors.pop(0)


class FakeLLM:
    def __init__(self):
        self.core = FakeCore()
        self.llm_engine = types.SimpleNamespace(engine_core=self.core)
        self.calls = []
        self.outputs = [output()]
        self.error = None
        self.decoded = {5: "True", 6: "\n", 8: "False"}
        self.decode_calls = []

    def get_tokenizer(self):
        def decode(token_ids):
            self.decode_calls.append(token_ids)
            return self.decoded[token_ids[0]]
        return types.SimpleNamespace(decode=decode)

    def chat(self, messages, **kwargs):
        self.calls.append((messages, kwargs))
        if self.error:
            raise self.error
        return self.outputs


class KernelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.model_dir = Path(self.temp.name) / "model"
        self.model_dir.mkdir()
        self.spec = EngineSpec(str(self.model_dir), sys.executable, sys.executable)
        self.model_identity = dict(
            executable=sys.executable, prefix=sys.prefix, base_prefix=sys.base_prefix,
            vllm_version="0.25.1", lotus_available=False, python_version=[3, 12, 6],
        )
        self.driver_identity = {**self.model_identity, "prefix": self.temp.name + "/driver"}
        self.events = []
        self.fake_llm = FakeLLM()
        self.fake_module = types.ModuleType("vllm")
        self.fake_module.__version__ = "0.25.1"
        self.fake_module.LLM = self.allocate
        self.fake_module.SamplingParams = self.sample
        self.fake_sampling_module = types.ModuleType("vllm.sampling_params")
        self.fake_sampling_module.StructuredOutputsParams = self.structured
        self.sampling_error = self.structured_error = None

    def allocate(self, **kwargs):
        self.events.append(("allocate", kwargs))
        return self.fake_llm

    def sample(self, **kwargs):
        self.events.append(("sample", kwargs))
        if self.sampling_error is not None:
            raise self.sampling_error
        return types.SimpleNamespace(**kwargs)

    def structured(self, **kwargs):
        self.events.append(("structured", kwargs))
        if self.structured_error is not None:
            raise self.structured_error
        return types.SimpleNamespace(**kwargs)

    def environment(self):
        stack = ExitStack()
        stack.enter_context(patch.object(
            kernel, "_probe_python", side_effect=[self.model_identity, self.driver_identity],
        ))
        stack.enter_context(patch.dict(sys.modules, {
            "vllm": self.fake_module, "vllm.sampling_params": self.fake_sampling_module,
        }))
        stack.enter_context(patch.object(kernel.importlib.metadata, "version", return_value="0.25.1"))
        stack.enter_context(patch.dict(os.environ))
        return stack

    def build(self, generation=None):
        with self.environment():
            return VllmEngine(self.spec, GenerationSpec() if generation is None else generation)

    def test_sampling_validation_precedes_allocation_and_forces_local_defaults(self):
        engine = self.build()
        self.assertEqual([event[0] for event in self.events], ["sample", "allocate"])
        sample, allocation = self.events[0][1], self.events[1][1]
        self.assertEqual(sample, GenerationSpec().kwargs())
        for name, expected in dict(
            tensor_parallel_size=1, pipeline_parallel_size=1,
            distributed_executor_backend="uni", trust_remote_code=False,
            generation_config="vllm", model=str(self.model_dir), tokenizer=str(self.model_dir),
            max_logprobs=20, logprobs_mode="raw_logprobs",
        ).items():
            self.assertEqual(allocation[name], expected)
        self.assertEqual(engine.ready()["batch_mode"], "finite_synchronous_chat")
        self.assertEqual(engine.ready()["max_logprobs"], 20)
        self.assertEqual(engine.ready()["logprobs_mode"], "raw_logprobs")
        json.dumps(engine.ready())

    def test_json_object_constructor_materializes_fixed_vllm_constraint_before_allocation(self):
        generation = GenerationSpec(response_format={"type": "json_object"}, stop=())
        engine = self.build(generation)
        self.assertEqual([event[0] for event in self.events], ["structured", "sample", "allocate"])
        self.assertEqual(self.events[0][1], {"json_object": True})
        self.assertIs(engine._sampling.structured_outputs.json_object, True)
        self.assertNotIn("response_format", self.events[1][1])
        self.assertEqual(engine.ready()["generation"], asdict(generation))

    def test_structured_constructor_failure_allocates_nothing(self):
        self.structured_error = ValueError("fake structured-output validation error")
        with self.environment(), self.assertRaises(ValueError):
            VllmEngine(self.spec, GenerationSpec(response_format={"type": "json_object"}))
        self.assertEqual([event[0] for event in self.events], ["structured"])

    def test_each_call_uses_fresh_sampling_and_default_generation_is_unchanged(self):
        default = GenerationSpec(max_tokens=16, temperature=0.5, stop=("END",))
        engine = self.build(default)
        original_sampling = dict(vars(engine._sampling))
        override = GenerationSpec(max_tokens=64, response_format={"type": "json_object"}, stop=())
        snapshots = [asdict(item) for item in (default, override)]
        for generation in (None, override, None):
            engine.run_prepared([prepared()], generation=generation)
        samples = [call[1]["sampling_params"] for call in self.fake_llm.calls]
        self.assertEqual(len({id(value) for value in [engine._sampling, *samples]}), 4)
        self.assertEqual([value.max_tokens for value in samples], [16, 64, 16])
        self.assertEqual([value.temperature for value in samples], [0.5, 0.0, 0.5])
        self.assertEqual([value.stop for value in samples], [["END"], [], ["END"]])
        self.assertFalse(hasattr(samples[0], "structured_outputs"))
        self.assertIs(samples[1].structured_outputs.json_object, True)
        self.assertFalse(hasattr(samples[2], "structured_outputs"))
        self.assertIs(engine.generation, default)
        self.assertEqual(vars(engine._sampling), original_sampling)
        self.assertEqual([asdict(item) for item in (default, override)], snapshots)
        self.assertEqual(sum(event[0] == "allocate" for event in self.events), 1)

    def test_sampling_mutation_cannot_reach_defaults_or_other_calls(self):
        default = GenerationSpec(response_format={"type": "json_object"})
        engine = self.build(default)
        response_format = default.response_format
        engine.run_prepared([prepared()])
        first = self.fake_llm.calls[-1][1]["sampling_params"]
        first.stop.append("changed")
        first.structured_outputs.json_object = False
        engine.run_prepared([prepared()])
        second = self.fake_llm.calls[-1][1]["sampling_params"]
        self.assertEqual(second.stop, ["\n"])
        self.assertIs(second.structured_outputs.json_object, True)
        self.assertIsNot(first.structured_outputs, second.structured_outputs)
        self.assertEqual(engine._sampling.stop, ["\n"])
        self.assertIs(engine._sampling.structured_outputs.json_object, True)
        self.assertIs(default.response_format, response_format)
        self.assertEqual(default.response_format, {"type": "json_object"})

    def test_override_revalidates_type_parameters_and_context_before_budget(self):
        self.spec = replace(self.spec, max_model_calls=1)
        engine = self.build()
        for value in ({}, True, "generation"):
            with self.subTest(value=value), self.assertRaises(TypeError):
                engine.run_prepared([prepared()], generation=value)
        for name, value in (("max_tokens", True), ("temperature", float("nan")),
                            ("frequency_penalty", 3), ("logprobs", True), ("logprobs", 21),
                            ("response_format", {"type": "json_schema"})):
            generation = GenerationSpec()
            object.__setattr__(generation, name, value)
            with self.subTest(name=name), self.assertRaises(ValueError):
                engine.run_prepared([prepared()], generation=generation)
        object.__setattr__(self.spec, "max_model_len", 8192)
        with self.assertRaisesRegex(ValueError, "max_model_len"):
            engine.run_prepared([prepared()], generation=GenerationSpec(max_tokens=4097))
        self.assertEqual(engine.ready()["reserved_model_calls"], 0)
        self.assertEqual(self.fake_llm.calls, [])
        self.assertEqual([event[0] for event in self.events], ["sample", "allocate"])

    def test_mutated_default_generation_is_rechecked_before_budget(self):
        generation = GenerationSpec(response_format={"type": "json_object"})
        engine = self.build(generation)
        generation.response_format["strict"] = True
        with self.assertRaisesRegex(ValueError, "response_format"):
            engine.run_prepared([prepared()])
        self.assertEqual(engine.ready()["reserved_model_calls"], 0)
        self.assertEqual(self.fake_llm.calls, [])

    def test_sampling_or_structured_override_failure_never_reserves_calls(self):
        self.spec = replace(self.spec, max_model_calls=1)
        engine = self.build()
        override = GenerationSpec(response_format={"type": "json_object"})
        for attribute in ("sampling_error", "structured_error"):
            setattr(self, attribute, ValueError("fake per-call parameter rejection"))
            with self.subTest(attribute=attribute), self.assertRaises(ValueError):
                engine.run_prepared([prepared()], generation=override)
            setattr(self, attribute, None)
            self.assertEqual(engine.ready()["reserved_model_calls"], 0)
            self.assertEqual(self.fake_llm.calls, [])
        engine.run_prepared([prepared()])
        self.assertEqual(engine.ready()["reserved_model_calls"], 1)

    def test_override_unknown_keeps_budget_and_does_not_change_default(self):
        self.spec = replace(self.spec, max_model_calls=1)
        engine = self.build()
        original = asdict(engine.generation)
        self.fake_llm.error = RuntimeError("fake possible acceptance")
        result = engine.run_prepared([prepared()], generation=GenerationSpec(
            response_format={"type": "json_object"}, max_tokens=32, stop=(),
        ))[0]
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["total_tokens"])
        self.assertIs(result["chat_invoked"], True)
        self.assertEqual(engine.ready()["reserved_model_calls"], 1)
        self.assertEqual(asdict(engine.generation), original)
        with self.assertRaisesRegex(RuntimeError, "max_model_calls"):
            engine.run_prepared([prepared(1)])
        self.assertEqual(len(self.fake_llm.calls), 1)

    def test_empty_expired_and_busy_override_calls_do_not_reserve_budget(self):
        self.spec = replace(self.spec, max_model_calls=1)
        engine = self.build()
        generation = GenerationSpec(response_format={"type": "json_object"})
        self.assertEqual(engine.run_prepared([], generation=generation), [])
        with patch.object(kernel.time, "monotonic", return_value=5.0):
            result = engine.run_prepared([prepared()], generation=generation,
                                         deadline_monotonic=4.0)[0]
        self.assertIs(result["deadline_expired"], True)
        self.assertIs(result["submission_confirmed"], False)
        with engine._lock, self.assertRaisesRegex(RuntimeError, "already running"):
            engine.run_prepared([prepared()], generation=generation)
        self.assertEqual(engine.ready()["reserved_model_calls"], 0)
        self.assertEqual(self.fake_llm.calls, [])

    def test_results_bind_actual_messages_and_prompt_token_ids(self):
        engine = self.build()
        row = prepared()
        result = engine.run_prepared([row])[0]
        canonical = json.dumps(json.loads(row["messages_json"]), ensure_ascii=False,
                               sort_keys=True, separators=(",", ":"), allow_nan=False)
        self.assertEqual(result["messages_sha256"], hashlib.sha256(canonical.encode()).hexdigest())
        self.assertEqual(json.loads(result["prompt_token_ids_json"]), [1, 2, 3])
        self.assertEqual(result["prompt_tokens"], len(json.loads(result["prompt_token_ids_json"])))

    def test_logprobs_override_preserves_raw_values_and_fresh_sampling(self):
        engine = self.build()
        results = []
        for generation in (GenerationSpec(logprobs=10), GenerationSpec(logprobs=0), None):
            value = probability_output()
            if generation is not None and generation.logprobs == 0:
                value.outputs[0].logprobs[0].pop(8)
            self.fake_llm.outputs = [value]
            results.append(engine.run_prepared([prepared()], generation=generation)[0])
        self.assertEqual([item[1]["sampling_params"].logprobs for item in self.fake_llm.calls],
                         [10, 0, None])
        self.assertEqual(len({id(item[1]["sampling_params"]) for item in self.fake_llm.calls}), 3)
        content = json.loads(results[0]["logprobs_json"])["content"]
        self.assertEqual([item["token"] for item in content], ["True", "\n"])
        self.assertEqual(content[0], dict(token="True", logprob=-0.2, bytes=[84, 114, 117, 101],
                                         top_logprobs=[
                                             dict(token="True", logprob=-0.2, bytes=[84, 114, 117, 101]),
                                             dict(token="False", logprob=-1.8, bytes=[70, 97, 108, 115, 101]),
                                         ]))
        self.assertTrue(all(not item["top_logprobs"] for item in
                            json.loads(results[1]["logprobs_json"])["content"]))
        self.assertEqual(results[2]["logprobs_json"], "null")
        self.assertIsNone(engine.generation.logprobs)
        self.assertIsNone(engine._sampling.logprobs)
        self.assertEqual(self.fake_llm.decode_calls, [])
        for result in results:
            self.assertEqual(result["status"], "succeeded")
            self.assertEqual((result["prompt_tokens"], result["completion_tokens"], result["total_tokens"]),
                             (3, 2, 5))
            self.assertEqual(json.loads(result["output_token_ids_json"]), [5, 6])

    def test_logprobs_follow_official_insertion_order_and_first_k_projection(self):
        engine = self.build()
        values = [{5: FakeLogprob(-3.0, 4, "Maybe"),
                   8: FakeLogprob(-0.6, 1, "False"), 9: FakeLogprob(-1.3, 2, "True")}]
        self.fake_llm.outputs = [probability_output(token_ids=(5,), values=values)]
        result = engine.run_prepared([prepared()], generation=GenerationSpec(logprobs=2))[0]
        content = json.loads(result["logprobs_json"])["content"][0]
        self.assertEqual(content["token"], "Maybe")
        self.assertEqual([item["token"] for item in content["top_logprobs"]], ["Maybe", "False"])
        self.assertEqual(content["logprob"], -3.0)

    def test_logprob_decoding_preserves_special_empty_and_exact_text_with_official_bytes(self):
        engine = self.build()
        values = [{5: FakeLogprob(-0.2, 1, None), 8: FakeLogprob(-2.0, 2, " False")},
                  {6: FakeLogprob(-0.3, 1, ""), 9: FakeLogprob(-2.0, 2, "é")}]
        self.fake_llm.outputs = [probability_output(values=values)]
        result = engine.run_prepared([prepared()], generation=GenerationSpec(logprobs=2))[0]
        content = json.loads(result["logprobs_json"])["content"]
        self.assertEqual(content[0]["token"], "True")
        self.assertIsNone(content[0]["bytes"])
        self.assertEqual(content[0]["top_logprobs"][0]["bytes"], [84, 114, 117, 101])
        self.assertEqual(content[0]["top_logprobs"][1]["token"], " False")
        self.assertEqual(content[1]["token"], "")
        self.assertEqual(content[1]["bytes"], [])
        self.assertEqual(content[1]["top_logprobs"][1]["bytes"], [195, 169])
        self.assertEqual(self.fake_llm.decode_calls, [[5]])

    def test_actual_extreme_logprobs_use_official_finite_wire_clamp(self):
        engine = self.build()
        values = [{5: FakeLogprob(float("-inf"), 2, "True"),
                   8: FakeLogprob(-10000.0, 1, "False")}]
        self.fake_llm.outputs = [probability_output(token_ids=(5,), values=values)]
        result = engine.run_prepared([prepared()], generation=GenerationSpec(logprobs=2))[0]
        content = json.loads(result["logprobs_json"])["content"][0]
        self.assertEqual(content["logprob"], -9999.0)
        self.assertEqual([item["logprob"] for item in content["top_logprobs"]], [-9999.0] * 2)
        json.dumps(result, allow_nan=False)

    def test_missing_malformed_or_unassociated_logprobs_fail_with_known_usage_and_no_refund(self):
        cases = [None, [], [None, {}], [{}, {}],
                 [{8: FakeLogprob(-0.2, 1, "False")}, {6: FakeLogprob(-0.3, 1, "\n")}],
                 [{5: FakeLogprob(-0.2, 1, "True"), True: FakeLogprob(-1, 2, "other")},
                  {6: FakeLogprob(-0.3, 1, "\n")}]]
        for probability in (float("nan"), float("inf"), True, "-0.2", 0.1):
            cases.append([{5: FakeLogprob(probability, 1, "True")},
                          {6: FakeLogprob(-0.3, 1, "\n")}])
        cases.append([{5: FakeLogprob(-0.2, 1, 123)}, {6: FakeLogprob(-0.3, 1, "\n")}])
        cases.append([{5: FakeLogprob(-0.2, 1, "True")}, {6: object()}])
        self.spec = replace(self.spec, max_model_calls=1)
        for values in cases:
            with self.subTest(values=values):
                self.fake_llm = FakeLLM()
                value = probability_output()
                value.outputs[0].logprobs = values
                self.fake_llm.outputs = [value]
                engine = self.build()
                result = engine.run_prepared([prepared()], generation=GenerationSpec(logprobs=10))[0]
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["text"])
                self.assertEqual(result["logprobs_json"], "null")
                self.assertEqual(result["total_tokens"], 5)
                self.assertEqual(json.loads(result["output_token_ids_json"]), [5, 6])
                self.assertEqual(engine.ready()["reserved_model_calls"], 1)
                with self.assertRaisesRegex(RuntimeError, "max_model_calls"):
                    engine.run_prepared([prepared()])
                self.assertEqual(len(self.fake_llm.calls), 1)

    def test_logprobs_require_output_ids_and_decode_failure_preserves_usage(self):
        engine = self.build()
        value = probability_output()
        value.outputs[0].token_ids = None
        self.fake_llm.outputs = [value]
        result = engine.run_prepared([prepared()], generation=GenerationSpec(logprobs=1))[0]
        self.assertEqual(result["status"], "failed")
        self.assertIsNone(result["completion_tokens"])
        self.assertEqual(result["prompt_tokens"], 3)
        value.outputs[0].token_ids = (5, 6)
        value.outputs[0].logprobs = [{5: FakeLogprob(-0.2)}, {6: FakeLogprob(-0.3, 1, "\n")}]
        self.fake_llm.decoded.clear()
        result = engine.run_prepared([prepared()], generation=GenerationSpec(logprobs=1))[0]
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["total_tokens"], 5)

    def test_requested_empty_completion_and_per_row_failure_preserve_actual_token_counts(self):
        engine = self.build()
        value = probability_output("request-0", token_ids=(), values=[])
        value.outputs[0].text = ""
        bad = probability_output("request-1")
        bad.outputs[0].logprobs = None
        self.fake_llm.outputs = [value, bad]
        results = engine.run_prepared([prepared(0), prepared(1)], generation=GenerationSpec(logprobs=10))
        self.assertEqual([item["status"] for item in results], ["succeeded", "failed"])
        self.assertEqual(results[0]["logprobs_json"], '{"content": []}')
        self.assertEqual(results[0]["text"], "")
        self.assertEqual([item["total_tokens"] for item in results], [3, 5])
        self.assertIsNone(results[1]["text"])
        self.assertEqual(engine.ready()["reserved_model_calls"], 2)

    def test_logprob_zero_and_small_top_k_reject_unrequested_candidate_counts(self):
        engine = self.build()
        for count in (0, 1):
            with self.subTest(count=count):
                values = [{5: FakeLogprob(-0.2, 1, "True"),
                           8: FakeLogprob(-2.0, 2, "False"),
                           9: FakeLogprob(-4.0, 3, "other")}]
                self.fake_llm.outputs = [probability_output(token_ids=(5,), values=values)]
                result = engine.run_prepared([prepared()], generation=GenerationSpec(logprobs=count))[0]
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["text"])
                self.assertEqual(result["total_tokens"], 4)

    def test_logprob_unknown_and_deadline_keep_reservation_and_known_completion_data(self):
        self.spec = replace(self.spec, max_model_calls=1)
        engine = self.build()
        self.fake_llm.error = RuntimeError("fake logprob chat possibly accepted")
        result = engine.run_prepared([prepared()], generation=GenerationSpec(logprobs=10))[0]
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["total_tokens"])
        self.assertEqual(engine.ready()["reserved_model_calls"], 1)
        self.fake_llm.error = None
        engine = self.build()
        self.fake_llm.outputs = [probability_output()]
        with patch.object(kernel.time, "monotonic", side_effect=[2.0, 7.0]):
            result = engine.run_prepared([prepared()], generation=GenerationSpec(logprobs=10),
                                         deadline_monotonic=5.0)[0]
        self.assertEqual(result["status"], "failed")
        self.assertIsNone(result["text"])
        self.assertIs(result["deadline_expired"], True)
        self.assertEqual(result["total_tokens"], 5)
        self.assertEqual(len(json.loads(result["logprobs_json"])["content"]), 2)
        self.assertEqual(engine.ready()["reserved_model_calls"], 1)

    @unittest.skipUnless(importlib.util.find_spec("lotus"),
                         "original LOTUS logprob consumer needs the pinned driver environment")
    def test_original_lotus_logprob_consumer_in_separate_python(self):
        """Real LOTUS/LiteLLM objects consume fake-vLLM bytes in another process."""
        import math

        engine = self.build()
        self.fake_llm.outputs = [
            probability_output("request-0", token_ids=(4, 5, 6), values=[
                {4: FakeLogprob(-0.4, 1, "Answer:")},
                {5: FakeLogprob(math.log(0.8), 1, "True"),
                 8: FakeLogprob(math.log(0.2), 2, "False")},
                {6: FakeLogprob(math.log(0.05), 3, "\n"),
                 5: FakeLogprob(math.log(0.095), 2, "True"),
                 8: FakeLogprob(math.log(0.855), 1, "False")},
            ]),
            probability_output("request-1", token_ids=(5,), values=[
                {5: FakeLogprob(-0.2, 1, "True")},
            ]),
            probability_output("request-2", token_ids=(5,), values=[
                {5: FakeLogprob(-0.2, 1, " true")},
            ]),
        ]
        results = engine.run_prepared([prepared(index) for index in range(3)],
                                     generation=GenerationSpec(logprobs=10))
        self.assertTrue(all(item["status"] == "succeeded" for item in results))
        source = """
import hashlib, importlib.metadata, json, sys
from pathlib import Path
from unittest.mock import patch
import lotus
from lotus.models import LM
from lotus.sem_ops.cascade_utils import calibrate_llm_logprobs
from lotus.types import CascadeArgs
from litellm.types.utils import ChatCompletionTokenLogprob, ModelResponse
assert importlib.metadata.version('lotus-ai') == '1.2.4'
package = Path(lotus.__file__).parent
assert hashlib.sha256((package/'models/lm.py').read_bytes()).hexdigest() == \
    '83e843ee8b381757525130ff09b0134fa77d3688a4321a9170e88ce79030e0e3'
assert hashlib.sha256((package/'sem_ops/cascade_utils.py').read_bytes()).hexdigest() == \
    '14fc1140939567559abde71976c1f3d21906864d720aa324f983f9dc8cf3b594'
lotus.settings.configure(enable_cache=False)
results = json.load(sys.stdin)
responses = [ModelResponse(id=item['engine_request_id'], model='openai/qwen2.5-7b',
    choices=[dict(index=0, finish_reason=item['finish_reason'],
                  message=dict(role='assistant',content=item['text']),
                  logprobs=json.loads(item['logprobs_json']))],
    usage={key:item[key] for key in ('prompt_tokens','completion_tokens','total_tokens')})
    for item in results]
lm = LM('openai/qwen2.5-7b', max_tokens=8)
messages = [[dict(role='user', content='fixture')]] * len(results)
with patch.object(lm, '_process_uncached_messages', return_value=responses) as seam:
    result = lm(messages, logprobs=True, show_progress_bar=False)
    assert seam.call_args.args[1]['logprobs'] is True
    assert seam.call_args.args[1]['top_logprobs'] == 10
assert isinstance(result.logprobs[0][0], ChatCompletionTokenLogprob)
formatted = lm.format_logprobs_for_filter_cascade(result.logprobs)
assert abs(formatted.positive_probs[0]-0.8) < 1e-12
assert formatted.positive_probs[1:] == [1, 0]
assert formatted.tokens == [['Answer:','True','\\n'],['True'],[' true']]
assert abs(formatted.confidences[0][1]-0.8) < 1e-12
assert list(calibrate_llm_logprobs(formatted.positive_probs, CascadeArgs())) == [0.5,1.0,0.0]
with patch.object(lm, '_process_uncached_messages', return_value=responses) as seam:
    assert lm(messages, logprobs=False, show_progress_bar=False).logprobs is None
    assert seam.call_args.args[1]['logprobs'] is False
    assert 'top_logprobs' not in seam.call_args.args[1]
assert lm.stats.physical_usage.total_tokens == 2 * sum(item['total_tokens'] for item in results)
assert 'vllm' not in sys.modules
print('original LOTUS typed content, top10, exact-token fallback, calibration and usage passed')
"""
        result = subprocess.run([sys.executable, "-c", source],
                                input=json.dumps(results, ensure_ascii=False, allow_nan=False),
                                capture_output=True, text=True, timeout=30,
                                env=dict(os.environ, HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                                         LITELLM_LOCAL_MODEL_COST_MAP="True"))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(any(name == "lotus" or name.startswith("lotus.") for name in sys.modules))

    def test_public_driver_preflight_rejects_other_interpreter_before_sampling(self):
        with patch.object(EngineSpec, "validate_local", return_value={
            "model": self.model_identity, "driver": self.driver_identity,
        }), self.assertRaisesRegex(RuntimeError, "driver_python"):
            self.spec.validate_driver_environment()
        self.assertEqual(self.events, [])

    def test_public_driver_preflight_accepts_matching_runtime(self):
        identity = {"model": self.driver_identity, "driver": self.model_identity}
        with patch.object(EngineSpec, "validate_local", return_value=identity):
            self.assertEqual(self.spec.validate_driver_environment(), identity)

    def test_vllm_sampling_constructor_failure_allocates_nothing(self):
        def reject(**kwargs):
            self.events.append(("sample", kwargs))
            raise ValueError("fake SamplingParams validation error")
        self.fake_module.SamplingParams = reject
        with self.environment(), self.assertRaises(ValueError):
            VllmEngine(self.spec, GenerationSpec())
        self.assertEqual([item[0] for item in self.events], ["sample"])

    def test_mutated_invalid_generation_is_rechecked_before_allocation(self):
        for name, value in (("presence_penalty", 3.0),
                            ("logprobs", True), ("logprobs", -1), ("logprobs", 21),
                            ("response_format", {"type": "json_object", "strict": True})):
            generation = GenerationSpec()
            object.__setattr__(generation, name, value)
            with self.subTest(name=name), self.environment(), self.assertRaises(ValueError):
                VllmEngine(self.spec, generation)
            self.assertEqual(self.events, [])

    def test_mutated_invalid_call_limit_is_rechecked_before_probe_or_allocation(self):
        for value in (True, 0, float("nan")):
            with self.subTest(value=value):
                object.__setattr__(self.spec, "max_model_calls", value)
                with self.environment(), patch.object(kernel, "_probe_python") as probe:
                    with self.assertRaisesRegex(ValueError, "max_model_calls"):
                        VllmEngine(self.spec, GenerationSpec())
                probe.assert_not_called()
                self.assertEqual(self.events, [])

    def test_invalid_explicit_template_is_rejected_before_model_allocation(self):
        chat_utils = types.ModuleType("vllm.entrypoints.chat_utils")

        def load_chat_template(value):
            self.events.append(("template", value))
            raise ValueError("fake missing chat template")

        chat_utils.load_chat_template = load_chat_template
        options = asdict(self.spec)
        options["chat_template"] = "/missing/template.jinja"
        with self.environment(), patch.dict(sys.modules, {
            "vllm.entrypoints.chat_utils": chat_utils,
        }), self.assertRaises(ValueError):
            VllmEngine(EngineSpec(**options), GenerationSpec())
        self.assertEqual([event[0] for event in self.events], ["sample", "template"])

    def test_public_constructor_rejects_driver_runtime_before_sampling(self):
        self.model_identity["prefix"] = self.temp.name + "/other-model-env"
        with self.environment(), self.assertRaisesRegex(RuntimeError, "model_python"):
            VllmEngine(self.spec, GenerationSpec())
        self.assertEqual(self.events, [])

    def test_public_constructor_checks_current_executable_directory(self):
        self.model_identity["executable"] = self.temp.name + "/model-env/bin/python"
        with self.environment(), self.assertRaisesRegex(RuntimeError, "current Python"):
            VllmEngine(self.spec, GenerationSpec())
        self.assertEqual(self.events, [])

    def test_imported_lotus_is_rejected_without_model_allocation(self):
        with self.environment(), patch.dict(sys.modules, {"lotus.models": types.ModuleType("lotus.models")}):
            with self.assertRaisesRegex(RuntimeError, "LOTUS"):
                VllmEngine(self.spec, GenerationSpec())
        self.assertEqual(self.events, [])

    def test_wrong_imported_vllm_version_is_rejected_without_allocation(self):
        self.fake_module.__version__ = "0.25.0"
        with self.environment(), self.assertRaisesRegex(RuntimeError, "0.25.1"):
            VllmEngine(self.spec, GenerationSpec())
        self.assertEqual(self.events, [])

    def test_generation_exceeding_context_allocates_nothing(self):
        with self.environment(), self.assertRaisesRegex(ValueError, "max_model_len"):
            VllmEngine(self.spec, GenerationSpec(max_tokens=5000))
        self.assertEqual(self.events, [])

    def test_chat_preserves_untemplated_messages_and_complete_call_identity(self):
        engine = self.build()
        self.fake_llm.outputs = [output("request-A"), output("request-B")]
        rows = [prepared(0), prepared(1)]
        results = engine.run_prepared(rows)
        conversations, kwargs = self.fake_llm.calls[0]
        self.assertEqual(conversations, [json.loads(row["messages_json"]) for row in rows])
        self.assertEqual(len(self.fake_llm.calls), 1)
        self.assertFalse(kwargs["use_tqdm"])
        for row, result in zip(rows, results):
            self.assertEqual({key: result[key] for key in kernel.IDENTITY_FIELDS},
                             {key: row[key] for key in kernel.IDENTITY_FIELDS})
            self.assertEqual(result["status"], "succeeded")
            self.assertIs(result["submission_confirmed"], True)
            self.assertEqual((result["prompt_tokens"], result["completion_tokens"],
                              result["total_tokens"]), (3, 2, 5))
            self.assertEqual(result["text"], "True")
            self.assertEqual(json.loads(result["output_token_ids_json"]), [5, 6])
            self.assertEqual(result["logprobs_json"], "null")
            self.assertGreaterEqual(result["model_elapsed_s"], 0)
            json.dumps(result, allow_nan=False)

    def test_missing_usage_is_none_not_zero_or_estimated(self):
        engine = self.build()
        value = output(prompt_token_ids=None)
        value.outputs[0].token_ids = None
        self.fake_llm.outputs = [value]
        result = engine.run_prepared([prepared()])[0]
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual([result[key] for key in (
            "prompt_tokens", "completion_tokens", "total_tokens",
        )], [None, None, None])
        self.assertEqual(result["output_token_ids_json"], "null")

    def test_invalid_finished_result_is_failed_without_legal_empty_text(self):
        engine = self.build()
        for change in (dict(finished=False), dict(outputs=[])):
            with self.subTest(change=change):
                self.fake_llm.outputs = [output(**change)]
                result = engine.run_prepared([prepared()])[0]
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["text"])
                self.assertEqual(result["prompt_tokens"], 3)
        for name, item in (("finish_reason", "abort"), ("index", 1), ("text", None),
                           ("token_ids", [True])):
            with self.subTest(field=name):
                value = output()
                setattr(value.outputs[0], name, item)
                self.fake_llm.outputs = [value]
                result = engine.run_prepared([prepared()])[0]
                self.assertEqual(result["status"], "failed")
                self.assertIsNone(result["text"])

    def test_output_count_or_request_identity_failure_is_unknown(self):
        engine = self.build()
        for outputs in ([], None, [output(), output()], [output("")]):
            with self.subTest(outputs=outputs):
                self.fake_llm.outputs = outputs
                result = engine.run_prepared([prepared()])[0]
                self.assertEqual(result["status"], "unknown")
                self.assertIsNone(result["text"])
                self.assertIsNone(result["total_tokens"])
        self.fake_llm.outputs = [output("duplicate"), output("duplicate")]
        self.assertTrue(all(result["status"] == "unknown" for result in
                            engine.run_prepared([prepared(0), prepared(1)])))

    def test_chat_error_has_unconfirmed_submission_and_unknown_usage(self):
        engine = self.build()
        self.fake_llm.error = RuntimeError("fake engine failed after possible acceptance")
        result = engine.run_prepared([prepared()])[0]
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["text"])
        self.assertIsNone(result["total_tokens"])
        self.assertIs(result["submission_confirmed"], False)
        self.assertIs(result["chat_invoked"], True)

    def test_default_call_limit_is_unbounded_and_reports_reserved_rows(self):
        engine = self.build()
        for index in range(2):
            self.assertEqual(engine.run_prepared([prepared(index)])[0]["status"], "succeeded")
        self.assertIsNone(engine.ready()["max_model_calls"])
        self.assertEqual(engine.ready()["reserved_model_calls"], 2)
        report = engine.close()
        self.assertIsNone(report["max_model_calls"])
        self.assertEqual(report["reserved_model_calls"], 2)

    def test_call_limit_counts_rows_across_batches_and_rejects_excess(self):
        self.spec = replace(self.spec, max_model_calls=3)
        engine = self.build()
        self.assertEqual(engine.ready()["reserved_model_calls"], 0)
        self.fake_llm.outputs = [output("request-0"), output("request-1")]
        self.assertTrue(all(result["status"] == "succeeded" for result in
                            engine.run_prepared([prepared(0), prepared(1)])))
        self.fake_llm.outputs = [output("request-2")]
        self.assertEqual(engine.run_prepared([prepared(2)])[0]["status"], "succeeded")
        with self.assertRaisesRegex(RuntimeError, "max_model_calls"):
            engine.run_prepared([prepared(3)])
        self.assertEqual(len(self.fake_llm.calls), 2)
        for report in (engine.ready(), engine.close(), engine.close()):
            self.assertEqual(report["max_model_calls"], 3)
            self.assertEqual(report["reserved_model_calls"], 3)

    def test_over_limit_batch_is_rejected_without_partial_chat_or_reservation(self):
        self.spec = replace(self.spec, max_model_calls=2)
        engine = self.build()
        with self.assertRaisesRegex(RuntimeError, "entire batch was not submitted"):
            engine.run_prepared([prepared(0), prepared(1), prepared(2)])
        self.assertEqual(self.fake_llm.calls, [])
        self.assertEqual(engine.ready()["reserved_model_calls"], 0)
        self.fake_llm.outputs = [output("request-0"), output("request-1")]
        self.assertTrue(all(result["status"] == "succeeded" for result in
                            engine.run_prepared([prepared(0), prepared(1)])))
        self.assertEqual(engine.ready()["reserved_model_calls"], 2)

    def test_unknown_and_failed_results_never_refund_reserved_calls(self):
        self.spec = replace(self.spec, max_model_calls=2)
        for failure in ("chat_exception", "association", "unfinished"):
            with self.subTest(failure=failure):
                self.fake_llm = FakeLLM()
                engine = self.build()
                if failure == "chat_exception":
                    self.fake_llm.error = RuntimeError("fake possible acceptance")
                elif failure == "association":
                    self.fake_llm.outputs = []
                else:
                    self.fake_llm.outputs = [output("request-0", finished=False),
                                             output("request-1", finished=False)]
                results = engine.run_prepared([prepared(0), prepared(1)])
                expected = "failed" if failure == "unfinished" else "unknown"
                self.assertEqual([result["status"] for result in results], [expected] * 2)
                self.assertEqual(engine.ready()["reserved_model_calls"], 2)
                with self.assertRaisesRegex(RuntimeError, "max_model_calls"):
                    engine.run_prepared([prepared(2)])
                self.assertEqual(len(self.fake_llm.calls), 1)

    def test_empty_and_expired_batches_do_not_consume_call_limit(self):
        self.spec = replace(self.spec, max_model_calls=1)
        engine = self.build()
        for reserved in (0, 1):
            with self.subTest(reserved=reserved):
                self.assertEqual(engine.run_prepared([]), [])
                with patch.object(kernel.time, "monotonic", return_value=5.0):
                    result = engine.run_prepared([prepared()], deadline_monotonic=4.0)[0]
                self.assertIs(result["submission_confirmed"], False)
                self.assertIs(result["deadline_expired"], True)
                self.assertEqual(engine.ready()["reserved_model_calls"], reserved)
                self.assertEqual(len(self.fake_llm.calls), reserved)
                if reserved == 0:
                    self.assertEqual(engine.run_prepared([prepared()])[0]["status"], "succeeded")

    def test_engine_limit_snapshot_cannot_be_increased_by_later_spec_mutation(self):
        self.spec = replace(self.spec, max_model_calls=1)
        engine = self.build()
        object.__setattr__(self.spec, "max_model_calls", None)
        engine.run_prepared([prepared()])
        with self.assertRaisesRegex(RuntimeError, "max_model_calls"):
            engine.run_prepared([prepared(1)])
        self.assertEqual(engine.ready()["max_model_calls"], 1)
        self.assertEqual(len(self.fake_llm.calls), 1)

    def test_bad_prepared_input_is_rejected_before_chat(self):
        engine = self.build()
        row = prepared()
        cases = [dict(row, call_index=True), dict(row, messages_json="{}"),
                 dict(row, messages_json=json.dumps([dict(role="user", content=[
                     dict(type="image_url", image_url="https://example.invalid/image"),
                 ])])), {key: value for key, value in row.items() if key != "query_id"}]
        for case in cases:
            with self.subTest(case=case), self.assertRaises(ValueError):
                engine.run_prepared([case])
        with self.assertRaises(ValueError):
            engine.run_prepared([row, row])
        self.assertEqual(self.fake_llm.calls, [])

    def test_expired_deadline_does_not_submit(self):
        engine = self.build()
        with patch.object(kernel.time, "monotonic", return_value=5.0):
            result = engine.run_prepared([prepared()], deadline_monotonic=4.0)[0]
        self.assertEqual(result["status"], "failed")
        self.assertIs(result["deadline_expired"], True)
        self.assertIs(result["submission_confirmed"], False)
        self.assertEqual(self.fake_llm.calls, [])

    def test_deadline_after_completion_keeps_usage_and_rejects_semantic_text(self):
        self.spec = replace(self.spec, max_model_calls=1)
        engine = self.build()
        with patch.object(kernel.time, "monotonic", side_effect=[2.0, 7.0]):
            result = engine.run_prepared([prepared()], deadline_monotonic=5.0)[0]
        self.assertEqual(result["status"], "failed")
        self.assertIs(result["deadline_expired"], True)
        self.assertIs(result["submission_confirmed"], True)
        self.assertEqual(result["total_tokens"], 5)
        self.assertIsNone(result["text"])
        self.assertEqual(engine.ready()["reserved_model_calls"], 1)
        with self.assertRaisesRegex(RuntimeError, "max_model_calls"):
            engine.run_prepared([prepared(1)])

    def test_bad_deadlines_and_timeouts_never_call_chat_or_shutdown(self):
        engine = self.build()
        for value in (True, float("nan"), float("inf"), 0, -1):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    engine.run_prepared([prepared()], deadline_monotonic=value)
                with self.assertRaises(ValueError):
                    engine.close(timeout=value)
        self.assertEqual(self.fake_llm.calls, [])
        self.assertEqual(self.fake_llm.core.calls, [])

    def test_close_failure_is_unknown_and_retry_retains_handle(self):
        self.spec = replace(self.spec, max_model_calls=1)
        engine = self.build()
        engine.run_prepared([prepared()])
        self.fake_llm.core.errors = [RuntimeError("fake shutdown failure")]
        first = engine.close(timeout=1.5)
        self.assertEqual(first["status"], "unknown")
        self.assertFalse(first["engine_api_shutdown_confirmed"])
        self.assertEqual((first["max_model_calls"], first["reserved_model_calls"]), (1, 1))
        self.assertIsNone(first["gpu_exit_confirmed"])
        self.assertFalse(engine.ready()["closed"])
        with self.assertRaisesRegex(RuntimeError, "cleanup is uncertain"):
            engine.run_prepared([prepared()])
        second = engine.close(timeout=2.5)
        self.assertEqual(second["status"], "closed")
        self.assertEqual(second["cleanup_scope"], "engine_core_api")
        self.assertTrue(second["engine_api_shutdown_confirmed"])
        self.assertEqual((second["max_model_calls"], second["reserved_model_calls"]), (1, 1))
        self.assertEqual(self.fake_llm.core.calls, [1.5, 2.5])
        self.assertEqual(engine.close(), second)
        with self.assertRaisesRegex(RuntimeError, "closed"):
            engine.run_prepared([prepared()])

    def test_detached_vllm_finalizer_cannot_make_failed_cleanup_confirmed(self):
        engine = self.build()
        self.fake_llm.core.errors = [RuntimeError("fake detach-then-fail")]
        self.assertEqual(engine.close()["status"], "unknown")
        self.fake_llm.core._finalizer = types.SimpleNamespace(alive=False)
        second = engine.close()
        self.assertEqual(second["status"], "unknown")
        self.assertEqual(second["error_type"], "ShutdownNotConfirmed")
        self.assertFalse(engine.ready()["closed"])


class SpecificationTests(unittest.TestCase):
    def test_optional_call_limit_preserves_old_configs_and_roundtrips(self):
        old_config = dict(model_path="/model", model_python="/model-python",
                          driver_python="/driver-python")
        self.assertIsNone(EngineSpec.from_dict(old_config).max_model_calls)
        spec = EngineSpec.from_dict({**old_config, "max_model_calls": 8})
        self.assertEqual(EngineSpec.from_dict(json.loads(json.dumps(asdict(spec)))), spec)
        self.assertEqual(spec.max_model_calls, 8)

    def test_generation_roundtrip_preserves_effective_parameters(self):
        generation = GenerationSpec.from_dict(json.loads(json.dumps(asdict(GenerationSpec()))))
        self.assertEqual(generation, GenerationSpec())
        self.assertEqual(generation.kwargs()["n"], 1)
        expected = asdict(GenerationSpec())
        expected.pop("response_format")
        self.assertEqual(generation.kwargs(), dict(expected, stop=["\n"], n=1))

    def test_json_object_roundtrip_and_kwargs_need_no_model_import(self):
        with patch.dict(sys.modules, {"vllm": None, "vllm.sampling_params": None}):
            generation = GenerationSpec.from_dict(dict(response_format={"type": "json_object"}))
            plain = generation.kwargs()
            restored = GenerationSpec.from_dict(json.loads(json.dumps(asdict(generation))))
        self.assertEqual(restored, generation)
        self.assertEqual(plain["structured_outputs"], {"json_object": True})
        self.assertNotIn("response_format", plain)
        json.dumps(plain, allow_nan=False)

    def test_logprob_counts_roundtrip_without_model_import(self):
        with patch.dict(sys.modules, {"vllm": None, "vllm.sampling_params": None}):
            for count in (None, 0, 10, 20):
                with self.subTest(count=count):
                    generation = GenerationSpec(logprobs=count)
                    restored = GenerationSpec.from_dict(json.loads(json.dumps(asdict(generation))))
                    self.assertEqual(restored, generation)
                    self.assertIs(generation.kwargs()["logprobs"], count)

    def test_response_format_and_kwargs_do_not_share_caller_mappings(self):
        original = {"type": "json_object"}
        generation = GenerationSpec(response_format=original)
        original["type"] = "json_schema"
        plain = generation.kwargs()
        plain["structured_outputs"]["json_object"] = False
        plain["stop"].append("changed")
        self.assertEqual(generation.response_format, {"type": "json_object"})
        self.assertEqual(generation.kwargs()["structured_outputs"], {"json_object": True})
        self.assertEqual(generation.stop, ("\n",))

    def test_unsupported_response_formats_are_rejected(self):
        for value in (True, False, 1, "json_object", [], {}, {"type": None},
                      {"type": True}, {"type": "text"}, {"type": "json_schema"},
                      {"type": "json_object", "strict": True},
                      {"type": "json_object", "json_schema": {}},
                      {"type": "json_object", "extra": None}):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "response_format"):
                GenerationSpec(response_format=value)

    def test_invalid_generation_values_are_rejected(self):
        for name, value in (
            ("max_tokens", True), ("max_tokens", 0), ("max_tokens", 1.5),
            ("temperature", float("nan")), ("temperature", float("inf")),
            ("temperature", 2.1), ("temperature", -0.1), ("temperature", True),
            ("top_p", 0), ("top_p", 1.01), ("top_p", "1"),
            ("presence_penalty", 3), ("frequency_penalty", -3),
            ("presence_penalty", float("nan")), ("frequency_penalty", True),
            ("repetition_penalty", 0), ("repetition_penalty", float("inf")),
            ("repetition_penalty", True), ("seed", True), ("seed", -1),
            ("stop", "\n"), ("stop", [""]), ("stop", [1]),
            ("logprobs", -1), ("logprobs", True), ("logprobs", False),
            ("logprobs", 21), ("logprobs", 1.0), ("logprobs", "10"),
            ("logprobs", float("nan")), ("logprobs", float("inf")),
        ):
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                GenerationSpec(**{name: value})

    def test_invalid_engine_capacities_and_numbers_are_rejected(self):
        for name, value in (
            ("max_num_seqs", True), ("max_num_batched_tokens", False),
            ("max_model_len", 0), ("gpu_memory_utilization", float("nan")),
            ("gpu_memory_utilization", float("inf")), ("gpu_memory_utilization", True),
            ("gpu_memory_utilization", 0), ("gpu_memory_utilization", 1.1),
            ("seed", True), ("dtype", "int8"),
            ("max_model_calls", True), ("max_model_calls", False),
            ("max_model_calls", 0), ("max_model_calls", -1),
            ("max_model_calls", 1.5), ("max_model_calls", "8"),
            ("max_model_calls", float("nan")), ("max_model_calls", float("inf")),
        ):
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                EngineSpec("/model", "/model-python", "/driver-python", **{name: value})

    def test_probe_reports_real_python_identity_without_importing_ml_packages(self):
        identity = kernel._probe_python(sys.executable)
        self.assertEqual(Path(identity["prefix"]).resolve(), Path(sys.prefix).resolve())
        self.assertEqual(Path(identity["executable"]).parent.resolve(),
                         Path(sys.executable).parent.resolve())
        self.assertIs(type(identity["lotus_available"]), bool)

    def test_local_validation_rejects_missing_artifacts_before_probe(self):
        spec = EngineSpec("/not-a-local-model-fixture", sys.executable, sys.executable)
        with patch.object(kernel, "_probe_python") as probe, self.assertRaises(ValueError):
            spec.validate_local()
        probe.assert_not_called()

    def test_local_validation_uses_prefixes_instead_of_resolved_venv_binary(self):
        with tempfile.TemporaryDirectory() as folder:
            model_dir = Path(folder) / "model"
            model_dir.mkdir()
            paths = []
            for name in ("model-env", "driver-env"):
                path = Path(folder) / name / "bin/python"
                path.parent.mkdir(parents=True)
                path.symlink_to(sys.executable)
                paths.append(path)
            self.assertEqual(paths[0].resolve(), paths[1].resolve())
            identities = [dict(executable=str(path), prefix=str(path.parent.parent),
                               base_prefix=sys.base_prefix, vllm_version="0.25.1",
                               lotus_available=False) for path in paths]
            spec = EngineSpec(str(model_dir), *(str(path) for path in paths))
            with patch.object(kernel, "_probe_python", side_effect=identities):
                self.assertEqual(spec.validate_local()["model"], identities[0])
            with patch.object(kernel, "_probe_python", return_value=identities[0]):
                with self.assertRaisesRegex(ValueError, "different Python"):
                    spec.validate_local()

    def test_model_environment_requires_fixed_version_and_no_lotus_installation(self):
        with tempfile.TemporaryDirectory() as folder:
            spec = EngineSpec(folder, sys.executable, sys.executable)
            model = dict(executable=sys.executable, prefix="/model-fixture",
                         base_prefix=sys.base_prefix, vllm_version="0.25.1", lotus_available=False)
            driver = dict(model, prefix="/driver-fixture")
            for change in (dict(vllm_version="0.25.0"), dict(lotus_available=True)):
                with self.subTest(change=change):
                    with patch.object(kernel, "_probe_python", side_effect=[dict(model, **change), driver]):
                        with self.assertRaises(ValueError):
                            spec.validate_local()

    @unittest.skipUnless(all(importlib.util.find_spec(name) for name in ("pyarrow", "numpy")),
                         "normal backend package import needs existing pyarrow and numpy")
    def test_normal_package_import_without_lotus_or_vllm_import(self):
        result = subprocess.run([
            sys.executable, "-c", "import sys; from src.serving.backends.vllm_engine import "
            "EngineSpec, GenerationSpec; "
            "assert GenerationSpec(response_format={'type':'json_object'}).kwargs()"
            "['structured_outputs'] == {'json_object':True}; "
            "assert 'lotus' not in sys.modules; assert 'vllm' not in sys.modules",
        ], cwd=CODE_ROOT, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
