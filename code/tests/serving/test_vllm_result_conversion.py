"""Pure result conversion goldens; no vLLM, LOTUS or model allocation."""

from copy import deepcopy
from dataclasses import asdict
import json
import threading
import types
import unittest
from unittest.mock import Mock, patch

from tests.serving import test_vllm_engine as fixtures

kernel = fixtures.kernel


def golden(identity, **changes):
    value = {
        **identity, "status": "succeeded", "text": "True",
        "prompt_token_ids_json": "[1, 2, 3]", "output_token_ids_json": "[5, 6]",
        "logprobs_json": "null", "prompt_tokens": 3, "completion_tokens": 2,
        "total_tokens": 5, "finish_reason": "stop", "engine_request_id": "request-0",
        "engine_uid": "cpu-result-conversion", "model_start_monotonic": 2.0,
        "model_end_monotonic": 7.0, "model_elapsed_s": 5.0,
        "submission_confirmed": True, "deadline_expired": False, "error_type": None,
    }
    value.update(changes)
    return value


def output_cases():
    """Inputs and literal field deltas for successful and incomplete outputs."""
    cases = [("healthy", fixtures.output(), {})]
    value = fixtures.output()
    value.outputs[0].finish_reason, value.outputs[0].text = "length", "原文本"
    cases.append(("length", value, dict(finish_reason="length", text="原文本")))
    value = fixtures.output(prompt_token_ids=None)
    cases.append(("missing_prompt", value, dict(prompt_tokens=None, total_tokens=None,
                                                prompt_token_ids_json="null")))
    value = fixtures.output()
    value.outputs[0].token_ids = None
    cases.append(("missing_completion", value, dict(completion_tokens=None, total_tokens=None,
                                                    output_token_ids_json="null")))
    value = fixtures.output()
    value.outputs[0].token_ids, value.outputs[0].text = (), ""
    cases.append(("empty_completion", value, dict(completion_tokens=0, total_tokens=3,
                                                  output_token_ids_json="[]", text="")))
    value = fixtures.output(prompt_token_ids=[True])
    cases.append(("invalid_prompt", value, dict(status="failed", text=None, error_type="ValueError",
        prompt_token_ids_json="null", output_token_ids_json="null", prompt_tokens=None,
        completion_tokens=None, total_tokens=None, finish_reason=None)))
    value = fixtures.output()
    value.outputs[0].token_ids = [False]
    cases.append(("invalid_completion", value, dict(status="failed", text=None, error_type="ValueError",
        output_token_ids_json="null", completion_tokens=None, total_tokens=None, finish_reason=None)))
    value = fixtures.output(outputs=[])
    cases.append(("missing_choice", value, dict(status="failed", text=None, error_type="ValueError",
        output_token_ids_json="null", completion_tokens=None, total_tokens=None, finish_reason=None)))
    for name, field, item in (("unfinished", "finished", False),
                              ("bad_finish", "finish_reason", "aborted"),
                              ("bad_index", "index", True), ("bad_text", "text", None)):
        value = fixtures.output()
        setattr(value if field == "finished" else value.outputs[0], field, item)
        changes = dict(status="failed", text=None, error_type="ValueError")
        if field == "finish_reason": changes["finish_reason"] = item
        cases.append((name, value, changes))
    return cases


def fake_engine(module=kernel, generation=None):
    """Use the public sync path without constructing any model/interpreter probe."""
    engine = module.VllmEngine.__new__(module.VllmEngine)
    engine.generation = module.GenerationSpec.from_dict(asdict(
        kernel.GenerationSpec() if generation is None else generation))
    engine.engine_uid = "cpu-result-conversion"
    engine._max_model_len, engine._max_model_calls, engine._reserved_model_calls = 4096, 8, 0
    engine._closed = engine._shutdown_failed = False
    engine._lock = threading.Lock()
    engine._sampling_type = engine._structured_outputs_type = types.SimpleNamespace
    engine._sampling = engine._new_sampling(engine.generation.kwargs())
    engine._llm = fixtures.FakeLLM()
    return engine


class ResultConversionTests(unittest.TestCase):
    def setUp(self):
        self.identity = kernel._prepared([fixtures.prepared()])[0][0]
        self.no_process = patch("subprocess.Popen", side_effect=AssertionError("Popen forbidden"))
        self.no_process.start()
        self.addCleanup(self.no_process.stop)

    def convert(self, value, generation=None, get_tokenizer=None):
        return kernel.result_from_output(
            self.identity, value, engine_uid="cpu-result-conversion", start=2.0, end=7.0,
            generation=kernel.GenerationSpec() if generation is None else generation,
            get_tokenizer=Mock(side_effect=AssertionError("tokenizer not needed"))
            if get_tokenizer is None else get_tokenizer,
        )

    def test_terminal_record_golden_has_no_invented_usage_or_output(self):
        original = deepcopy(self.identity)
        for status, submitted, error in (("failed", False, "DeadlineExceeded"),
                                         ("unknown", False, "RuntimeError"),
                                         ("failed", True, "ValueError")):
            with self.subTest(status=status, submitted=submitted):
                actual = kernel.result_record(self.identity, engine_uid="cpu-result-conversion",
                    start=2.0, end=7.0, status=status, submitted=submitted, error_type=error)
                self.assertEqual(actual, golden(self.identity, status=status, text=None,
                    prompt_token_ids_json="null", output_token_ids_json="null", prompt_tokens=None,
                    completion_tokens=None, total_tokens=None, finish_reason=None, engine_request_id=None,
                    submission_confirmed=submitted, error_type=error))
        self.assertEqual(self.identity, original)
        actual["call_id"] = "changed-result"
        self.assertEqual(self.identity, original)

    def test_terminal_default_error_and_elapsed_clamp_keep_private_entry_compatible(self):
        engine = fake_engine()
        actual = kernel.result_record(self.identity, engine_uid=engine.engine_uid,
            start=7.0, end=2.0, status="unknown", submitted=False)
        self.assertEqual(actual["model_elapsed_s"], 0.0)
        self.assertIsNone(actual["error_type"])
        self.assertEqual(engine._result(self.identity, 7.0, 2.0, status="unknown", submitted=False), actual)

    def test_output_golden_preserves_every_field_and_known_partial_usage(self):
        for name, value, changes in output_cases():
            with self.subTest(case=name):
                self.assertEqual(self.convert(value), golden(self.identity, **changes))

    def test_logprob_content_and_missing_logs_keep_actual_token_counts(self):
        value = fixtures.probability_output()
        actual = self.convert(value, kernel.GenerationSpec(logprobs=10))
        content = [
            dict(token="True", logprob=-0.2, bytes=[84, 114, 117, 101], top_logprobs=[
                dict(token="True", logprob=-0.2, bytes=[84, 114, 117, 101]),
                dict(token="False", logprob=-1.8, bytes=[70, 97, 108, 115, 101])]),
            dict(token="\n", logprob=-0.3, bytes=[10], top_logprobs=[
                dict(token="\n", logprob=-0.3, bytes=[10])]),
        ]
        self.assertEqual(actual, golden(self.identity, logprobs_json=json.dumps(content, ensure_ascii=False)))
        for logs in (None, [], [{8: fixtures.FakeLogprob(-0.2, 1, "False")}, {}]):
            with self.subTest(logs=logs):
                value.outputs[0].logprobs = logs
                self.assertEqual(self.convert(value, kernel.GenerationSpec(logprobs=10)),
                    golden(self.identity, status="failed", text=None, error_type="ValueError"))

    def test_decode_failure_retains_usage_and_missing_token_ids_are_not_estimated(self):
        value = fixtures.probability_output()
        value.outputs[0].logprobs[0][5].decoded_token = None
        tokenizer = Mock()
        tokenizer.decode.side_effect = LookupError("self-written decode failure")
        get_tokenizer = Mock(return_value=tokenizer)
        actual = self.convert(value, kernel.GenerationSpec(logprobs=10), get_tokenizer)
        self.assertEqual(actual, golden(self.identity, status="failed", text=None, error_type="ValueError"))
        get_tokenizer.assert_called_once_with()
        tokenizer.decode.assert_called_once_with([5])
        value.outputs[0].token_ids = None
        actual = self.convert(value, kernel.GenerationSpec(logprobs=10))
        self.assertEqual(actual, golden(self.identity, status="failed", text=None, error_type="ValueError",
            output_token_ids_json="null", completion_tokens=None, total_tokens=None))

    def test_conversion_has_no_model_import_and_does_not_mutate_inputs(self):
        import sys

        value, generation = fixtures.probability_output(), kernel.GenerationSpec(logprobs=10)
        before = deepcopy((self.identity, value, asdict(generation)))
        get_tokenizer = Mock(side_effect=AssertionError("decoded text needs no tokenizer"))
        self.convert(value, generation, get_tokenizer)
        self.assertEqual((self.identity, value, asdict(generation)), before)
        get_tokenizer.assert_not_called()
        self.assertNotIn("vllm", sys.modules)
        self.assertNotIn("lotus", sys.modules)

    def test_sync_default_override_and_default_return_share_conversion_without_mutation(self):
        default = kernel.GenerationSpec(max_tokens=16, temperature=0.5)
        override = kernel.GenerationSpec(logprobs=10, response_format={"type": "json_object"}, stop=())
        engine = fake_engine(generation=default)
        snapshots = deepcopy((asdict(engine.generation), vars(engine._sampling)))
        samples = []
        for generation in (None, override, None):
            value = fixtures.probability_output()
            engine._llm.outputs = [value]
            with patch.object(kernel.time, "monotonic", side_effect=[2.0, 7.0]):
                actual = engine.run_prepared([fixtures.prepared()], generation=generation)[0]
            self.assertEqual(actual, self.convert(value, engine.generation if generation is None else generation))
            samples.append(engine._llm.calls[-1][1]["sampling_params"])
        self.assertEqual([item.logprobs for item in samples], [None, 10, None])
        self.assertEqual(len({id(item) for item in [engine._sampling, *samples]}), 4)
        self.assertEqual((asdict(engine.generation), vars(engine._sampling)), snapshots)
        self.assertEqual(engine._reserved_model_calls, 3)


if __name__ == "__main__":
    unittest.main()
