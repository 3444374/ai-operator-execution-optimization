"""Differential checks against actual pinned LOTUS; no provider or model calls."""

import importlib.util
import json
import unittest
from dataclasses import asdict

from src.semantic_methods.lotus.records import FilterMethod, identity


LIBRARIES = all(importlib.util.find_spec(name) is not None for name in ("lotus", "litellm", "pandas"))


@unittest.skipUnless(LIBRARIES, "requires the isolated pinned LOTUS CPU environment")
class LotusEngineTests(unittest.TestCase):
    def setUp(self):
        import lotus
        from src.serving.backends.vllm_engine import GenerationSpec
        from src.semantic_methods.lotus.engine import validate_filter_source
        validate_filter_source()
        self.method = FilterMethod("openai/local-fixture")
        self.generation = GenerationSpec()
        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(self.scope.__exit__, None, None, None)

    def results(self):
        return [dict(identity("q", "row-" + str(i), i), status="succeeded", text=text,
                     prompt_tokens=10 + i, completion_tokens=3, total_tokens=13 + i,
                     finish_reason="stop", engine_request_id="engine-" + str(i))
                for i, text in enumerate(("Answer: True", "Answer: False", "uncertain"))]

    def test_worker_messages_match_native_formatter_with_empty_and_duplicate_text(self):
        import pandas as pd
        from lotus.templates import task_instructions
        import lotus
        from src.semantic_methods.lotus.engine import LotusFilterMethod
        method = LotusFilterMethod(self.method)
        texts = ["same", "", "same"]
        rows = [dict(identity("q", "row-" + str(i), i), reviewText=text)
                for i, text in enumerate(texts)]
        prepared = method.prepare_rows(rows)
        frame = pd.DataFrame({"reviewText": texts}, index=[1, 5, 10])
        docs = task_instructions.df2multimodal_info(frame, ["reviewText"])
        instruction = lotus.nl_expression.nle2str(self.method.instruction, ["reviewText"])
        native = [task_instructions.filter_formatter(method.model, doc, instruction) for doc in docs]
        self.assertEqual([json.loads(row["messages_json"]) for row in prepared], native)
        self.assertEqual([row["call_id"] for row in prepared], [row["call_id"] for row in rows])
        self.assertNotIn("reviewText", prepared[0])

    def test_native_usage_and_parser_match_with_unparseable_output(self):
        from lotus.sem_ops.postprocessors import filter_postprocess
        from src.semantic_methods.lotus.engine import engine_response, interpret_results, make_lm
        results = self.results()
        actual_lm = make_lm(self.method, self.generation)
        native_lm = make_lm(self.method, self.generation)
        actual = interpret_results(results, actual_lm, self.method)
        native_lm._process_uncached_messages = lambda *_: [engine_response(row, native_lm) for row in results]
        native = native_lm([[{"role": "user", "content": "fixture"}]] * len(results), show_progress_bar=False)
        parsed = filter_postprocess(native.outputs, native_lm, True)
        self.assertEqual([row["decision"] for row in actual], parsed.outputs)
        self.assertEqual([row["raw_output"] for row in actual], parsed.raw_outputs)
        self.assertEqual(asdict(actual_lm.stats), asdict(native_lm.stats))

    def test_finite_usage_limit_raises_like_original_lm(self):
        from lotus.types import UsageLimit
        from src.semantic_methods.lotus.engine import engine_response, interpret_results, make_lm
        results = self.results()
        actual_lm = make_lm(self.method, self.generation)
        native_lm = make_lm(self.method, self.generation)
        for lm in (actual_lm, native_lm):
            lm.physical_usage_limit = UsageLimit(prompt_tokens_limit=1)
        native_lm._process_uncached_messages = lambda *_: [engine_response(row, native_lm) for row in results]
        with self.assertRaises(Exception) as actual:
            interpret_results(results, actual_lm, self.method)
        with self.assertRaises(type(actual.exception)):
            native_lm([[{"role": "user", "content": "fixture"}]] * len(results), show_progress_bar=False)
        self.assertEqual(asdict(actual_lm.stats), asdict(native_lm.stats))

    def test_unknown_usage_and_failed_text_are_rejected(self):
        from src.semantic_methods.lotus.engine import engine_response, make_lm
        lm = make_lm(self.method, self.generation)
        result = self.results()[0]
        for changes in ({"prompt_tokens": None}, {"status": "failed"}, {"finish_reason": "abort"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                engine_response(dict(result, **changes), lm)

    def test_cache_guard_is_outside_any_operator_cache_call(self):
        import lotus
        from src.semantic_methods.lotus.engine import make_lm, validate_lm
        lm = make_lm(self.method, self.generation)
        with lotus.settings.context(enable_cache=True), self.assertRaisesRegex(ValueError, "caching disabled"):
            validate_lm(lm, self.method, self.generation)


if __name__ == "__main__":
    unittest.main()
