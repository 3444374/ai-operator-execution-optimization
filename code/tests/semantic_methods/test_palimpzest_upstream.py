"""Opt-in checks against the actual pinned PZ library; all completions are fixtures."""

from contextlib import contextmanager
import io
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments.palimpzest.ledger import PalimpzestLedger
from src.semantic_methods.palimpzest import completion_bridge, execute_frozen_plan, freeze_best_plan
from src.semantic_methods.palimpzest.source import validate_source


def response(answer="TRUE", *, usage=True):
    value = SimpleNamespace(model="fixture-local-model",
        choices=[SimpleNamespace(message=SimpleNamespace(content="ANSWER: " + answer + "\n---"))])
    if usage:
        value.usage = SimpleNamespace(model_dump=lambda: dict(prompt_tokens=11, completion_tokens=3))
    return value


@contextmanager
def temporary_working_directory():
    previous = Path.cwd()
    with tempfile.TemporaryDirectory() as temporary:
        try:
            os.chdir(temporary)
            yield Path(temporary)
        finally:
            os.chdir(previous)


@unittest.skipUnless(os.environ.get("SEMLOOM_PALIMPZEST_TEST") == "1",
                     "actual pinned PZ library checks require an isolated dependency environment")
class PalimpzestUpstreamTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import palimpzest as pz
        from palimpzest.constants import PromptStrategy
        from palimpzest.core.elements.records import DataRecord
        from palimpzest.query.generators import generators
        from palimpzest.query.processor.query_processor_factory import QueryProcessorFactory
        from pydantic import BaseModel
        from pydantic.fields import FieldInfo
        class Review(BaseModel):
            text: str
        cls.pz, cls.modules, cls.record = pz, generators, DataRecord
        cls.factory, cls.schema = QueryProcessorFactory, Review
        cls.fields = {"passed_operator": FieldInfo(annotation=bool)}
        cls.prompt_strategy = PromptStrategy.COT_BOOL
        validate_source()

    def generator(self):
        return self.modules.Generator(self.pz.Model.VLLM_QWEN_1_5_0_5B_CHAT,
                                      self.prompt_strategy, api_base="http://localhost:8000/v1")

    def candidate(self):
        candidate = self.record(self.schema, source_indices=["fixture-review-source-0"])
        candidate.text = "negative fixture review"
        return candidate

    def generate(self, generator):
        return generator(self.candidate(), self.fields, filter_condition="the review is negative",
                         project_cols=["text"])

    def test_original_prompt_parser_and_usage_match(self):
        calls = []
        def complete(**kwargs):
            calls.append(kwargs)
            return response()
        generator = self.generator()
        with patch.object(self.modules, "litellm", SimpleNamespace(completion=complete)):
            native = self.generate(generator)
        observer = PalimpzestLedger(max_calls=1)
        with completion_bridge(completion=complete, observer=observer) as state:
            controlled = self.generate(generator)
        self.assertEqual(native[0], controlled[0])
        self.assertEqual(native[1], controlled[1])
        self.assertEqual(native[3], controlled[3])
        self.assertEqual(calls[0], calls[1])
        self.assertNotIn("fixture-review-source-0", str(calls[1]["messages"]))
        self.assertEqual(native[2].total_input_tokens, controlled[2].total_input_tokens)
        self.assertEqual(native[2].total_output_tokens, controlled[2].total_output_tokens)
        self.assertEqual(observer.snapshot()["usage_status"], "available")
        self.assertTrue(state.snapshot()["patches_restored"])

    def test_original_completion_exception_stays_default_with_unavailable_usage(self):
        failure = TimeoutError("zero-model transport fixture")
        def complete(**kwargs):
            raise failure
        observer = PalimpzestLedger(max_calls=1)
        with patch("sys.stdout", new_callable=io.StringIO), \
                completion_bridge(completion=complete, observer=observer):
            result = self.generate(self.generator())
        self.assertEqual(result[0], {"passed_operator": False})
        self.assertEqual(result[2].total_llm_calls, 1)
        self.assertEqual(observer.snapshot()["completion_failed"], 1)
        self.assertEqual(observer.snapshot()["usage_status"], "unavailable")
        self.assertEqual(observer.predictions({"fixture-review-source-0": "movie-row-0"}),
                         [("movie-row-0", None)])

    def test_original_parse_failure_keeps_default_and_original_error_file(self):
        observer = PalimpzestLedger(max_calls=1)
        with temporary_working_directory() as temporary:
            with completion_bridge(completion=lambda **kw: response("unparseable"), observer=observer):
                result = self.generate(self.generator())
            self.assertEqual(result[0], {"passed_operator": False})
            self.assertEqual(len(list((temporary / "parse-answer-errors").iterdir())), 1)
        self.assertEqual(observer.snapshot()["parser_default_retained"], 1)
        self.assertEqual(observer.predictions({"fixture-review-source-0": "movie-row-0"}),
                         [("movie-row-0", None)])

    def test_missing_usage_preserves_original_exception_and_unavailable_ledger(self):
        observer = PalimpzestLedger(max_calls=1)
        with completion_bridge(completion=lambda **kw: response(usage=False), observer=observer):
            with self.assertRaises(AttributeError):
                self.generate(self.generator())
        summary = observer.snapshot()
        self.assertEqual(summary["usage_status"], "unavailable")
        self.assertIsNone(summary["monetary_cost_usd"])
        self.assertEqual(summary["generator_failed"], 1)

    def processor(self, query):
        config = self.pz.QueryProcessorConfig(execution_strategy="parallel", optimizer_strategy="none",
            available_models=[self.pz.Model.VLLM_QWEN_1_5_0_5B_CHAT], max_workers=4, join_parallelism=4,
            progress=False, use_vertex=False, api_base="http://localhost:8000/v1",
            allow_model_selection=False, allow_bonded_query=False, allow_rag_reduction=False,
            allow_mixtures=False, allow_critic=False)
        processor = self.factory.create_processor(query, config)
        self.assertEqual(processor.execution_strategy.max_workers, 4)
        self.assertIsNone(processor.validator)
        return processor

    def query(self):
        rows = [{"movieId": "taken_3", "text": "negative-fixture-" + str(index)} for index in range(8)]
        rows.append({"movieId": "another_movie", "text": "excluded-fixture"})
        source = self.pz.MemoryDataset("fixture-reviews", rows)
        return source.filter(lambda row: row["movieId"] == "taken_3").sem_filter(
            "the review is negative", depends_on=["text"]).count()

    def test_actual_parallel_count_uses_same_plan_and_all_filter_completions(self):
        processor = self.processor(self.query())
        frozen = freeze_best_plan(processor)
        observer = PalimpzestLedger(max_calls=8)
        with completion_bridge(completion=lambda **kw: response(), observer=observer) as bridge:
            with patch.object(processor.optimizer, "optimize", side_effect=AssertionError("reoptimized")):
                records, stats = execute_frozen_plan(processor, frozen)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].count, 8)
        self.assertEqual(stats.plan_id, frozen.plan_id)
        self.assertEqual(observer.snapshot()["completion_submitted"], 8)
        self.assertEqual(observer.snapshot()["completion_completed"], 8)
        self.assertEqual(bridge.snapshot()["active_generators"], 0)
        self.assertTrue(bridge.snapshot()["patches_restored"])
        self.assertIn("NonLLMFilter", frozen.describe()["operators"])
        self.assertIn("LLMFilter", frozen.describe()["operators"])

    def test_actual_generator_model_mutation_is_detected_before_completion(self):
        processor = self.processor(self.query())
        frozen = freeze_best_plan(processor)
        semantic_filter = next(op for op in frozen.plan if hasattr(op, "generator"))
        semantic_filter.generator.model_name = "unexpected-provider/model"
        with self.assertRaisesRegex(ValueError, "parameters changed"):
            execute_frozen_plan(processor, frozen)


if __name__ == "__main__":
    unittest.main()
