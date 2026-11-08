"""Parser behavior and semantic selection with CPU-only response fixtures."""

from dataclasses import asdict
import json
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.semantic_methods.lotus import engine, record_method, cascade
from src.semantic_methods.lotus.filter_answers import (
    ANSWER_FIELD, LOTUS_NATIVE, FilterAnswers, parse_filter_answers,
)
from src.semantic_methods.lotus.records import FilterMethod
from src.semantic_methods.lotus.record_types import StageSpec
from src.serving.backends.vllm_protocol import GenerationSpec


FALSE_WITH_EXPLANATION = "Answer: False\nExplanation: The claim is not true."


def native_modules(parser):
    lotus, ops = ModuleType("lotus"), ModuleType("lotus.sem_ops")
    lotus.__path__, ops.__path__ = [], []
    post = ModuleType("lotus.sem_ops.postprocessors")
    post.filter_postprocess = parser
    types = ModuleType("lotus.types")
    types.SemanticFilterOutput = lambda **values: SimpleNamespace(**values)
    return {"lotus": lotus, "lotus.sem_ops": ops,
            "lotus.sem_ops.postprocessors": post, "lotus.types": types}


class LM:
    model = "fixture-model"

    def __init__(self):
        self.accounted = []

    def _update_stats(self, response, *, is_cached):
        self.accounted.append(response)

    def _get_top_choice(self, response):
        return response.text


class FilterAnswerTests(unittest.TestCase):
    def test_answer_field_excludes_explanation_and_accepts_ordinary_answers(self):
        cases = [(FALSE_WITH_EXPLANATION, False, ""),
                 ("Answer: True\nExplanation: Calling it false would be wrong.", True, ""),
                 ("Reasoning: It could be true elsewhere.\nAnswer: False", False,
                  "Reasoning: It could be true elsewhere."),
                 ("True", True, ""), ("False", False, ""), ("Answer: false.\r\n", False, "")]
        for text, expected, explanation in cases:
            with self.subTest(text=text):
                parsed = parse_filter_answers([text], None, answer_parser=ANSWER_FIELD)
                self.assertEqual(parsed.outputs, [expected])
                self.assertEqual(parsed.raw_outputs, [text])
                self.assertEqual(parsed.explanations, [explanation])

    def test_ambiguous_and_unknown_answers_keep_the_declared_default(self):
        for text in ("Answer: True or False", "Answer: False\nAnswer: True",
                     "Answer: undecided\nAnswer: False", "I cannot decide.",
                     '{"answer": false, "explanation": "not true"}'):
            for default in (True, False):
                with self.subTest(text=text, default=default):
                    parsed = parse_filter_answers([text], None, default, answer_parser=ANSWER_FIELD)
                    self.assertEqual(parsed.outputs, [default])
                    self.assertEqual(parsed.explanations, [""])
                    self.assertEqual(parsed.raw_outputs, [text])

    def test_native_choice_delegates_original_text_and_options(self):
        texts, model, result = [FALSE_WITH_EXPLANATION], object(), object()
        native = Mock(return_value=result)
        with patch.dict(sys.modules, native_modules(native)):
            self.assertIs(parse_filter_answers(texts, model, False), result)
        native.assert_called_once_with(texts, model, False, output_tokens=("True", "False"))
        self.assertIs(native.call_args.args[0], texts)

    def test_method_and_stage_serialize_the_choice_without_changing_legacy_default(self):
        legacy = FilterMethod.from_dict(dict(model_name="fixture", instruction="Keep {text}"))
        self.assertEqual(legacy.answer_parser, LOTUS_NATIVE)
        method = FilterMethod("fixture", "Keep {text}", answer_parser=ANSWER_FIELD)
        stage = StageSpec("keep", method, GenerationSpec())
        encoded = json.loads(json.dumps(stage.describe()))
        self.assertEqual(StageSpec.from_dict(encoded), stage)
        self.assertEqual(encoded["method"]["answer_parser"], ANSWER_FIELD)
        self.assertEqual(FilterMethod.from_dict(asdict(method)), method)
        with self.assertRaisesRegex(ValueError, "answer_parser"):
            FilterMethod("fixture", "Keep {text}", answer_parser="unspecified")

    def test_ordinary_and_chain_interpreters_share_the_selected_semantics(self):
        for parser, expected in ((ANSWER_FIELD, False), (LOTUS_NATIVE, True)):
            with self.subTest(parser=parser):
                method = FilterMethod("fixture", "Keep {text}", answer_parser=parser)
                lm = LM()
                worker = record_method.LotusRecordMethod.__new__(record_method.LotusRecordMethod)
                worker.stage, worker.method, worker.lm = SimpleNamespace(kind="filter"), method, lm
                native = Mock(return_value=FilterAnswers([FALSE_WITH_EXPLANATION], [True], [""]))
                response = SimpleNamespace(text=FALSE_WITH_EXPLANATION)
                with patch.dict(sys.modules, native_modules(native)), \
                     patch.object(engine, "engine_response", return_value=response), \
                     patch.object(record_method, "engine_response", return_value=response):
                    ordinary = engine.interpret_results([{}], lm, method)
                    chain = worker.interpret_results([{}])
                self.assertEqual(ordinary[0]["decision"], expected)
                self.assertEqual(chain.outputs, [expected])
                self.assertEqual(ordinary[0]["raw_output"], FALSE_WITH_EXPLANATION)
                self.assertEqual(len(lm.accounted), 2)
                self.assertEqual(native.call_count, 0 if parser == ANSWER_FIELD else 2)

    def test_unknown_receipts_are_not_parsed_as_boolean_answers(self):
        method = FilterMethod("fixture", "Keep {text}", answer_parser=ANSWER_FIELD)
        with patch.object(engine, "engine_response", side_effect=ValueError("unknown receipt")), \
             patch("src.semantic_methods.lotus.filter_answers.parse_filter_answers") as parse:
            with self.assertRaisesRegex(RuntimeError, "unknown"):
                engine.interpret_results([dict(status="unknown")], LM(), method)
        parse.assert_not_called()

    def test_native_entrypoints_reject_a_corrected_parser_before_execution(self):
        from src.baselines.text.frameworks.lotus_native_stage import execute_native
        method = FilterMethod("fixture", "Keep {text}", answer_parser=ANSWER_FIELD)
        stage = StageSpec("keep", method, GenerationSpec())
        with self.assertRaisesRegex(ValueError, "unmodified LOTUS"):
            execute_native(None, stage, [], invoke=Mock(), engine=None, resources=None,
                           query_id="fixture", profile={}, budget=None)
        with patch.dict(sys.modules, {"litellm": SimpleNamespace(CustomLLM=object)}):
            from src.baselines.text.frameworks.lotus_native_local import run
            with self.assertRaisesRegex(ValueError, "unmodified LOTUS"):
                run(None, None, method, None, None, None, query_id="fixture", profile={})

    def test_corrected_choice_reaches_movie_cascade_preflight(self):
        from src.experiments.lotus_cascade_movie import run_movie_cascade_query
        source = SimpleNamespace(preflight=Mock(side_effect=RuntimeError("fixture stops")))
        with self.assertRaisesRegex(RuntimeError, "fixture stops"):
            run_movie_cascade_query(source, None, LM(), None, None, None,
                                   query_id="fixture", max_calls=1, answer_parser=ANSWER_FIELD)
        self.assertEqual(source.preflight.call_args.args[0].answer_parser, ANSWER_FIELD)

    def test_cascade_dispatch_uses_the_same_choice_without_changing_stage_control(self):
        def evaluate(stage):
            return [dict(row, status="succeeded", submission_confirmed=True,
                         prompt_tokens=1, completion_tokens=2, text=FALSE_WITH_EXPLANATION)
                    for row in stage.records]
        lm = LM()
        run = cascade.CascadeRun(evaluate, query_id="fixture", max_calls=2, answer_parser=ANSWER_FIELD)
        run.main_lm, run.helper_lm = lm, LM()
        run.role_columns = {"main": ("text",), "helper": ("text",)}
        run.effective = {"main": dict(logprobs=False)}
        with patch.dict(sys.modules, native_modules(Mock())), \
             patch.object(cascade, "engine_response", return_value=SimpleNamespace(text=FALSE_WITH_EXPLANATION)):
            parsed = run.dispatch([dict(row_id="r", source_position=0, text="fixture")],
                                  lm, "Keep {text}")
        self.assertEqual(parsed.outputs, [False])
        self.assertEqual([event["event"] for event in run.events], ["stage_declared", "stage_completed"])
        self.assertEqual(len(lm.accounted), 1)
        self.assertEqual(run.report()["answer_parser"], ANSWER_FIELD)
        with patch.dict(sys.modules, native_modules(Mock())), \
             patch.object(run, "evaluate", side_effect=RuntimeError("backend fixture stopped")):
            with self.assertRaisesRegex(RuntimeError, "backend fixture stopped"):
                run.dispatch([dict(row_id="r", source_position=0, text="fixture")], lm, "Keep {text}")
        failure = run.report()
        self.assertEqual(failure["answer_parser"], ANSWER_FIELD)
        self.assertEqual([row['status'] for row in failure['records']], ['succeeded', 'unknown'])


if __name__ == "__main__":
    unittest.main()
