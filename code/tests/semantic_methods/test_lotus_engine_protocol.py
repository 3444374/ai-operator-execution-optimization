"""Known model receipts remain accounting evidence after a transport fault.

The pinned LOTUS objects and parsers are real; all responses are CPU fixtures.
"""

from copy import deepcopy
from dataclasses import asdict
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus.engine import interpret_results
from src.semantic_methods.lotus.record_chain import (
    ExtractMethod, LotusRecordChain, LotusRecordMethod, MapMethod,
    RecordOccurrence, StageSpec,
)
from src.semantic_methods.lotus.records import FilterMethod, MOVIE_Q3_INSTRUCTION
from src.serving.backends.vllm_engine import GenerationSpec
from tests.semantic_methods.test_lotus_cascade import models, terminal


def succeeded(call_id, text="True"):
    return dict(call_id=call_id, engine_request_id="fixture-" + call_id,
                status="succeeded", submission_confirmed=True, text=text,
                prompt_tokens=11, completion_tokens=3, total_tokens=14,
                finish_reason="stop")


class ProtocolFailureTests(unittest.TestCase):
    def setUp(self):
        import lotus

        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)

    def assert_usage(self, lm, prompt, completion):
        for usage in (lm.stats.physical_usage, lm.stats.virtual_usage):
            self.assertEqual(usage.prompt_tokens, prompt)
            self.assertEqual(usage.completion_tokens, completion)
            self.assertEqual(usage.total_tokens, prompt + completion)

    def test_filter_keeps_native_stats_but_never_parses_a_faulty_batch(self):
        method = FilterMethod("openai/fixture-main", "Accept {text}")
        results = [succeeded("first"), succeeded("last", "False")]
        healthy, _ = models()
        self.assertEqual([r["decision"] for r in interpret_results(results, healthy, method)],
                         [True, False])
        faulty, _ = models()
        observed = deepcopy(results)
        observed[1]["engine_protocol_error"] = "private engine changed call association"
        with patch("lotus.sem_ops.postprocessors.filter_postprocess") as parser:
            with self.assertRaises(RuntimeError) as failure:
                interpret_results(observed, faulty, method)
        parser.assert_not_called()
        self.assertEqual(failure.exception.engine_failures[0][0], observed[1])
        self.assertEqual(asdict(faulty.stats), asdict(healthy.stats))
        self.assert_usage(faulty, 22, 6)

    def test_presence_of_malformed_error_marker_also_prevents_semantic_success(self):
        for marker in (None, "", False, {}):
            with self.subTest(marker=marker):
                lm, _ = models()
                row = dict(succeeded("call"), engine_protocol_error=marker)
                with self.assertRaises(RuntimeError):
                    interpret_results([row], lm, FilterMethod(lm.model, "Accept {text}"))
                self.assert_usage(lm, 11, 3)

    def test_unknown_receipts_do_not_become_native_successes_or_zero_usage(self):
        lm, _ = models()
        rows = [dict(succeeded("known"), engine_protocol_error="batch fault"),
                dict(call_id="missing", status="unknown", submission_confirmed=False,
                     prompt_tokens=None, completion_tokens=None, total_tokens=None,
                     engine_protocol_error="batch fault")]
        with self.assertRaises(RuntimeError) as failure:
            interpret_results(rows, lm, FilterMethod(lm.model, "Accept {text}"))
        self.assertEqual(len(failure.exception.engine_failures), 2)
        self.assert_usage(lm, 11, 3)
        self.assertIsNone(rows[1]["prompt_tokens"])

    def test_each_record_method_accounts_the_batch_before_rejecting_protocol_error(self):
        methods = (
            (FilterMethod("openai/fixture-main", "Accept {text}"), GenerationSpec(), "True", "filter"),
            (MapMethod("openai/fixture-main", "Summarize {text}"), GenerationSpec(), "summary", "map"),
            (ExtractMethod("openai/fixture-main", ("text",), {"aspect": None}),
             GenerationSpec(response_format={"type": "json_object"}), '{"aspect":"x"}', "extract"),
        )
        for method, generation, text, kind in methods:
            with self.subTest(kind=kind):
                runner = LotusRecordMethod(StageSpec("method", method, generation).bind())
                results = [succeeded("first", text),
                           dict(succeeded("last", text), engine_protocol_error="batch fault")]
                with patch("lotus.sem_ops.postprocessors." + kind + "_postprocess") as parser:
                    with self.assertRaises(RuntimeError):
                        runner.interpret_results(results)
                parser.assert_not_called()
                self.assert_usage(runner.lm, 22, 6)

    def test_faulty_map_stops_the_shared_chain_before_dependent_filter(self):
        chain = LotusRecordChain((
            StageSpec("summary", MapMethod("openai/fixture-main", "Summarize {text}"), GenerationSpec()),
            StageSpec("keep", FilterMethod("openai/fixture-main", "Accept {summary}"), GenerationSpec()),
        ), query_id="faulty-chain")
        batches = []

        def infer(prepared, *, generation):
            batches.append(deepcopy(prepared))
            return [dict(row, **succeeded(row["call_id"], "summary"),
                         engine_protocol_error="batch fault") for row in prepared]

        records = [RecordOccurrence("r", 0, 17, {"text": "review"})]
        with self.assertRaises(RuntimeError) as failure:
            chain.run_batch(records, infer)
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(failure.exception.chain_calls), 1)
        self.assertEqual(failure.exception.chain_calls[0]["status"], "succeeded")
        self.assertNotIn("summary", records[0].values)
        self.assert_usage(chain.methods[0].lm, 11, 3)
        self.assert_usage(chain.methods[1].lm, 0, 0)

    def test_each_cascade_role_keeps_known_receipts_without_parsing(self):
        from src.semantic_methods.lotus.cascade import CascadeRun, _effective

        docs = [dict(row_id="r", source_position=0, reviewText="review")]
        for name in ("helper_score", "threshold_sample", "fallback"):
            with self.subTest(stage=name):
                main, helper = models()
                observed = []

                def evaluate(stage):
                    observed.append(stage)
                    return [dict(terminal(row, stage.model_role), engine_protocol_error="batch fault")
                            for row in stage.records]

                run = CascadeRun(evaluate, query_id="cascade", max_calls=6)
                run.main_lm, run.helper_lm = main, helper
                run.effective = {"main": _effective(main, {}, False),
                                 "helper": _effective(helper, {}, True)}
                run.role_columns = {"main": ("reviewText",), "helper": ("reviewText",)}
                run.active_stage = None if name == "helper_score" else name
                helper_stage = name == "helper_score"
                with patch("lotus.sem_ops.postprocessors.filter_postprocess") as parser:
                    with self.assertRaises(RuntimeError):
                        run.dispatch(docs, helper if helper_stage else main, MOVIE_Q3_INSTRUCTION,
                                     logprobs=helper_stage)
                parser.assert_not_called()
                self.assertEqual([stage.name for stage in observed], [name])
                self.assertEqual(run.report()["usage"]["known_prompt_tokens"], 13)
                self.assertEqual(run.report()["records"][0]["status"], "succeeded")
                self.assertFalse(any(event["event"] == "stage_completed" for event in run.events))
                self.assert_usage(helper if helper_stage else main, 13, 1)
                self.assert_usage(main if helper_stage else helper, 0, 0)

    def test_helper_protocol_failure_prevents_global_calibration_and_main_calls(self):
        import pandas as pd
        from lotus.types import CascadeArgs
        from src.semantic_methods.lotus.cascade import run_cascade

        main, helper = models()
        stages = []

        def evaluate(stage):
            stages.append(stage.name)
            return [dict(terminal(row, stage.model_role), engine_protocol_error="batch fault")
                    for row in stage.records]

        frame = pd.DataFrame([dict(row_id="r" + str(i), source_position=i, reviewText="review")
                              for i in range(2)])
        with patch("lotus.sem_ops.cascade_utils.calibrate_llm_logprobs") as calibrate:
            with self.assertRaises(RuntimeError) as failure:
                run_cascade(frame, main, helper, CascadeArgs(sampling_percentage=0.5), evaluate,
                            instruction=MOVIE_Q3_INSTRUCTION, query_id="cascade", max_rows=2, max_calls=5)
        calibrate.assert_not_called()
        self.assertEqual(stages, ["helper_score"])
        report = failure.exception.query_report
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 27)
        self.assertEqual(len(report["calls"]["records"]), 2)
        self.assert_usage(helper, 27, 2)
        self.assert_usage(main, 0, 0)


if __name__ == "__main__":
    unittest.main()
