"""Original LOTUS 1.2.4 differential, with declared software model responses.

RUN_LOTUS_CASCADE_CPU=1 enables the pinned optional LOTUS environment. There is
no provider request, model inference, or pricing-service lookup in these tests.
"""

from importlib import import_module
import json
import math
import os
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus.cascade import (
    CascadePreparer, cascade_identity, cascade_response, run_cascade, validate_cascade_source,
    _generation_options,
)
from src.semantic_methods.lotus.engine import lm_report
from src.semantic_methods.lotus.records import MOVIE_Q3_INSTRUCTION


def models():
    from lotus.cache import InMemoryCache
    from lotus.models import LM
    from tokenizers import Tokenizer, models as token_models

    def model(name):
        tokenizer = Tokenizer(token_models.WordLevel(
            {"[UNK]": 0, "True": 1, "False": 2}, unk_token="[UNK]",
        ))
        return LM(name, tokenizer=tokenizer, cache=InMemoryCache(max_size=32),
                  max_tokens=9, temperature=0, top_p=0.95, stop=["\n"], seed=7)
    return model("openai/fixture-main"), model("openai/fixture-helper")


PROBS = [0.02, 0.05, 0.1, 0.25, 0.4, 0.6, 0.7, 0.85, 0.95, 0.99]


def terminal(row, role, *, omit_pair=False):
    position = row["source_position"]
    probability = PROBS[position % len(PROBS)]
    decision = position % 3 != 0
    token = "True" if (decision if role == "main" else probability > 0.5) else "False"
    tops = ([dict(token="other", logprob=math.log(0.1))] if omit_pair else [
        dict(token="True", logprob=math.log(probability)),
        dict(token="False", logprob=math.log(1 - probability)),
    ])
    content = [dict(token=token, logprob=math.log(0.6), top_logprobs=tops)]
    return dict({key: row[key] for key in (
        "query_id", "model_role", "algorithm_stage", "row_id", "source_position", "call_index", "call_id",
    )}, status="succeeded", submission_confirmed=True, text=token,
        finish_reason="stop", prompt_tokens=13 + position, completion_tokens=1,
        total_tokens=14 + position, engine_request_id=row["call_id"],
        engine_uid="declared-cpu-fixture", output_token_ids_json=json.dumps([1 if token == "True" else 2]),
        prompt_token_ids_json=json.dumps(list(range(13 + position))),
        logprobs_json=json.dumps(content) if role == "helper" else "null",
        usage_source="declared software fixture; zero model inference")


class RecordsFixture:
    def __init__(self, batch_size=3, omit_pair=False):
        self.batch_size, self.omit_pair = batch_size, omit_pair
        self.prepared, self.stages = [], []

    def __call__(self, stage):
        self.stages.append(stage)
        preparer = CascadePreparer(stage)
        rows = []
        for start in range(0, len(stage.records), self.batch_size):
            batch = stage.records[start:start + self.batch_size]
            prepared = preparer.prepare_rows(list(batch))
            self.prepared.extend((stage.name, json.loads(row["messages_json"]), stage.effective_kwargs)
                                 for row in prepared)
            rows.extend(terminal(row, stage.model_role, omit_pair=self.omit_pair) for row in batch)
        # Result ordering is a physical choice; algorithm positions are restored by call ID.
        return iter(reversed(rows))


@unittest.skipUnless(os.environ.get("RUN_LOTUS_CASCADE_CPU") == "1", "optional pinned LOTUS CPU differential")
class CascadeDifferentialTests(unittest.TestCase):
    def setUp(self):
        import lotus
        import pandas as pd

        self.scope = lotus.settings.context(enable_cache=False)
        self.scope.__enter__()
        self.addCleanup(lambda: self.scope.__exit__(None, None, None))
        self.frame = pd.DataFrame([
            dict(row_id="row-" + str(i), source_position=i,
                 reviewText="duplicate review" if i in (3, 9) else "review " + str(i))
            for i in range(10)
        ])

    def args(self, **options):
        from lotus.types import CascadeArgs

        return CascadeArgs(sampling_percentage=0.5, cascade_IS_random_seed=5,
                           cascade_num_calibration_quantiles=5, **options)

    def run_records(self, args=None, fixture=None, **options):
        main, helper = models()
        fixture = fixture or RecordsFixture()
        report = run_cascade(self.frame, main, helper, args or self.args(), fixture,
                             query_id="query", max_rows=20, max_calls=30, **options)
        return report, fixture, main, helper

    def reference(self, args, *, omit_pair=False, model_kwargs=None, default=True, system_prompt=None):
        import lotus

        module = import_module("lotus.sem_ops.sem_filter")
        main, helper = models()
        calls = []
        cursors = dict(helper=0, main=0)
        # Native messages carry the review text; the captured call's original
        # sample positions are recorded by the original sampling function.
        sampled = []
        original_sample = module.importance_sampling

        def sample(scores, settings):
            indices, weights = original_sample(scores, settings)
            sampled.extend(indices.tolist())
            return indices, weights

        for role, lm in (("main", main), ("helper", helper)):
            def fixed(data, kwargs, *_, _role=role, _lm=lm):
                if _role == "helper":
                    stage = "helper_score"
                    positions = list(range(len(self.frame)))
                elif args.filter_pos_cascade_threshold is None and cursors["main"] == 0:
                    stage = "threshold_sample"
                    positions = list(sampled)
                else:
                    stage = "fallback"
                    # Match native low-confidence calls from the original,
                    # globally calibrated scores and learned thresholds.
                    positions = []
                    for messages, _ in data:
                        text = json.dumps(messages)
                        candidates = [i for i in range(len(self.frame))
                                      if self.frame.iloc[i]["reviewText"] in text]
                        positions.append(candidates[0])
                if len(data) != len(positions):
                    raise AssertionError("reference fixture failed to associate native calls")
                cursors[_role] += 1
                calls.extend((stage, messages, kwargs) for messages, _ in data)
                results = [terminal(dict(cascade_identity(
                    "native", "row-" + str(i), i, occurrence, model_role=_role, stage=stage)),
                    _role, omit_pair=omit_pair) for occurrence, i in enumerate(positions)]
                # Construct native SDK fixtures independently of the records
                # response adapter so a conversion defect cannot mask itself.
                from litellm.types.utils import ModelResponse
                return [ModelResponse(id=row["engine_request_id"], model=_lm.model,
                    choices=[dict(index=0, finish_reason=row["finish_reason"],
                        message=dict(role="assistant", content=row["text"]),
                        logprobs=dict(content=json.loads(row["logprobs_json"]))
                            if kwargs.get("logprobs") else None)],
                    usage={key: row[key] for key in ("prompt_tokens", "completion_tokens", "total_tokens")})
                    for row in results]
            lm._process_uncached_messages = fixed
        # Duplicate reviews at 3 and 9 share the same deterministic oracle answer
        # and confidence side. The actual native sample retains both occurrences.
        with lotus.settings.context(lm=main, helper_lm=helper, enable_cache=False):
            with patch.object(module, "importance_sampling", sample):
                result, stats = self.frame.reset_index(drop=True).sem_filter(
                    MOVIE_Q3_INSTRUCTION, cascade_args=args, return_all=True,
                    return_raw_outputs=True, return_explanations=True, return_stats=True,
                    default=default, system_prompt=system_prompt,
                    **(model_kwargs or {}),
                )
        return result, stats, calls, sampled, main, helper

    def test_original_controller_learned_thresholds_messages_sampling_stats_usage(self):
        args = self.args()
        # Use distinct text here so the native response fixture can identify any
        # low-confidence source position without borrowing the new controller.
        self.frame["reviewText"] = ["unique review " + str(i) + ";" for i in range(10)]
        native, stats, calls, sampled, main, helper = self.reference(args)
        report, fixture, actual_main, actual_helper = self.run_records(args)
        self.assertEqual([row["decision"] for row in report["decisions"]], native["_filter"].tolist())
        self.assertEqual(report["positive_review_cnt"], sum(native["_filter"]))
        self.assertEqual([row["raw_output"] for row in report["decisions"]], native["raw_output_filter"].tolist())
        self.assertEqual([row["explanation"] for row in report["decisions"]], native["explanation_filter"].tolist())
        self.assertEqual(report["stats"], {key: value.model_dump(mode="json") if hasattr(value, "model_dump") else value
                                           for key, value in stats.items()})
        self.assertEqual(fixture.prepared, calls)
        self.assertEqual(lm_report(actual_main), lm_report(main))
        self.assertEqual(lm_report(actual_helper), lm_report(helper))
        selected = next(item for item in report["calls"]["events"] if item["event"] == "sample_selected")
        self.assertEqual(selected["indices"], sampled)
        self.assertLess(len(set(sampled)), len(sampled))
        records = report["calls"]["records"]
        self.assertEqual(len(records), 10 + len(sampled) + stats["filters_resolved_by_large_model"])
        self.assertEqual(len({row["call_id"] for row in records}), len(records))
        self.assertTrue(all(stage.effective_kwargs["top_logprobs"] == 10
                            for stage in fixture.stages if stage.model_role == "helper"))

    def test_physical_batch_sizes_do_not_change_global_calibration_or_thresholds(self):
        reference, _, _, _ = self.run_records(fixture=RecordsFixture(batch_size=1))
        for size in (2, 4, 25):
            with self.subTest(batch_size=size):
                actual, _, _, _ = self.run_records(fixture=RecordsFixture(batch_size=size))
                for key in ("decisions", "stats", "calls", "positive_review_cnt"):
                    self.assertEqual(actual[key], reference[key])

    def test_fixed_thresholds_and_equal_threshold_priority_match_native(self):
        self.frame["reviewText"] = ["unique review " + str(i) + ";" for i in range(10)]
        for positive, negative in ((0.8, 0.2), (0.4, 0.4), (0.0, 0.0)):
            with self.subTest(positive=positive, negative=negative):
                args = self.args(filter_pos_cascade_threshold=positive, filter_neg_cascade_threshold=negative)
                native, stats, calls, _, _, _ = self.reference(args)
                actual, fixture, _, _ = self.run_records(args)
                self.assertEqual([row["decision"] for row in actual["decisions"]], native["_filter"].tolist())
                self.assertEqual(actual["stats"]["filters_resolved_by_large_model"], stats["filters_resolved_by_large_model"])
                self.assertEqual(fixture.prepared, calls)
                self.assertNotIn("threshold_sample", [stage.name for stage in fixture.stages])
                if positive == negative:
                    self.assertEqual([stage.name for stage in fixture.stages], ["helper_score"])

    def test_native_missing_true_false_pair_fallback_is_preserved(self):
        args = self.args(filter_pos_cascade_threshold=0.8, filter_neg_cascade_threshold=0.2)
        actual, _, _, _ = self.run_records(args, fixture=RecordsFixture(omit_pair=True))
        native, _, _, _, _, _ = self.reference(args, omit_pair=True)
        self.assertEqual([row["decision"] for row in actual["decisions"]], native["_filter"].tolist())

    def test_sparse_index_normalization_and_repeated_draw_identities(self):
        normal, _, _, _ = self.run_records()
        self.frame.index = [50 + i * 3 for i in range(len(self.frame))]
        sparse, _, _, _ = self.run_records()
        self.assertEqual(sparse["calls"], normal["calls"])
        self.assertEqual(sparse["decisions"], normal["decisions"])
        samples = [row for row in sparse["calls"]["records"] if row["algorithm_stage"] == "threshold_sample"]
        self.assertLess(len({row["source_position"] for row in samples}), len(samples))
        self.assertEqual(len({row["call_id"] for row in samples}), len(samples))

    def test_empty_extension_never_allocates_a_backend(self):
        self.frame = self.frame.iloc[:0]
        report, fixture, _, _ = self.run_records()
        self.assertTrue(report["empty_input_extension"])
        self.assertEqual(report["positive_review_cnt"], 0)
        self.assertEqual(report["calls"]["records"], [])
        self.assertEqual(fixture.stages, [])

    def test_partial_stage_error_preserves_known_usage_and_missing_unknown(self):
        main, helper = models()
        closed = []

        def partial(stage):
            try:
                yield terminal(stage.records[0], stage.model_role)
                yield dict(terminal(stage.records[1], stage.model_role), status="failed",
                           prompt_tokens=9, completion_tokens=None, total_tokens=None)
                raise RuntimeError("fixture actor lost")
            finally:
                closed.append(True)
        with self.assertRaisesRegex(RuntimeError, "actor lost") as caught:
            run_cascade(self.frame, main, helper, self.args(), partial,
                        query_id="failure", max_rows=20, max_calls=30)
        report = caught.exception.query_report
        self.assertEqual(len(report["calls"]["records"]), 10)
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 22)
        self.assertIsNone(report["calls"]["usage"]["total_tokens"])
        self.assertEqual(sum(row["status"] == "unknown" for row in report["calls"]["records"]), 8)
        self.assertEqual(len(report["calls"]["stages"]), 1)
        self.assertEqual(closed, [True])

    def test_missing_helper_logprobs_fails_without_main_fallback(self):
        def missing(stage):
            return [dict(terminal(row, stage.model_role), logprobs_json="null") for row in stage.records]
        with self.assertRaisesRegex(RuntimeError, "malformed") as caught:
            self.run_records(fixture=missing)
        self.assertEqual([stage["name"] for stage in caught.exception.query_report["calls"]["stages"]],
                         ["helper_score"])
        self.assertGreater(caught.exception.query_report["calls"]["usage"]["known_prompt_tokens"], 0)

    def test_budget_and_zero_sample_are_checked_before_backend(self):
        main, helper = models()
        backend = RecordsFixture()
        with self.assertRaisesRegex(ValueError, "max_calls"):
            run_cascade(self.frame, main, helper, self.args(), backend,
                        query_id="q", max_rows=20, max_calls=10)
        self.frame = self.frame.iloc[:1]
        with self.assertRaisesRegex(ValueError, "at least one"):
            run_cascade(self.frame, main, helper, self.args(), backend,
                        query_id="q", max_rows=20, max_calls=30)
        self.assertEqual(backend.stages, [])

    def test_unsupported_reasoning_examples_columns_cache_and_tokenizer_fail_early(self):
        import lotus
        from lotus.types import ReasoningStrategy

        main, helper = models()
        backend = RecordsFixture()
        for options in (dict(strategy=ReasoningStrategy.COT), dict(examples=self.frame),
                        dict(helper_examples=self.frame)):
            with self.subTest(options=list(options)), self.assertRaises(ValueError):
                run_cascade(self.frame, main, helper, self.args(), backend,
                            query_id="q", max_rows=20, max_calls=30, **options)
        with lotus.settings.context(enable_cache=True), self.assertRaisesRegex(ValueError, "caches"):
            run_cascade(self.frame, main, helper, self.args(), backend,
                        query_id="q", max_rows=20, max_calls=30)
        original = helper.encode_text
        helper.encode_text = lambda token: [1, 2]
        with self.assertRaisesRegex(ValueError, "single-token"):
            run_cascade(self.frame, main, helper, self.args(), backend,
                        query_id="q", max_rows=20, max_calls=30)
        helper.encode_text = original
        self.assertEqual(backend.stages, [])

    def test_original_modules_and_lm_methods_are_not_mutated(self):
        module = import_module("lotus.sem_ops.sem_filter")
        original = (module.sem_filter, module.learn_filter_cascade_thresholds,
                    module.SemFilterDataframe.__call__, module.task_instructions.df2multimodal_info)
        self.run_records()
        self.assertEqual(original, (module.sem_filter, module.learn_filter_cascade_thresholds,
                                   module.SemFilterDataframe.__call__, module.task_instructions.df2multimodal_info))
        self.assertEqual(validate_cascade_source()["profile"], "lotus-helper-cascade-records-v1")

    def test_generation_override_merge_and_explicit_top_logprobs_match_native(self):
        self.frame["reviewText"] = ["unique review " + str(i) + ";" for i in range(10)]
        options = dict(temperature=0.1, max_completion_tokens=4, top_logprobs=3)
        native, _, calls, _, main, helper = self.reference(self.args(), model_kwargs=options)
        actual, fixture, actual_main, actual_helper = self.run_records(model_kwargs=options)
        self.assertEqual(fixture.prepared, calls)
        self.assertEqual([row["decision"] for row in actual["decisions"]], native["_filter"].tolist())
        self.assertEqual(lm_report(main), lm_report(actual_main))
        self.assertEqual(lm_report(helper), lm_report(actual_helper))
        self.assertEqual(_generation_options(dict(logprobs=False, max_completion_tokens=9, temperature=0))["stop"], ())

    def test_reserved_model_kwargs_and_lm_kwargs_are_rejected_before_execution(self):
        backend = RecordsFixture()
        for key, value in (("helper_examples", self.frame.assign(Answer=True)),
                           ("safe_mode", True), ("return_all", False), ("strategy", "COT"),
                           ("logprobs", True), ("response_format", {"type": "json_object"})):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.run_records(fixture=backend, model_kwargs={key: value})
        main, helper = models()
        helper.kwargs["helper_examples"] = self.frame.assign(Answer=True)
        with self.assertRaises(ValueError):
            run_cascade(self.frame, main, helper, self.args(), backend,
                        query_id="q", max_rows=20, max_calls=30)
        self.assertEqual(backend.stages, [])

    def test_worker_rejects_serialization_or_cache_changes(self):
        import lotus
        from lotus.types import SerializationFormat

        _, fixture, _, _ = self.run_records()
        stage = fixture.stages[0]
        for policy in (dict(enable_cache=True), dict(serialization_format=SerializationFormat.JSON)):
            with self.subTest(policy=policy), lotus.settings.context(**policy), self.assertRaisesRegex(ValueError, "worker"):
                CascadePreparer(stage)

    def test_generated_token_ids_are_typed_and_nonfinite_probabilities_fail(self):
        main, helper = models()
        row = terminal(dict(cascade_identity("q", "r", 0, 0, model_role="helper", stage="helper_score")), "helper")
        for token in (True, -1, "not-a-token", 1.5):
            with self.subTest(token=token), self.assertRaisesRegex(ValueError, "token IDs"):
                cascade_response(dict(row, output_token_ids_json=json.dumps([token])), helper, logprobs=True)
        def underflow(stage):
            rows = [terminal(record, stage.model_role) for record in stage.records]
            for value in rows:
                logs = json.loads(value["logprobs_json"])
                for candidate in logs[0]["top_logprobs"]:
                    candidate["logprob"] = -10000
                value["logprobs_json"] = json.dumps(logs)
            return rows
        with self.assertRaisesRegex(ValueError, "nonfinite"):
            self.run_records(fixture=underflow)

    def test_result_identity_mutation_missing_and_duplicate_results_fail(self):
        for mutation in (dict(source_position=True), dict(call_index=0.0), dict(call_id="forged")):
            def changed(stage):
                values = [terminal(row, stage.model_role) for row in stage.records]
                values[0].update(mutation)
                return values
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.run_records(fixture=changed)
        def duplicate(stage):
            result = terminal(stage.records[0], stage.model_role)
            return [result, result]
        with self.assertRaisesRegex(ValueError, "duplicated"):
            self.run_records(fixture=duplicate)
        with self.assertRaisesRegex(ValueError, "omitted") as caught:
            self.run_records(fixture=lambda stage: [terminal(stage.records[0], stage.model_role)])
        self.assertEqual(sum(row["status"] == "unknown" for row in caught.exception.query_report["calls"]["records"]), 9)

    def test_lifecycle_is_rechecked_after_last_stage_close(self):
        class CancelAfterClose(RecordsFixture):
            def __init__(self):
                super().__init__()
                self.cancelled = False
            def check_lifecycle(self):
                if self.cancelled:
                    raise InterruptedError("fixture cancelled after engine close")
            def __call__(self, stage):
                values = list(super().__call__(stage))
                owner = self
                class Rows:
                    def __init__(self):
                        self.rows = iter(values)
                    def __iter__(self):
                        return self
                    def __next__(self):
                        return next(self.rows)
                    def close(self):
                        owner.cancelled = True
                return Rows()
        args = self.args(filter_pos_cascade_threshold=0, filter_neg_cascade_threshold=0)
        with self.assertRaisesRegex(InterruptedError, "after engine close") as caught:
            self.run_records(args, fixture=CancelAfterClose())
        self.assertEqual(caught.exception.query_report["calls"]["usage"]["prompt_tokens"], sum(13 + i for i in range(10)))

    def test_success_report_has_strict_json_and_infinite_weight_configuration_is_rejected(self):
        report, _, _, _ = self.run_records()
        json.dumps(report, allow_nan=False)
        args = self.args()
        args.cascade_IS_weight = 1
        with self.assertRaisesRegex(ValueError, "finite correction"):
            self.run_records(args)

    def test_distinct_helper_instruction_system_prompt_and_parser_policy_match_native(self):
        self.frame["reviewText"] = ["unique review " + str(i) + ";" for i in range(10)]
        args = self.args(helper_filter_instruction="Is this review positive? {reviewText}")
        native, _, calls, _, main, helper = self.reference(args, default=False, system_prompt="Answer the claim.")
        report, fixture, actual_main, actual_helper = self.run_records(
            args, default=False, system_prompt="Answer the claim.")
        self.assertEqual(fixture.prepared, calls)
        self.assertEqual([row["decision"] for row in report["decisions"]], native["_filter"].tolist())
        self.assertEqual(lm_report(main), lm_report(actual_main))
        self.assertEqual(lm_report(helper), lm_report(actual_helper))

    def test_finite_usage_limit_preserves_native_update_order_and_complete_call_evidence(self):
        from lotus.types import UsageLimit

        self.frame["reviewText"] = ["unique review " + str(i) + ";" for i in range(10)]
        actual_main, actual_helper = models()
        actual_main.virtual_usage_limit = UsageLimit(prompt_tokens_limit=20)
        with self.assertRaises(Exception) as caught:
            run_cascade(self.frame, actual_main, actual_helper, self.args(), RecordsFixture(),
                        query_id="limit", max_rows=20, max_calls=30)
        error = caught.exception
        self.assertEqual(type(error).__name__, "LotusUsageLimitException")
        samples = [row for row in error.query_report["calls"]["records"] if row["algorithm_stage"] == "threshold_sample"]
        self.assertEqual(len(samples), 5)
        self.assertEqual(error.query_report["calls"]["usage"]["unknown_prompt_tokens_calls"], 0)
        prompt = [row["prompt_tokens"] for row in samples]
        stats = lm_report(actual_main)["stats"]
        self.assertEqual(stats["virtual_usage"]["prompt_tokens"], sum(prompt[:2]))
        self.assertEqual(stats["physical_usage"]["prompt_tokens"], prompt[0])

    def test_first_generated_position_with_both_labels_controls_native_score(self):
        _, helper = models()
        identity = cascade_identity("q", "r", 0, 0, model_role="helper", stage="helper_score")
        row = terminal(identity, "helper")
        logs = [dict(token="noise", logprob=-0.2, top_logprobs=[dict(token="noise", logprob=-0.2)]),
                dict(token="False", logprob=-0.3, top_logprobs=[
                    dict(token="True", logprob=math.log(0.3)), dict(token="False", logprob=math.log(0.7))]),
                dict(token="True", logprob=-0.5, top_logprobs=[
                    dict(token="True", logprob=math.log(0.9)), dict(token="False", logprob=math.log(0.1))])]
        row.update(completion_tokens=3, total_tokens=row["prompt_tokens"]+3,
                   output_token_ids_json="[3,2,1]", logprobs_json=json.dumps(logs))
        response = cascade_response(row, helper, logprobs=True)
        probabilities = helper.format_logprobs_for_filter_cascade([helper._get_top_choice_logprobs(response)])
        self.assertAlmostEqual(probabilities.positive_probs[0], 0.3)


if __name__ == "__main__":
    unittest.main()
