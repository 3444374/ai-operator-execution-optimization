"""Original helper cascade versus record stages over deterministic CPU responses."""

from dataclasses import asdict
from importlib import import_module
import json
import math
import re
import unittest

from src.semantic_methods.lotus.cascade import CascadePreparer, cascade_response, run_cascade


PROBABILITIES = (0.99, 0.1, 0.4, 0.6, 0.8, 0.01) * 2
INSTRUCTION = "Keep positive reviews: {body}"


def answer(position, role, identity):
    probability = PROBABILITIES[position]
    text = str(probability > 0.5 if role == "helper" else position % 2 == 0)
    result = dict(identity, status="succeeded", text=text, prompt_tokens=7, completion_tokens=1,
                  total_tokens=8, finish_reason="stop", engine_request_id="cpu-fixture",
                  submission_confirmed=True)
    if role == "helper":
        tops = [dict(token="True", logprob=math.log(probability)),
                dict(token="False", logprob=math.log(1 - probability))]
        result.update(logprobs_json=json.dumps([dict(token=text,
            logprob=math.log(probability if text == "True" else 1 - probability), top_logprobs=tops)]),
            output_token_ids_json="[1]")
    return result


def models(batches):
    from lotus.cache import InMemoryCache
    from lotus.models import LM

    result = []
    for role in ("main", "helper"):
        lm = LM("openai/" + role + "-fixture", max_tokens=8, temperature=0,
                cache=InMemoryCache(max_size=1))
        lm.encode_text = lambda text: [1 if text == "True" else 2]

        def respond(data, options, *_args, role=role, lm=lm):
            messages = [message for message, _ in data]
            positions = [int(re.search(r"case-(\d+)", json.dumps(m)).group(1)) for m in messages]
            batches.append((role, positions, messages))
            return [cascade_response(answer(p, role, {}), lm, logprobs=role == "helper")
                    for p in positions]

        lm._process_uncached_messages = respond
        result.append(lm)
    return result


def frame():
    import pandas as pd

    return pd.DataFrame(dict(row_id=["row-" + str(i) for i in range(12)],
        source_position=list(range(12)), body=["case-" + str(i) for i in range(12)]))


def arguments(learned):
    from lotus.types import CascadeArgs

    options = dict(recall_target=0.5, precision_target=0.5, sampling_percentage=1,
                   failure_probability=0.2, cascade_IS_weight=0.5,
                   cascade_IS_random_seed=13, cascade_IS_max_sample_range=3,
                   cascade_num_calibration_quantiles=5,
                   helper_filter_instruction="Judge sentiment: {body}")
    if not learned:
        options.update(filter_pos_cascade_threshold=0.75, filter_neg_cascade_threshold=0.25)
    return CascadeArgs(**options)


class OriginalCascadeDifferentialTests(unittest.TestCase):
    def compare(self, *, learned):
        import lotus

        native_batches = []
        native_main, native_helper = models(native_batches)
        args = arguments(learned)
        with lotus.settings.context(lm=native_main, helper_lm=native_helper, enable_cache=False):
            expected, stats = frame().sem_filter(INSTRUCTION, cascade_args=args,
                return_all=True, return_raw_outputs=True, return_explanations=True, return_stats=True)
        main, helper = models([])
        batches = []

        def evaluate(stage):
            prepared = CascadePreparer(stage).prepare_rows(list(stage.records))
            batches.append((stage.model_role, [row["source_position"] for row in stage.records],
                            [json.loads(row["messages_json"]) for row in prepared]))
            return [answer(row["source_position"], stage.model_role, row) for row in stage.records]

        upstream = import_module("lotus.sem_ops.sem_filter")
        original = (upstream.SemFilterDataframe.__call__, upstream.learn_filter_cascade_thresholds,
                    upstream.sem_filter)
        with lotus.settings.context(enable_cache=False):
            actual = run_cascade(frame(), main, helper, args, evaluate,
                instruction=INSTRUCTION, query_id="cpu-cascade", max_rows=12, max_calls=36)
        self.assertEqual(original, (upstream.SemFilterDataframe.__call__,
            upstream.learn_filter_cascade_thresholds, upstream.sem_filter))
        self.assertEqual([row["decision"] for row in actual["decisions"]], expected["_filter"].tolist())
        self.assertEqual([row["raw_output"] for row in actual["decisions"]],
                         expected["raw_output_filter"].tolist())
        self.assertEqual([row["explanation"] for row in actual["decisions"]],
                         expected["explanation_filter"].tolist())
        self.assertEqual(batches, native_batches)
        self.assertEqual(actual["stats"], {k: v.model_dump(mode="json") if hasattr(v, "model_dump")
                                         else v for k, v in stats.items()})
        self.assertEqual(asdict(main.stats), asdict(native_main.stats))
        self.assertEqual(asdict(helper.stats), asdict(native_helper.stats))
        calls = actual["calls"]["records"]
        self.assertEqual(len({row["call_id"] for row in calls}), len(calls))
        self.assertEqual(actual["calls"]["usage"]["known_prompt_tokens"], 7 * len(calls))
        if learned:
            sampled = [r["source_position"] for r in calls if r["algorithm_stage"] == "threshold_sample"]
            self.assertGreater(len(sampled), len(set(sampled)))
            self.assertTrue(any(e["event"] == "thresholds_learned" for e in actual["calls"]["events"]))
        else:
            self.assertTrue(any(r["algorithm_stage"] == "fallback" for r in calls))

    def test_fixed_thresholds_keep_helper_and_fallback_prompts_decisions_and_usage(self):
        self.compare(learned=False)

    def test_learned_thresholds_keep_repeated_sample_occurrences_and_original_learning(self):
        self.compare(learned=True)

    def test_invalid_helper_metadata_retains_known_usage_without_main_fallback(self):
        import lotus

        main, helper = models([])

        def evaluate(stage):
            return [dict(answer(row["source_position"], stage.model_role, row), logprobs_json="null")
                    for row in stage.records]

        with lotus.settings.context(enable_cache=False), self.assertRaisesRegex(RuntimeError,
                "malformed token metadata") as caught:
            run_cascade(frame(), main, helper, arguments(False), evaluate,
                instruction=INSTRUCTION, query_id="invalid-helper", max_rows=12, max_calls=36)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"]["usage"]["known_prompt_tokens"], 84)
        self.assertEqual(report["lm"]["helper"]["stats"]["physical_usage"]["prompt_tokens"], 84)
        self.assertEqual([s["name"] for s in report["calls"]["stages"]], ["helper_score"])


if __name__ == "__main__":
    unittest.main()
