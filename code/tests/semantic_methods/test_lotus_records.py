"""Call identity, finite resource declarations and independent usage accounting."""

from dataclasses import asdict
import json
import unittest

from src.semantic_methods.lotus.records import (
    CallLedger, FilterMethod, MOVIE_Q3_INSTRUCTION, RecordsResources, identity,
)


class FilterMethodTests(unittest.TestCase):
    def test_plain_config_roundtrips_through_worker_json(self):
        method = FilterMethod("fixture-model", default=False, system_prompt="custom")
        restored = FilterMethod.from_dict(json.loads(json.dumps(method.describe())))
        self.assertEqual(restored, method)
        self.assertEqual(restored.instruction, MOVIE_Q3_INSTRUCTION)
        self.assertEqual(restored.output_tokens, ("True", "False"))

    def test_rejects_unsupported_semantics_and_wrong_types(self):
        cases = (
            dict(model_name=""), dict(model_name=True), dict(instruction="other {reviewText}"),
            dict(default=1), dict(output_tokens=["True", "False"]),
            dict(output_tokens=("False", "True")), dict(system_prompt=True),
        )
        for options in cases:
            with self.subTest(options=options), self.assertRaises(ValueError):
                FilterMethod(**dict(dict(model_name="fixture"), **options))
        for options in ({"model_name": "fixture", "cascade_args": {}}, [],
                        {"model_name": "fixture", "output_tokens": "True"}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                FilterMethod.from_dict(options)


class RecordsResourcesTests(unittest.TestCase):
    def test_default_single_gpu_profile_is_vendor_independent(self):
        resources = RecordsResources()
        self.assertEqual(resources.gpu_workers, 1)
        self.assertEqual(resources.describe(), asdict(resources))
        self.assertEqual(RecordsResources(max_active_batches=1).max_active_batches, 1)

    def test_rejects_non_integer_or_nonpositive_capacities(self):
        for name in ("cpu_workers", "gpu_workers", "prepare_batch_rows", "model_batch_rows",
                     "max_active_batches"):
            for value in (True, False, 0, -1, 1.0, float("nan"), float("inf")):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    RecordsResources(**{name: value})
        with self.assertRaisesRegex(ValueError, "one GPU"):
            RecordsResources(gpu_workers=2)

    def test_rejects_nonfinite_or_nonpositive_deadlines(self):
        for name in ("query_timeout_s", "startup_timeout_s"):
            for value in (True, False, 0, -1, float("nan"), float("inf"), "1", 10 ** 1000):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    RecordsResources(**{name: value})


class CallIdentityTests(unittest.TestCase):
    def test_default_filter_id_is_byte_compatible(self):
        default = identity("query", "row", 0)
        self.assertEqual(default["call_id"], 'lotus-filter-v1:["query","row",0,0]')
        self.assertEqual(default, identity("query", "row", 0, model_role="main",
                                           algorithm_stage="filter"))

    def test_roles_stages_and_repeated_samples_have_distinct_ids(self):
        calls = [identity("q", "row", 0)]
        for role, stage in (("helper", "helper_score"), ("main", "threshold_sample"),
                            ("main", "fallback"), ("helper", "fallback")):
            calls.append(identity("q", "row", 0, model_role=role, algorithm_stage=stage))
        calls.append(identity("q", "row", 0, 1, model_role="main",
                              algorithm_stage="threshold_sample"))
        self.assertEqual(len({call["call_id"] for call in calls}), len(calls))
        for call in calls[1:]:
            self.assertEqual(json.loads(call["call_id"].split(":", 1)[1]),
                             ["q", "row", 0, call["call_index"], call["model_role"],
                              call["algorithm_stage"]])

    def test_roles_and_stages_reject_missing_or_nonstring_values(self):
        for name in ("model_role", "algorithm_stage"):
            for value in (None, True, 1, "", " \t"):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    identity("q", "row", 0, **{name: value})

    def test_id_is_stable_and_delimiters_do_not_alias(self):
        self.assertEqual(identity("query", "row", 0), identity("query", "row", 0))
        ids = [identity("a:b", "c", 0), identity("a", "b:c", 0),
               identity("a:b", "c", 1), identity("a:b", "c", 0, 1)]
        self.assertEqual(len({item["call_id"] for item in ids}), 4)
        self.assertEqual(ids[0]["model_role"], "main")
        self.assertEqual(ids[0]["algorithm_stage"], "filter")

    def test_positions_and_call_ordinals_reject_bool_and_non_integer(self):
        for value in (True, -1, 1.0, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                identity("q", "row", value)
            with self.subTest(value=value), self.assertRaises(ValueError):
                identity("q", "row", 0, value)
        for query, row in (("", "row"), ("q", ""), (1, "row")):
            with self.subTest(query=query, row=row), self.assertRaises(ValueError):
                identity(query, row, 0)


class CallLedgerTests(unittest.TestCase):
    def setUp(self):
        self.expected = [identity("q", "movie-row-" + str(i), i) for i in range(3)]
        self.ledger = CallLedger(self.expected, max_calls=3)

    def result(self, index, **options):
        return dict(self.expected[index], status="succeeded", prompt_tokens=10,
                    completion_tokens=2, **options)

    def test_declared_cascade_calls_keep_independent_usage_and_unknowns(self):
        calls = [identity("q", "row", 0, model_role="helper", algorithm_stage="helper_score"),
                 identity("q", "row", 0, model_role="main", algorithm_stage="threshold_sample"),
                 identity("q", "row", 0, 1, model_role="main", algorithm_stage="threshold_sample"),
                 identity("q", "row", 0, model_role="main", algorithm_stage="fallback")]
        ledger = CallLedger(calls, max_calls=4)
        ledger.record(dict(calls[2], status="succeeded", prompt_tokens=13, completion_tokens=2))
        ledger.record(dict(calls[0], status="succeeded", prompt_tokens=7, completion_tokens=1))
        ledger.record(dict(calls[1], status="failed", submission_confirmed=True,
                           prompt_tokens=11, completion_tokens=2))
        ledger.mark_unconfirmed("fallback_cancelled")
        report = ledger.report()
        self.assertEqual([row["call_id"] for row in report["records"]],
                         [row["call_id"] for row in calls])
        self.assertEqual(report["counts"]["submission_confirmed"], 3)
        self.assertEqual(report["counts"]["unknown"], 1)
        self.assertEqual(report["usage"]["known_prompt_tokens"], 31)
        self.assertIsNone(report["usage"]["prompt_tokens"])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            ledger.record(dict(calls[2], status="succeeded"))

    def test_legitimate_other_role_or_stage_still_cannot_enter_plain_filter_ledger(self):
        for options in (dict(model_role="helper", algorithm_stage="helper_score"),
                        dict(model_role="main", algorithm_stage="fallback")):
            foreign = identity("q", "movie-row-0", 0, **options)
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, "undeclared"):
                self.ledger.record(dict(foreign, status="succeeded", prompt_tokens=5,
                                        completion_tokens=1))
        self.assertEqual(self.ledger.report()["counts"]["recorded"], 0)

    def test_extended_identity_cannot_forge_a_declared_role_or_stage(self):
        expected = identity("q", "row", 0, model_role="helper", algorithm_stage="helper_score")
        ledger = CallLedger([expected], max_calls=1)
        for field, value in (("model_role", "main"), ("algorithm_stage", "fallback")):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "identity"):
                ledger.record(dict(expected, status="succeeded", **{field: value}))
        self.assertEqual(ledger.report()["counts"]["recorded"], 0)

    def test_out_of_order_results_use_declaration_order_and_keep_failure_usage(self):
        self.ledger.record(self.result(2))
        self.ledger.record(dict(self.expected[0], status="failed", error_type="parse_error",
                                submission_confirmed=True, prompt_tokens=17, completion_tokens=5))
        self.ledger.record(self.result(1))
        report = self.ledger.report()
        self.assertEqual([row["row_id"] for row in report["records"]],
                         [item["row_id"] for item in self.expected])
        self.assertEqual(report["counts"]["failed"], 1)
        self.assertEqual(report["counts"]["submission_confirmed"], 3)
        self.assertEqual(report["usage"]["prompt_tokens"], 37)
        self.assertEqual(report["usage"]["completion_tokens"], 9)
        self.assertEqual(report["usage"]["total_tokens"], 46)

    def test_rejects_missing_foreign_or_forged_full_identity(self):
        for key in self.expected[0]:
            result = self.result(0)
            del result[key]
            with self.subTest(missing=key), self.assertRaises(ValueError):
                self.ledger.record(result)
        for key, value in (('query_id', 'other'), ('row_id', 'other'),
                           ('source_position', 9), ('call_index', 1),
                           ('model_role', 'helper'), ('algorithm_stage', 'fallback'),
                           ('call_id', 'foreign')):
            result = dict(self.result(0), **{key: value})
            with self.subTest(changed=key), self.assertRaises(ValueError):
                self.ledger.record(result)
        with self.assertRaisesRegex(ValueError, "undeclared"):
            self.ledger.record(dict(identity("other", "movie-row-0", 0), status="succeeded"))
        self.assertEqual(self.ledger.report()["counts"]["recorded"], 0)

    def test_rejects_duplicates_and_nonterminal_results(self):
        for status in ("running", "closed", None):
            with self.subTest(status=status), self.assertRaises(ValueError):
                self.ledger.record(dict(self.expected[0], status=status))
        self.ledger.record(self.result(0))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            self.ledger.record(self.result(0))

    def test_unconfirmed_and_missing_usage_are_not_counted_as_zero_or_submitted(self):
        self.ledger.record(dict(self.expected[0], status="failed", prompt_tokens=7,
                                completion_tokens=None, submission_confirmed=True))
        initial = self.ledger.report()
        self.assertIsNone(initial["usage"]["prompt_tokens"])
        self.assertEqual(initial["usage"]["known_prompt_tokens"], 7)
        self.assertEqual(initial["counts"]["missing"], 2)
        self.ledger.mark_unconfirmed("graph_stopped")
        self.ledger.mark_unconfirmed("must_not_overwrite")
        report = self.ledger.report()
        self.assertEqual(report["counts"]["unknown"], 2)
        self.assertEqual(report["counts"]["submission_confirmed"], 1)
        self.assertEqual(report["counts"]["submission_unconfirmed"], 2)
        self.assertEqual(report["usage"]["known_prompt_tokens"], 7)
        self.assertEqual(report["usage"]["unknown_prompt_tokens_calls"], 2)
        self.assertIsNone(report["usage"]["completion_tokens"])
        self.assertIsNone(report["usage"]["total_tokens"])
        for row in report["records"][1:]:
            self.assertFalse(row["submission_confirmed"])
            self.assertIsNone(row["prompt_tokens"])
            self.assertEqual(row["error_type"], "graph_stopped")

    def test_unknown_submission_defaults_to_unconfirmed(self):
        self.ledger.record(dict(self.expected[0], status="unknown"))
        self.assertEqual(self.ledger.report()["counts"]["submission_confirmed"], 0)

    def test_ledger_copies_results_and_reports_do_not_depend_on_resource_close(self):
        result = self.result(0)
        result["detail"] = {"engine": "fixture"}
        self.ledger.record(result)
        result["detail"]["engine"] = "mutated"
        report = self.ledger.report()
        report["records"][0]["detail"]["engine"] = "changed-report"
        resource_report = {"status": "closed"}
        self.assertEqual(resource_report["status"], "closed")
        self.assertEqual(self.ledger.report()["records"][0]["detail"]["engine"], "fixture")
        self.assertEqual(self.ledger.report()["counts"]["recorded"], 1)

    def test_rejects_invalid_usage_without_recording_it(self):
        for value in (True, -1, 1.2, float("nan"), float("inf")):
            for name in ("prompt_tokens", "completion_tokens"):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    self.ledger.record(dict(self.result(0), **{name: value}))
        with self.assertRaisesRegex(ValueError, "confirmed submission"):
            self.ledger.record(dict(self.result(0), submission_confirmed=False))
        self.assertEqual(self.ledger.report()["counts"]["recorded"], 0)

    def test_capacity_and_duplicate_declarations_are_rejected(self):
        for value in (True, 0, 1.0, float("nan")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                CallLedger([], max_calls=value)
        with self.assertRaises(ValueError):
            CallLedger(self.expected, max_calls=2)
        with self.assertRaisesRegex(ValueError, "duplicate"):
            CallLedger([self.expected[0], self.expected[0]], max_calls=2)
        self.assertEqual(CallLedger([], max_calls=1).report()["usage"]["total_tokens"], 0)


if __name__ == "__main__":
    unittest.main()
