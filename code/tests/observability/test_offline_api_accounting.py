"""Exact regressions for missing and contradictory retained API accounting."""

import json
import unittest

from tests.observability.test_offline_results import FIXTURES, query, run
from src.observability.metrics.offline_results import FIFO, summarize


class OfflineAPIAccountingTests(unittest.TestCase):
    def test_each_missing_counter_prevents_complete_and_p99(self):
        for key in ("attempted", "completed", "timed", "successful_timed", "missing_clock", "unresolved_identity_records"):
            for missing in ("absent", "null"):
                with self.subTest(counter=key, missing=missing):
                    q = query()
                    if missing == "absent":
                        q["API_counts"].pop(key)
                    else:
                        q["API_counts"][key] = None
                    api = run(q)["groups"][0]["model_API"]
                    self.assertFalse(api["accounting_complete"])
                    self.assertIsNone(api["distribution"]["empirical_p99_s"])
                    self.assertIn(key, api["accounting_missing_fields"]["receipt"])

    def test_missing_success_counter_is_not_replaced_by_raw_array_length(self):
        q = query(); q["API_counts"].pop("successful_timed")
        api = run(q)["groups"][0]["model_API"]
        self.assertEqual(api["n_retained_samples"], 100)
        self.assertIsNone(api["n_successful_timed"])
        self.assertFalse(api["sample_success_verified"])

    def test_absent_counts_or_status_map_do_not_create_success_statuses(self):
        for mode in ("no_counts", "no_statuses", "null_statuses"):
            q = query()
            if mode == "no_counts":
                q.pop("API_counts")
            elif mode == "no_statuses":
                q["API_counts"].pop("statuses")
            else:
                q["API_counts"]["statuses"] = None
            with self.subTest(mode=mode):
                api = run(q)["groups"][0]["model_API"]
                self.assertEqual(api["n_retained_samples"], 100)
                self.assertIsNone(api["n_successful_timed"])
                self.assertEqual(api["status_counts"], dict(succeeded=None, failed=None, unknown=None))
                self.assertIsNone(api["distribution"]["empirical_p99_s"])

    def test_known_counter_contradictions_are_invalid(self):
        changes = [
            dict(completed=0, timed=0),
            dict(completed=0),
            dict(timed=0),
            dict(attempted=99),
            dict(completed=101),
            dict(timed=101),
            dict(missing_clock=1),
            dict(statuses={"succeeded": 0}),
            dict(statuses={"succeeded": 101}),
            dict(successful_timed=99),
        ]
        for change in changes:
            with self.subTest(change=change):
                q = query(); q["API_counts"].update(change)
                with self.assertRaisesRegex(ValueError, "invalid API accounting"):
                    run(q)

    def test_zero_samples_require_explicit_zero_counters(self):
        q = query(n=0)
        q["API_counts"]["statuses"] = {}
        api = run(q, [])["groups"][0]["model_API"]
        self.assertTrue(api["accounting_complete"])
        self.assertEqual(api["status_counts"], dict(succeeded=0, failed=0, unknown=0))
        self.assertEqual(api["n_successful_timed"], 0)
        self.assertIsNone(api["distribution"]["empirical_p99_s"])

        q["API_counts"].pop("successful_timed")
        missing = run(q, [])["groups"][0]["model_API"]
        self.assertFalse(missing["accounting_complete"])
        self.assertIsNone(missing["n_successful_timed"])

    def test_real_success_relationships_and_small_sample_tail(self):
        for n in (0, 1, 99, 100):
            q = query(n=n)
            api = run(q, list(range(n)))["groups"][0]["model_API"]
            self.assertTrue(api["accounting_complete"])
            self.assertTrue(api["sample_success_verified"])
            self.assertEqual(api["n_successful_timed"], n)
            if n < 100:
                self.assertIsNone(api["distribution"]["empirical_p99_s"])
            else:
                self.assertEqual(api["distribution"]["empirical_p99_s"], 98)

    def test_partial_failures_and_timed_unknown_remain_separate(self):
        q = query()
        q.update(records=103, failed_records=1, unknown_records=2)
        q["API_counts"].update(attempted=103, completed=101, timed=103,
            successful_timed=100, statuses=dict(succeeded=100, failed=1, unknown=2))
        api = run(q)["groups"][0]["model_API"]
        self.assertEqual(api["n_retained_samples"], 100)
        self.assertEqual(api["n_successful_timed"], 100)
        self.assertEqual(api["status_counts"], dict(succeeded=100, failed=1, unknown=2))
        self.assertFalse(api["accounting_complete"])
        self.assertIsNone(api["distribution"]["empirical_p99_s"])

    def test_timed_and_successful_counts_may_be_lower_with_missing_clocks(self):
        q = query(n=99)
        q["API_counts"].update(attempted=100, completed=100, timed=99,
            successful_timed=99, missing_clock=1, statuses={"succeeded": 100})
        api = run(q, list(range(99)))["groups"][0]["model_API"]
        self.assertEqual(api["n_successful_timed"], 99)
        self.assertEqual(api["status_counts"]["succeeded"], 100)
        self.assertFalse(api["accounting_complete"])
        self.assertIsNone(api["distribution"]["empirical_p99_s"])


class FIFOSourceAccountingTests(unittest.TestCase):
    def source(self):
        return json.loads((FIXTURES / "fifo-results.json").read_text())

    def summarize(self, data):
        return summarize(input_schema=FIFO, comparison_scope="CPU_FIFO_fixture", results=data)

    def test_source_failures_and_unknown_are_preserved_without_query_allocation(self):
        data = self.source(); data.update(actual_failure=3, actual_unknown=2)
        result = self.summarize(data)
        ledger = result["source_accounting"]
        self.assertEqual(ledger["scope"], "entire_input_summary; not allocated to individual queries")
        self.assertEqual(ledger["counters"]["actual_failure"], 3)
        self.assertEqual(ledger["counters"]["actual_unknown"], 2)
        self.assertEqual(ledger["counters"]["actual_success"], 12)
        self.assertTrue(ledger["failures_or_unknown_present"])
        self.assertFalse(ledger["success_counts_reconciled"])
        for g in result["groups"]:
            self.assertEqual(g["query_statuses"], {"unknown": 1})
            self.assertEqual(g["model_API"]["status_counts"], dict(succeeded=None, failed=None, unknown=None))
            self.assertEqual(g["model_API"]["n_retained_samples"], 6)
            self.assertIsNone(g["model_API"]["n_successful_timed"])
            self.assertIsNotNone(g["model_API"]["distribution"]["median_s"])
            self.assertIsNone(g["model_API"]["distribution"]["empirical_p99_s"])
            self.assertFalse(g["model_API"]["accounting_complete"])

    def test_absent_source_counters_are_not_defaulted_to_zero(self):
        data = self.source(); data.pop("actual_failure"); data.pop("actual_unknown")
        ledger = self.summarize(data)["source_accounting"]
        self.assertIsNone(ledger["counters"]["actual_failure"])
        self.assertIsNone(ledger["counters"]["actual_unknown"])
        self.assertIsNone(ledger["failures_or_unknown_present"])
        self.assertFalse(ledger["success_counts_reconciled"])

    def test_explicit_source_zeroes_remain_zero_and_scope_is_not_per_query(self):
        data = self.source()
        ledger = self.summarize(data)["source_accounting"]
        for key in ("actual_success", "actual_failure", "actual_unknown", "automatic_model_retries",
                    "historical_allocated", "historical_claimed_cap"):
            self.assertEqual(ledger["counters"][key], data[key])
        self.assertFalse(ledger["failures_or_unknown_present"])
        self.assertTrue(ledger["success_counts_reconciled"])
