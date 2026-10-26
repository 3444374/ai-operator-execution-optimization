"""CPU differential fixtures and population/accounting properties."""

from copy import deepcopy
import gzip
import json
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.observability.metrics.offline_results import FIFO, PUBLIC, analyze, summarize
from src.observability.metrics.statistics import latency_observations


FIXTURES = Path(__file__).parent / "fixtures" / "offline_results"
CLI = Path(__file__).resolve().parents[2] / "scripts" / "analysis" / "summarize_offline_results.py"


def query(*, n=100, batch=1, key="receipt", **extra):
    return dict(query_id=key, configuration="opaque-config", task="opaque-task", phase="measured", role="first",
        records=n * batch, successful_records=n * batch, failed_records=0, unknown_records=0,
        timing=dict(API_return_s=10., answer_ready_s=9., API_timing_complete=True),
        model_API=dict(n=n), API_units=["complete_batch"], actual_batch_sizes={str(batch): n},
        API_counts=dict(attempted=n, completed=n, timed=n, successful_timed=n, statuses={"succeeded": n}, missing_clock=0,
                        unresolved_identity_records=0), **extra)


def run(q=None, values=None, **kwargs):
    q = query() if q is None else q
    values = list(range(1, 101)) if values is None else values
    return summarize(input_schema=PUBLIC, comparison_scope="CPU_fixture", queries=[q],
                     api_samples=[dict(query_id=q["query_id"], elapsed_s=values)], **kwargs)


class OfflineResultsTests(unittest.TestCase):
    def test_two_retained_schemas_reproduce_original_numeric_summaries(self):
        expected = json.loads((FIXTURES / "expected.json").read_text())
        for name in ("public", "fifo"):
            result = analyze(manifest=FIXTURES / (name + "-manifest.json"))
            for source in expected[name]:
                matches = [g for g in result["groups"] if (source.get("query_id") in g["query_ids"]
                    if name == "public" else source["mode"] == g["group"]["configuration"])]
                self.assertEqual(len(matches), 1)
                g = matches[0]
                original = source["model_API"] if name == "public" else source["API"]
                actual = g["model_API"]["distribution"]
                self.assertEqual(actual["completed_units"], original["n"])
                for field, old in (("median_s", "median"), ("maximum_s", "maximum")):
                    self.assertAlmostEqual(actual[field], original[old], delta=1e-12)
                self.assertIsNone(actual["empirical_p99_s"])
                self.assertAlmostEqual(g["clocks"]["complete_query"]["median_s"], source["full_s"], delta=1e-12)
                self.assertAlmostEqual(g["clocks"]["query_ready"]["median_s"], source["ready_s"], delta=1e-12)

    def test_permuting_queries_and_sample_collections_does_not_change_summary(self):
        qs = [query(n=4, key=str(i)) for i in range(8)]
        samples = [dict(query_id=q["query_id"], elapsed_s=[.1, .2, .3, .4]) for q in qs]
        expected = summarize(input_schema=PUBLIC, comparison_scope="fixture", queries=qs, api_samples=samples)
        rng = random.Random(18)
        for _ in range(40):
            rng.shuffle(qs); rng.shuffle(samples)
            for sample in samples:
                rng.shuffle(sample["elapsed_s"])
            actual = summarize(input_schema=PUBLIC, comparison_scope="fixture", queries=qs, api_samples=samples)
            self.assertEqual(actual, expected)

    def test_sample_statistics_differential_for_sizes_and_batch_shapes(self):
        rng = random.Random(19)
        for n in (0, 1, 2, 99, 100, 101, 217):
            values = [rng.uniform(0., 20.) for _ in range(n)]
            for batch in (1, 4, 16):
                actual = run(query(n=n, batch=batch), values)["groups"][0]
                expected = latency_observations([dict(id=str(i), elapsed_s=v, status="succeeded") for i, v in enumerate(values)])
                for key in ("completed_units", "median_s", "maximum_s", "empirical_p99_s", "empirical_p99_rank"):
                    self.assertEqual(actual["model_API"]["distribution"][key], expected[key])
                self.assertEqual(actual["n_query"], 1)
                self.assertEqual(actual["records"]["records"], n * batch)
                self.assertEqual(actual["model_API"]["n_successful_timed"], n)
                self.assertFalse(actual["model_API"]["distribution"]["population_P99_established"])

    def test_one_large_batch_never_becomes_a_row_p99_population(self):
        g = run(query(n=1, batch=200), [4.])["groups"][0]
        self.assertEqual(g["model_API"]["n_successful_timed"], 1)
        self.assertIsNone(g["model_API"]["distribution"]["empirical_p99_s"])
        self.assertEqual(g["model_API"]["actual_batch_sizes"], {"200": 1})

    def test_failed_unknown_and_retry_history_remain_distinct(self):
        q = query()
        q["failed_records"] = 1; q["unknown_records"] = 2; q["records"] = 103
        q["API_counts"]["statuses"].update(failed=1, unknown=2)
        q["API_counts"]["attempted"] = 103
        q["API_counts"].update(completed=101, timed=103)
        history = dict(failed=1, unknown=2, retries=3, allocated=112, unexecuted_reserved=9)
        result = run(q, metadata=history)
        g = result["groups"][0]
        self.assertEqual(g["query_statuses"], {"unknown": 1})
        self.assertEqual(g["model_API"]["status_counts"], dict(succeeded=100, failed=1, unknown=2))
        self.assertEqual(g["model_API"]["n_successful_timed"], 100)
        self.assertIsNone(g["model_API"]["distribution"]["empirical_p99_s"])
        self.assertEqual(result["metadata"], history)

    def test_missing_fields_never_become_zero_or_complete_evidence(self):
        for field in ("API_counts", "actual_batch_sizes"):
            q = query(); q.pop(field)
            g = run(q)["groups"][0]
            self.assertFalse(g["model_API"]["accounting_complete"])
            self.assertIsNone(g["model_API"]["distribution"]["empirical_p99_s"])
        q = query(); q.pop("unknown_records"); q["timing"].pop("API_return_s")
        result = run(q)
        g = result["groups"][0]
        self.assertEqual(g["query_statuses"], {"unknown": 1})
        self.assertIsNone(g["clocks"]["complete_query"]["median_s"])
        self.assertIsNone(g["records"]["unknown_records"])
        self.assertIsNone(g["GPU"]["n_samples"])
        self.assertIsNone(g["quality"][0]["zero_Map"])
        self.assertEqual(g["quality"][0]["metrics"]["status"], "NA")
        self.assertEqual(result["unavailable"]["ready_to_admit"]["status"], "NA")

    def test_missing_samples_preserve_declared_counts_but_create_no_observations(self):
        result = summarize(input_schema=PUBLIC, comparison_scope="fixture", queries=[query()])
        api = result["groups"][0]["model_API"]
        self.assertIsNone(api["n_successful_timed"])
        self.assertEqual(api["n_retained_samples"], 0)
        self.assertEqual(api["status_counts"]["succeeded"], 100)
        self.assertEqual(api["samples_missing_queries"], ["receipt"])
        self.assertEqual(api["distribution"]["status"], "NA")

    def test_unknown_status_count_cannot_enable_p99(self):
        q = query(); q["API_counts"]["statuses"]["unknown"] = None
        result = run(q)["groups"][0]["model_API"]
        self.assertIsNone(result["status_counts"]["unknown"])
        self.assertIsNone(result["distribution"]["empirical_p99_s"])

    def test_unexecuted_query_is_retained_without_a_latency_sample(self):
        q = query(n=0); q["status"] = "not_executed"; q["timing"] = {}; q.pop("API_counts")
        g = run(q, [])["groups"][0]
        self.assertEqual(g["n_query"], 1)
        self.assertEqual(g["query_statuses"], {"not_executed": 1})
        self.assertEqual(g["clocks"]["complete_query"]["completed_units"], 0)

    def test_flops_tail_never_certifies_an_earlier_api_window(self):
        data = json.loads((FIXTURES / "fifo-results.json").read_text())
        data["queries"][0]["MFU_caller"] = dict(estimated_MFU_pct=.214, terminal_watermark_complete=True)
        data["queries"][0]["MFU_tail"] = dict(extra_flops=10**12, tokens_and_finished_unchanged=True)
        result = summarize(input_schema=FIFO, comparison_scope="fixture", results=data)
        self.assertEqual(result["unavailable"]["API_window_MFU"]["status"], "unsupported")
        self.assertIsNone(result["unavailable"]["MFU"]["value"])
        self.assertNotIn("estimated_MFU_pct", json.dumps(result))

    def test_fifo_stage_call_totals_are_not_reclassified_as_input_records(self):
        result = analyze(manifest=FIXTURES / "fifo-manifest.json")
        for g in result["groups"]:
            self.assertEqual(g["model_API"]["n_successful_timed"], 6)
            self.assertTrue(all(v is None for v in g["records"].values()))
            self.assertIn("attempted", next(iter(g["model_API"]["accounting_missing_fields"].values())))

    def test_quality_zero_map_and_empty_successor_require_explicit_evidence(self):
        audit = dict(TP=0, FP=0, TN=16, FN=0, invalid=0, Top5=[], COUNT=0)
        q = query(quality=audit, stage_counts={"Filter": 16, "Map": 0}, result_rows=0)
        g = run(q)["groups"][0]
        quality = g["quality"][0]
        self.assertEqual(quality["metrics"], audit)
        self.assertTrue(quality["zero_Map"])
        self.assertTrue(quality["empty_successor"])
        cases = [
            ({"Map": 0}, 0, "succeeded", True),
            ({"Map": 1}, 0, "succeeded", False),
            ({"Map": 0}, 1, "succeeded", False),
            (None, 0, "succeeded", None),
            ({}, 0, "succeeded", None),
            ({"Map": None}, 0, "succeeded", None),
            ({"Map": 0}, None, "succeeded", None),
            ({"Map": 0}, 0, "unknown", None),
            ({"Map": 0}, 0, "failed", None),
            ({"Map": 0}, 0, "not_executed", None),
        ]
        for stages, rows, status, expected in cases:
            with self.subTest(stages=stages, result_rows=rows, status=status):
                q = query(stage_counts=stages, result_rows=rows, status=status)
                self.assertIs(run(q)["groups"][0]["quality"][0]["empty_successor"], expected)

    def test_opaque_names_never_change_metric_semantics_or_scope(self):
        original = run()["groups"][0]
        q = query(); q.update(configuration="unseen-backend", task="unseen-dataset-model")
        renamed = run(q)["groups"][0]
        for key in ("clocks", "records", "model_API", "GPU", "quality", "comparison_scope"):
            self.assertEqual(original[key], renamed[key])

    def test_input_objects_are_not_mutated(self):
        q = query(); before = deepcopy(q)
        run(q)
        self.assertEqual(q, before)

    def test_missing_gpu_metric_does_not_become_a_complete_group_mean(self):
        qs = [query(n=1, key=str(i), GPU=dict(samples=2, metrics={})) for i in range(2)]
        qs[0]["GPU"]["metrics"] = dict(busy=dict(valid_samples=2, attempted_samples=2,
            sample_mean=20., observed_max=30., missing_reasons=[]))
        samples = [dict(query_id=q["query_id"], elapsed_s=[1.]) for q in qs]
        g = summarize(input_schema=PUBLIC, comparison_scope="fixture", queries=qs, api_samples=samples)["groups"][0]
        busy = g["GPU"]["metrics"]["busy"]
        self.assertIsNone(busy["sample_mean"])
        self.assertIsNone(busy["valid_samples"])
        self.assertEqual(busy["summaries_missing"], 1)
        self.assertEqual(g["GPU"]["n_samples"], 4)

    def test_input_limits_apply_to_compressed_and_decoded_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            raw = Path(d) / "large.json"; raw.write_text(json.dumps(dict(value="x" * 5000)))
            compressed = Path(d) / "small.json.gz"; compressed.write_bytes(gzip.compress(raw.read_bytes()))
            with patch("src.observability.metrics.offline_results.MAX_BYTES", 1024):
                for source in (raw, compressed):
                    with self.assertRaisesRegex(ValueError, "exceeds"):
                        analyze(input_schema=FIFO, comparison_scope="fixture", results=source)

    def test_identity_invalid_count_and_clock_inconsistencies_are_rejected(self):
        cases = []
        q = query(); q["actual_batch_sizes"] = {"4": 100}; cases.append(q)
        q = query(); q["timing"]["API_return_s"] = -1; cases.append(q)
        q = query(); q["timing"]["answer_ready_s"] = 11; cases.append(q)
        q = query(); q["API_counts"]["attempted"] = True; cases.append(q)
        q = query(); q["status"] = "unrecognized"; cases.append(q)
        for q in cases:
            with self.assertRaises(ValueError):
                run(q)
        with self.assertRaisesRegex(ValueError, "query IDs"):
            summarize(input_schema=PUBLIC, comparison_scope="x", queries=[query(), query()])
        for samples in ([dict(query_id="orphan", elapsed_s=[])],
                        [dict(query_id="receipt", elapsed_s=[])] * 2):
            with self.assertRaises(ValueError):
                summarize(input_schema=PUBLIC, comparison_scope="x", queries=[query()], api_samples=samples)

    def test_file_hash_and_schema_mismatch_are_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "manifest.json"
            config = json.loads((FIXTURES / "public-manifest.json").read_text())
            config["files"]["queries"]["sha256"] = "0" * 64
            p.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                analyze(manifest=p, root=FIXTURES)
        with self.assertRaisesRegex(ValueError, "schemas disagree"):
            analyze(manifest=FIXTURES / "public-manifest.json", input_schema=FIFO)
        with self.assertRaisesRegex(ValueError, "unsupported input schema"):
            summarize(input_schema="future.v42", comparison_scope="x")

    def test_duplicate_json_keys_and_nonfinite_json_are_rejected(self):
        for payload in ('{"schema":1,"schema":2}', '{"value":NaN}'):
            with tempfile.TemporaryDirectory() as d:
                p = Path(d) / "manifest.json"; p.write_text(payload)
                with self.assertRaises(ValueError):
                    analyze(manifest=p)

    def test_cli_accepts_paths_and_refuses_to_overwrite_evidence(self):
        with tempfile.TemporaryDirectory() as d:
            output = Path(d) / "new-summary.json"
            command = [sys.executable, str(CLI), "--manifest", str(FIXTURES / "fifo-manifest.json"), "--output", str(output)]
            first = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(first.returncode, 0, first.stderr)
            before = output.read_bytes()
            second = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(second.returncode, 2)
            self.assertEqual(output.read_bytes(), before)
            direct = subprocess.run([sys.executable, str(CLI), "--schema", FIFO,
                "--comparison-scope", "finite_chain_diagnostic", "--results", str(FIXTURES / "fifo-results.json")], capture_output=True, text=True)
            self.assertEqual(direct.returncode, 0, direct.stderr)
            self.assertEqual(json.loads(direct.stdout)["n_query"], 2)


if __name__ == "__main__":
    unittest.main()
