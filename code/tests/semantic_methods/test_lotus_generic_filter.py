"""An explicit non-Movie source and original LOTUS CPU differential; no model calls."""

import csv
from dataclasses import asdict
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordScan, identity


LIBRARIES = all(importlib.util.find_spec(name) for name in ("lotus", "litellm", "pandas"))


class BoundTextFixtureSource:
    """A test task owns a pinned CSV, its text columns, predicate and method."""

    def __init__(self, path, method):
        self.csv_path, self.method = Path(path), method
        self.digest = hashlib.sha256(self.csv_path.read_bytes()).hexdigest()
        self.spec = RecordScan(
            (("row_id", "string"), ("source_position", "int64"), ("category", "string"),
             ("paragraph", "string"), ("topic", "string")),
            ("paragraph", "topic"), ("category", "keep"),
        )

    def preflight(self, method):
        import lotus

        if type(method) is not FilterMethod or method.describe() != self.method.describe():
            raise ValueError("text fixture method differs from its explicit task binding")
        if hashlib.sha256(self.csv_path.read_bytes()).hexdigest() != self.digest:
            raise ValueError("text fixture CSV changed after task binding")
        columns = lotus.nl_expression.parse_cols(method.instruction)
        if not columns or not set(columns) <= set(self.spec.payload_columns):
            raise ValueError("text fixture method references unavailable payload columns")
        with self.csv_path.open(newline="") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != [name for name, _ in self.spec.csv_schema]:
                raise ValueError("text fixture CSV schema differs")
            rows = list(reader)
        if any(set(row) != set(reader.fieldnames) or any(value is None for value in row.values())
               for row in rows):
            raise ValueError("text fixture row is malformed")
        return dict(status="passed", role="software_fixture", csv_sha256=self.digest,
                    method=method.describe(), scan=self.spec.describe())

    def scan_spec(self, method):
        self.preflight(method)
        return RecordScan.from_dict(self.spec.describe())

    def prepared_input(self, query_id):
        self.preflight(self.method)
        # This finite test scan demonstrates the value contract; it is not a
        # framework implementation or model/backend qualification.
        with self.csv_path.open(newline="") as stream:
            rows = list(csv.DictReader(stream))
        column, value = self.spec.predicate
        return [dict(identity(query_id, row["row_id"], int(row["source_position"])),
                     **{name: row[name] for name in self.spec.payload_columns})
                for row in rows if row[column] == value]

    def expected(self, query_id):
        return [identity(query_id, row["row_id"], row["source_position"])
                for row in self.prepared_input(query_id)]


@unittest.skipUnless(LIBRARIES, "requires the isolated pinned LOTUS CPU environment")
class GenericFilterTests(unittest.TestCase):
    def setUp(self):
        import lotus
        from src.serving.backends.vllm_engine import GenerationSpec

        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.csv_path = Path(self.temp.name) / "paragraphs.csv"
        with self.csv_path.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["row_id", "source_position", "category", "paragraph", "topic"])
            writer.writerows([
                ["outside", 0, "skip", "Not submitted", "outside"],
                ["p1", 1, "keep", "Excellent writing, 内容很清楚。", "writing"],
                ["p2", 2, "keep", "", "writing"],
            ])
        self.method = FilterMethod("openai/local-fixture", "Keep positive {paragraph} about {topic}",
                                   default=False, system_prompt="Return True or False.")
        self.generation = GenerationSpec()
        self.source = BoundTextFixtureSource(self.csv_path, self.method)
        self.context = lotus.settings.context(enable_cache=False)
        self.context.__enter__()
        self.addCleanup(self.context.__exit__, None, None, None)

    def results(self, prepared):
        return [dict(row, status="succeeded", text=text, prompt_tokens=10 + position,
                     completion_tokens=2, total_tokens=12 + position, finish_reason="stop",
                     engine_request_id="fixture-" + str(position), engine_uid="cpu-response-fixture",
                     submission_confirmed=True, logprobs_json="null")
                for position, (row, text) in enumerate(zip(prepared, ["Answer: True", "uncertain"], strict=True))]

    def test_bound_non_movie_input_matches_original_accessor_messages_parser_usage_and_ids(self):
        import lotus
        import pandas as pd
        from src.semantic_methods.lotus.engine import (
            LotusFilterMethod, engine_response, interpret_results, make_lm, validate_filter_source,
        )

        self.assertEqual(self.source.preflight(self.method)["status"], "passed")
        spec = self.source.scan_spec(self.method)
        self.assertEqual(spec.payload_columns, ("paragraph", "topic"))
        self.assertEqual(spec.predicate, ("category", "keep"))
        upstream = validate_filter_source()
        self.assertEqual(upstream["versions"]["lotus-ai"], "1.2.4")
        rows = self.source.prepared_input("generic-q")
        formatter = LotusFilterMethod(self.method)
        prepared = formatter.prepare_rows(rows)
        self.assertEqual(formatter.columns, lotus.nl_expression.parse_cols(self.method.instruction))
        self.assertEqual([row["call_id"] for row in prepared],
                         [row["call_id"] for row in self.source.expected("generic-q")])
        self.assertNotIn("reviewText", rows[0])
        results = self.results(prepared)
        actual_lm, native_lm = (make_lm(self.method, self.generation) for _ in range(2))
        interpreted = interpret_results(results, actual_lm, self.method)
        captured = []

        def respond(data, options, *_args):
            captured.append(([message for message, _ in data], dict(options)))
            return [engine_response(row, native_lm) for row in results]

        native_lm._process_uncached_messages = respond
        frame = pd.DataFrame([{name: row[name] for name in spec.payload_columns} for row in rows], index=[5, 9])
        with lotus.settings.context(lm=native_lm):
            native = frame.sem_filter(self.method.instruction, default=self.method.default,
                                      system_prompt=self.method.system_prompt,
                                      output_tokens=self.method.output_tokens)
        self.assertEqual([json.loads(row["messages_json"]) for row in prepared], captured[0][0])
        self.assertEqual(captured[0][1], dict(actual_lm.kwargs, logprobs=False))
        self.assertEqual([row["decision"] for row in interpreted], [True, False])
        self.assertEqual(native.index.tolist(), [5])
        self.assertEqual(asdict(actual_lm.stats), asdict(native_lm.stats))
        self.assertEqual([row["call_id"] for row in interpreted], [row["call_id"] for row in rows])

    def test_source_preflight_and_referenced_text_values_are_required(self):
        from src.semantic_methods.lotus.engine import LotusFilterMethod

        with patch("lotus.models.LM") as constructor:
            with self.assertRaisesRegex(ValueError, "task binding"):
                self.source.scan_spec(FilterMethod("fixture", "Other {paragraph}"))
            constructor.assert_not_called()
        formatter = LotusFilterMethod(self.method)
        rows = self.source.prepared_input("q")
        for value in (None, 1, False):
            with self.subTest(value=value), self.assertRaisesRegex(TypeError, "text input"):
                formatter.prepare_rows([dict(rows[0], paragraph=value)])
        missing = dict(rows[0])
        del missing["topic"]
        with self.assertRaisesRegex(TypeError, "text input"):
            formatter.prepare_rows([missing])
        self.csv_path.write_bytes(self.csv_path.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "changed"):
            self.source.scan_spec(self.method)

    def test_failed_call_keeps_its_identity_and_usage_without_parser_fallback(self):
        from src.semantic_methods.lotus.engine import LotusFilterMethod, interpret_results, make_lm

        prepared = LotusFilterMethod(self.method).prepare_rows(self.source.prepared_input("q"))
        results = self.results(prepared)
        results[1].update(status="unknown", text=None, error_type="FixtureUnknown")
        ledger = CallLedger(self.source.expected("q"), max_calls=2)
        for row in results:
            ledger.record(row)
        lm = make_lm(self.method, self.generation)
        with self.assertRaises(RuntimeError) as failure:
            interpret_results(results, lm, self.method)
        self.assertEqual(failure.exception.engine_failures[0][0]["call_id"], results[1]["call_id"])
        self.assertEqual(ledger.report()["counts"]["unknown"], 1)
        self.assertEqual(ledger.report()["usage"]["total_tokens"], 25)
        self.assertEqual(lm.stats.physical_usage.total_tokens, 12)


if __name__ == "__main__":
    unittest.main()
