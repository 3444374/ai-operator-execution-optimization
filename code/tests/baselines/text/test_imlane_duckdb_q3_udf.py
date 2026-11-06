"""Arrow/callable contract tests; no DuckDB, author runtime or model starts."""

import builtins
from copy import deepcopy
import hashlib
import importlib
import json
import os
from pathlib import Path
import py_compile
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from integrations.imlane_duckdb import q3_udf as udf
from src.baselines.text.sembench_movie_native import evaluate_q3, q3_messages
from src.infrastructure.private_artifacts import content_digest


class IMLaneDuckDBQ3Tests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.receipts = self.root / "receipts"
        self.receipts.mkdir(mode=0o700)
        udf._configuration.cache_clear()
        previous_modules = set(sys.modules)
        self.addCleanup(lambda: [sys.modules.pop(name) for name in tuple(sys.modules)
            if name.startswith("_imlane_duckdb_q3_provider_") and name not in previous_modules])
        self.addCleanup(udf._configuration.cache_clear)
        self.environment = patch.dict(os.environ, {udf.CONFIG_ENV: str(self.root / "config.json")})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        original_import = builtins.__import__

        def cpu_import(name, *args, **kwargs):
            if name.split(".", 1)[0] in {"ray", "daft", "vllm", "lotus", "torch", "transformers", "duckdb"}:
                raise AssertionError("unexpected framework/model import: " + name)
            return original_import(name, *args, **kwargs)

        for guard in (
                patch.object(builtins, "__import__", side_effect=cpu_import),
                patch.object(subprocess, "Popen", side_effect=AssertionError("process start")),
                patch.object(socket.socket, "connect", side_effect=AssertionError("network connect")),
                patch.object(socket.socket, "bind", side_effect=AssertionError("network bind"))):
            guard.start()
            self.addCleanup(guard.stop)

    def configured(self, body):
        provider = self.root / "reply_fixture.py"
        provider.write_text(body, encoding="utf-8")
        config = dict(provider_path=str(provider), provider_function="reply_batch",
                      provider_mode="software_fixture", receipt_dir=str(self.receipts),
                      secret="private-fixture-credential")
        (self.root / "config.json").write_text(json.dumps(config), encoding="utf-8")
        return config

    def fixed(self, replies):
        self.configured("def reply_batch(messages_batch):\n    return " + repr(replies) + "\n")

    def rows(self):
        return [dict(occurrence_id="occ-2", id="taken_3", reviewId="same-business-id",
                     reviewText="雪 «review»\nsecond line\0"),
                dict(occurrence_id="occ-1", id="taken_3", reviewId="same-business-id",
                     reviewText="another independent review")]

    def table(self, rows=None):
        rows = self.rows() if rows is None else rows
        return pa.Table.from_arrays([
            pa.array([row[name] for row in rows], type=pa.string()) for name in udf.IDENTITY
        ], schema=udf.INPUT)

    def saved(self):
        return [json.loads(path.read_text()) for path in sorted(self.receipts.glob("q3-batch-*.json"))]

    def test_whole_chunked_table_calls_batch_provider_once_with_exact_shared_messages(self):
        rows = self.rows()
        table = pa.concat_tables([self.table(rows[:1]), self.table(rows[1:])])
        before = table.to_pylist()
        captured = []

        def provider(messages_batch):
            captured.append(deepcopy(messages_batch))
            return ["Answer: True", "Answer: False"]

        with patch.object(udf, "_configuration", return_value=(provider, self.receipts, "software_fixture", "a" * 64)):
            output = udf.infer_q3(table)
        self.assertEqual(captured, [[q3_messages(row["reviewText"]) for row in rows]])
        self.assertEqual(table.to_pylist(), before)
        self.assertEqual(output.schema, udf.OUTPUT)
        self.assertEqual(output.column(0).to_pylist(), [1, 0])
        self.assertEqual(output.column(0).num_chunks, 1)
        self.assertEqual(output.column(0).chunk(0).buffers()[1].size, len(rows))
        receipt, = self.saved()
        self.assertEqual(len(receipt["provider_calls"]), 1)
        self.assertEqual(receipt["provider_calls"][0]["logical_rows"], 2)
        self.assertTrue(receipt["provider_calls"][0]["returned"])
        self.assertEqual(receipt["status"], "succeeded")
        self.assertEqual([row["occurrence_id"] for row in receipt["predictions"]], ["occ-2", "occ-1"])
        self.assertEqual([row["messages_sha256"] for row in receipt["predictions"]],
                         [content_digest(q3_messages(row["reviewText"])) for row in rows])

    def test_raw_replies_keep_original_prefix_true_priority_and_invalid_default_true(self):
        replies = ["Reasoning: True\nAnswer: False", "Answer: true and false", "unparsed",
                   "False", "Answer: False\nAnswer: True"]
        rows = [dict(self.rows()[0], occurrence_id=str(index)) for index in range(len(replies))]
        self.fixed(replies)
        output = udf.infer_q3(self.table(rows))
        self.assertEqual(output.column(0).to_pylist(), [0, 1, 1, 0, 1])
        receipt, = self.saved()
        self.assertEqual([row["generated_text"] for row in receipt["predictions"]], replies)
        serialized = json.dumps(receipt)
        self.assertNotIn("private-fixture-credential", serialized)
        self.assertNotIn(str(self.root), serialized)
        self.assertNotIn("provider_path", receipt)
        self.assertEqual(receipt["provider_mode"], "software_fixture")
        self.assertTrue(all(path.stat().st_mode & 0o777 == 0o600 for path in self.receipts.iterdir()))

    def test_empty_table_returns_typed_single_chunk_without_configuration_or_provider(self):
        with patch.object(udf, "_configuration") as configure:
            output = udf.infer_q3(self.table([]))
        configure.assert_not_called()
        self.assertEqual(output.schema, udf.OUTPUT)
        self.assertEqual(len(output), 0)
        self.assertEqual(output.column(0).num_chunks, 1)
        self.assertEqual(self.saved(), [])

    def test_author_boundary_rejects_bad_schema_null_identity_or_unfiltered_rows_before_provider(self):
        wrong_id = [dict(self.rows()[0], id="another_movie")]
        duplicate_occurrences = [self.rows()[0], dict(self.rows()[1], occurrence_id="occ-2")]
        empty_identity = [dict(self.rows()[0], occurrence_id="")]
        nullable = [dict(self.rows()[0], reviewText=None)]
        cases = [self.table().rename_columns(["occurrence_id", "id", "reviewId", "reviewText"]),
                 self.table().append_column("scoreSentiment", pa.array(["POSITIVE", "NEGATIVE"])),
                 self.table().set_column(0, "c0", pa.array([1, 2])), self.table(wrong_id),
                 self.table(duplicate_occurrences), self.table(empty_identity), self.table(nullable)]
        with patch.object(udf, "_configuration") as configure:
            for table in cases:
                with self.subTest(schema=table.schema), self.assertRaises(ValueError):
                    udf.infer_q3(table)
            with self.assertRaises(TypeError):
                udf.infer_q3(self.rows())
        configure.assert_not_called()
        self.assertEqual(self.saved(), [])

    def test_invalid_utf8_is_rejected_at_the_author_table_boundary(self):
        array = pa.Array.from_buffers(pa.string(), 1,
            [None, pa.py_buffer(b"\0\0\0\0\1\0\0\0"), pa.py_buffer(b"\xff")])
        table = self.table(self.rows()[:1]).set_column(3, "c3", array)
        with patch.object(udf, "_configuration") as configure, self.assertRaises(pa.ArrowInvalid):
            udf.infer_q3(table)
        configure.assert_not_called()

    def test_provider_exception_object_is_preserved_with_unknown_batch_status_and_no_retry(self):
        error = RuntimeError("private-fixture-credential")
        provider = Mock(side_effect=error)
        with patch.object(udf, "_configuration", return_value=(provider, self.receipts, "software_fixture", "a" * 64)):
            with self.assertRaises(RuntimeError) as caught:
                udf.infer_q3(self.table())
        self.assertIs(caught.exception, error)
        provider.assert_called_once()
        receipt, = self.saved()
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["error_type"], "RuntimeError")
        self.assertEqual(receipt["provider_calls"][0]["status"], "unknown")
        self.assertFalse(receipt["provider_calls"][0]["returned"])
        self.assertEqual(receipt["predictions"], [])
        self.assertNotIn("private-fixture-credential", json.dumps(receipt))

    def test_wrong_provider_result_has_no_guessed_association_or_fallback(self):
        for index, replies in enumerate(("True", ("True", "False"), ["True", 1], ["True"], [])):
            with self.subTest(replies=replies):
                directory = self.receipts / str(index)
                directory.mkdir()
                provider = Mock(return_value=replies)
                with patch.object(udf, "_configuration", return_value=(provider, directory, "software_fixture", "a" * 64)):
                    with self.assertRaises((TypeError, ValueError)):
                        udf.infer_q3(self.table())
                provider.assert_called_once()
                receipt, = [json.loads(path.read_text()) for path in directory.iterdir()]
                self.assertEqual(receipt["predictions"], [])
                self.assertTrue(receipt["provider_calls"][0]["returned"])
                self.assertEqual(receipt["provider_calls"][0]["status"], "unknown")
                if isinstance(replies, list) and all(isinstance(reply, str) for reply in replies):
                    self.assertEqual(receipt["unassociated_reply_texts"], replies)

    def test_callable_is_loaded_once_and_native_batches_are_not_rebatched(self):
        self.configured("calls = 0\ndef reply_batch(messages_batch):\n"
                        "    global calls\n    calls += 1\n"
                        "    return ['True' if calls == 1 else 'False' for _ in messages_batch]\n")
        first = udf.infer_q3(self.table(self.rows()[:1]))
        second = udf.infer_q3(self.table())
        self.assertEqual(first.column(0).to_pylist(), [1])
        self.assertEqual(second.column(0).to_pylist(), [0, 0])
        self.assertEqual(sorted(receipt["logical_rows"] for receipt in self.saved()), [1, 2])
        self.assertTrue(all(len(receipt["provider_calls"]) == 1 for receipt in self.saved()))

    def test_provider_receipt_hash_describes_loaded_source_even_with_stale_bytecode(self):
        old = "def reply_batch(messages_batch):\n    return ['True ' for _ in messages_batch]\n"
        new = old.replace("'True '", "'False'")
        self.configured(old)
        source = self.root / "reply_fixture.py"
        original = source.stat()
        py_compile.compile(str(source), doraise=True)
        source.write_text(new, encoding="utf-8")
        os.utime(source, ns=(original.st_atime_ns, original.st_mtime_ns))
        output = udf.infer_q3(self.table())
        receipt, = self.saved()
        self.assertEqual(output.column(0).to_pylist(), [0, 0])
        self.assertEqual(receipt["provider_source_sha256"], hashlib.sha256(new.encode()).hexdigest())

    def test_author_basename_import_exposes_a_plain_table_function(self):
        with patch.dict(sys.modules), patch.object(sys, "path", [*sys.path, str(Path(udf.__file__).parent)]):
            sys.modules.pop("q3_udf", None)
            loaded = importlib.import_module("q3_udf")
            self.assertEqual(Path(loaded.__file__).resolve(), Path(udf.__file__).resolve())
            self.assertEqual(loaded.infer_q3.__name__, "infer_q3")
            self.assertEqual(loaded.infer_q3(self.table([])).schema, udf.OUTPUT)

    def test_nonempty_unbound_provider_fails_explicitly(self):
        with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(RuntimeError, "pending"):
            udf.infer_q3(self.table())
        self.assertEqual(self.saved(), [])

    def test_module_import_failure_and_noncallable_are_preserved_without_provider_receipt(self):
        self.configured("raise LookupError('private-fixture-credential')\n")
        with self.assertRaisesRegex(LookupError, "private-fixture-credential"):
            udf.infer_q3(self.table())
        self.configured("reply_batch = 3\n")
        with self.assertRaisesRegex(TypeError, "callable"):
            udf.infer_q3(self.table())
        self.assertEqual(self.saved(), [])

    def test_receipt_failure_prevents_success_and_never_replaces_provider_exception(self):
        provider = Mock(return_value=["True", "False"])
        with patch.object(udf, "_configuration", return_value=(provider, self.receipts, "software_fixture", "a" * 64)):
            with patch.object(udf, "publish_private_json", side_effect=PermissionError("private path")):
                with self.assertRaises(PermissionError):
                    udf.infer_q3(self.table())
                error = LookupError("original provider failure")
                provider.side_effect = error
                with self.assertRaises(LookupError) as caught:
                    udf.infer_q3(self.table())
        self.assertIs(caught.exception, error)
        self.assertEqual(error.__notes__, ["IMLane Q3 receipt publication failed: PermissionError"])

    def test_saved_occurrences_and_arrow_count_feed_existing_quality_evaluator(self):
        self.fixed(["False", "True"])
        rows = self.rows()
        output = udf.infer_q3(self.table(rows))
        receipt, = self.saved()
        source = dict(rows=rows, labeled=[dict(rows[0], scoreSentiment="POSITIVE"),
                                          dict(rows[1], scoreSentiment="NEGATIVE")])
        with patch("src.baselines.text.sembench_movie.evaluate_original", return_value={}) as original:
            quality = evaluate_q3(source, receipt["predictions"],
                                  positive_review_cnt=sum(output.column(0).to_pylist()), checkout="fixture")
        self.assertEqual(quality["classification"]["false_positive"], 1)
        self.assertEqual(quality["classification"]["false_negative"], 1)
        self.assertEqual(quality["classification"]["count_error_from_classifications"], 0)
        self.assertEqual(quality["result"]["columns"], ["positive_review_cnt"])
        self.assertEqual(original.call_args.args[1], 3)


if __name__ == "__main__":
    unittest.main()
