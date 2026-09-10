"""Exercise verbatim workload storage and tokenizer shape regressions without a model."""

import contextlib
import hashlib
import io
import json
from pathlib import Path
import stat
import tempfile
import unittest

from scripts.baselines.map_workload_tools import main
from src.baselines.common.private_artifacts import (
    content_digest, new_private_directory, write_private_json, workload_summary,
)
from src.baselines.common.redact import redact_text
from src.baselines.text.map_inputs import context_report, verify_text_roundtrip
from src.baselines.text.sharegpt_inputs import (
    first_human_verbatim, prepare_sharegpt_manifest, validate_sharegpt_manifest,
)
from src.modalities.text.tokenization import chat_token_count


class Tokenizer:
    def __init__(self, returned):
        self.returned = returned
        self.calls = []

    def apply_chat_template(self, messages, **options):
        self.calls.append((messages, options))
        return self.returned


class MapInputTests(unittest.TestCase):
    def test_text_roundtrip_preserves_whitespace_and_unicode(self):
        expected = [("first", "  Café\r\n\tencoded \\n"), ("second", "e\u0301")]
        self.assertTrue(verify_text_roundtrip(expected, reversed(expected))["matched"])
        for actual in ([expected[0]], expected + [expected[0]],
                       [("first", "Café encoded \\n"), expected[1]],
                       [expected[0], ("second", "é")]):
            with self.subTest(actual=actual), self.assertRaises(ValueError):
                verify_text_roundtrip(expected, actual)

    def test_count_uses_token_ids_instead_of_mapping_length(self):
        for value in ([1, 2, 3, 4, 5], {"input_ids": [1, 2, 3, 4, 5], "attention_mask": [1]*5}):
            tokenizer = Tokenizer(value)
            self.assertEqual(chat_token_count(tokenizer, [{"role": "user", "content": "raw"}]), 5)
            self.assertEqual(tokenizer.calls[0][1], {"tokenize": True, "add_generation_prompt": True, "return_dict": False})

    def test_malformed_token_results_rejected(self):
        for value in ({"attention_mask": [1]}, [[1, 2]], [True], [-1], "1234", [], None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                chat_token_count(Tokenizer(value), [])

    def test_context_limit_is_inclusive_without_changing_input(self):
        tokenizer = Tokenizer({"input_ids": [1]*5, "attention_mask": [1]*5})
        inputs = [("row", "  preserve\n raw ")]
        report = context_report(inputs, "Summarize", tokenizer, model_id="test-model",
            model_revision="fixture", output_tokens=3, context_tokens=8)
        self.assertTrue(report["fits_context"])
        self.assertEqual(tokenizer.calls[0][0][1]["content"], inputs[0][1])
        report = context_report(inputs, "Summarize", tokenizer, model_id="test-model",
            model_revision="fixture", output_tokens=3, context_tokens=7)
        self.assertFalse(report["fits_context"])


class ShareGPTPreparationTests(unittest.TestCase):
    def test_first_human_is_verbatim_and_not_replaced_by_later_turn(self):
        raw = " \r\nCafé\t  "
        self.assertEqual(first_human_verbatim({"conversations": [{"from": "human", "value": raw}]}), raw)
        self.assertIsNone(first_human_verbatim({"conversations": [
            {"from": "human", "value": " "}, {"from": "human", "value": "later"}]}))

    def test_selection_uses_bytes_and_never_truncates(self):
        records = [{"id": str(i), "conversations": [{"from": "human", "value": value}]}
                   for i, value in enumerate(("a", "éé", "abcdef", "  z  "))]
        manifest = prepare_sharegpt_manifest(records, "1"*64, count=2, minimum_bytes=4, maximum_bytes=5)
        self.assertEqual([r["input_text"] for r in manifest["rows"]], ["éé", "  z  "])
        self.assertEqual(manifest["skipped_before_selection"], {"outside_byte_range": 2})
        validate_sharegpt_manifest(manifest)

    def test_reject_missing_duplicate_ids_and_tampered_text(self):
        row = {"id": "id", "conversations": [{"from": "human", "value": "text"}]}
        with self.assertRaises(ValueError):
            prepare_sharegpt_manifest([row, row], "1"*64, count=2, minimum_bytes=1)
        with self.assertRaises(ValueError):
            prepare_sharegpt_manifest([dict(row, id=None)], "1"*64, count=1, minimum_bytes=1)
        manifest = prepare_sharegpt_manifest([row], "1"*64, count=1, minimum_bytes=1)
        manifest["rows"][0]["input_text"] = "changed"
        with self.assertRaises(ValueError):
            validate_sharegpt_manifest(manifest)

    def test_private_cli_roundtrip_and_public_summary_are_separate(self):
        # A synthetic named secret reproduces the original evidence-redaction error.
        text = '  {"password":"fixture-value"}\n\tKeep spaces. '
        self.assertNotEqual(redact_text(text), text)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.json"
            source.write_text(json.dumps([{"id": "original", "conversations": [{"from": "human", "value": text}]}]))
            out = root / "private"
            args = ["sharegpt", "--source", str(source), "--source-sha256", hashlib.sha256(source.read_bytes()).hexdigest(),
                "--count", "1", "--minimum-bytes", "1", "--output-dir", str(out)]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(main(args), 0)
            manifest = json.loads((out / "manifest.json").read_text())
            self.assertEqual(manifest["rows"][0]["input_text"], text)
            self.assertEqual(stat.S_IMODE(out.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE((out / "manifest.json").stat().st_mode), 0o600)
            summary = workload_summary(manifest)
            self.assertNotIn("fixture-value", json.dumps(summary))
            self.assertNotIn("original", json.dumps(summary))
            with self.assertRaises(ValueError):
                validate_sharegpt_manifest(summary)
            before = (out / "manifest.json").read_bytes()
            with self.assertRaises(FileExistsError):
                main(args)
            self.assertEqual((out / "manifest.json").read_bytes(), before)

    def test_private_writer_refuses_git_and_preserves_values(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".git").write_text("gitdir: placeholder")
            with self.assertRaises(ValueError):
                new_private_directory(root / "private")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private.json"
            value = {"password": "fixture-only", "raw": "\t原文\r\n"}
            write_private_json(path, value)
            self.assertEqual(json.loads(path.read_text()), value)
            with self.assertRaises(FileExistsError):
                write_private_json(path, {})
            self.assertEqual(content_digest(json.loads(path.read_text())), content_digest(value))


if __name__ == "__main__":
    unittest.main()
