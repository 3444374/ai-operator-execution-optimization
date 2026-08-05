from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

_GATE_PATH = CODE_ROOT / "scripts" / "baselines" / "duckdb_ai_sentence_count_gate.py"
_spec = importlib.util.spec_from_file_location("duckdb_ai_sentence_count_gate", _GATE_PATH)
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)


class SentenceCountGateHelperTests(unittest.TestCase):
    def test_sentence_count_basic(self) -> None:
        self.assertEqual(gate._sentence_count("Hello world."), 1)
        self.assertEqual(gate._sentence_count("A. B? C!"), 3)
        self.assertEqual(gate._sentence_count(""), 0)
        self.assertEqual(gate._sentence_count("no punctuation here"), 1)

    def test_recover_original_strips_wrap_prefix(self) -> None:
        wrapped = gate._WRAP_PREFIX + "The real text. Two sentences."
        self.assertEqual(
            gate._recover_original(wrapped), "The real text. Two sentences."
        )
        # prefix absent -> returned unchanged (no crash)
        self.assertEqual(gate._recover_original("plain text"), "plain text")

    def test_integer_match_is_strict_fullmatch(self) -> None:
        # The #6 fix: strict fullmatch, not substring search. A model that returns
        # "There are 3 sentences" must be rejected as invalid-format.
        self.assertIsNotNone(gate._INTEGER_RE.fullmatch("3"))
        self.assertIsNotNone(gate._INTEGER_RE.fullmatch("42"))
        self.assertIsNone(gate._INTEGER_RE.fullmatch("There are 3 sentences"))
        self.assertIsNone(gate._INTEGER_RE.fullmatch("3.0"))
        self.assertIsNone(gate._INTEGER_RE.fullmatch(""))
        self.assertIsNone(gate._INTEGER_RE.fullmatch(" 3 "))

    def test_dist_buckets(self) -> None:
        distribution = gate._dist([0, 1, 2, 3, 5, 9, 12])
        self.assertEqual(distribution["0"], 1)
        self.assertEqual(distribution["1-2"], 2)
        self.assertEqual(distribution["3-5"], 2)
        self.assertEqual(distribution["6-10"], 1)
        self.assertEqual(distribution["11+"], 1)

    def test_validate_exact_row_count_catches_too_many_and_too_few(self) -> None:
        # exact count passes (no raise)
        gate._validate_exact_row_count(list(range(2048)), 2048, "wl")
        # a workload with 2049 rows must be caught even though LIMIT 2048 would
        # silently hide the extra row -- the caller queries LIMIT expected+1.
        with self.assertRaisesRegex(SystemExit, "more than"):
            gate._validate_exact_row_count(list(range(2049)), 2048, "wl")
        with self.assertRaisesRegex(SystemExit, "fewer than"):
            gate._validate_exact_row_count(list(range(2047)), 2048, "wl")


if __name__ == "__main__":
    unittest.main()
