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

_GATE_PATH = CODE_ROOT / "scripts" / "baselines" / "squad_capability_gate.py"
_spec = importlib.util.spec_from_file_location("squad_capability_gate", _GATE_PATH)
gate = importlib.util.module_from_spec(_spec)
sys.modules["squad_capability_gate"] = gate
_spec.loader.exec_module(gate)


def _row(example_id: str, answer: str) -> dict:
    return {
        "doc_id": abs(hash(example_id)) % (10**12),
        "text": "prompt " + example_id,
        "source_example_id": example_id,
        "answers": [answer],
    }


def _fixture() -> list[dict]:
    # 30 short (1 word) + 40 medium (2 words) + 30 long (5 words) = 100
    rows = []
    for i in range(30):
        rows.append(_row(f"short{i:02d}", "yes"))
    for i in range(40):
        rows.append(_row(f"medium{i:02d}", "two words"))
    for i in range(30):
        rows.append(_row(f"long{i:02d}", "one two three four five"))
    return rows


class StratifiedSampleTests(unittest.TestCase):
    def test_answer_bucket(self) -> None:
        self.assertEqual(gate._answer_bucket(["yes"]), "short")
        self.assertEqual(gate._answer_bucket(["the answer"]), "medium")
        self.assertEqual(gate._answer_bucket(["one two three four five"]), "long")

    def test_deterministic_and_exact_count(self) -> None:
        rows = _fixture()
        first = gate.stratified_sample(rows, 10)
        second = gate.stratified_sample(rows, 10)
        self.assertEqual(len(first), 10)
        self.assertEqual(
            [r["source_example_id"] for r in first],
            [r["source_example_id"] for r in second],
        )
        ids = [r["source_example_id"] for r in first]
        self.assertEqual(len(set(ids)), 10)

    def test_target_exceeds_total_returns_all(self) -> None:
        rows = _fixture()[:5]
        sampled = gate.stratified_sample(rows, 10)
        self.assertEqual(len(sampled), 5)

    def test_stratification_covers_all_buckets(self) -> None:
        rows = _fixture()
        sampled = gate.stratified_sample(rows, 30)
        buckets_present = {gate._answer_bucket(r["answers"]) for r in sampled}
        # all three buckets should be represented (proportional allocation)
        self.assertEqual(buckets_present, {"short", "medium", "long"})

    def test_sample_hash_stable(self) -> None:
        rows = _fixture()
        sampled = gate.stratified_sample(rows, 12)
        self.assertEqual(gate._sample_hash(sampled), gate._sample_hash(sampled))
        other = gate.stratified_sample(rows, 12)
        self.assertEqual(gate._sample_hash(sampled), gate._sample_hash(other))


if __name__ == "__main__":
    unittest.main()
