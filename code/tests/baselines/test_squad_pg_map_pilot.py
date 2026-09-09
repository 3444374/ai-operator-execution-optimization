"""Check data partition, message identity, and failed-row quality accounting."""

import copy
import unittest
from types import SimpleNamespace

from src.baselines.text.squad_map import (
    INSTRUCTION, content_digest, evaluate_predictions, map_messages,
    prepare_manifest, validate_manifest,
)


def examples():
    return [SimpleNamespace(
        source_example_id=f"q{group}-{index}", context=f"Café\ncontext {group}",
        question=f"Question {index}?", reference_answers=("Paris", "City of Paris"),
    ) for group in range(8) for index in range(2)]


class SquadMapPilotTests(unittest.TestCase):
    def setUp(self):
        self.manifest = prepare_manifest(examples(), "source-sha", rows_per_split=4)

    def test_deterministic_disjoint_contexts_and_original_order(self):
        self.assertEqual(self.manifest, prepare_manifest(examples(), "source-sha", rows_per_split=4))
        splits = self.manifest["splits"]
        self.assertFalse({r["context_sha256"] for r in splits["tuning"]} &
                         {r["context_sha256"] for r in splits["evaluation"]})
        for rows in splits.values():
            positions = [r["source_position"] for r in rows]
            self.assertEqual(positions, sorted(positions))
        validate_manifest(self.manifest)

    def test_complete_unicode_messages(self):
        row = self.manifest["splits"]["tuning"][0]
        self.assertIn("Café\n", row["input_text"])
        self.assertEqual(row["input_bytes"], len(row["input_text"].encode()))
        self.assertEqual(map_messages(row["input_text"]), [
            {"role": "system", "content": INSTRUCTION},
            {"role": "user", "content": row["input_text"]},
        ])

    def test_source_and_answers_change_identity(self):
        changed = examples()
        changed[0].reference_answers = ("London",)
        self.assertNotEqual(self.manifest["sha256"],
                            prepare_manifest(changed, "source-sha", rows_per_split=4)["sha256"])
        self.assertNotEqual(self.manifest["sha256"],
                            prepare_manifest(examples(), "other-sha", rows_per_split=4)["sha256"])

    def test_reject_duplicate_source_ids_and_insufficient_groups(self):
        with self.assertRaisesRegex(ValueError, "unique"):
            prepare_manifest(examples() * 2, "sha")
        with self.assertRaisesRegex(ValueError, "insufficient"):
            prepare_manifest(examples()[:2], "sha", rows_per_split=1)
        with self.assertRaisesRegex(ValueError, "positive"):
            prepare_manifest(examples(), "sha", rows_per_split=0)

    def test_tampered_manifest_rejected(self):
        self.manifest["splits"]["tuning"][0]["input_text"] = "tampered"
        with self.assertRaisesRegex(ValueError, "identity"):
            validate_manifest(self.manifest)

    def test_rehashed_context_leakage_rejected(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["splits"]["evaluation"][0]["context_sha256"] = manifest["splits"]["tuning"][0]["context_sha256"]
        manifest["sha256"] = content_digest({k: v for k, v in manifest.items() if k != "sha256"})
        with self.assertRaisesRegex(ValueError, "leakage"):
            validate_manifest(manifest)

    def predictions(self, answers):
        return [{"source_example_id": row["source_example_id"], "prediction": answer}
                for row, answer in zip(self.manifest["splits"]["tuning"], answers)]

    def test_quality_uses_aliases_and_keeps_failures_in_denominator(self):
        report = evaluate_predictions(self.manifest, "tuning", self.predictions(["Paris", "City of Paris", "", None]))
        self.assertTrue(report["association_complete"])
        self.assertEqual(report["null_predictions"], 1)
        self.assertEqual(report["empty_predictions"], 1)
        self.assertEqual(report["metrics"]["squad_exact_match_percent"], 50)
        self.assertEqual(report["metrics"]["squad_token_f1_percent"], 50)

    def test_missing_rows_are_zero_without_changing_denominator(self):
        report = evaluate_predictions(self.manifest, "tuning", self.predictions(["Paris"]))
        self.assertFalse(report["association_complete"])
        self.assertEqual(report["metrics"]["squad_evaluated_rows"], 4)
        self.assertEqual(report["metrics"]["squad_exact_match_percent"], 25)

    def test_duplicate_unknown_and_nontext_predictions_rejected(self):
        records = self.predictions(["Paris"])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            evaluate_predictions(self.manifest, "tuning", records * 2)
        with self.assertRaisesRegex(ValueError, "unknown"):
            evaluate_predictions(self.manifest, "tuning", [{"source_example_id": "alien", "prediction": "Paris"}])
        with self.assertRaisesRegex(ValueError, "text or null"):
            evaluate_predictions(self.manifest, "tuning", self.predictions([7]))


if __name__ == "__main__":
    unittest.main()
