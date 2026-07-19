from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.workloads import AI_COMPLETE_CONTROLLED_WORKLOAD, SYNTHETIC_WORKLOAD, generate_document_rows


class WorkloadTests(unittest.TestCase):
    def test_synthetic_workload_preserves_legacy_shape(self) -> None:
        rows = generate_document_rows(0, 2, SYNTHETIC_WORKLOAD)

        self.assertEqual(rows[0].doc_id, 0)
        self.assertEqual(rows[0].tenant_id, 0)
        self.assertEqual(rows[0].category, "cat_0")
        self.assertIn("token token", rows[0].text)
        self.assertEqual(rows[1].category, "cat_1")

    def test_ai_complete_controlled_has_task_and_length_variance(self) -> None:
        rows = generate_document_rows(0, 24, AI_COMPLETE_CONTROLLED_WORKLOAD)
        categories = {row.category for row in rows}
        word_counts = [len(row.text.split()) for row in rows]

        self.assertGreaterEqual(len(categories), 6)
        self.assertTrue(any(category.startswith("short_") for category in categories))
        self.assertTrue(any(category.startswith("medium_") for category in categories))
        self.assertTrue(any(category.startswith("long_") for category in categories))
        self.assertLess(min(word_counts), max(word_counts))
        self.assertGreater(max(word_counts) - min(word_counts), 50)
        self.assertTrue(any(row.text.startswith("Summarize the following customer support ticket") for row in rows))
        self.assertTrue(any(row.text.startswith("Extract contract fields as compact JSON") for row in rows))

    def test_invalid_workload_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            generate_document_rows(0, 1, "missing")


if __name__ == "__main__":
    unittest.main()
