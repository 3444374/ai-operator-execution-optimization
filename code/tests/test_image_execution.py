import unittest
import sys
from pathlib import Path

import numpy as np


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.image.execution import EmbeddingAudit, ExecutionResult


class EmbeddingAuditTest(unittest.TestCase):
    def test_accepts_streamed_exactly_once_unit_embeddings(self):
        audit = EmbeddingAudit(frozenset({"1", "2"}), dimension=2)
        audit.add(("1",), np.asarray([[1.0, 0.0]], dtype=np.float32))
        audit.add(("2",), np.asarray([[0.0, 1.0]], dtype=np.float32))

        summary = audit.finish()

        self.assertEqual(summary["output_rows"], 2)
        self.assertTrue(summary["exactly_once"])
        self.assertEqual(summary["max_norm_error"], 0.0)

    def test_rejects_duplicate_doc_id(self):
        audit = EmbeddingAudit(frozenset({"1"}), dimension=2)
        audit.add(("1",), np.asarray([[1.0, 0.0]], dtype=np.float32))

        with self.assertRaisesRegex(ValueError, "duplicate"):
            audit.add(("1",), np.asarray([[1.0, 0.0]], dtype=np.float32))

    def test_rejects_missing_doc_id(self):
        audit = EmbeddingAudit(frozenset({"1", "2"}), dimension=2)
        audit.add(("1",), np.asarray([[1.0, 0.0]], dtype=np.float32))

        with self.assertRaisesRegex(ValueError, "exactly-once"):
            audit.finish()


class ExecutionResultTest(unittest.TestCase):
    def test_rejects_first_output_after_total(self):
        with self.assertRaisesRegex(ValueError, "first_output"):
            ExecutionResult(total_s=1.0, first_output_s=2.0, audit={})


if __name__ == "__main__":
    unittest.main()
