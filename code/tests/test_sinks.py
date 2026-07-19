from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.sinks import batched_rows, vector_to_pg_literal, write_completions, write_embeddings


class FakeCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def executemany(self, statement, rows):
        self.executed.append((statement, list(rows)))


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.commits = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1


class SinkTests(unittest.TestCase):
    def test_vector_literal_is_pgvector_compatible_json(self) -> None:
        self.assertEqual(vector_to_pg_literal(np.asarray([1.0, 2.5], dtype=np.float32)), "[1.0,2.5]")

    def test_batched_rows_respects_batch_size(self) -> None:
        chunks = list(batched_rows([(1,), (2,), (3,)], batch_rows=2))

        self.assertEqual(chunks, [[(1,), (2,)], [(3,)]])

    def test_write_embeddings_json_text_batches_rows(self) -> None:
        conn = FakeConnection()
        result = {
            "doc_id": [1, 2, 3],
            "tenant_id": [10, 10, 11],
            "category": ["a", "a", "b"],
            "embedding": np.ones((3, 2), dtype=np.float32),
        }

        written = write_embeddings(conn, [result], "json_text", write_batch_rows=2)

        self.assertEqual(written, 3)
        self.assertEqual(conn.commits, 1)
        self.assertEqual([len(rows) for _, rows in conn.cursor_obj.executed], [2, 1])

    def test_write_embeddings_none_skips_connection(self) -> None:
        conn = FakeConnection()

        self.assertEqual(write_embeddings(conn, [], "none", write_batch_rows=0), 0)
        self.assertEqual(conn.commits, 0)

    def test_write_completions_json_text_batches_rows(self) -> None:
        conn = FakeConnection()
        result = {
            "doc_id": [1, 2],
            "tenant_id": [10, 11],
            "category": ["a", "b"],
            "output_text": ["answer one", "answer two"],
        }

        written = write_completions(conn, [result], "json_text", write_batch_rows=1)

        self.assertEqual(written, 2)
        self.assertEqual(conn.commits, 1)
        self.assertEqual([len(rows) for _, rows in conn.cursor_obj.executed], [1, 1])

    def test_write_completions_rejects_pgvector(self) -> None:
        conn = FakeConnection()

        with self.assertRaises(ValueError):
            write_completions(conn, [], "pgvector", write_batch_rows=0)


if __name__ == "__main__":
    unittest.main()
