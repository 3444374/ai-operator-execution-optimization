from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.sources import PostgresArrowSource, SourceConfig, daft_sql_query, make_source, postgres_documents_query


class SourceTests(unittest.TestCase):
    def test_postgres_documents_query_uses_limit_and_offset(self) -> None:
        sql, params = postgres_documents_query(SourceConfig(limit=128, offset=256))

        self.assertIn("doc_id", sql)
        self.assertIn("workload_name", sql)
        self.assertIn("ORDER BY doc_id", sql)
        self.assertEqual(params, (128, 256))

    def test_postgres_documents_query_can_filter_workload_name(self) -> None:
        sql, params = postgres_documents_query(SourceConfig(limit=128, offset=256, workload_name="sharegpt_burstgpt"))

        self.assertIn("WHERE workload_name = %s", sql)
        self.assertEqual(params, ("sharegpt_burstgpt", 128, 256))

    def test_postgres_documents_query_can_order_by_arrival_time(self) -> None:
        sql, params = postgres_documents_query(SourceConfig(limit=128, offset=256, order="arrival_time"))

        self.assertIn("ORDER BY arrival_time_s NULLS LAST, doc_id", sql)
        self.assertEqual(params, (128, 256))

    def test_postgres_documents_query_rejects_unknown_order(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown source order"):
            postgres_documents_query(SourceConfig(limit=128, offset=256, order="bad"))


class FakeCursor:
    def __init__(self):
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed = (sql, params)

    def fetchall(self):
        return [
            (1, 1, "cat_a", "hello world", "test", 10, 2, 0.0, "s1", "p1"),
            (2, 1, "cat_a", "goodbye world", "test", 12, 3, 1.0, "s2", "p2"),
        ]


class FakeConnection:
    def __init__(self):
        self.cursor_obj = FakeCursor()

    def cursor(self):
        return self.cursor_obj


class PostgresArrowSourceTests(unittest.TestCase):
    def test_fetch_returns_arrow_table(self) -> None:
        conn = FakeConnection()
        source = PostgresArrowSource()

        batch = source.fetch(conn, SourceConfig(limit=2, offset=0))

        self.assertEqual(batch.table.num_rows, 2)
        self.assertEqual(
            batch.table.column_names,
            [
                "doc_id",
                "tenant_id",
                "category",
                "text",
                "workload_name",
                "prompt_tokens",
                "target_output_tokens",
                "arrival_time_s",
                "session_id",
                "prefix_key",
            ],
        )
        self.assertGreaterEqual(batch.metrics["db_fetch_s"], 0.0)
        self.assertGreaterEqual(batch.metrics["arrow_build_s"], 0.0)


class DaftSqlTests(unittest.TestCase):
    def test_daft_sql_query_uses_literal_limit_offset(self) -> None:
        sql = daft_sql_query(SourceConfig(limit=32, offset=64))

        self.assertIn("workload_name", sql)
        self.assertIn("LIMIT 32 OFFSET 64", sql)

    def test_daft_sql_query_can_filter_workload_name(self) -> None:
        sql = daft_sql_query(SourceConfig(limit=32, offset=64, workload_name="sharegpt_burstgpt"))

        self.assertIn("WHERE workload_name = 'sharegpt_burstgpt'", sql)
        self.assertIn("LIMIT 32 OFFSET 64", sql)

    def test_daft_sql_query_can_order_by_arrival_time(self) -> None:
        sql = daft_sql_query(SourceConfig(limit=32, offset=64, order="arrival_time"))

        self.assertIn("ORDER BY arrival_time_s NULLS LAST, doc_id", sql)
        self.assertIn("LIMIT 32 OFFSET 64", sql)


class SourceFactoryTests(unittest.TestCase):
    def test_make_source_returns_named_source(self) -> None:
        self.assertEqual(make_source("arrow_postgres").name, "arrow_postgres")
        self.assertEqual(make_source("daft_postgres").name, "daft_postgres")


if __name__ == "__main__":
    unittest.main()
