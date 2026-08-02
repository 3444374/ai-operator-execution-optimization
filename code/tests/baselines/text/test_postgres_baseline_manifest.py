from __future__ import annotations

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

from src.baselines.text.orchestration.postgres_manifest import load_postgres_requests


class RecordingCursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.executed: tuple[str, tuple[object, ...]] | None = None

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.executed = (sql, params)

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows

    def __enter__(self) -> RecordingCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class RecordingConnection:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.cursor_instance = RecordingCursor(rows)

    def cursor(self) -> RecordingCursor:
        return self.cursor_instance


class PostgresBaselineManifestTests(unittest.TestCase):
    def test_loads_stable_complete_rows_with_trace_target_estimates(
        self,
    ) -> None:
        connection = RecordingConnection(
            [
                (10, "prompt-a", 0.25, 20, 300),
                (11, "prompt-b", None, 12, 32),
            ]
        )

        requests = load_postgres_requests(
            connection,
            workload_name="sharegpt_burstgpt",
            row_count=2,
            row_offset=32,
            max_output_tokens=256,
            estimated_output_mode="trace_target",
        )

        sql, params = connection.cursor_instance.executed or ("", ())
        self.assertIn("ORDER BY doc_id", sql)
        self.assertEqual(
            params,
            ("sharegpt_burstgpt", 2, 32),
        )
        self.assertEqual([request.doc_id for request in requests], [10, 11])
        self.assertEqual(requests[0].max_output_tokens, 256)
        self.assertEqual(requests[0].estimated_output_tokens, 256)
        self.assertEqual(requests[1].estimated_output_tokens, 32)
        self.assertEqual(requests[1].arrival_time_s, 0.0)
        self.assertEqual(len(requests[0].source_row_hash), 64)
        self.assertNotEqual(
            requests[0].source_row_hash,
            requests[1].source_row_hash,
        )

    def test_rejects_short_or_duplicate_database_result(self) -> None:
        short = RecordingConnection(
            [(10, "prompt-a", 0.0, 20, 16)]
        )
        with self.assertRaisesRegex(ValueError, "expected 2 rows"):
            load_postgres_requests(
                short,
                workload_name="sharegpt_burstgpt",
                row_count=2,
                row_offset=0,
                max_output_tokens=256,
                estimated_output_mode="fixed_cap",
            )

        duplicate = RecordingConnection(
            [
                (10, "prompt-a", 0.0, 20, 16),
                (10, "prompt-b", 0.1, 21, 17),
            ]
        )
        with self.assertRaisesRegex(ValueError, "duplicate doc_id"):
            load_postgres_requests(
                duplicate,
                workload_name="sharegpt_burstgpt",
                row_count=2,
                row_offset=0,
                max_output_tokens=256,
                estimated_output_mode="fixed_cap",
            )


if __name__ == "__main__":
    unittest.main()
