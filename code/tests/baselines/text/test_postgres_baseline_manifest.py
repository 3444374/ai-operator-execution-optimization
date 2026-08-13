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

from src.baselines.common.contracts import ChatRequest
from src.baselines.text.orchestration.postgres_manifest import (
    load_manifest_postgres_requests,
    load_postgres_requests,
    source_row_hash,
)


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
    @staticmethod
    def _manifest_request(
        *, doc_id: int, prompt: str, arrival_time_s: float, endpoint_index: int
    ) -> ChatRequest:
        return ChatRequest(
            doc_id=doc_id,
            prompt=prompt,
            arrival_time_s=arrival_time_s,
            prompt_tokens=10,
            max_output_tokens=256,
            estimated_output_tokens=32,
            source_row_hash=source_row_hash(
                workload_name="sharegpt",
                doc_id=doc_id,
                prompt=prompt,
                arrival_time_s=arrival_time_s,
                prompt_tokens=10,
                target_output_tokens=32,
            ),
            endpoint_index=endpoint_index,
        )

    def test_loads_exact_manifest_rows_in_manifest_order(self) -> None:
        endpoint_0 = self._manifest_request(
            doc_id=10, prompt="prompt-a", arrival_time_s=0.0, endpoint_index=0
        )
        endpoint_1 = self._manifest_request(
            doc_id=11, prompt="prompt-b", arrival_time_s=0.25, endpoint_index=1
        )
        connection = RecordingConnection(
            [
                (10, "prompt-a", 0.0, 10, 32),
                (11, "prompt-b", 0.25, 10, 32),
            ]
        )

        requests = load_manifest_postgres_requests(
            connection,
            workload_name="sharegpt",
            manifest=(endpoint_1, endpoint_0),
        )

        sql, params = connection.cursor_instance.executed or ("", ())
        self.assertIn("WHERE workload_name = %s", sql)
        self.assertIn("doc_id = ANY(%s)", sql)
        self.assertEqual(params, ("sharegpt", [11, 10]))
        self.assertEqual([item.doc_id for item in requests], [11, 10])
        self.assertEqual([item.endpoint_index for item in requests], [1, 0])

    def test_rejects_manifest_source_drift_and_result_cardinality_errors(self) -> None:
        manifest = (
            self._manifest_request(
                doc_id=10, prompt="prompt-a", arrival_time_s=0.0, endpoint_index=0
            ),
        )
        for rows, expected in (
            ([], "missing database row"),
            ([(10, "prompt-a", 0.0, 10, 32), (10, "prompt-a", 0.0, 10, 32)], "duplicate database row"),
            ([(10, "changed", 0.0, 10, 32)], "immutable field mismatch"),
        ):
            with self.subTest(expected=expected), self.assertRaisesRegex(ValueError, expected):
                load_manifest_postgres_requests(
                    RecordingConnection(rows), workload_name="sharegpt", manifest=manifest
                )
        with self.assertRaisesRegex(ValueError, "immutable field mismatch"):
            load_manifest_postgres_requests(
                RecordingConnection([(10, "prompt-a", 0.0, 10, 32)]),
                workload_name="other_workload",
                manifest=manifest,
            )
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
