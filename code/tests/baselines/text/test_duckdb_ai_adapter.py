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

from src.baselines.common.contracts import ChatRequest  # noqa: E402
from src.baselines.text.products.duckdb_ai import (  # noqa: E402
    DuckDBAiConfig,
    build_ai_complete_query,
    configure_ai_endpoint,
    run_duckdb_ai_complete,
)


def sample_request(
    doc_id: int,
    *,
    endpoint_index: int = 0,
    max_output_tokens: int = 128,
) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=f"question-{doc_id}",
        arrival_time_s=0.0,
        prompt_tokens=4,
        max_output_tokens=max_output_tokens,
        estimated_output_tokens=max_output_tokens,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=endpoint_index,
    )


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConnection:
    """Records every statement and returns canned rows for the ai_complete call."""

    def __init__(self, completion_rows):
        self.statements: list[str] = []
        self.executemany_calls: list[tuple[str, tuple]] = []
        self.closed = False
        self._completion_rows = completion_rows

    def execute(self, statement, *_args, **_kwargs):
        self.statements.append(statement)
        if "ai_complete" in statement:
            return _FakeResult(self._completion_rows)
        return None

    def executemany(self, statement, params):
        self.statements.append(statement)
        self.executemany_calls.append((statement, tuple(params)))

    def close(self):
        self.closed = True


class DuckDbAiAdapterTests(unittest.TestCase):
    def test_config_rejects_empty_and_negative(self) -> None:
        with self.assertRaisesRegex(ValueError, "endpoint_base_url"):
            DuckDBAiConfig(
                endpoint_base_url="",
                model="qwen",
                api_key="EMPTY",
                max_tokens=64,
            )
        with self.assertRaisesRegex(ValueError, "max_tokens"):
            DuckDBAiConfig(
                endpoint_base_url="http://127.0.0.1:8000/v1",
                model="qwen",
                api_key="EMPTY",
                max_tokens=-1,
            )

    def test_sql_literal_doubles_embedded_quotes(self) -> None:
        # Imported helper is private; verify behaviour through configure endpoint.
        connection = FakeConnection([])
        configure_ai_endpoint(
            connection,
            DuckDBAiConfig(
                endpoint_base_url="http://127.0.0.1:8000/v1",
                model="qwen's-model",
                api_key="EMP'TY",
                max_tokens=64,
            ),
        )
        model_stmt = next(s for s in connection.statements if "duckdb_ai_model" in s)
        secret_stmt = next(s for s in connection.statements if "SECRET" in s)
        # Each embedded single quote is doubled inside the SQL literal.
        self.assertIn("'qwen''s-model'", model_stmt)
        self.assertIn("'EMP''TY'", secret_stmt)
        self.assertIn("'http://127.0.0.1:8000/v1'", secret_stmt)
        self.assertIn("openai_compatible", secret_stmt)
        self.assertIn(
            "SET duckdb_ai_provider = 'openai_compatible'",
            connection.statements,
        )

    def test_build_query_uses_named_args_and_identifier_guard(self) -> None:
        query = build_ai_complete_query("duckdb_ai_source_ep0", max_tokens=96)
        self.assertIn("ai_complete(prompt, max_tokens => 96", query)
        self.assertIn("temperature => 0.0", query)
        self.assertIn("FROM duckdb_ai_source_ep0", query)
        with self.assertRaisesRegex(ValueError, "invalid source table"):
            build_ai_complete_query("duckdb_ai_source_ep0; DROP", max_tokens=1)

    def test_validate_rejects_mixed_shards(self) -> None:
        config = DuckDBAiConfig(
            endpoint_base_url="http://127.0.0.1:8000/v1",
            model="qwen",
            api_key="EMPTY",
            max_tokens=128,
        )
        with self.assertRaisesRegex(ValueError, "one endpoint shard"):
            run_duckdb_ai_complete(
                (sample_request(1, endpoint_index=0), sample_request(2, endpoint_index=1)),
                config,
                connection_factory=lambda _c: FakeConnection([]),
            )
        with self.assertRaisesRegex(ValueError, "same max_output_tokens"):
            run_duckdb_ai_complete(
                (
                    sample_request(1, max_output_tokens=64),
                    sample_request(2, max_output_tokens=128),
                ),
                config,
                connection_factory=lambda _c: FakeConnection([]),
            )

    def test_run_executes_set_orientated_query_and_closes_connection(self) -> None:
        requests = tuple(sample_request(i) for i in range(3))
        completion_rows = [(request.doc_id, f"answer-{request.doc_id}") for request in requests]
        captured: dict[str, FakeConnection] = {}

        def factory(config):
            connection = FakeConnection(completion_rows)
            captured["conn"] = connection
            return connection

        results = run_duckdb_ai_complete(
            requests,
            DuckDBAiConfig(
                endpoint_base_url="http://127.0.0.1:8000/v1",
                model="qwen2.5-7b",
                api_key="EMPTY",
                max_tokens=128,
            ),
            connection_factory=factory,
        )

        connection = captured["conn"]
        # The endpoint is configured before the data query runs.
        self.assertTrue(
            any("CREATE OR REPLACE SECRET duckdb_ai_endpoint" in s for s in connection.statements)
        )
        self.assertTrue(
            any("INSERT INTO duckdb_ai_source_ep0" in stmt for stmt in connection.statements)
        )
        self.assertEqual(len(connection.executemany_calls), 1)
        insert_stmt, params = connection.executemany_calls[0]
        self.assertEqual(params, tuple((r.doc_id, r.prompt) for r in requests))
        self.assertTrue(connection.closed)

        # Results are set-oriented: one shared submit/complete barrier per shard.
        self.assertEqual(len(results), len(requests))
        self.assertEqual({r.doc_id for r in results}, {r.doc_id for r in requests})
        self.assertTrue(all(r.status == "completed" for r in results))
        self.assertEqual(results[0].output_text, "answer-0")
        self.assertEqual(results[0].submitted_at_s, results[1].submitted_at_s)
        self.assertEqual(results[0].completed_at_s, results[2].completed_at_s)
        self.assertEqual(results[0].input_tokens, 4)
        self.assertEqual(results[0].output_tokens, 0)

    def test_run_rejects_exactly_once_violation(self) -> None:
        requests = tuple(sample_request(i) for i in range(3))
        # Duplicate doc_id 1 and drop doc_id 2 -> exactly-once fails.
        bad_rows = [(0, "a"), (1, "b"), (1, "c")]

        def factory(_config):
            return FakeConnection(bad_rows)

        with self.assertRaisesRegex(ValueError, "exactly-once"):
            run_duckdb_ai_complete(
                requests,
                DuckDBAiConfig(
                    endpoint_base_url="http://127.0.0.1:8000/v1",
                    model="qwen2.5-7b",
                    api_key="EMPTY",
                    max_tokens=128,
                ),
                connection_factory=factory,
            )


if __name__ == "__main__":
    unittest.main()
