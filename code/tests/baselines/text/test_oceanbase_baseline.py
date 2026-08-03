from __future__ import annotations

import json
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
from src.baselines.text.products import (
    OceanBaseConfig,
    build_ai_complete_sql,
    build_register_model_sql,
    run_oceanbase_ai_complete,
)


def sample_request(doc_id: int) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=f"question-{doc_id}",
        arrival_time_s=0.0,
        prompt_tokens=4,
        max_output_tokens=128,
        estimated_output_tokens=128,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=0,
    )


def sample_config(**overrides) -> OceanBaseConfig:
    values = {
        "host": "127.0.0.1",
        "port": 2881,
        "user": "root@test",
        "password": "",
        "database": "test",
        "model_key": "baseline_qwen",
        "model_name": "qwen2.5-1.5b",
        "endpoint_url": (
            "http://127.0.0.1:8000/v1/chat/completions"
        ),
        "access_key": "not-needed",
        "parallel_degree": 1,
        "source_table": "baseline_requests_ep0",
        "result_table": "baseline_results_ep0",
        "register_model": False,
    }
    values.update(overrides)
    return OceanBaseConfig(**values)


class RecordingCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, sql, params=None):
        self.connection.executed.append((sql, params))
        if (
            self.connection.fail_ai_complete
            and "AI_COMPLETE" in sql
        ):
            raise RuntimeError("model call failed")

    def executemany(self, sql, rows):
        self.connection.executed_many.append((sql, tuple(rows)))

    def fetchall(self):
        return self.connection.result_rows


class RecordingConnection:
    def __init__(self, result_rows, *, fail_ai_complete=False):
        self.result_rows = result_rows
        self.fail_ai_complete = fail_ai_complete
        self.executed = []
        self.executed_many = []
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return RecordingCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        return None


class OceanBaseBaselineTests(unittest.TestCase):
    def test_registration_targets_same_vllm_chat_endpoint(self) -> None:
        statements = build_register_model_sql(sample_config())

        self.assertIn("CREATE_AI_MODEL", statements[0].sql)
        self.assertIn("CREATE_AI_MODEL_ENDPOINT", statements[1].sql)
        endpoint = json.loads(statements[1].params[1])
        self.assertEqual(
            endpoint["url"],
            "http://127.0.0.1:8000/v1/chat/completions",
        )
        self.assertEqual(endpoint["provider"], "openai")

    def test_ai_complete_sql_binds_model_and_output_cap(self) -> None:
        statement = build_ai_complete_sql(
            "baseline_requests_ep0",
            "baseline_results_ep0",
            model_key="baseline_qwen",
            max_tokens=128,
            parallel_degree=4,
        )

        self.assertIn("/*+ PARALLEL(4) */", statement.sql)
        self.assertIn("AI_COMPLETE(", statement.sql)
        self.assertEqual(
            statement.params,
            ("baseline_qwen", 128),
        )

    def test_identifier_validation_rejects_sql_injection(self) -> None:
        with self.assertRaisesRegex(ValueError, "identifier"):
            build_ai_complete_sql(
                "requests; DROP TABLE x",
                "results",
                model_key="model",
                max_tokens=128,
                parallel_degree=1,
            )

    def test_transaction_commits_only_after_exactly_once_results(
        self,
    ) -> None:
        connection = RecordingConnection(
            [(1, "answer-1"), (2, "answer-2")]
        )
        results = run_oceanbase_ai_complete(
            (sample_request(1), sample_request(2)),
            sample_config(),
            connection_factory=lambda _config: connection,
        )

        self.assertTrue(connection.committed)
        self.assertFalse(connection.rolled_back)
        self.assertEqual(len(connection.executed_many), 1)
        self.assertEqual(
            [result.doc_id for result in results],
            [1, 2],
        )
        ai_calls = [
            sql
            for sql, _params in connection.executed
            if "INSERT INTO baseline_results_ep0" in sql
        ]
        self.assertEqual(len(ai_calls), 1)

    def test_model_failure_rolls_back(self) -> None:
        connection = RecordingConnection(
            [],
            fail_ai_complete=True,
        )

        with self.assertRaisesRegex(RuntimeError, "model call failed"):
            run_oceanbase_ai_complete(
                (sample_request(1),),
                sample_config(),
                connection_factory=lambda _config: connection,
            )

        self.assertFalse(connection.committed)
        self.assertTrue(connection.rolled_back)


if __name__ == "__main__":
    unittest.main()
