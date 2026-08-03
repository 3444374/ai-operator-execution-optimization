from __future__ import annotations

import sys
import unittest
from dataclasses import replace
from io import BytesIO
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
SCRIPTS_ROOT = CODE_ROOT / "scripts"
for path in (CODE_ROOT, SCRIPTS_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from scripts.data.import_ai_complete_workload import (  # noqa: E402
    BurstTraceRow,
    WorkloadRow,
    apply_controlled_prefix,
    build_workload_rows,
    burstgpt_rows_from_dicts,
    category_for,
    first_human_prompt,
    insert_rows,
    length_bucket,
    load_http_prompt_token_counter,
    prefix_key,
    verify_workload_prefix,
)


class RecordingCursor:
    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.executed_many: list[tuple[str, list[tuple]]] = []
        self.fail_on_call = fail_on_call

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def executemany(self, sql, values) -> None:
        self.executed_many.append((sql, list(values)))
        if len(self.executed_many) == self.fail_on_call:
            raise RuntimeError("simulated doc_id conflict")


class RecordingConnection:
    def __init__(self, *, fail_on_call: int | None = None) -> None:
        self.recording_cursor = RecordingCursor(fail_on_call=fail_on_call)
        self.commit_count = 0

    def cursor(self) -> RecordingCursor:
        return self.recording_cursor

    def commit(self) -> None:
        self.commit_count += 1


class ImportAiCompleteWorkloadTests(unittest.TestCase):
    def test_controlled_prefix_materialization_is_exact_and_nested(self) -> None:
        rows = [
            WorkloadRow(
                doc_id=index,
                tenant_id=0,
                category="short_chatgpt",
                text=f"original prompt {index}",
                workload_name="base",
                prompt_tokens=3,
                target_output_tokens=8,
                arrival_time_s=float(index),
                session_id=f"session-{index}",
                prefix_key=f"original-{index}",
            )
            for index in range(10)
        ]

        thirty = apply_controlled_prefix(
            rows,
            ratio=0.3,
            common_prefix="shared instruction",
            workload_name="prefix_30",
            start_doc_id=100,
            prompt_token_counter=lambda text: len(text.split()),
            max_prompt_tokens=100,
        )
        seventy = apply_controlled_prefix(
            rows,
            ratio=0.7,
            common_prefix="shared instruction",
            workload_name="prefix_70",
            start_doc_id=200,
            prompt_token_counter=lambda text: len(text.split()),
            max_prompt_tokens=100,
        )
        selected_30 = {
            row.session_id for row in thirty if row.prefix_key.startswith("controlled:")
        }
        selected_70 = {
            row.session_id for row in seventy if row.prefix_key.startswith("controlled:")
        }

        self.assertEqual(len(selected_30), 3)
        self.assertEqual(len(selected_70), 7)
        self.assertLessEqual(selected_30, selected_70)
        self.assertEqual([row.doc_id for row in thirty], list(range(100, 110)))
        for original, transformed in zip(rows, thirty):
            self.assertTrue(transformed.text.endswith(original.text))
            self.assertEqual(transformed.session_id, original.session_id)
            self.assertEqual(transformed.workload_name, "prefix_30")

    def test_controlled_prefix_rejects_invalid_ratio_and_overflow(self) -> None:
        row = WorkloadRow(
            doc_id=1,
            tenant_id=0,
            category="short_chatgpt",
            text="original",
            workload_name="base",
            prompt_tokens=1,
            target_output_tokens=8,
            arrival_time_s=0.0,
            session_id="session",
            prefix_key="original",
        )

        with self.assertRaisesRegex(ValueError, "ratio"):
            apply_controlled_prefix(
                [row],
                ratio=1.1,
                common_prefix="shared",
                workload_name="invalid",
                start_doc_id=0,
                prompt_token_counter=lambda text: 1,
                max_prompt_tokens=10,
            )
        with self.assertRaisesRegex(ValueError, "max_prompt_tokens"):
            apply_controlled_prefix(
                [row],
                ratio=1.0,
                common_prefix="shared",
                workload_name="overflow",
                start_doc_id=0,
                prompt_token_counter=lambda text: 11,
                max_prompt_tokens=10,
            )
        with self.assertRaisesRegex(ValueError, "source row"):
            apply_controlled_prefix(
                [replace(row, prompt_tokens=11)],
                ratio=0.0,
                common_prefix="shared",
                workload_name="overflow",
                start_doc_id=0,
                prompt_token_counter=lambda text: 1,
                max_prompt_tokens=10,
            )

    def test_http_token_counter_uses_vllm_tokenize_response(self) -> None:
        captured = {}

        class Response:
            def __enter__(self):
                return BytesIO(b'{"count": 7, "max_model_len": 2048}')

            def __exit__(self, exc_type, exc, traceback):
                return False

        def open_request(request, timeout):
            captured["url"] = request.full_url
            captured["body"] = request.data
            captured["timeout"] = timeout
            return Response()

        counter = load_http_prompt_token_counter(
            "http://localhost:8000/tokenize",
            "qwen2.5-1.5b",
            open_request=open_request,
        )

        self.assertEqual(counter("hello"), 7)
        self.assertEqual(captured["url"], "http://localhost:8000/tokenize")
        self.assertEqual(captured["timeout"], 10.0)
        self.assertIn(b'"model": "qwen2.5-1.5b"', captured["body"])
        self.assertIn(b'"prompt": "hello"', captured["body"])

    def test_http_token_counter_rejects_missing_count(self) -> None:
        class Response:
            def __enter__(self):
                return BytesIO(b'{"max_model_len": 2048}')

            def __exit__(self, exc_type, exc, traceback):
                return False

        counter = load_http_prompt_token_counter(
            "http://localhost:8000/tokenize",
            "qwen2.5-1.5b",
            open_request=lambda request, timeout: Response(),
        )

        with self.assertRaisesRegex(ValueError, "count"):
            counter("hello")

    def test_first_human_prompt_extracts_and_normalizes_text(self) -> None:
        record = {
            "conversations": [
                {"from": "system", "value": "ignored"},
                {"from": "human", "value": " hello\n\nworld  "},
                {"from": "gpt", "value": "answer"},
            ]
        }

        self.assertEqual(first_human_prompt(record), "hello world")

    def test_length_bucket_boundaries(self) -> None:
        self.assertEqual(length_bucket(511), "short")
        self.assertEqual(length_bucket(512), "medium")
        self.assertEqual(length_bucket(1535), "medium")
        self.assertEqual(length_bucket(1536), "long")

    def test_category_for_normalizes_model_name(self) -> None:
        trace = BurstTraceRow(
            timestamp_s=1.0,
            model="GPT-4",
            request_tokens=1600,
            response_tokens=100,
            total_tokens=1700,
            log_type="Conversation log",
        )

        self.assertEqual(category_for(trace), "long_gpt_4")

    def test_build_workload_rows_pairs_prompts_and_trace_metadata(self) -> None:
        prompts = [("conv-a", "Summarize this customer ticket.")]
        traces = [
            BurstTraceRow(
                timestamp_s=45.0,
                model="ChatGPT",
                request_tokens=472,
                response_tokens=18,
                total_tokens=490,
                log_type="Conversation log",
            )
        ]

        rows = build_workload_rows(prompts, traces, "sharegpt_burstgpt", start_doc_id=10, max_rows=1)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].doc_id, 10)
        self.assertEqual(rows[0].category, "short_chatgpt")
        self.assertEqual(rows[0].prompt_tokens, 472)
        self.assertEqual(rows[0].target_output_tokens, 18)
        self.assertEqual(rows[0].arrival_time_s, 45.0)
        self.assertEqual(rows[0].session_id, "conv-a")
        self.assertEqual(rows[0].prefix_key, prefix_key("Summarize this customer ticket."))

    def test_build_workload_rows_filters_by_model_tokenizer_length(self) -> None:
        prompts = [("too-long", "long prompt"), ("fits", "short prompt")]
        traces = [
            BurstTraceRow(
                timestamp_s=1.0,
                model="ChatGPT",
                request_tokens=10,
                response_tokens=10,
                total_tokens=20,
                log_type="Conversation log",
            ),
            BurstTraceRow(
                timestamp_s=2.0,
                model="ChatGPT",
                request_tokens=10,
                response_tokens=10,
                total_tokens=20,
                log_type="Conversation log",
            ),
        ]

        rows = build_workload_rows(
            prompts,
            traces,
            "sharegpt_burstgpt",
            start_doc_id=100,
            max_rows=2,
            prompt_token_counter=lambda text: 2040 if text == "long prompt" else 20,
            max_model_len=2048,
            completion_max_tokens=16,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].doc_id, 100)
        self.assertEqual(rows[0].session_id, "fits")
        self.assertEqual(rows[0].prompt_tokens, 20)
        self.assertEqual(rows[0].category, "short_chatgpt")

    def test_build_workload_rows_applies_explicit_prompt_token_limit(self) -> None:
        prompts = [("too-long", "long prompt"), ("fits", "short prompt")]
        traces = [
            BurstTraceRow(
                timestamp_s=float(index),
                model="ChatGPT",
                request_tokens=10,
                response_tokens=20,
                total_tokens=30,
                log_type="Conversation log",
            )
            for index in range(2)
        ]

        rows = build_workload_rows(
            prompts,
            traces,
            "heldout",
            start_doc_id=0,
            max_rows=2,
            prompt_token_counter=lambda text: (
                1501 if text == "long prompt" else 1500
            ),
            max_prompt_tokens=1500,
            max_model_len=8192,
            completion_max_tokens=256,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].session_id, "fits")
        self.assertEqual(rows[0].prompt_tokens, 1500)

    def test_build_workload_rows_uses_disjoint_source_offset(self) -> None:
        prompts = [
            ("session-0", "prompt zero"),
            ("session-1", "prompt one"),
            ("session-2", "prompt two"),
        ]
        traces = [
            BurstTraceRow(
                timestamp_s=float(index),
                model="ChatGPT",
                request_tokens=10 + index,
                response_tokens=20 + index,
                total_tokens=30 + 2 * index,
                log_type="Conversation log",
            )
            for index in range(3)
        ]

        rows = build_workload_rows(
            prompts,
            traces,
            "heldout",
            start_doc_id=2048,
            max_rows=1,
            prompt_token_counter=(
                lambda text: 999 if text == "prompt zero" else 12
            ),
            max_model_len=64,
            completion_max_tokens=16,
            source_row_offset=1,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].doc_id, 2048)
        self.assertEqual(rows[0].session_id, "session-2")
        self.assertEqual(rows[0].text, "prompt two")
        self.assertEqual(rows[0].prompt_tokens, 12)
        self.assertEqual(rows[0].target_output_tokens, 22)

    def test_build_workload_rows_rejects_invalid_source_offset(self) -> None:
        prompts = [("session-0", "prompt zero")]
        traces = [
            BurstTraceRow(
                timestamp_s=0.0,
                model="ChatGPT",
                request_tokens=10,
                response_tokens=20,
                total_tokens=30,
                log_type="Conversation log",
            )
        ]

        with self.assertRaisesRegex(ValueError, "source_row_offset"):
            build_workload_rows(
                prompts,
                traces,
                "heldout",
                start_doc_id=2048,
                max_rows=1,
                source_row_offset=-1,
            )
        with self.assertRaisesRegex(ValueError, "max_prompt_tokens"):
            build_workload_rows(
                prompts,
                traces,
                "heldout",
                start_doc_id=2048,
                max_rows=1,
                max_prompt_tokens=0,
            )
        with self.assertRaisesRegex(ValueError, "No workload rows"):
            build_workload_rows(
                prompts,
                traces,
                "heldout",
                start_doc_id=2048,
                max_rows=1,
                source_row_offset=1,
            )

    def test_verify_workload_prefix_accepts_exact_rows_and_rejects_mismatch(self) -> None:
        expected = WorkloadRow(
            doc_id=0,
            tenant_id=1,
            category="short_chatgpt",
            text="prompt zero",
            workload_name="heldout",
            prompt_tokens=12,
            target_output_tokens=20,
            arrival_time_s=1.0,
            session_id="session-0",
            prefix_key="prefix-0",
        )

        verify_workload_prefix([expected], [expected])

        with self.assertRaisesRegex(ValueError, "prompt_tokens"):
            verify_workload_prefix(
                [replace(expected, prompt_tokens=13)],
                [expected],
            )
        with self.assertRaisesRegex(ValueError, "returned 0 rows; expected 1"):
            verify_workload_prefix([], [expected])

    def test_insert_rows_append_only_never_updates_conflicting_doc_ids(self) -> None:
        row = WorkloadRow(
            doc_id=2048,
            tenant_id=1,
            category="short_chatgpt",
            text="held-out prompt",
            workload_name="heldout",
            prompt_tokens=12,
            target_output_tokens=20,
            arrival_time_s=1.0,
            session_id="session-2048",
            prefix_key="prefix-2048",
        )
        conn = RecordingConnection()

        insert_rows(conn, [row], batch_rows=100, append_only=True)

        sql, values = conn.recording_cursor.executed_many[0]
        self.assertNotIn("ON CONFLICT", sql)
        self.assertEqual(values[0][0], 2048)
        self.assertEqual(conn.commit_count, 1)

    def test_insert_rows_append_only_does_not_commit_after_later_batch_failure(
        self,
    ) -> None:
        row = WorkloadRow(
            doc_id=2048,
            tenant_id=1,
            category="short_chatgpt",
            text="held-out prompt",
            workload_name="heldout",
            prompt_tokens=12,
            target_output_tokens=20,
            arrival_time_s=1.0,
            session_id="session-2048",
            prefix_key="prefix-2048",
        )
        conn = RecordingConnection(fail_on_call=2)

        with self.assertRaisesRegex(RuntimeError, "doc_id conflict"):
            insert_rows(
                conn,
                [row, replace(row, doc_id=2049)],
                batch_rows=1,
                append_only=True,
            )

        self.assertEqual(len(conn.recording_cursor.executed_many), 2)
        self.assertEqual(conn.commit_count, 0)

    def test_burstgpt_rows_from_dicts_filters_zero_token_rows(self) -> None:
        rows = burstgpt_rows_from_dicts(
            [
                {
                    "Timestamp": "1",
                    "Model": "ChatGPT",
                    "Request tokens": "0",
                    "Response tokens": "10",
                    "Total tokens": "10",
                    "Log Type": "Conversation log",
                },
                {
                    "Timestamp": "2",
                    "Model": "ChatGPT",
                    "Request tokens": "10",
                    "Response tokens": "0",
                    "Total tokens": "10",
                    "Log Type": "Conversation log",
                },
                {
                    "Timestamp": "3",
                    "Model": "ChatGPT",
                    "Request tokens": "11",
                    "Response tokens": "12",
                    "Total tokens": "23",
                    "Log Type": "Conversation log",
                },
            ],
            max_request_tokens=100,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].request_tokens, 11)
        self.assertEqual(rows[0].response_tokens, 12)


if __name__ == "__main__":
    unittest.main()
