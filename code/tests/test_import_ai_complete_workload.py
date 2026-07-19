from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = CODE_ROOT / "scripts"
for path in (CODE_ROOT, SCRIPTS_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from import_ai_complete_workload import (  # noqa: E402
    BurstTraceRow,
    build_workload_rows,
    burstgpt_rows_from_dicts,
    category_for,
    first_human_prompt,
    length_bucket,
    prefix_key,
)


class ImportAiCompleteWorkloadTests(unittest.TestCase):
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
