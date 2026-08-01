from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.controls import (  # noqa: E402
    BatchedCompletionsConfig,
    run_batched_completions,
)
from src.baselines.contracts import ChatRequest  # noqa: E402


class BatchedCompletionsTests(unittest.TestCase):
    def test_fixed_rows_preserve_one_result_per_prompt(self) -> None:
        requests = tuple(
            ChatRequest(
                doc_id=index,
                prompt=f"prompt-{index}",
                arrival_time_s=0.0,
                prompt_tokens=4,
                max_output_tokens=8,
                estimated_output_tokens=8,
                source_row_hash=f"row-{index}",
                endpoint_index=index % 2,
            )
            for index in range(6)
        )
        calls = []

        async def transport(url, payload):
            calls.append((url, payload))
            return {
                "choices": [
                    {
                        "index": index,
                        "text": f"answer-{prompt}",
                        "token_ids": [1, 2],
                        "finish_reason": "stop",
                    }
                    for index, prompt in enumerate(payload["prompt"])
                ]
            }

        results = asyncio.run(
            run_batched_completions(
                requests,
                BatchedCompletionsConfig(
                    endpoint_urls=("http://gpu0", "http://gpu1"),
                    model="model",
                    batch_rows=2,
                    concurrency_per_endpoint=2,
                    timeout_s=10.0,
                    api_key=None,
                ),
                transport,
            )
        )

        self.assertEqual([result.doc_id for result in results], list(range(6)))
        self.assertTrue(all(result.status == "completed" for result in results))
        self.assertTrue(all(result.output_tokens == 2 for result in results))
        self.assertEqual(len(calls), 4)
        self.assertTrue(
            all(len(payload["prompt"]) <= 2 for _, payload in calls)
        )
        self.assertTrue(
            all(payload["return_token_ids"] for _, payload in calls)
        )

    def test_mixed_output_caps_fail_before_transport(self) -> None:
        requests = (
            ChatRequest(
                doc_id=0,
                prompt="a",
                arrival_time_s=0.0,
                prompt_tokens=1,
                max_output_tokens=8,
                estimated_output_tokens=8,
                source_row_hash="a",
                endpoint_index=0,
            ),
            ChatRequest(
                doc_id=1,
                prompt="b",
                arrival_time_s=0.0,
                prompt_tokens=1,
                max_output_tokens=16,
                estimated_output_tokens=16,
                source_row_hash="b",
                endpoint_index=0,
            ),
        )

        with self.assertRaisesRegex(ValueError, "one max output cap"):
            asyncio.run(
                run_batched_completions(
                    requests,
                    BatchedCompletionsConfig(
                        endpoint_urls=("http://gpu0",),
                        model="model",
                        batch_rows=2,
                        concurrency_per_endpoint=1,
                        timeout_s=10.0,
                        api_key=None,
                    ),
                    lambda *_args: None,
                )
            )


if __name__ == "__main__":
    unittest.main()
