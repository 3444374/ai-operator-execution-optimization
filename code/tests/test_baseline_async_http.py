from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.async_http import BoundedHttpConfig, run_bounded_http
from src.baselines.contracts import ChatRequest


def sample_request(
    doc_id: int,
    *,
    endpoint_index: int,
) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=f"question-{doc_id}",
        arrival_time_s=0.0,
        prompt_tokens=4,
        max_output_tokens=8,
        estimated_output_tokens=8,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=endpoint_index,
    )


class BoundedHttpBaselineTests(unittest.TestCase):
    def test_concurrency_is_bounded_per_endpoint(self) -> None:
        active = 0
        peak = 0
        payloads: list[dict] = []

        async def fake_transport(url: str, payload: dict) -> dict:
            nonlocal active, peak
            self.assertEqual(
                url,
                "http://ep0/v1/chat/completions",
            )
            payloads.append(payload)
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            return {
                "choices": [
                    {
                        "message": {"content": "ok"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 4,
                    "completion_tokens": 1,
                },
            }

        results = asyncio.run(
            run_bounded_http(
                tuple(
                    sample_request(i, endpoint_index=0)
                    for i in range(8)
                ),
                BoundedHttpConfig(
                    endpoint_urls=(
                        "http://ep0/v1/chat/completions",
                    ),
                    model="model",
                    concurrency_per_endpoint=2,
                    timeout_s=30,
                    api_key=None,
                ),
                transport=fake_transport,
            )
        )

        self.assertEqual(peak, 2)
        self.assertEqual(
            {row.status for row in results},
            {"completed"},
        )
        self.assertEqual(len(payloads), 8)
        self.assertTrue(
            all(
                payload["messages"]
                == [{"role": "user", "content": f"question-{index}"}]
                for index, payload in enumerate(payloads)
            )
        )
        self.assertTrue(
            all("prompt" not in payload for payload in payloads)
        )

    def test_transport_failure_is_preserved_as_failed_result(self) -> None:
        async def failing_transport(_url: str, _payload: dict) -> dict:
            raise RuntimeError("endpoint unavailable")

        results = asyncio.run(
            run_bounded_http(
                (sample_request(1, endpoint_index=0),),
                BoundedHttpConfig(
                    endpoint_urls=(
                        "http://ep0/v1/chat/completions",
                    ),
                    model="model",
                    concurrency_per_endpoint=1,
                    timeout_s=30,
                    api_key=None,
                ),
                transport=failing_transport,
            )
        )

        self.assertEqual(results[0].status, "failed")
        self.assertIn("endpoint unavailable", results[0].error or "")


if __name__ == "__main__":
    unittest.main()
