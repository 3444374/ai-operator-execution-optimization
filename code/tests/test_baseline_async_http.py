from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


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
    def test_default_transport_scales_httpx_pool_to_requested_concurrency(
        self,
    ) -> None:
        captured: dict[str, object] = {}

        class FakeLimits:
            def __init__(
                self,
                *,
                max_connections: int,
                max_keepalive_connections: int,
            ) -> None:
                captured["limits_instance"] = self
                captured["max_connections"] = max_connections
                captured["max_keepalive_connections"] = (
                    max_keepalive_connections
                )

        class FakeResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
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

        class FakeClient:
            def __init__(self, **kwargs: object) -> None:
                captured["client_kwargs"] = kwargs

            async def __aenter__(self) -> "FakeClient":
                return self

            async def __aexit__(self, *_args: object) -> None:
                return None

            async def post(
                self,
                _url: str,
                *,
                json: dict[str, object],
            ) -> FakeResponse:
                self.assert_payload(json)
                return FakeResponse()

            @staticmethod
            def assert_payload(payload: dict[str, object]) -> None:
                if payload["model"] != "model":
                    raise AssertionError(payload)

        fake_httpx = SimpleNamespace(
            AsyncClient=FakeClient,
            Limits=FakeLimits,
        )
        with patch.dict(sys.modules, {"httpx": fake_httpx}):
            results = asyncio.run(
                run_bounded_http(
                    (
                        sample_request(0, endpoint_index=0),
                        sample_request(1, endpoint_index=1),
                    ),
                    BoundedHttpConfig(
                        endpoint_urls=(
                            "http://ep0/v1/chat/completions",
                            "http://ep1/v1/chat/completions",
                        ),
                        model="model",
                        concurrency_per_endpoint=128,
                        timeout_s=30,
                        api_key=None,
                    ),
                )
            )

        self.assertEqual(len(results), 2)
        self.assertEqual(captured["max_connections"], 256)
        self.assertEqual(captured["max_keepalive_connections"], 256)
        self.assertIs(
            captured["client_kwargs"]["limits"],
            captured["limits_instance"],
        )

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
                tuple(sample_request(i, endpoint_index=0) for i in range(8)),
                BoundedHttpConfig(
                    endpoint_urls=("http://ep0/v1/chat/completions",),
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
                payload["messages"] == [{"role": "user", "content": f"question-{index}"}]
                for index, payload in enumerate(payloads)
            )
        )
        self.assertTrue(all("prompt" not in payload for payload in payloads))

    def test_transport_failure_is_preserved_as_failed_result(self) -> None:
        async def failing_transport(_url: str, _payload: dict) -> dict:
            raise RuntimeError("endpoint unavailable")

        results = asyncio.run(
            run_bounded_http(
                (sample_request(1, endpoint_index=0),),
                BoundedHttpConfig(
                    endpoint_urls=("http://ep0/v1/chat/completions",),
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

    def test_single_shard_preserves_global_endpoint_index(self) -> None:
        seen_urls: list[str] = []

        async def fake_transport(url: str, _payload: dict) -> dict:
            seen_urls.append(url)
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
                (sample_request(1, endpoint_index=1),),
                BoundedHttpConfig(
                    endpoint_urls=("http://ep1/v1/chat/completions",),
                    endpoint_index_offset=1,
                    model="model",
                    concurrency_per_endpoint=1,
                    timeout_s=30,
                    api_key=None,
                ),
                transport=fake_transport,
            )
        )

        self.assertEqual(
            seen_urls,
            ["http://ep1/v1/chat/completions"],
        )
        self.assertEqual(results[0].endpoint_index, 1)


if __name__ == "__main__":
    unittest.main()
