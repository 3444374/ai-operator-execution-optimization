from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.serving.backends import (
    CompatibleAsyncHTTPCompletionActor,
    FakeCompletionActor,
    call_compatible_completion_endpoint,
    fake_complete_batch,
    fake_embed_batch,
    format_completion_prompts,
    model_request_wall_time,
    normalize_completion_backend,
    normalize_embedding_backend,
    ollama_generate_url,
)


def sample_table() -> pa.Table:
    return pa.table(
        {
            "doc_id": [1, 2],
            "tenant_id": [10, 10],
            "category": ["cat_a", "cat_b"],
            "text": ["hello world", "another document"],
        }
    )


class ModelBackendTests(unittest.TestCase):
    def test_chatml_prompt_format_preserves_request_content(self) -> None:
        prompts = format_completion_prompts(
            ["first request", "second request"],
            "chatml",
        )

        self.assertEqual(
            prompts,
            [
                (
                    "<|im_start|>user\nfirst request<|im_end|>\n"
                    "<|im_start|>assistant\n"
                ),
                (
                    "<|im_start|>user\nsecond request<|im_end|>\n"
                    "<|im_start|>assistant\n"
                ),
            ],
        )
        self.assertEqual(
            format_completion_prompts(["unchanged"], "raw"),
            ["unchanged"],
        )

    def test_completion_endpoint_can_return_exact_per_request_tokens(
        self,
    ) -> None:
        response_body = json.dumps(
            {
                "choices": [
                    {
                        "index": 1,
                        "text": "second",
                        "finish_reason": "length",
                        "token_ids": [21, 22],
                    },
                    {
                        "index": 0,
                        "text": "first",
                        "finish_reason": "stop",
                        "token_ids": [11],
                    },
                ],
                "usage": {"total_tokens": 9},
            }
        ).encode("utf-8")

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def read(self):
                return response_body

        with patch(
            "src.serving.backends.completion.request.urlopen",
            return_value=Response(),
        ) as urlopen:
            result = call_compatible_completion_endpoint(
                "http://localhost/v1/completions",
                "model",
                ["p0", "p1"],
                None,
                1.0,
                8,
                return_token_ids=True,
                temperature=0.0,
            )

        sent = json.loads(urlopen.call_args.args[0].data.decode("utf-8"))
        self.assertTrue(sent["return_token_ids"])
        self.assertEqual(sent["temperature"], 0.0)
        self.assertEqual(result.outputs, ["first", "second"])
        self.assertEqual(result.total_tokens, 9)
        self.assertEqual(result.output_token_counts, [1, 2])
        self.assertEqual(result.finish_reasons, ["stop", "length"])

    def test_completion_endpoint_records_non_streaming_http_boundaries(
        self,
    ) -> None:
        response_body = json.dumps(
            {
                "choices": [
                    {
                        "index": 0,
                        "message": {"content": "answer"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"total_tokens": 6},
            }
        ).encode("utf-8")

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def read(self):
                return response_body

        with (
            patch(
                "src.serving.backends.completion.request.urlopen",
                return_value=Response(),
            ),
            patch(
                "src.serving.backends.completion.time.time",
                side_effect=[100.0, 101.25, 101.5],
            ),
            patch(
                "src.serving.backends.completion.time.perf_counter",
                side_effect=[10.0, 11.0, 11.2],
            ),
        ):
            result = call_compatible_completion_endpoint(
                "http://localhost/v1/chat/completions",
                "model",
                ["question"],
                None,
                5.0,
                8,
                protocol="chat_completions",
            )

        self.assertEqual(result.http_request_start_epoch_s, 100.0)
        self.assertEqual(result.http_response_headers_epoch_s, 101.25)
        self.assertEqual(result.http_response_body_epoch_s, 101.5)
        self.assertAlmostEqual(result.http_headers_wait_s, 1.0)
        self.assertAlmostEqual(result.http_body_read_s, 0.2)

    def test_fake_actor_ready_is_side_effect_free(self) -> None:
        worker = FakeCompletionActor()

        first = worker.ready()
        second = worker.ready()

        self.assertEqual(first["actor_worker_pid"], os.getpid())
        self.assertEqual(second, first)
        self.assertEqual(first["actor_type"], "FakeCompletionActor")

    def test_completion_endpoint_omits_vllm_extension_by_default(
        self,
    ) -> None:
        response_body = json.dumps(
            {
                "choices": [
                    {
                        "index": 0,
                        "text": "answer",
                        "finish_reason": "stop",
                    }
                ],
                "usage": {},
            }
        ).encode("utf-8")

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def read(self):
                return response_body

        with patch(
            "src.serving.backends.completion.request.urlopen",
            return_value=Response(),
        ) as urlopen:
            result = call_compatible_completion_endpoint(
                "http://localhost/v1/completions",
                "model",
                ["prompt"],
                None,
                1.0,
                8,
            )

        sent = json.loads(urlopen.call_args.args[0].data.decode("utf-8"))
        self.assertNotIn("return_token_ids", sent)
        self.assertEqual(result.output_token_counts, [None])
        self.assertEqual(result.finish_reasons, ["stop"])

    def test_chat_completion_endpoint_sends_one_message_per_prompt(
        self,
    ) -> None:
        response_body = json.dumps(
            {
                "choices": [
                    {
                        "index": 0,
                        "message": {"content": "answer"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 4,
                    "completion_tokens": 2,
                    "total_tokens": 6,
                },
            }
        ).encode("utf-8")

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def read(self):
                return response_body

        with patch(
            "src.serving.backends.completion.request.urlopen",
            return_value=Response(),
        ) as urlopen:
            result = call_compatible_completion_endpoint(
                "http://localhost/v1/chat/completions",
                "model",
                ["question"],
                None,
                1.0,
                8,
                protocol="chat_completions",
                temperature=0.0,
            )

        sent = json.loads(urlopen.call_args.args[0].data.decode("utf-8"))
        self.assertEqual(
            sent["messages"],
            [{"role": "user", "content": "question"}],
        )
        self.assertNotIn("prompt", sent)
        self.assertEqual(result.outputs, ["answer"])

    def test_fake_embedding_batch_returns_expected_shape(self) -> None:
        result = fake_embed_batch(sample_table(), embedding_dim=4, service_tokens_per_s=1_000_000.0)

        self.assertEqual(result["doc_id"], [1, 2])
        self.assertEqual(result["rows"], 2)
        self.assertEqual(result["embedding"].shape, (2, 4))
        self.assertGreater(result["token_count"], 0)
        self.assertEqual(result["actor_worker_pid"], os.getpid())

    def test_model_request_wall_time_uses_epoch_bounds(self) -> None:
        wall_s = model_request_wall_time(
            [
                {"service_start_epoch_s": 10.0, "service_end_epoch_s": 11.5},
                {"service_start_epoch_s": 10.5, "service_end_epoch_s": 12.0},
            ]
        )

        self.assertEqual(wall_s, 2.0)

    def test_http_openai_alias_maps_to_compatible_http(self) -> None:
        self.assertEqual(normalize_embedding_backend("http_openai"), "compatible_http")
        self.assertEqual(normalize_embedding_backend("compatible_http"), "compatible_http")

    def test_fake_completion_batch_returns_text_outputs(self) -> None:
        result = fake_complete_batch(sample_table(), output_tokens_per_row=3, service_tokens_per_s=1_000_000.0)

        self.assertEqual(result["doc_id"], [1, 2])
        self.assertEqual(result["rows"], 2)
        self.assertEqual(len(result["output_text"]), 2)
        self.assertEqual(result["output_token_count"], 6)
        self.assertGreater(result["input_token_count"], 0)
        self.assertEqual(result["actor_worker_pid"], os.getpid())

    def test_completion_backend_alias_maps_to_compatible_http(self) -> None:
        self.assertEqual(normalize_completion_backend("http_openai"), "compatible_http")
        self.assertEqual(normalize_completion_backend("compatible_http"), "compatible_http")

    def test_completion_backend_accepts_ollama(self) -> None:
        self.assertEqual(normalize_completion_backend("ollama"), "ollama")

    def test_ollama_generate_url_accepts_base_or_generate_path(self) -> None:
        self.assertEqual(ollama_generate_url("http://localhost:11434"), "http://localhost:11434/api/generate")
        self.assertEqual(
            ollama_generate_url("http://localhost:11434/api/generate"),
            "http://localhost:11434/api/generate",
        )


class AsyncModelBackendTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_actor_reuses_one_bounded_http_client(self) -> None:
        response_body = json.dumps(
            {
                "choices": [
                    {
                        "index": 0,
                        "text": "first",
                        "finish_reason": "stop",
                    },
                    {
                        "index": 1,
                        "text": "second",
                        "finish_reason": "length",
                    },
                ],
                "usage": {"total_tokens": 9},
            }
        ).encode("utf-8")

        class FakeLimits:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        class FakeResponse:
            def raise_for_status(self):
                return None

            async def aread(self):
                return response_body

        class FakeStream:
            async def __aenter__(self):
                return FakeResponse()

            async def __aexit__(self, *_args):
                return None

        class FakeClient:
            instances = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.calls = []
                self.is_closed = False
                self.__class__.instances.append(self)

            def stream(self, method, url, **kwargs):
                self.calls.append((method, url, kwargs))
                return FakeStream()

            async def aclose(self):
                self.is_closed = True

        fake_httpx = SimpleNamespace(
            AsyncClient=FakeClient,
            Limits=FakeLimits,
            HTTPError=RuntimeError,
        )
        with patch.dict(sys.modules, {"httpx": fake_httpx}):
            actor = CompatibleAsyncHTTPCompletionActor(
                "http://localhost/v1/completions",
                "model",
                None,
                10.0,
                8,
                max_connections=4,
            )

        readiness = await actor.ready()
        first = await actor.complete(sample_table())
        second = await actor.complete(sample_table())

        self.assertEqual(len(FakeClient.instances), 1)
        client = FakeClient.instances[0]
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(
            client.kwargs["limits"].kwargs,
            {
                "max_connections": 4,
                "max_keepalive_connections": 4,
            },
        )
        self.assertEqual(readiness["http_transport"], "httpx_async")
        self.assertTrue(readiness["client_initialized"])
        self.assertEqual(first["output_text"], ["first", "second"])
        self.assertEqual(second["token_count"], 9)

        await actor.close()
        self.assertTrue(client.is_closed)

    async def test_async_actor_batches_ray_calls_without_batching_chat_rows(
        self,
    ) -> None:
        response_body = json.dumps(
            {
                "choices": [
                    {
                        "index": 0,
                        "message": {"content": "answer"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"total_tokens": 6},
            }
        ).encode("utf-8")

        class FakeResponse:
            def raise_for_status(self):
                return None

            async def aread(self):
                return response_body

        class FakeStream:
            async def __aenter__(self):
                return FakeResponse()

            async def __aexit__(self, *_args):
                return None

        class FakeClient:
            def __init__(self, **_kwargs):
                self.calls = []
                self.is_closed = False

            def stream(self, method, url, **kwargs):
                self.calls.append((method, url, kwargs))
                return FakeStream()

            async def aclose(self):
                self.is_closed = True

        fake_httpx = SimpleNamespace(
            AsyncClient=FakeClient,
            Limits=lambda **kwargs: kwargs,
            HTTPError=RuntimeError,
        )
        with patch.dict(sys.modules, {"httpx": fake_httpx}):
            actor = CompatibleAsyncHTTPCompletionActor(
                "http://localhost/v1/chat/completions",
                "model",
                None,
                10.0,
                8,
                protocol="chat_completions",
                max_connections=4,
            )

        result = await actor.complete(sample_table())

        self.assertEqual(len(actor._client.calls), 2)
        self.assertEqual(result["rows"], 2)
        self.assertEqual(result["output_text"], ["answer", "answer"])
        self.assertEqual(result["token_count"], 12)
        for _, _, kwargs in actor._client.calls:
            self.assertEqual(len(kwargs["json"]["messages"]), 1)
            self.assertNotIn("prompt", kwargs["json"])


if __name__ == "__main__":
    unittest.main()
