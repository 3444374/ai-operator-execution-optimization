from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.model_backends import (
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
            "src.model_backends.request.urlopen",
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
            "src.model_backends.request.urlopen",
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

    def test_fake_embedding_batch_returns_expected_shape(self) -> None:
        result = fake_embed_batch(sample_table(), embedding_dim=4, service_tokens_per_s=1_000_000.0)

        self.assertEqual(result["doc_id"], [1, 2])
        self.assertEqual(result["rows"], 2)
        self.assertEqual(result["embedding"].shape, (2, 4))
        self.assertGreater(result["token_count"], 0)

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


if __name__ == "__main__":
    unittest.main()
