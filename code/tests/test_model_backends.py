from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.model_backends import (
    fake_complete_batch,
    fake_embed_batch,
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
