from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.contracts import ChatRequest
from src.baselines.vllm_bench import (
    VllmBenchConfig,
    build_vllm_bench_command,
    write_vllm_custom_dataset,
)


def sample_request(doc_id: int) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=f"question-{doc_id}",
        arrival_time_s=0.0,
        prompt_tokens=4,
        max_output_tokens=8,
        estimated_output_tokens=8,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=0,
    )


class VllmBenchAdapterTests(unittest.TestCase):
    def test_vllm_bench_uses_custom_chat_dataset_without_shuffle(
        self,
    ) -> None:
        command = build_vllm_bench_command(
            VllmBenchConfig(
                python_executable="/venv/bin/python",
                base_url="http://127.0.0.1:8000",
                model="qwen",
                tokenizer="/models/qwen",
                dataset_path=Path("/tmp/shard0.jsonl"),
                result_dir=Path("/tmp/results"),
                result_filename="ep0.json",
                num_prompts=64,
                max_concurrency=32,
            )
        )

        self.assertEqual(
            command[:5],
            [
                "/venv/bin/python",
                "-m",
                "vllm.entrypoints.cli.main",
                "bench",
                "serve",
            ],
        )
        self.assertIn("openai-chat", command)
        self.assertIn("--dataset-name", command)
        self.assertIn("custom", command)
        self.assertIn("--disable-shuffle", command)
        self.assertIn("--custom-output-len", command)
        self.assertIn("-1", command)
        self.assertIn("--save-detailed", command)
        self.assertEqual(
            command[command.index("--tokenizer") + 1],
            "/models/qwen",
        )
        self.assertEqual(
            command[command.index("--temperature") + 1],
            "0",
        )
        self.assertEqual(
            command[command.index("--endpoint") + 1],
            "/v1/chat/completions",
        )

    def test_custom_dataset_preserves_request_order_and_caps(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "dataset.jsonl"
            write_vllm_custom_dataset(
                path,
                (sample_request(2), sample_request(1)),
            )

            rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]

        self.assertEqual(
            rows,
            [
                {"prompt": "question-2", "output_tokens": 8},
                {"prompt": "question-1", "output_tokens": 8},
            ],
        )


if __name__ == "__main__":
    unittest.main()
