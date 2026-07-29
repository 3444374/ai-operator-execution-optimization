from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.baselines.contracts import ChatRequest
from src.baselines.official_runtime import (
    DaftPromptConfig,
    RayDataHttpConfig,
    daft_prompt_options,
    ray_data_postprocess,
    ray_data_preprocess,
    run_daft_prompt,
    run_ray_data_http,
)


def sample_request(
    doc_id: int,
    *,
    max_output_tokens: int = 128,
) -> ChatRequest:
    return ChatRequest(
        doc_id=doc_id,
        prompt=f"question-{doc_id}",
        arrival_time_s=0.0,
        prompt_tokens=4,
        max_output_tokens=max_output_tokens,
        estimated_output_tokens=max_output_tokens,
        source_row_hash=f"row-{doc_id}",
        endpoint_index=0,
    )


class OfficialRuntimeAdapterTests(unittest.TestCase):
    def test_requirements_include_ray_data_llm_runtime_extras(
        self,
    ) -> None:
        requirements = {
            line.strip().lower()
            for line in (CODE_ROOT / "requirements.txt")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }

        self.assertIn("ray[data,serve]", requirements)

    def test_daft_prompt_options_are_same_request_chat_semantics(
        self,
    ) -> None:
        options = daft_prompt_options(
            model="qwen",
            max_tokens=128,
        )

        self.assertEqual(
            options,
            {
                "model": "qwen",
                "use_chat_completions": True,
                "temperature": 0.0,
                "max_tokens": 128,
                "max_retries": 0,
                "on_error": "raise",
            },
        )

    def test_ray_data_preprocess_emits_one_chat_request_per_row(
        self,
    ) -> None:
        row = ray_data_preprocess(
            {"doc_id": 7, "prompt": "question"},
            model="qwen",
            max_tokens=128,
        )

        self.assertEqual(
            row["payload"]["messages"],
            [{"role": "user", "content": "question"}],
        )
        self.assertEqual(row["payload"]["temperature"], 0.0)
        self.assertNotIn("prompt", row["payload"])

    def test_ray_data_postprocess_preserves_usage_and_identity(
        self,
    ) -> None:
        row = ray_data_postprocess(
            {
                "doc_id": 7,
                "endpoint_index": 0,
                "http_response": {
                    "choices": [
                        {
                            "message": {"content": "answer"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 4,
                        "completion_tokens": 2,
                    },
                },
            }
        )

        self.assertEqual(row["doc_id"], 7)
        self.assertEqual(row["output_text"], "answer")
        self.assertEqual(row["input_tokens"], 4)
        self.assertEqual(row["output_tokens"], 2)

    def test_daft_adapter_rejects_mixed_output_caps_before_import(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "same max_output_tokens",
        ):
            run_daft_prompt(
                (
                    sample_request(1, max_output_tokens=64),
                    sample_request(2, max_output_tokens=128),
                ),
                DaftPromptConfig(
                    runner="native",
                    base_url="http://127.0.0.1:8000/v1",
                    api_key=None,
                    model="qwen",
                    max_tokens=128,
                ),
            )

    def test_daft_adapter_uses_injected_official_surface(self) -> None:
        calls: dict[str, object] = {}

        class FakeCollected:
            def to_pylist(self):
                return [{"doc_id": 1, "output_text": "answer"}]

        class FakeFrame:
            def with_column(self, name, expression):
                calls["with_column"] = (name, expression)
                return self

            def collect(self):
                return FakeCollected()

        class FakeDaft:
            def set_runner_native(self):
                calls["runner"] = "native"

            def from_pydict(self, data):
                calls["data"] = data
                return FakeFrame()

            def col(self, name):
                return f"col:{name}"

        class FakeProvider:
            def __init__(self, **options):
                calls["provider"] = options

        def fake_prompt(expression, **options):
            calls["prompt"] = (expression, options)
            return "prompt-expression"

        results = run_daft_prompt(
            (sample_request(1),),
            DaftPromptConfig(
                runner="native",
                base_url="http://127.0.0.1:8000/v1",
                api_key=None,
                model="qwen",
                max_tokens=128,
            ),
            modules=SimpleNamespace(
                daft=FakeDaft(),
                prompt=fake_prompt,
                provider_class=FakeProvider,
            ),
        )

        self.assertEqual(calls["runner"], "native")
        self.assertEqual(
            calls["provider"],
            {
                "base_url": "http://127.0.0.1:8000/v1",
                "api_key": "not-needed",
            },
        )
        self.assertEqual(results[0].output_text, "answer")

    def test_ray_data_adapter_builds_no_retry_processor(self) -> None:
        calls: dict[str, object] = {}

        class FakeConfig:
            def __init__(self, **options):
                calls["config"] = options

        class FakeDataset:
            def __init__(self, rows):
                self.rows = rows

            def take_all(self):
                return self.rows

        class FakeData:
            def from_items(self, rows):
                return FakeDataset(rows)

        class FakeRay:
            data = FakeData()

            def init(self, **options):
                calls["ray_init"] = options

        def build_processor(_config, *, preprocess, postprocess):
            def processor(dataset):
                rows = []
                for item in dataset.rows:
                    prepared = preprocess(item)
                    prepared["http_response"] = {
                        "choices": [
                            {
                                "message": {"content": "answer"},
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {
                            "prompt_tokens": 4,
                            "completion_tokens": 2,
                        },
                    }
                    rows.append(postprocess(prepared))
                return FakeDataset(rows)

            return processor

        results = run_ray_data_http(
            (sample_request(1),),
            RayDataHttpConfig(
                endpoint_url=(
                    "http://127.0.0.1:8000/v1/chat/completions"
                ),
                api_key=None,
                model="qwen",
                max_tokens=128,
                batch_size=16,
                concurrency=4,
                ray_address="127.0.0.1:6380",
            ),
            modules=SimpleNamespace(
                ray=FakeRay(),
                config_class=FakeConfig,
                build_processor=build_processor,
            ),
        )

        self.assertEqual(
            calls["ray_init"],
            {
                "address": "127.0.0.1:6380",
                "ignore_reinit_error": True,
                "runtime_env": {
                    "env_vars": {
                        "PYTHONPATH": str(CODE_ROOT),
                    },
                },
            },
        )
        self.assertEqual(calls["config"]["max_retries"], 0)
        self.assertEqual(calls["config"]["batch_size"], 16)
        self.assertEqual(calls["config"]["concurrency"], 4)
        self.assertEqual(results[0].output_tokens, 2)


if __name__ == "__main__":
    unittest.main()
