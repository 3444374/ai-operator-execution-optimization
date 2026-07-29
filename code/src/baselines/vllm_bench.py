"""Official vLLM benchmark dataset and command construction."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .contracts import ChatRequest


@dataclass(frozen=True)
class VllmBenchConfig:
    python_executable: str
    base_url: str
    model: str
    tokenizer: str
    dataset_path: Path
    result_dir: Path
    result_filename: str
    num_prompts: int
    max_concurrency: int


def write_vllm_custom_dataset(
    path: str | Path,
    requests: Iterable[ChatRequest],
) -> None:
    """Write the exact custom dataset consumed by `vllm bench serve`."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(
            {
                "prompt": request.prompt,
                "output_tokens": request.max_output_tokens,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for request in requests
    ]
    payload = "\n".join(lines) + ("\n" if lines else "")
    with destination.open("x", encoding="utf-8", newline="\n") as stream:
        stream.write(payload)


def build_vllm_bench_command(
    config: VllmBenchConfig,
) -> list[str]:
    """Build a deterministic official vLLM Chat benchmark command."""

    if config.num_prompts <= 0:
        raise ValueError("num_prompts must be positive")
    if config.max_concurrency <= 0:
        raise ValueError("max_concurrency must be positive")
    return [
        config.python_executable,
        "-m",
        "vllm.entrypoints.cli.main",
        "bench",
        "serve",
        "--backend",
        "openai-chat",
        "--base-url",
        config.base_url.rstrip("/"),
        "--endpoint",
        "/v1/chat/completions",
        "--model",
        config.model,
        "--tokenizer",
        config.tokenizer,
        "--dataset-name",
        "custom",
        "--dataset-path",
        str(config.dataset_path),
        "--custom-output-len",
        "-1",
        "--disable-shuffle",
        "--request-rate",
        "inf",
        "--temperature",
        "0",
        "--num-prompts",
        str(config.num_prompts),
        "--max-concurrency",
        str(config.max_concurrency),
        "--save-result",
        "--save-detailed",
        "--result-dir",
        str(config.result_dir),
        "--result-filename",
        config.result_filename,
    ]
