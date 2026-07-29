"""Official vLLM benchmark dataset and command construction."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

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
        "--skip-chat-template",
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


def extract_vllm_bench_request_timings(
    raw: Mapping[str, Any],
    expected_count: int,
) -> tuple[tuple[float, ...], tuple[float, ...]]:
    """Return relative submission times and E2E latencies from detailed JSON."""

    if expected_count <= 0:
        raise ValueError("expected_count must be positive")
    completed = raw.get("completed")
    failed = raw.get("failed")
    errors = raw.get("errors")
    if completed is not None and int(completed) != expected_count:
        raise ValueError("vLLM detailed result completed count mismatch")
    if failed is not None and int(failed) != 0:
        raise ValueError("vLLM detailed result contains failed requests")
    if errors is not None:
        if not isinstance(errors, list) or len(errors) != expected_count:
            raise ValueError("vLLM detailed result errors length mismatch")
        if any(str(error).strip() for error in errors):
            raise ValueError("vLLM detailed result contains request errors")

    direct_latencies = raw.get("request_latencies") or raw.get("e2els") or raw.get("e2e_latencies")
    if isinstance(direct_latencies, list):
        latencies = tuple(float(value) for value in direct_latencies)
    else:
        ttfts = raw.get("ttfts")
        itls = raw.get("itls")
        if not (
            isinstance(ttfts, list)
            and isinstance(itls, list)
            and len(ttfts) == len(itls)
            and all(isinstance(row, list) for row in itls)
        ):
            raise ValueError(
                "unsupported vLLM detailed result: expected direct E2E "
                "latencies or ttfts/itls arrays"
            )
        latencies = tuple(
            float(ttft) + sum(float(value) for value in intervals)
            for ttft, intervals in zip(ttfts, itls)
        )
    if len(latencies) != expected_count or any(latency < 0 for latency in latencies):
        raise ValueError("vLLM detailed result latency length/value mismatch")

    raw_start_times = raw.get("start_times")
    if raw_start_times is None:
        submitted_at = (0.0,) * expected_count
    elif isinstance(raw_start_times, list) and len(raw_start_times) == expected_count:
        start_times = tuple(float(value) for value in raw_start_times)
        origin = min(start_times)
        submitted_at = tuple(value - origin for value in start_times)
    else:
        raise ValueError("vLLM detailed result start_times length mismatch")

    duration = raw.get("duration")
    reconstructed_duration = max(
        submitted + latency for submitted, latency in zip(submitted_at, latencies)
    )
    if duration is not None and abs(reconstructed_duration - float(duration)) > max(
        0.1, float(duration) * 0.02
    ):
        raise ValueError("vLLM detailed result reconstructed duration mismatch")
    return submitted_at, latencies
