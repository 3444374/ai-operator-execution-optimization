"""Official vLLM Bench Serve ceiling dataset and command construction."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from ..contracts import ChatRequest


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


def extract_vllm_bench_latency_distribution(
    raw: Mapping[str, Any],
    expected_count: int,
) -> tuple[tuple[float, ...], tuple[tuple[float, ...], ...]]:
    """Return the per-request TTFT and per-token ITL distributions.

    The E2E ``latencies`` returned by :func:`extract_vllm_bench_request_timings`
    fold ``ttfts`` and ``itls`` into a single scalar (``ttft + sum(intervals)``),
    which discards both the first-token latency and the per-token inter-token
    latency (TBT/ITL) distribution. Literature treats TTFT and TBT/ITL as
    distinct, non-interchangeable metrics: TPOT is a coarser per-request
    average, TBT is the strict per-token tail metric (Mooncake deploys with
    TBT <= 0.1 s/token). This helper preserves that distribution.

    Real vLLM detailed output carries ``ttfts``/``itls`` alongside the folded
    ``request_latencies``; the arrays are optional, so missing or mismatched
    arrays yield empty tuples rather than raising. Callers report ``None``
    distribution stats in that case via
    :func:`summarize_vllm_bench_latency_distribution`.
    """
    raw_ttfts = raw.get("ttfts")
    raw_itls = raw.get("itls")
    if not isinstance(raw_ttfts, list) or not isinstance(raw_itls, list):
        return (), ()
    if len(raw_ttfts) != expected_count or len(raw_itls) != expected_count:
        return (), ()
    if not all(isinstance(row, list) for row in raw_itls):
        return (), ()
    ttfts = tuple(float(value) for value in raw_ttfts)
    itls = tuple(tuple(float(value) for value in row) for row in raw_itls)
    return ttfts, itls


def _quantile(values: list[float], probability: float) -> float:
    """Linear-interpolated percentile; mirrors ``results._quantile``."""
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def summarize_vllm_bench_latency_distribution(
    ttfts: tuple[float, ...],
    itls: tuple[tuple[float, ...], ...],
) -> dict[str, float | None]:
    """Aggregate TTFT and inter-token-latency (TBT/ITL) tail statistics.

    TTFT percentiles aggregate over per-request first-token latencies; ITL
    percentiles aggregate over every per-token interval flattened across all
    requests (the strict time-between-tokens tail metric, not the coarser
    per-request TPOT). The two distributions are reported as separate fields
    alongside the existing E2E latency and are not interchangeable.

    All six keys are always present so the summary schema stays stable for
    downstream consumers; values are ``None`` when the source JSON did not
    carry the corresponding ``ttfts``/``itls`` arrays.
    """
    if ttfts:
        ttft_stats: dict[str, float | None] = {
            "ttft_mean_s": sum(ttfts) / len(ttfts),
            "ttft_p95_s": _quantile(list(ttfts), 0.95),
            "ttft_p99_s": _quantile(list(ttfts), 0.99),
        }
    else:
        ttft_stats = {
            "ttft_mean_s": None,
            "ttft_p95_s": None,
            "ttft_p99_s": None,
        }
    flat_itls = [value for intervals in itls for value in intervals]
    if flat_itls:
        itl_stats: dict[str, float | None] = {
            "itl_mean_s": sum(flat_itls) / len(flat_itls),
            "itl_p95_s": _quantile(flat_itls, 0.95),
            "itl_p99_s": _quantile(flat_itls, 0.99),
        }
    else:
        itl_stats = {
            "itl_mean_s": None,
            "itl_p95_s": None,
            "itl_p99_s": None,
        }
    return {**ttft_stats, **itl_stats}
