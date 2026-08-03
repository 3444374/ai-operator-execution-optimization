"""Ray Data HTTP Processor baseline with vendor-owned actor scheduling."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

from src.baselines.common.contracts import BaselineRequestResult, ChatRequest
from .common import validate_single_endpoint_shard


_CODE_ROOT = Path(__file__).resolve().parents[4]


@dataclass(frozen=True)
class RayDataHttpConfig:
    endpoint_url: str
    api_key: str | None
    model: str
    max_tokens: int
    batch_size: int
    concurrency: int
    ray_address: str | None = None


def ray_data_preprocess(
    row: dict[str, Any],
    *,
    model: str,
    max_tokens: int,
) -> dict[str, Any]:
    """Map one database row to one OpenAI-compatible Chat request."""

    return {
        **row,
        "payload": {
            "model": model,
            "messages": [
                {"role": "user", "content": str(row["prompt"])}
            ],
            "temperature": 0.0,
            "max_tokens": max_tokens,
        },
    }


def ray_data_postprocess(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize one official processor response without scheduling logic."""

    response = row.get("http_response")
    if not isinstance(response, dict):
        raise ValueError("Ray Data row is missing http_response")
    choices = response.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        raise ValueError("Ray Data response must contain one choice")
    choice = choices[0]
    if not isinstance(choice, dict):
        raise ValueError("Ray Data response choice must be an object")
    message = choice.get("message")
    if not isinstance(message, dict):
        raise ValueError("Ray Data response choice is missing message")
    usage = response.get("usage") or {}
    if not isinstance(usage, dict):
        raise ValueError("Ray Data response usage must be an object")
    finish_reason = choice.get("finish_reason")
    return {
        "doc_id": int(row["doc_id"]),
        "endpoint_index": int(row["endpoint_index"]),
        "output_text": str(message.get("content", "")),
        "input_tokens": int(
            usage.get("prompt_tokens", row.get("prompt_tokens", 0))
        ),
        "output_tokens": int(usage.get("completion_tokens", 0)),
        "finish_reason": (
            str(finish_reason) if finish_reason is not None else None
        ),
    }


def _load_ray_data_modules() -> SimpleNamespace:
    try:
        import ray
        from ray.data.llm import (
            HttpRequestProcessorConfig,
            build_processor,
        )
    except ImportError as exc:
        raise RuntimeError(
            "Ray Data HTTP baseline requires ray[data,serve]"
        ) from exc
    return SimpleNamespace(
        ray=ray,
        config_class=HttpRequestProcessorConfig,
        build_processor=build_processor,
    )


def _ray_runtime_env() -> dict[str, dict[str, str]]:
    pythonpath = str(_CODE_ROOT)
    existing_pythonpath = os.environ.get("PYTHONPATH")
    if existing_pythonpath:
        pythonpath = os.pathsep.join(
            [pythonpath, existing_pythonpath]
        )
    return {"env_vars": {"PYTHONPATH": pythonpath}}


def run_ray_data_http(
    requests: Iterable[ChatRequest],
    config: RayDataHttpConfig,
    modules: object | None = None,
) -> tuple[BaselineRequestResult, ...]:
    """Execute one endpoint shard through Ray Data HTTP Processor."""

    materialized = tuple(requests)
    validate_single_endpoint_shard(materialized, config.max_tokens)
    if config.batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if config.concurrency <= 0:
        raise ValueError("concurrency must be positive")
    runtime = modules or _load_ray_data_modules()
    if config.ray_address is not None:
        runtime.ray.init(
            address=config.ray_address,
            ignore_reinit_error=True,
            runtime_env=_ray_runtime_env(),
        )
    headers = (
        {"Authorization": f"Bearer {config.api_key}"}
        if config.api_key
        else None
    )
    processor_config = runtime.config_class(
        batch_size=config.batch_size,
        url=config.endpoint_url,
        headers=headers,
        concurrency=(config.concurrency, config.concurrency),
        max_retries=0,
    )
    processor = runtime.build_processor(
        processor_config,
        preprocess=lambda row: ray_data_preprocess(
            row,
            model=config.model,
            max_tokens=config.max_tokens,
        ),
        postprocess=ray_data_postprocess,
    )
    items = [
        {
            "doc_id": request.doc_id,
            "endpoint_index": request.endpoint_index,
            "prompt": request.prompt,
            "prompt_tokens": request.prompt_tokens,
        }
        for request in materialized
    ]
    submitted_at_s = time.time()
    rows = processor(runtime.ray.data.from_items(items)).take_all()
    completed_at_s = time.time()
    row_by_id = {int(row["doc_id"]): row for row in rows}
    return tuple(
        BaselineRequestResult(
            doc_id=request.doc_id,
            endpoint_index=request.endpoint_index,
            status="completed",
            error=None,
            submitted_at_s=submitted_at_s,
            started_at_s=submitted_at_s,
            completed_at_s=completed_at_s,
            input_tokens=int(row_by_id[request.doc_id]["input_tokens"]),
            output_tokens=int(
                row_by_id[request.doc_id]["output_tokens"]
            ),
            output_text=str(
                row_by_id[request.doc_id]["output_text"]
            ),
            finish_reason=row_by_id[request.doc_id]["finish_reason"],
        )
        for request in materialized
    )
