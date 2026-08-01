"""Daft built-in ``prompt`` baseline; Daft owns execution and batching."""

from __future__ import annotations

import time
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Iterable, Literal

from ..contracts import BaselineRequestResult, ChatRequest
from .common import validate_single_endpoint_shard


DaftRunner = Literal["native", "ray"]


@dataclass(frozen=True)
class DaftPromptConfig:
    runner: DaftRunner
    base_url: str
    api_key: str | None
    model: str
    max_tokens: int
    ray_address: str | None = None


def daft_prompt_options(
    *,
    model: str,
    max_tokens: int,
) -> dict[str, object]:
    """Freeze same-request Chat semantics on Daft's public AI Function."""

    return {
        "model": model,
        "use_chat_completions": True,
        "temperature": 0.0,
        "max_tokens": max_tokens,
        "max_retries": 0,
        "on_error": "raise",
    }


def _load_daft_modules() -> SimpleNamespace:
    try:
        import daft
        from daft.ai.openai.provider import OpenAIProvider
        from daft.functions import prompt
    except ImportError as exc:
        raise RuntimeError(
            "Daft prompt baseline requires daft and openai"
        ) from exc
    return SimpleNamespace(
        daft=daft,
        prompt=prompt,
        provider_class=OpenAIProvider,
    )


def run_daft_prompt(
    requests: Iterable[ChatRequest],
    config: DaftPromptConfig,
    modules: object | None = None,
) -> tuple[BaselineRequestResult, ...]:
    """Execute one endpoint shard through official ``daft.functions.prompt``."""

    materialized = tuple(requests)
    validate_single_endpoint_shard(materialized, config.max_tokens)
    runtime = modules or _load_daft_modules()
    if config.runner == "native":
        runtime.daft.set_runner_native()
    elif config.runner == "ray":
        runtime.daft.set_runner_ray(
            address=config.ray_address,
            noop_if_initialized=True,
        )
    else:
        raise ValueError(f"unknown Daft runner: {config.runner}")
    provider = runtime.provider_class(
        base_url=config.base_url,
        api_key=config.api_key or "not-needed",
    )
    frame = runtime.daft.from_pydict(
        {
            "doc_id": [request.doc_id for request in materialized],
            "prompt": [request.prompt for request in materialized],
        }
    )
    expression = runtime.prompt(
        runtime.daft.col("prompt"),
        provider=provider,
        **daft_prompt_options(
            model=config.model,
            max_tokens=config.max_tokens,
        ),
    )
    submitted_at_s = time.time()
    rows = (
        frame.with_column("output_text", expression)
        .collect()
        .to_pylist()
    )
    completed_at_s = time.time()
    output_by_id = {
        int(row["doc_id"]): str(row["output_text"])
        for row in rows
    }
    return tuple(
        BaselineRequestResult(
            doc_id=request.doc_id,
            endpoint_index=request.endpoint_index,
            status="completed",
            error=None,
            submitted_at_s=submitted_at_s,
            started_at_s=submitted_at_s,
            completed_at_s=completed_at_s,
            input_tokens=request.prompt_tokens,
            output_tokens=0,
            output_text=output_by_id[request.doc_id],
            finish_reason=None,
        )
        for request in materialized
    )
