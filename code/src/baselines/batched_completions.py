"""Strong no-Ray multi-prompt Completions baseline."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from .contracts import BaselineRequestResult, ChatRequest


CompletionTransport = Callable[
    [str, dict[str, object]],
    Awaitable[dict[str, Any]],
]


@dataclass(frozen=True)
class BatchedCompletionsConfig:
    endpoint_urls: tuple[str, ...]
    model: str
    batch_rows: int
    concurrency_per_endpoint: int
    timeout_s: float
    api_key: str | None
    endpoint_index_offset: int = 0


def _validate_config(
    requests: tuple[ChatRequest, ...],
    config: BatchedCompletionsConfig,
) -> None:
    if not config.endpoint_urls:
        raise ValueError("endpoint_urls must not be empty")
    if config.batch_rows <= 0:
        raise ValueError("batch_rows must be positive")
    if config.concurrency_per_endpoint <= 0:
        raise ValueError("concurrency_per_endpoint must be positive")
    if config.timeout_s <= 0:
        raise ValueError("timeout_s must be positive")
    caps = {request.max_output_tokens for request in requests}
    if len(caps) > 1:
        raise ValueError(
            "multi-prompt Completions batches require one max output cap"
        )
    for request in requests:
        local_index = request.endpoint_index - config.endpoint_index_offset
        if not 0 <= local_index < len(config.endpoint_urls):
            raise ValueError(
                "request endpoint_index is outside configured endpoints: "
                f"doc_id={request.doc_id} "
                f"endpoint_index={request.endpoint_index}"
            )


def _endpoint_batches(
    requests: tuple[ChatRequest, ...],
    config: BatchedCompletionsConfig,
) -> tuple[tuple[int, tuple[ChatRequest, ...]], ...]:
    grouped: list[list[ChatRequest]] = [
        [] for _ in config.endpoint_urls
    ]
    for request in requests:
        grouped[
            request.endpoint_index - config.endpoint_index_offset
        ].append(request)
    batches = []
    for endpoint_index, rows in enumerate(grouped):
        for start in range(0, len(rows), config.batch_rows):
            batches.append(
                (endpoint_index, tuple(rows[start : start + config.batch_rows]))
            )
    return tuple(batches)


def _completed_batch(
    requests: tuple[ChatRequest, ...],
    response: dict[str, Any],
    submitted_at_s: float,
    started_at_s: float,
    completed_at_s: float,
) -> tuple[BaselineRequestResult, ...]:
    choices = response.get("choices")
    if not isinstance(choices, list) or len(choices) != len(requests):
        raise ValueError(
            "Completions response choice count must match prompt count"
        )
    ordered = sorted(
        choices,
        key=lambda choice: int(choice.get("index", -1)),
    )
    results = []
    for request, choice in zip(requests, ordered):
        if not isinstance(choice, dict) or "text" not in choice:
            raise ValueError("Completions choice must contain text")
        token_ids = choice.get("token_ids")
        finish_reason = choice.get("finish_reason")
        results.append(
            BaselineRequestResult(
                doc_id=request.doc_id,
                endpoint_index=request.endpoint_index,
                status="completed",
                error=None,
                submitted_at_s=submitted_at_s,
                started_at_s=started_at_s,
                completed_at_s=completed_at_s,
                input_tokens=request.prompt_tokens,
                output_tokens=(
                    len(token_ids) if isinstance(token_ids, list) else 0
                ),
                output_text=str(choice["text"]),
                finish_reason=(
                    str(finish_reason)
                    if finish_reason is not None
                    else None
                ),
            )
        )
    return tuple(results)


async def _run_batches(
    requests: tuple[ChatRequest, ...],
    config: BatchedCompletionsConfig,
    transport: CompletionTransport,
) -> tuple[BaselineRequestResult, ...]:
    semaphores = tuple(
        asyncio.Semaphore(config.concurrency_per_endpoint)
        for _ in config.endpoint_urls
    )

    async def run_one(
        endpoint_index: int,
        rows: tuple[ChatRequest, ...],
    ) -> tuple[BaselineRequestResult, ...]:
        submitted_at_s = time.time()
        async with semaphores[endpoint_index]:
            started_at_s = time.time()
            try:
                response = await transport(
                    config.endpoint_urls[endpoint_index],
                    {
                        "model": config.model,
                        "prompt": [request.prompt for request in rows],
                        "temperature": 0.0,
                        "max_tokens": rows[0].max_output_tokens,
                        "return_token_ids": True,
                    },
                )
                return _completed_batch(
                    rows,
                    response,
                    submitted_at_s,
                    started_at_s,
                    time.time(),
                )
            except Exception as exc:
                completed_at_s = time.time()
                return tuple(
                    BaselineRequestResult(
                        doc_id=request.doc_id,
                        endpoint_index=request.endpoint_index,
                        status="failed",
                        error=f"{type(exc).__name__}: {exc}",
                        submitted_at_s=submitted_at_s,
                        started_at_s=started_at_s,
                        completed_at_s=completed_at_s,
                        input_tokens=0,
                        output_tokens=0,
                        output_text=None,
                        finish_reason=None,
                    )
                    for request in rows
                )

    nested = await asyncio.gather(
        *(
            run_one(endpoint_index, rows)
            for endpoint_index, rows in _endpoint_batches(requests, config)
        )
    )
    by_doc_id = {
        result.doc_id: result
        for batch_results in nested
        for result in batch_results
    }
    return tuple(by_doc_id[request.doc_id] for request in requests)


async def run_batched_completions(
    requests: Iterable[ChatRequest],
    config: BatchedCompletionsConfig,
    transport: CompletionTransport | None = None,
) -> tuple[BaselineRequestResult, ...]:
    """Run fixed-row multi-prompt HTTP batches with endpoint-local bounds."""

    materialized = tuple(requests)
    _validate_config(materialized, config)
    if not materialized:
        return ()
    if transport is not None:
        return await _run_batches(materialized, config, transport)
    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError(
            "batched Completions baseline requires the 'httpx' package"
        ) from exc
    connection_capacity = (
        config.concurrency_per_endpoint * len(config.endpoint_urls)
    )
    headers = {"Content-Type": "application/json"}
    if config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"
    async with httpx.AsyncClient(
        headers=headers,
        timeout=config.timeout_s,
        limits=httpx.Limits(
            max_connections=connection_capacity,
            max_keepalive_connections=connection_capacity,
        ),
    ) as client:

        async def http_transport(
            url: str,
            payload: dict[str, object],
        ) -> dict[str, Any]:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            decoded = response.json()
            if not isinstance(decoded, dict):
                raise ValueError("Completions response must be an object")
            return decoded

        return await _run_batches(
            materialized,
            config,
            http_transport,
        )
