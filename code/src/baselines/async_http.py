"""Strong no-Ray/no-Daft Chat Completions baseline."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

from .contracts import BaselineRequestResult, ChatRequest


HttpTransport = Callable[
    [str, dict[str, object]],
    Awaitable[dict[str, Any]],
]


@dataclass(frozen=True)
class BoundedHttpConfig:
    endpoint_urls: tuple[str, ...]
    model: str
    concurrency_per_endpoint: int
    timeout_s: float
    api_key: str | None
    replay_arrivals: bool = True
    arrival_time_scale: float = 1.0


def _validate_config(
    requests: tuple[ChatRequest, ...],
    config: BoundedHttpConfig,
) -> None:
    if not config.endpoint_urls:
        raise ValueError("endpoint_urls must not be empty")
    if config.concurrency_per_endpoint <= 0:
        raise ValueError("concurrency_per_endpoint must be positive")
    if config.timeout_s <= 0:
        raise ValueError("timeout_s must be positive")
    if config.arrival_time_scale <= 0:
        raise ValueError("arrival_time_scale must be positive")
    for request in requests:
        if not 0 <= request.endpoint_index < len(config.endpoint_urls):
            raise ValueError(
                "request endpoint_index is outside configured endpoints: "
                f"doc_id={request.doc_id} "
                f"endpoint_index={request.endpoint_index}"
            )


def _completed_result(
    request: ChatRequest,
    response: dict[str, Any],
    submitted_at_s: float,
    started_at_s: float,
    completed_at_s: float,
) -> BaselineRequestResult:
    choices = response.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        raise ValueError("Chat Completions response must contain one choice")
    choice = choices[0]
    if not isinstance(choice, dict):
        raise ValueError("Chat Completions choice must be an object")
    message = choice.get("message")
    if not isinstance(message, dict):
        raise ValueError("Chat Completions choice is missing message")
    usage = response.get("usage") or {}
    if not isinstance(usage, dict):
        raise ValueError("Chat Completions usage must be an object")
    content = message.get("content", "")
    finish_reason = choice.get("finish_reason")
    return BaselineRequestResult(
        doc_id=request.doc_id,
        endpoint_index=request.endpoint_index,
        status="completed",
        error=None,
        submitted_at_s=submitted_at_s,
        started_at_s=started_at_s,
        completed_at_s=completed_at_s,
        input_tokens=int(
            usage.get("prompt_tokens", request.prompt_tokens)
        ),
        output_tokens=int(usage.get("completion_tokens", 0)),
        output_text=str(content),
        finish_reason=(
            str(finish_reason) if finish_reason is not None else None
        ),
    )


async def _run_requests(
    requests: tuple[ChatRequest, ...],
    config: BoundedHttpConfig,
    transport: HttpTransport,
) -> tuple[BaselineRequestResult, ...]:
    semaphores = tuple(
        asyncio.Semaphore(config.concurrency_per_endpoint)
        for _ in config.endpoint_urls
    )
    loop = asyncio.get_running_loop()
    replay_start = loop.time()

    async def run_one(request: ChatRequest) -> BaselineRequestResult:
        if config.replay_arrivals:
            target_s = request.arrival_time_s * config.arrival_time_scale
            remaining_s = target_s - (loop.time() - replay_start)
            if remaining_s > 0:
                await asyncio.sleep(remaining_s)
        submitted_at_s = time.time()
        async with semaphores[request.endpoint_index]:
            started_at_s = time.time()
            try:
                response = await transport(
                    config.endpoint_urls[request.endpoint_index],
                    {
                        "model": config.model,
                        "messages": list(request.messages),
                        "temperature": 0.0,
                        "max_tokens": request.max_output_tokens,
                    },
                )
                completed_at_s = time.time()
                return _completed_result(
                    request,
                    response,
                    submitted_at_s,
                    started_at_s,
                    completed_at_s,
                )
            except Exception as exc:
                return BaselineRequestResult(
                    doc_id=request.doc_id,
                    endpoint_index=request.endpoint_index,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                    submitted_at_s=submitted_at_s,
                    started_at_s=started_at_s,
                    completed_at_s=time.time(),
                    input_tokens=0,
                    output_tokens=0,
                    output_text=None,
                    finish_reason=None,
                )

    return tuple(await asyncio.gather(*(run_one(row) for row in requests)))


async def run_bounded_http(
    requests: Iterable[ChatRequest],
    config: BoundedHttpConfig,
    transport: HttpTransport | None = None,
) -> tuple[BaselineRequestResult, ...]:
    """Run one Chat Completions request per row with endpoint-local bounds."""

    materialized = tuple(requests)
    _validate_config(materialized, config)
    if transport is not None:
        return await _run_requests(materialized, config, transport)

    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError(
            "bounded HTTP baseline requires the 'httpx' package"
        ) from exc

    headers = {"Content-Type": "application/json"}
    if config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"
    async with httpx.AsyncClient(
        headers=headers,
        timeout=config.timeout_s,
    ) as client:
        async def http_transport(
            url: str,
            payload: dict[str, object],
        ) -> dict[str, Any]:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            decoded = response.json()
            if not isinstance(decoded, dict):
                raise ValueError("Chat Completions response must be an object")
            return decoded

        return await _run_requests(
            materialized,
            config,
            http_transport,
        )
