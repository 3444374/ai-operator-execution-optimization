"""Project-authored bounded Chat control with request-level timing."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, replace
from typing import Any, Literal

from src.baselines.common.contracts import BaselineRequestResult, ChatRequest


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
    endpoint_index_offset: int = 0
    replay_arrivals: bool = True
    arrival_time_scale: float = 1.0
    ignore_eos: bool = False
    protocol: Literal["completions", "chat_completions"] = "chat_completions"
    prompt_format: Literal["raw", "chatml"] = "raw"
    temperature: float | None = 0.0
    return_token_ids: bool = False
    replay_start_epoch_s: float | None = None


@dataclass(frozen=True)
class TimedHttpJob:
    """One logical client whose immutable requests share an arrival offset."""

    job_id: str
    requests: tuple[ChatRequest, ...]
    arrival_offset_s: float = 0.0


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
    if config.protocol not in {"completions", "chat_completions"}:
        raise ValueError("unsupported completion protocol")
    if config.prompt_format not in {"raw", "chatml"}:
        raise ValueError("unsupported completion prompt format")
    if config.temperature is not None and (
        not math.isfinite(config.temperature) or config.temperature < 0
    ):
        raise ValueError("temperature must be finite and non-negative")
    if config.replay_start_epoch_s is not None and not math.isfinite(
        config.replay_start_epoch_s
    ):
        raise ValueError("replay_start_epoch_s must be finite")
    for request in requests:
        local_index = request.endpoint_index - config.endpoint_index_offset
        if not 0 <= local_index < len(config.endpoint_urls):
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
    usage = response.get("usage") or {}
    if not isinstance(usage, dict):
        raise ValueError("Chat Completions usage must be an object")
    if "message" in choice:
        message = choice.get("message")
        if not isinstance(message, dict):
            raise ValueError("Chat Completions choice is missing message")
        content = message.get("content", "")
    elif "text" in choice:
        content = choice.get("text", "")
    else:
        raise ValueError("completion choice contains neither message nor text")
    finish_reason = choice.get("finish_reason")
    return BaselineRequestResult(
        doc_id=request.doc_id,
        endpoint_index=request.endpoint_index,
        status="completed",
        error=None,
        submitted_at_s=submitted_at_s,
        started_at_s=started_at_s,
        completed_at_s=completed_at_s,
        input_tokens=int(usage.get("prompt_tokens", request.prompt_tokens)),
        output_tokens=int(usage.get("completion_tokens", 0)),
        output_text=str(content),
        finish_reason=(str(finish_reason) if finish_reason is not None else None),
    )


async def _run_requests(
    requests: tuple[ChatRequest, ...],
    config: BoundedHttpConfig,
    transport: HttpTransport,
) -> tuple[BaselineRequestResult, ...]:
    semaphores = tuple(
        asyncio.Semaphore(config.concurrency_per_endpoint) for _ in config.endpoint_urls
    )
    loop = asyncio.get_running_loop()
    if config.replay_start_epoch_s is not None:
        remaining_s = config.replay_start_epoch_s - time.time()
        if remaining_s > 0:
            await asyncio.sleep(remaining_s)
    replay_start = loop.time()

    def request_prompt(request: ChatRequest) -> str:
        if config.prompt_format == "raw":
            return request.prompt
        return (
            f"<|im_start|>user\n{request.prompt}<|im_end|>\n"
            "<|im_start|>assistant\n"
        )

    async def run_one(request: ChatRequest) -> BaselineRequestResult:
        if config.replay_arrivals:
            target_s = request.arrival_time_s * config.arrival_time_scale
            remaining_s = target_s - (loop.time() - replay_start)
            if remaining_s > 0:
                await asyncio.sleep(remaining_s)
        submitted_at_s = time.time()
        local_index = request.endpoint_index - config.endpoint_index_offset
        async with semaphores[local_index]:
            started_at_s = time.time()
            try:
                prompt = request_prompt(request)
                payload: dict[str, object] = {
                    "model": config.model,
                    "max_tokens": request.max_output_tokens,
                }
                if config.protocol == "chat_completions":
                    payload["messages"] = [
                        {"role": "user", "content": prompt}
                    ]
                else:
                    # Match the project completion actor, which always sends
                    # its request-level prompt as a one-element batch.
                    payload["prompt"] = [prompt]
                if config.temperature is not None:
                    payload["temperature"] = config.temperature
                if config.return_token_ids:
                    payload["return_token_ids"] = True
                if config.ignore_eos:
                    payload["ignore_eos"] = True
                response = await transport(
                    config.endpoint_urls[local_index],
                    payload,
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
        raise RuntimeError("bounded HTTP baseline requires the 'httpx' package") from exc

    headers = {"Content-Type": "application/json"}
    if config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"
    connection_capacity = (
        config.concurrency_per_endpoint * len(config.endpoint_urls)
    )
    limits = httpx.Limits(
        max_connections=connection_capacity,
        max_keepalive_connections=connection_capacity,
    )
    async with httpx.AsyncClient(
        headers=headers,
        timeout=config.timeout_s,
        limits=limits,
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


async def run_bounded_http_jobs(
    jobs: Iterable[TimedHttpJob],
    config: BoundedHttpConfig,
    transport: HttpTransport | None = None,
) -> dict[str, tuple[BaselineRequestResult, ...]]:
    """Replay multiple logical jobs through one shared direct-client control.

    This only merges immutable arrival traces. It adds no per-job credit,
    routing, or fairness policy, leaving endpoint-local bounds and vLLM as the
    admission and scheduling owners.
    """

    materialized = tuple(jobs)
    if not materialized:
        raise ValueError("timed HTTP jobs must not be empty")
    job_ids = [job.job_id for job in materialized]
    if any(not job_id.strip() for job_id in job_ids):
        raise ValueError("timed HTTP job_id must be non-empty")
    if len(set(job_ids)) != len(job_ids):
        raise ValueError("timed HTTP job_id values must be unique")

    merged: list[ChatRequest] = []
    owner_by_doc_id: dict[int, str] = {}
    for job in materialized:
        if not math.isfinite(job.arrival_offset_s) or job.arrival_offset_s < 0:
            raise ValueError(
                "timed HTTP arrival_offset_s must be finite and non-negative"
            )
        if not job.requests:
            raise ValueError(f"timed HTTP job is empty: {job.job_id}")
        first_arrival_s = min(request.arrival_time_s for request in job.requests)
        for request in job.requests:
            if request.doc_id in owner_by_doc_id:
                raise ValueError(
                    f"timed HTTP jobs contain duplicate doc_id: {request.doc_id}"
                )
            owner_by_doc_id[request.doc_id] = job.job_id
            merged.append(
                replace(
                    request,
                    arrival_time_s=(
                        request.arrival_time_s
                        - first_arrival_s
                        + job.arrival_offset_s
                    ),
                )
            )

    results = await run_bounded_http(merged, config, transport=transport)
    grouped: dict[str, list[BaselineRequestResult]] = {
        job_id: [] for job_id in job_ids
    }
    for result in results:
        grouped[owner_by_doc_id[result.doc_id]].append(result)
    return {job_id: tuple(rows) for job_id, rows in grouped.items()}
