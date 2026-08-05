"""Direct HTTP client AI_COMPLETE arm (native client, fixed concurrency).

This is the ``direct_client`` arm of the SQuAD database-E2E runner: a plain
async HTTP client that calls the OpenAI-compatible vLLM ``/v1/chat/completions``
endpoint **per request** at a **fixed** concurrency (an ``asyncio.Semaphore``).
It is the "no database, no extension" reference: unlike the DuckDB-ai arm
(which hides ``finish_reason`` / output tokens behind a set-oriented barrier),
this arm exposes ``finish_reason``, ``completion_tokens`` and per-request
latency directly.

Design contract (codex ruling):
* FIXED concurrency only -- no project credit, actor pool, or dynamic
  backpressure. The semaphore caps in-flight requests exactly like the DuckDB
  ``max_concurrent_requests`` knob, so the two arms offer the same load to vLLM
  and the only difference is "DuckDB barrier" vs "direct HTTP per request".
* The request body is built by the shared ``build_completion_request_body`` --
  the same canonical builder the DuckDB path and the request-equivalence gate
  use -- so the two arms send semantically identical requests.
* Per-request timing: ``submitted`` = queued (before the semaphore);
  ``started`` = after acquiring the slot (HTTP begins); ``completed`` = HTTP
  response received. The runner's ``_operator_span`` (min started -> max
  completed) turns these into the arm's operator span.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Callable, Iterable

from src.baselines.common.contracts import BaselineRequestResult, ChatRequest


@dataclass(frozen=True)
class DirectClientConfig:
    """Endpoint + generation + fixed-concurrency controls for the direct arm.

    ``max_concurrent_requests`` defaults to 32 to match the DuckDB-ai extension
    so the two arms offer vLLM the same concurrency (fairness); only the
    execution model (barrier vs per-request) differs.
    """

    endpoint_url: str
    model: str
    max_tokens: int
    api_key: str = "EMPTY"
    max_concurrent_requests: int = 32
    temperature: float = 0.0
    timeout_s: float = 180.0

    def __post_init__(self) -> None:
        if not self.endpoint_url:
            raise ValueError("endpoint_url must be non-empty")
        if not self.model:
            raise ValueError("model must be non-empty")
        if self.max_tokens < 0:
            raise ValueError("max_tokens must be non-negative")
        if not 1 <= self.max_concurrent_requests <= 1024:
            raise ValueError("max_concurrent_requests must be in [1, 1024]")


def _validate_requests(requests: tuple[ChatRequest, ...]) -> None:
    if not requests:
        raise ValueError("direct_client shard is empty")
    if len({request.endpoint_index for request in requests}) > 1:
        raise ValueError("direct_client adapter accepts one endpoint shard at a time")
    caps = {request.max_output_tokens for request in requests}
    if len(caps) > 1:
        raise ValueError("direct_client shard requires the same max_output_tokens")


def _fail(request: ChatRequest, submitted: float, started: float,
          completed: float, error: str) -> BaselineRequestResult:
    return BaselineRequestResult(
        doc_id=request.doc_id, endpoint_index=request.endpoint_index,
        status="failed", error=error,
        submitted_at_s=submitted, started_at_s=started, completed_at_s=completed,
        input_tokens=request.prompt_tokens, output_tokens=0,
        output_text=None, finish_reason=None,
    )


async def _one(client, sem: asyncio.Semaphore, request: ChatRequest,
               config: DirectClientConfig, build_body: Callable) -> BaselineRequestResult:
    submitted = time.time()
    body = build_body(
        config.model, [request.prompt], config.max_tokens,
        "chat_completions", temperature=config.temperature,
    )
    headers = {"Authorization": f"Bearer {config.api_key}"}
    # Hold the semaphore only for the HTTP call (the fixed-concurrency cap).
    async with sem:
        started = time.time()
        try:
            resp = await client.post(
                config.endpoint_url, json=body, headers=headers,
                timeout=config.timeout_s,
            )
            completed = time.time()
        except Exception as exc:  # noqa: BLE001 - record any transport failure
            completed = time.time()
            return _fail(request, submitted, started, completed,
                         f"{type(exc).__name__}: {exc}"[:300])

    if resp.status_code != 200:
        return _fail(request, submitted, started, completed,
                     f"http {resp.status_code}: {resp.text[:200]}")
    try:
        data = resp.json()
        choice = (data.get("choices") or [{}])[0]
        usage = data.get("usage") or {}
        output_text = (choice.get("message") or {}).get("content")
        finish_reason = choice.get("finish_reason")
        return BaselineRequestResult(
            doc_id=request.doc_id, endpoint_index=request.endpoint_index,
            status="completed", error=None,
            submitted_at_s=submitted, started_at_s=started, completed_at_s=completed,
            input_tokens=usage.get("prompt_tokens", request.prompt_tokens),
            output_tokens=usage.get("completion_tokens", 0),
            output_text=output_text, finish_reason=finish_reason,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(request, submitted, started, completed,
                     f"parse: {type(exc).__name__}: {exc}"[:300])


async def _run_all(requests: tuple[ChatRequest, ...],
                   config: DirectClientConfig) -> tuple[BaselineRequestResult, ...]:
    # Lazy import: httpx + the canonical body builder live in modules that pull
    # heavier deps (pyarrow) not needed at import time. Tests mock run_direct_client
    # so this only runs on the server, where those deps exist.
    import httpx
    from src.serving.backends.completion import build_completion_request_body
    sem = asyncio.Semaphore(config.max_concurrent_requests)
    limits = httpx.Limits(
        max_connections=config.max_concurrent_requests,
        max_keepalive_connections=config.max_concurrent_requests,
    )
    async with httpx.AsyncClient(limits=limits) as client:
        results = await asyncio.gather(
            *[_one(client, sem, request, config, build_completion_request_body)
              for request in requests]
        )
    return results


def run_direct_client(
    requests: Iterable[ChatRequest],
    config: DirectClientConfig,
) -> tuple[BaselineRequestResult, ...]:
    """Execute one direct per-request HTTP completion per shard row."""

    materialized = tuple(requests)
    _validate_requests(materialized)
    shard_cap = {request.max_output_tokens for request in materialized}
    if next(iter(shard_cap)) != config.max_tokens:
        raise ValueError(
            f"config.max_tokens={config.max_tokens} does not match shard "
            f"max_output_tokens={next(iter(shard_cap))}"
        )
    return asyncio.run(_run_all(materialized, config))
