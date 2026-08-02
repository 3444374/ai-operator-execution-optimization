"""Text completion and embedding model backends for AI operator profiling.

The HTTP path targets OpenAI-compatible embedding APIs, including vLLM-style
servers. The module keeps that compatibility detail out of the orchestration
script so future completion backends can live beside it without renaming the
whole pipeline around a single provider.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Literal
from urllib import error, request

import numpy as np
import pyarrow as pa


EmbeddingBackendName = Literal["fake", "compatible_http", "http_openai"]
CompletionBackendName = Literal["fake", "compatible_http", "http_openai", "ollama"]
CompletionPromptFormat = Literal["raw", "chatml"]
CompletionProtocol = Literal["completions", "chat_completions"]


@dataclass(frozen=True)
class CompletionEndpointResult:
    outputs: list[str]
    total_tokens: int | None
    output_token_counts: list[int | None]
    finish_reasons: list[str | None]
    http_request_start_epoch_s: float
    http_response_headers_epoch_s: float
    http_response_body_epoch_s: float
    http_headers_wait_s: float
    http_body_read_s: float


class _ReadyActor:
    """Side-effect-free readiness contract for explicit Ray startup barriers."""

    def ready(self) -> dict[str, object]:
        return {
            "actor_worker_pid": os.getpid(),
            "actor_type": type(self).__name__,
        }


def format_completion_prompts(
    prompts: list[str],
    prompt_format: CompletionPromptFormat,
) -> list[str]:
    if prompt_format == "raw":
        return prompts
    if prompt_format == "chatml":
        return [
            (
                f"<|im_start|>user\n{prompt}<|im_end|>\n"
                "<|im_start|>assistant\n"
            )
            for prompt in prompts
        ]
    raise ValueError(f"Unknown completion prompt format: {prompt_format}")


def _completion_request_body(
    model_name: str,
    prompts: list[str],
    max_tokens: int,
    protocol: CompletionProtocol,
) -> dict:
    if protocol == "completions":
        return {
            "model": model_name,
            "prompt": prompts,
            "max_tokens": max_tokens,
        }
    if protocol == "chat_completions":
        if len(prompts) != 1:
            raise ValueError(
                "Chat Completions requires one complete prompt per HTTP request"
            )
        return {
            "model": model_name,
            "messages": [{"role": "user", "content": prompts[0]}],
            "max_tokens": max_tokens,
        }
    raise ValueError(f"Unknown completion protocol: {protocol}")


def normalize_embedding_backend(name: EmbeddingBackendName) -> Literal["fake", "compatible_http"]:
    if name == "fake":
        return "fake"
    if name in {"compatible_http", "http_openai"}:
        return "compatible_http"
    raise ValueError(f"Unknown embedding backend: {name}")


def normalize_completion_backend(name: CompletionBackendName) -> Literal["fake", "compatible_http", "ollama"]:
    if name == "fake":
        return "fake"
    if name in {"compatible_http", "http_openai"}:
        return "compatible_http"
    if name == "ollama":
        return "ollama"
    raise ValueError(f"Unknown completion backend: {name}")


def text_token_count(text: str) -> int:
    return max(1, len(text.split()))


def call_compatible_embedding_endpoint(
    endpoint_url: str,
    model_name: str,
    texts: list[str],
    api_key: str | None,
    timeout_s: float,
) -> tuple[np.ndarray, int | None]:
    payload = json.dumps({"model": model_name, "input": texts}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = request.Request(endpoint_url, data=payload, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=timeout_s) as response:
            body = response.read()
    except error.URLError as exc:
        raise RuntimeError(f"Embedding endpoint request failed: {exc}") from exc
    decoded = json.loads(body.decode("utf-8"))
    data = sorted(decoded["data"], key=lambda item: item.get("index", 0))
    vectors = np.asarray([item["embedding"] for item in data], dtype=np.float32)
    usage = decoded.get("usage") or {}
    total_tokens = usage.get("total_tokens")
    return vectors, int(total_tokens) if total_tokens is not None else None


def call_compatible_completion_endpoint(
    endpoint_url: str,
    model_name: str,
    prompts: list[str],
    api_key: str | None,
    timeout_s: float,
    max_tokens: int,
    *,
    return_token_ids: bool = False,
    prompt_format: CompletionPromptFormat = "raw",
    temperature: float | None = None,
    protocol: CompletionProtocol = "completions",
) -> CompletionEndpointResult:
    request_body = _completion_request_body(
        model_name,
        format_completion_prompts(prompts, prompt_format),
        max_tokens,
        protocol,
    )
    if return_token_ids:
        request_body["return_token_ids"] = True
    if temperature is not None:
        if not math.isfinite(temperature) or temperature < 0:
            raise ValueError("temperature must be finite and non-negative")
        request_body["temperature"] = temperature
    payload = json.dumps(request_body).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = request.Request(endpoint_url, data=payload, headers=headers, method="POST")
    http_request_start_epoch_s = time.time()
    http_request_start_s = time.perf_counter()
    try:
        with request.urlopen(req, timeout=timeout_s) as response:
            http_response_headers_epoch_s = time.time()
            http_response_headers_s = time.perf_counter()
            body = response.read()
            http_response_body_epoch_s = time.time()
            http_response_body_s = time.perf_counter()
    except error.URLError as exc:
        raise RuntimeError(f"Completion endpoint request failed: {exc}") from exc
    decoded = json.loads(body.decode("utf-8"))
    return _decode_completion_endpoint_result(
        decoded,
        http_request_start_epoch_s=http_request_start_epoch_s,
        http_response_headers_epoch_s=http_response_headers_epoch_s,
        http_response_body_epoch_s=http_response_body_epoch_s,
        http_headers_wait_s=max(
            0.0,
            http_response_headers_s - http_request_start_s,
        ),
        http_body_read_s=max(
            0.0,
            http_response_body_s - http_response_headers_s,
        ),
    )


def _decode_completion_endpoint_result(
    decoded: dict,
    *,
    http_request_start_epoch_s: float,
    http_response_headers_epoch_s: float,
    http_response_body_epoch_s: float,
    http_headers_wait_s: float,
    http_body_read_s: float,
) -> CompletionEndpointResult:
    choices = sorted(decoded["choices"], key=lambda item: item.get("index", 0))
    outputs = []
    output_token_counts = []
    finish_reasons = []
    for choice in choices:
        if "text" in choice:
            outputs.append(str(choice["text"]))
        else:
            outputs.append(str(choice.get("message", {}).get("content", "")))
        token_ids = choice.get("token_ids")
        output_token_counts.append(
            len(token_ids) if isinstance(token_ids, list) else None
        )
        finish_reason = choice.get("finish_reason")
        finish_reasons.append(
            str(finish_reason) if finish_reason is not None else None
        )
    usage = decoded.get("usage") or {}
    total_tokens = usage.get("total_tokens")
    return CompletionEndpointResult(
        outputs=outputs,
        total_tokens=(
            int(total_tokens) if total_tokens is not None else None
        ),
        output_token_counts=output_token_counts,
        finish_reasons=finish_reasons,
        http_request_start_epoch_s=http_request_start_epoch_s,
        http_response_headers_epoch_s=http_response_headers_epoch_s,
        http_response_body_epoch_s=http_response_body_epoch_s,
        http_headers_wait_s=http_headers_wait_s,
        http_body_read_s=http_body_read_s,
    )


def ollama_generate_url(endpoint_url: str) -> str:
    cleaned = endpoint_url.rstrip("/")
    if cleaned.endswith("/api/generate"):
        return cleaned
    return f"{cleaned}/api/generate"


def call_ollama_completion_endpoint(
    endpoint_url: str,
    model_name: str,
    prompts: list[str],
    timeout_s: float,
    max_tokens: int,
) -> tuple[list[str], int | None]:
    outputs = []
    total_tokens = 0
    saw_token_metrics = False
    url = ollama_generate_url(endpoint_url)
    headers = {"Content-Type": "application/json"}
    for prompt in prompts:
        payload = json.dumps(
            {
                "model": model_name,
                "prompt": prompt,
                "stream": False,
                "options": {"num_predict": max_tokens},
            }
        ).encode("utf-8")
        req = request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with request.urlopen(req, timeout=timeout_s) as response:
                body = response.read()
        except error.URLError as exc:
            raise RuntimeError(f"Ollama completion endpoint request failed: {exc}") from exc
        decoded = json.loads(body.decode("utf-8"))
        outputs.append(str(decoded.get("response", "")))
        prompt_tokens = decoded.get("prompt_eval_count")
        output_tokens = decoded.get("eval_count")
        if prompt_tokens is not None or output_tokens is not None:
            saw_token_metrics = True
            total_tokens += int(prompt_tokens or 0) + int(output_tokens or 0)
    return outputs, total_tokens if saw_token_metrics else None


class FakeEmbeddingActor(_ReadyActor):
    def __init__(self, embedding_dim: int, service_tokens_per_s: float = 50000.0):
        self.embedding_dim = embedding_dim
        self.service_tokens_per_s = service_tokens_per_s

    def embed(self, batch: pa.RecordBatch | pa.Table) -> dict:
        service_start = time.perf_counter()
        service_start_epoch = time.time()
        texts = batch.column("text").to_pylist()
        token_count = sum(text_token_count(text) for text in texts)
        target_s = token_count / self.service_tokens_per_s
        if target_s > 0:
            time.sleep(target_s)
        vectors = np.empty((batch.num_rows, self.embedding_dim), dtype=np.float32)
        for i, text in enumerate(texts):
            seed = hash(text) & 0xFFFFFFFF
            rng = np.random.default_rng(seed)
            vectors[i, :] = rng.random(self.embedding_dim, dtype=np.float32)
        service_s = time.perf_counter() - service_start
        service_end_epoch = time.time()
        return {
            "doc_id": batch.column("doc_id").to_pylist(),
            "tenant_id": batch.column("tenant_id").to_pylist(),
            "category": batch.column("category").to_pylist(),
            "embedding": vectors,
            "rows": batch.num_rows,
            "token_count": token_count,
            "service_s": service_s,
            "service_start_epoch_s": service_start_epoch,
            "service_end_epoch_s": service_end_epoch,
            "actor_worker_pid": os.getpid(),
        }


class CompatibleHTTPEmbeddingActor(_ReadyActor):
    def __init__(self, endpoint_url: str, model_name: str, api_key: str | None, timeout_s: float):
        self.endpoint_url = endpoint_url
        self.model_name = model_name
        self.api_key = api_key
        self.timeout_s = timeout_s

    def embed(self, batch: pa.RecordBatch | pa.Table) -> dict:
        service_start = time.perf_counter()
        service_start_epoch = time.time()
        texts = batch.column("text").to_pylist()
        vectors, endpoint_tokens = call_compatible_embedding_endpoint(
            self.endpoint_url,
            self.model_name,
            texts,
            self.api_key,
            self.timeout_s,
        )
        token_count = endpoint_tokens
        if token_count is None:
            token_count = sum(text_token_count(text) for text in texts)
        service_s = time.perf_counter() - service_start
        service_end_epoch = time.time()
        return {
            "doc_id": batch.column("doc_id").to_pylist(),
            "tenant_id": batch.column("tenant_id").to_pylist(),
            "category": batch.column("category").to_pylist(),
            "embedding": vectors,
            "rows": batch.num_rows,
            "token_count": token_count,
            "service_s": service_s,
            "service_start_epoch_s": service_start_epoch,
            "service_end_epoch_s": service_end_epoch,
            "actor_worker_pid": os.getpid(),
        }


def fake_embed_batch(batch: pa.RecordBatch | pa.Table, embedding_dim: int, service_tokens_per_s: float = 50000.0) -> dict:
    return FakeEmbeddingActor(embedding_dim, service_tokens_per_s).embed(batch)


def compatible_http_embed_batch(
    batch: pa.RecordBatch | pa.Table,
    endpoint_url: str,
    model_name: str,
    api_key: str | None,
    timeout_s: float,
) -> dict:
    return CompatibleHTTPEmbeddingActor(endpoint_url, model_name, api_key, timeout_s).embed(batch)


class FakeCompletionActor(_ReadyActor):
    def __init__(self, output_tokens_per_row: int = 16, service_tokens_per_s: float = 50000.0):
        self.output_tokens_per_row = output_tokens_per_row
        self.service_tokens_per_s = service_tokens_per_s

    def complete(self, batch: pa.RecordBatch | pa.Table) -> dict:
        service_start = time.perf_counter()
        service_start_epoch = time.time()
        prompts = batch.column("text").to_pylist()
        input_token_count = sum(text_token_count(prompt) for prompt in prompts)
        output_token_count = max(0, self.output_tokens_per_row) * batch.num_rows
        token_count = input_token_count + output_token_count
        target_s = token_count / self.service_tokens_per_s
        if target_s > 0:
            time.sleep(target_s)
        outputs = [f"fake completion for doc {doc_id}" for doc_id in batch.column("doc_id").to_pylist()]
        service_s = time.perf_counter() - service_start
        service_end_epoch = time.time()
        return {
            "doc_id": batch.column("doc_id").to_pylist(),
            "tenant_id": batch.column("tenant_id").to_pylist(),
            "category": batch.column("category").to_pylist(),
            "output_text": outputs,
            "rows": batch.num_rows,
            "input_token_count": input_token_count,
            "output_token_count": output_token_count,
            "token_count": token_count,
            "service_s": service_s,
            "service_start_epoch_s": service_start_epoch,
            "service_end_epoch_s": service_end_epoch,
            "actor_worker_pid": os.getpid(),
        }


class CompatibleHTTPCompletionActor(_ReadyActor):
    def __init__(
        self,
        endpoint_url: str,
        model_name: str,
        api_key: str | None,
        timeout_s: float,
        max_tokens: int,
        return_token_ids: bool = False,
        prompt_format: CompletionPromptFormat = "raw",
        temperature: float | None = None,
        protocol: CompletionProtocol = "completions",
    ):
        self.endpoint_url = endpoint_url
        self.model_name = model_name
        self.api_key = api_key
        self.timeout_s = timeout_s
        self.max_tokens = max_tokens
        self.return_token_ids = return_token_ids
        self.prompt_format = prompt_format
        self.temperature = temperature
        self.protocol = protocol

    def complete(self, batch: pa.RecordBatch | pa.Table) -> dict:
        service_start = time.perf_counter()
        service_start_epoch = time.time()
        prompts = batch.column("text").to_pylist()
        endpoint_result = call_compatible_completion_endpoint(
            self.endpoint_url,
            self.model_name,
            prompts,
            self.api_key,
            self.timeout_s,
            self.max_tokens,
            return_token_ids=self.return_token_ids,
            prompt_format=self.prompt_format,
            temperature=self.temperature,
            protocol=self.protocol,
        )
        return _completion_actor_result(
            batch,
            prompts,
            endpoint_result,
            service_start=service_start,
            service_start_epoch=service_start_epoch,
        )


class CompatibleAsyncHTTPCompletionActor(_ReadyActor):
    """Persistent async HTTP client for request-level vLLM forwarding."""

    def __init__(
        self,
        endpoint_url: str,
        model_name: str,
        api_key: str | None,
        timeout_s: float,
        max_tokens: int,
        return_token_ids: bool = False,
        prompt_format: CompletionPromptFormat = "raw",
        temperature: float | None = None,
        protocol: CompletionProtocol = "completions",
        max_connections: int = 1,
    ):
        if max_connections <= 0:
            raise ValueError("max_connections must be positive")
        try:
            import httpx
        except ImportError as exc:
            raise RuntimeError(
                "async completion transport requires the 'httpx' package"
            ) from exc
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self.endpoint_url = endpoint_url
        self.model_name = model_name
        self.timeout_s = timeout_s
        self.max_tokens = max_tokens
        self.return_token_ids = return_token_ids
        self.prompt_format = prompt_format
        self.temperature = temperature
        self.protocol = protocol
        self.max_connections = max_connections
        self._httpx = httpx
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=timeout_s,
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            ),
        )

    async def ready(self) -> dict[str, object]:
        return {
            "actor_worker_pid": os.getpid(),
            "actor_type": type(self).__name__,
            "http_transport": "httpx_async",
            "max_connections": self.max_connections,
            "client_initialized": not self._client.is_closed,
        }

    async def close(self) -> None:
        await self._client.aclose()

    async def complete(self, batch: pa.RecordBatch | pa.Table) -> dict:
        service_start = time.perf_counter()
        service_start_epoch = time.time()
        prompts = batch.column("text").to_pylist()
        if self.protocol == "chat_completions" and len(prompts) > 1:
            endpoint_results = await asyncio.gather(
                *(self._complete_prompts([prompt]) for prompt in prompts)
            )
            endpoint_result = _combine_completion_endpoint_results(
                endpoint_results
            )
        else:
            endpoint_result = await self._complete_prompts(prompts)
        return _completion_actor_result(
            batch,
            prompts,
            endpoint_result,
            service_start=service_start,
            service_start_epoch=service_start_epoch,
        )

    async def _complete_prompts(
        self,
        prompts: list[str],
    ) -> CompletionEndpointResult:
        request_body = _completion_request_body(
            self.model_name,
            format_completion_prompts(prompts, self.prompt_format),
            self.max_tokens,
            self.protocol,
        )
        if self.return_token_ids:
            request_body["return_token_ids"] = True
        if self.temperature is not None:
            if (
                not math.isfinite(self.temperature)
                or self.temperature < 0
            ):
                raise ValueError(
                    "temperature must be finite and non-negative"
                )
            request_body["temperature"] = self.temperature
        http_request_start_epoch_s = time.time()
        http_request_start_s = time.perf_counter()
        try:
            async with self._client.stream(
                "POST",
                self.endpoint_url,
                json=request_body,
            ) as response:
                response.raise_for_status()
                http_response_headers_epoch_s = time.time()
                http_response_headers_s = time.perf_counter()
                body = await response.aread()
                http_response_body_epoch_s = time.time()
                http_response_body_s = time.perf_counter()
        except self._httpx.HTTPError as exc:
            raise RuntimeError(
                f"Completion endpoint request failed: {exc}"
            ) from exc
        endpoint_result = _decode_completion_endpoint_result(
            json.loads(body.decode("utf-8")),
            http_request_start_epoch_s=http_request_start_epoch_s,
            http_response_headers_epoch_s=http_response_headers_epoch_s,
            http_response_body_epoch_s=http_response_body_epoch_s,
            http_headers_wait_s=max(
                0.0,
                http_response_headers_s - http_request_start_s,
            ),
            http_body_read_s=max(
                0.0,
                http_response_body_s - http_response_headers_s,
            ),
        )
        return endpoint_result


def _combine_completion_endpoint_results(
    results: list[CompletionEndpointResult],
) -> CompletionEndpointResult:
    if not results:
        raise ValueError("completion endpoint results must not be empty")
    total_tokens = (
        sum(result.total_tokens for result in results)
        if all(result.total_tokens is not None for result in results)
        else None
    )
    return CompletionEndpointResult(
        outputs=[
            output
            for result in results
            for output in result.outputs
        ],
        total_tokens=total_tokens,
        output_token_counts=[
            count
            for result in results
            for count in result.output_token_counts
        ],
        finish_reasons=[
            reason
            for result in results
            for reason in result.finish_reasons
        ],
        http_request_start_epoch_s=min(
            result.http_request_start_epoch_s for result in results
        ),
        http_response_headers_epoch_s=max(
            result.http_response_headers_epoch_s for result in results
        ),
        http_response_body_epoch_s=max(
            result.http_response_body_epoch_s for result in results
        ),
        http_headers_wait_s=max(
            result.http_headers_wait_s for result in results
        ),
        http_body_read_s=max(
            result.http_body_read_s for result in results
        ),
    )


def _completion_actor_result(
    batch: pa.RecordBatch | pa.Table,
    prompts: list[str],
    endpoint_result: CompletionEndpointResult,
    *,
    service_start: float,
    service_start_epoch: float,
) -> dict:
    outputs = endpoint_result.outputs
    input_token_count = sum(text_token_count(prompt) for prompt in prompts)
    output_token_count = sum(text_token_count(output) for output in outputs)
    token_count = (
        endpoint_result.total_tokens
        if endpoint_result.total_tokens is not None
        else input_token_count + output_token_count
    )
    service_s = time.perf_counter() - service_start
    service_end_epoch = time.time()
    return {
        "doc_id": batch.column("doc_id").to_pylist(),
        "tenant_id": batch.column("tenant_id").to_pylist(),
        "category": batch.column("category").to_pylist(),
        "output_text": outputs,
        "output_token_counts": endpoint_result.output_token_counts,
        "finish_reasons": endpoint_result.finish_reasons,
        "rows": batch.num_rows,
        "input_token_count": input_token_count,
        "output_token_count": output_token_count,
        "token_count": token_count,
        "service_s": service_s,
        "service_start_epoch_s": service_start_epoch,
        "service_end_epoch_s": service_end_epoch,
        "http_request_start_epoch_s": (
            endpoint_result.http_request_start_epoch_s
        ),
        "http_response_headers_epoch_s": (
            endpoint_result.http_response_headers_epoch_s
        ),
        "http_response_body_epoch_s": (
            endpoint_result.http_response_body_epoch_s
        ),
        "http_headers_wait_s": endpoint_result.http_headers_wait_s,
        "http_body_read_s": endpoint_result.http_body_read_s,
        "actor_worker_pid": os.getpid(),
    }


class OllamaCompletionActor(_ReadyActor):
    def __init__(self, endpoint_url: str, model_name: str, api_key: str | None, timeout_s: float, max_tokens: int):
        self.endpoint_url = endpoint_url
        self.model_name = model_name
        self.timeout_s = timeout_s
        self.max_tokens = max_tokens

    def complete(self, batch: pa.RecordBatch | pa.Table) -> dict:
        service_start = time.perf_counter()
        service_start_epoch = time.time()
        prompts = batch.column("text").to_pylist()
        outputs, endpoint_tokens = call_ollama_completion_endpoint(
            self.endpoint_url,
            self.model_name,
            prompts,
            self.timeout_s,
            self.max_tokens,
        )
        input_token_count = sum(text_token_count(prompt) for prompt in prompts)
        output_token_count = sum(text_token_count(output) for output in outputs)
        token_count = endpoint_tokens if endpoint_tokens is not None else input_token_count + output_token_count
        service_s = time.perf_counter() - service_start
        service_end_epoch = time.time()
        return {
            "doc_id": batch.column("doc_id").to_pylist(),
            "tenant_id": batch.column("tenant_id").to_pylist(),
            "category": batch.column("category").to_pylist(),
            "output_text": outputs,
            "rows": batch.num_rows,
            "input_token_count": input_token_count,
            "output_token_count": output_token_count,
            "token_count": token_count,
            "service_s": service_s,
            "service_start_epoch_s": service_start_epoch,
            "service_end_epoch_s": service_end_epoch,
            "actor_worker_pid": os.getpid(),
        }


def fake_complete_batch(
    batch: pa.RecordBatch | pa.Table,
    output_tokens_per_row: int = 16,
    service_tokens_per_s: float = 50000.0,
) -> dict:
    return FakeCompletionActor(output_tokens_per_row, service_tokens_per_s).complete(batch)


def compatible_http_complete_batch(
    batch: pa.RecordBatch | pa.Table,
    endpoint_url: str,
    model_name: str,
    api_key: str | None,
    timeout_s: float,
    max_tokens: int,
    return_token_ids: bool = False,
    prompt_format: CompletionPromptFormat = "raw",
    temperature: float | None = None,
    protocol: CompletionProtocol = "completions",
) -> dict:
    return CompatibleHTTPCompletionActor(
        endpoint_url,
        model_name,
        api_key,
        timeout_s,
        max_tokens,
        return_token_ids,
        prompt_format,
        temperature,
        protocol,
    ).complete(batch)


def ollama_complete_batch(
    batch: pa.RecordBatch | pa.Table,
    endpoint_url: str,
    model_name: str,
    api_key: str | None,
    timeout_s: float,
    max_tokens: int,
) -> dict:
    return OllamaCompletionActor(endpoint_url, model_name, api_key, timeout_s, max_tokens).complete(batch)


def model_request_wall_time(results: list[dict]) -> float:
    starts = [float(result["service_start_epoch_s"]) for result in results if "service_start_epoch_s" in result]
    ends = [float(result["service_end_epoch_s"]) for result in results if "service_end_epoch_s" in result]
    if not starts or not ends:
        return 0.0
    return max(ends) - min(starts)
