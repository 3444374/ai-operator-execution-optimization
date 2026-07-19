"""Embedding model backends for AI operator profiling.

The HTTP path targets OpenAI-compatible embedding APIs, including vLLM-style
servers. The module keeps that compatibility detail out of the orchestration
script so future completion backends can live beside it without renaming the
whole pipeline around a single provider.
"""

from __future__ import annotations

import json
import time
from typing import Literal
from urllib import error, request

import numpy as np
import pyarrow as pa


EmbeddingBackendName = Literal["fake", "compatible_http", "http_openai"]
CompletionBackendName = Literal["fake", "compatible_http", "http_openai", "ollama"]


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
) -> tuple[list[str], int | None]:
    payload = json.dumps({"model": model_name, "prompt": prompts, "max_tokens": max_tokens}).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = request.Request(endpoint_url, data=payload, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=timeout_s) as response:
            body = response.read()
    except error.URLError as exc:
        raise RuntimeError(f"Completion endpoint request failed: {exc}") from exc
    decoded = json.loads(body.decode("utf-8"))
    choices = sorted(decoded["choices"], key=lambda item: item.get("index", 0))
    outputs = []
    for choice in choices:
        if "text" in choice:
            outputs.append(str(choice["text"]))
        else:
            outputs.append(str(choice.get("message", {}).get("content", "")))
    usage = decoded.get("usage") or {}
    total_tokens = usage.get("total_tokens")
    return outputs, int(total_tokens) if total_tokens is not None else None


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


class FakeEmbeddingActor:
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
        }


class CompatibleHTTPEmbeddingActor:
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


class FakeCompletionActor:
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
        }


class CompatibleHTTPCompletionActor:
    def __init__(self, endpoint_url: str, model_name: str, api_key: str | None, timeout_s: float, max_tokens: int):
        self.endpoint_url = endpoint_url
        self.model_name = model_name
        self.api_key = api_key
        self.timeout_s = timeout_s
        self.max_tokens = max_tokens

    def complete(self, batch: pa.RecordBatch | pa.Table) -> dict:
        service_start = time.perf_counter()
        service_start_epoch = time.time()
        prompts = batch.column("text").to_pylist()
        outputs, endpoint_tokens = call_compatible_completion_endpoint(
            self.endpoint_url,
            self.model_name,
            prompts,
            self.api_key,
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
        }


class OllamaCompletionActor:
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
) -> dict:
    return CompatibleHTTPCompletionActor(endpoint_url, model_name, api_key, timeout_s, max_tokens).complete(batch)


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
