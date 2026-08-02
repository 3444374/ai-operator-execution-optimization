"""Embedding endpoint calls and Ray actor adapters."""

from __future__ import annotations

import json
import os
import time
from urllib import request

import numpy as np
import pyarrow as pa

from .common import _ReadyActor, text_token_count


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
