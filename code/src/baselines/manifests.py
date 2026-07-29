"""Canonical JSONL request manifests and deterministic endpoint sharding."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Iterable

from .contracts import ChatRequest, ManifestMetadata


def _validated_requests(
    requests: Iterable[ChatRequest],
) -> tuple[ChatRequest, ...]:
    materialized = tuple(requests)
    seen: set[int] = set()
    for request in materialized:
        if request.doc_id in seen:
            raise ValueError(f"duplicate doc_id in manifest: {request.doc_id}")
        if request.prompt_tokens < 0 or request.estimated_output_tokens < 0:
            raise ValueError("manifest token estimates must be non-negative")
        if request.max_output_tokens < 0:
            raise ValueError("max_output_tokens must be non-negative")
        seen.add(request.doc_id)
    return materialized


def _canonical_bytes(requests: tuple[ChatRequest, ...]) -> bytes:
    rows = (
        json.dumps(
            asdict(request),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for request in requests
    )
    return ("\n".join(rows) + ("\n" if requests else "")).encode("utf-8")


def write_manifest(
    path: str | Path,
    requests: Iterable[ChatRequest],
) -> ManifestMetadata:
    """Write a new canonical manifest without overwriting prior evidence."""

    destination = Path(path)
    materialized = _validated_requests(requests)
    payload = _canonical_bytes(materialized)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as stream:
        stream.write(payload)
    return ManifestMetadata(
        row_count=len(materialized),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def read_manifest(path: str | Path) -> tuple[ChatRequest, ...]:
    """Read and validate a canonical request manifest."""

    source = Path(path)
    requests = tuple(
        ChatRequest(**json.loads(line))
        for line in source.read_text(encoding="utf-8").splitlines()
    )
    return _validated_requests(requests)


def assign_endpoint_shards(
    requests: Iterable[ChatRequest],
    endpoint_count: int,
) -> tuple[ChatRequest, ...]:
    """Assign work using stable largest-work-first while preserving row order."""

    if endpoint_count <= 0:
        raise ValueError("endpoint_count must be positive")
    materialized = _validated_requests(requests)
    endpoint_work = [0] * endpoint_count
    assignments: dict[int, int] = {}
    for request in sorted(
        materialized,
        key=lambda row: (-row.estimated_work, row.doc_id),
    ):
        endpoint_index = min(
            range(endpoint_count),
            key=lambda index: (endpoint_work[index], index),
        )
        assignments[request.doc_id] = endpoint_index
        endpoint_work[endpoint_index] += request.estimated_work
    return tuple(
        replace(
            request,
            endpoint_index=assignments[request.doc_id],
        )
        for request in materialized
    )
