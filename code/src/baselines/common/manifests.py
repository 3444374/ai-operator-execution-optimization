"""Canonical JSONL request manifests and deterministic endpoint sharding."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path
from typing import Iterable, Mapping

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
    """Assign work using stable largest-work-first while preserving row order.

    This is the ``preexecution_token_work_balanced`` policy: it balances
    ``estimated_work = prompt_tokens + estimated_output_tokens`` -- a value
    computable BEFORE execution (``estimated_output_tokens`` is a pre-submission
    estimate such as the fixed output cap, NOT the real generation length). It is
    therefore a static baseline, NOT an oracle (a real oracle would need the
    actual output/service time, which is future-leakage).
    """

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


# Stable partition-policy names. ``preexecution_token_work_balanced`` is the
# largest-work-first policy above (kept spelled out -- not "prompt_token" and not
# "oracle"; for SQuAD fixed-cap it balances prompt_tokens + the fixed cap).
PARTITION_POLICIES = ("equal_rows", "preexecution_token_work_balanced")


def assign_endpoint_equal_rows(
    requests: Iterable[ChatRequest],
    endpoint_count: int,
    seed: int = 0,
) -> tuple[ChatRequest, ...]:
    """Deterministic equal-rows sharding (stable SHA256 sort + round-robin).

    Does NOT use Python ``hash()`` (process-randomized via PYTHONHASHSEED). Sorts
    doc_ids by ``sha256(f"{seed}:{doc_id}")`` -- a stable, seed-controlled key
    (doc_id as a deterministic tie-break) -- then round-robin assigns that sorted
    stream to endpoints. Guarantees ``|rows_i - rows_j| <= 1`` for any input
    (exact 128:128 for 256 rows / 2 endpoints). Input order does NOT change the
    doc_id -> endpoint mapping; the returned tuple preserves the original manifest
    row order. Represents the simplest no-cost-estimate static database sharding.
    """

    if endpoint_count <= 0:
        raise ValueError("endpoint_count must be positive")
    materialized = _validated_requests(requests)

    def sort_key(request: ChatRequest) -> tuple[str, int]:
        digest = hashlib.sha256(
            f"{seed}:{request.doc_id}".encode("utf-8")
        ).hexdigest()
        return (digest, request.doc_id)

    hash_sorted = sorted(materialized, key=sort_key)
    assignments = {
        request.doc_id: position % endpoint_count
        for position, request in enumerate(hash_sorted)
    }
    return tuple(
        replace(request, endpoint_index=assignments[request.doc_id])
        for request in materialized
    )


def assign_endpoints(
    requests: Iterable[ChatRequest],
    endpoint_count: int,
    *,
    policy: str,
    seed: int = 0,
) -> tuple[ChatRequest, ...]:
    """Dispatch to the named partition policy (single route point for the CLI)."""

    if policy == "equal_rows":
        return assign_endpoint_equal_rows(requests, endpoint_count, seed)
    if policy == "preexecution_token_work_balanced":
        return assign_endpoint_shards(requests, endpoint_count)
    raise ValueError(
        f"unsupported partition policy {policy!r}; "
        f"expected one of {PARTITION_POLICIES}"
    )


def partition_summary(
    assigned: Iterable[ChatRequest],
    endpoint_count: int,
) -> dict[str, object]:
    """Per-endpoint row/token-work distribution + row-count diff + work skew.

    Pure; used by the manifest exporter metadata AND auditable independently of
    the gate. ``endpoint_row_count_diff`` is an absolute count (the equal-rows
    gate hard-checks ``<= 1``); ``endpoint_work_skew`` is a ratio in [0, 1) (the
    work-balanced gate hard-checks ``<= max_endpoint_work_skew``).
    """

    if endpoint_count <= 0:
        raise ValueError("endpoint_count must be positive")
    rows = [0] * endpoint_count
    prompt_tokens = [0] * endpoint_count
    output_work = [0] * endpoint_count
    total_work = [0] * endpoint_count
    observed_endpoints: set[int] = set()
    for request in assigned:
        if not 0 <= request.endpoint_index < endpoint_count:
            raise ValueError(
                f"endpoint_index {request.endpoint_index} outside "
                f"[0, {endpoint_count})"
            )
        observed_endpoints.add(request.endpoint_index)
        index = request.endpoint_index
        rows[index] += 1
        prompt_tokens[index] += request.prompt_tokens
        output_work[index] += request.estimated_output_tokens
        total_work[index] += request.estimated_work
    if observed_endpoints != set(range(endpoint_count)):
        raise ValueError(
            f"assignment did not use every endpoint: used {sorted(observed_endpoints)}"
        )
    row_diff = max(rows) - min(rows) if rows else 0
    work_skew = (
        (max(total_work) - min(total_work)) / max(total_work)
        if total_work and max(total_work) > 0
        else 0.0
    )
    return {
        "endpoint_row_counts": {i: rows[i] for i in range(endpoint_count)},
        "endpoint_prompt_tokens": {i: prompt_tokens[i] for i in range(endpoint_count)},
        "endpoint_estimated_output_work": {
            i: output_work[i] for i in range(endpoint_count)
        },
        "endpoint_total_estimated_work": {i: total_work[i] for i in range(endpoint_count)},
        # Backward-compatible alias (pre-existing consumers read "endpoint_work").
        "endpoint_work": {i: total_work[i] for i in range(endpoint_count)},
        "endpoint_row_count_diff": row_diff,
        "endpoint_work_skew": work_skew,
    }


def manifest_metadata_path(manifest_path: str | Path) -> Path:
    """The sidecar that records a manifest's partition provenance."""

    return Path(str(manifest_path) + ".meta.json")


def write_manifest_metadata(
    manifest_path: str | Path,
    *,
    partition_policy: str | None,
    partition_seed: int,
    row_count: int,
    manifest_sha256: str,
    partition_summary_dict: Mapping[str, object],
) -> Path:
    """Write ``<manifest>.meta.json`` recording the partition policy actually used.

    The gate reads this to verify the manifest's REAL policy matches the policy the
    gate config declares (closes the config-vs-manifest mismatch hole: an operator can
    no longer silently apply the wrong hard gate by mis-declaring the policy). The
    manifest JSONL itself stays canonical (ChatRequest-per-line, unchanged).
    """

    meta_path = manifest_metadata_path(manifest_path)
    payload = {
        "partition_policy": partition_policy,
        "partition_seed": partition_seed,
        "row_count": row_count,
        "manifest_sha256": manifest_sha256,
        **partition_summary_dict,
    }
    meta_path.write_text(
        json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8"
    )
    return meta_path


def read_manifest_metadata(manifest_path: str | Path) -> dict[str, object] | None:
    """Read the partition-provenance sidecar, or None if absent (legacy manifest)."""

    meta_path = manifest_metadata_path(manifest_path)
    if not meta_path.is_file():
        return None
    return json.loads(meta_path.read_text(encoding="utf-8"))
