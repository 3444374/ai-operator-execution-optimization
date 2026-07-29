"""Read an immutable baseline request slice from PostgreSQL."""

from __future__ import annotations

import hashlib
import json
from typing import Literal, Protocol

from .contracts import ChatRequest


EstimatedOutputMode = Literal["fixed_cap", "trace_target"]


class CursorLike(Protocol):
    def execute(
        self,
        sql: str,
        params: tuple[object, ...],
    ) -> object: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...

    def __enter__(self) -> CursorLike: ...

    def __exit__(self, *args: object) -> object: ...


class ConnectionLike(Protocol):
    def cursor(self) -> CursorLike: ...


def source_row_hash(
    *,
    workload_name: str,
    doc_id: int,
    prompt: str,
    arrival_time_s: float,
    prompt_tokens: int,
    target_output_tokens: int,
) -> str:
    payload = json.dumps(
        {
            "arrival_time_s": arrival_time_s,
            "doc_id": doc_id,
            "prompt": prompt,
            "prompt_tokens": prompt_tokens,
            "target_output_tokens": target_output_tokens,
            "workload_name": workload_name,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def load_postgres_requests(
    connection: ConnectionLike,
    *,
    workload_name: str,
    row_count: int,
    row_offset: int,
    max_output_tokens: int,
    estimated_output_mode: EstimatedOutputMode,
) -> tuple[ChatRequest, ...]:
    """Load one stable complete-row slice without modifying PostgreSQL."""

    if not workload_name:
        raise ValueError("workload_name must be non-empty")
    if row_count <= 0:
        raise ValueError("row_count must be positive")
    if row_offset < 0:
        raise ValueError("row_offset must be non-negative")
    if max_output_tokens < 0:
        raise ValueError("max_output_tokens must be non-negative")
    if estimated_output_mode not in {"fixed_cap", "trace_target"}:
        raise ValueError(
            f"unknown estimated_output_mode: {estimated_output_mode}"
        )

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                doc_id,
                text,
                arrival_time_s,
                prompt_tokens,
                target_output_tokens
            FROM documents
            WHERE workload_name = %s
            ORDER BY doc_id
            LIMIT %s OFFSET %s
            """,
            (workload_name, row_count, row_offset),
        )
        rows = cursor.fetchall()

    if len(rows) != row_count:
        raise ValueError(
            f"expected {row_count} rows, observed {len(rows)}"
        )

    requests: list[ChatRequest] = []
    doc_ids: set[int] = set()
    for raw_row in rows:
        (
            raw_doc_id,
            raw_prompt,
            raw_arrival_time_s,
            raw_prompt_tokens,
            raw_target_output_tokens,
        ) = raw_row
        doc_id = int(raw_doc_id)
        if doc_id in doc_ids:
            raise ValueError(f"duplicate doc_id in source slice: {doc_id}")
        doc_ids.add(doc_id)
        prompt = str(raw_prompt)
        arrival_time_s = float(raw_arrival_time_s or 0.0)
        prompt_tokens = int(raw_prompt_tokens)
        target_output_tokens = int(raw_target_output_tokens)
        if prompt_tokens < 0 or target_output_tokens < 0:
            raise ValueError(
                f"negative token metadata for doc_id={doc_id}"
            )
        estimated_output_tokens = (
            max_output_tokens
            if estimated_output_mode == "fixed_cap"
            else min(target_output_tokens, max_output_tokens)
        )
        requests.append(
            ChatRequest(
                doc_id=doc_id,
                prompt=prompt,
                arrival_time_s=arrival_time_s,
                prompt_tokens=prompt_tokens,
                max_output_tokens=max_output_tokens,
                estimated_output_tokens=estimated_output_tokens,
                source_row_hash=source_row_hash(
                    workload_name=workload_name,
                    doc_id=doc_id,
                    prompt=prompt,
                    arrival_time_s=arrival_time_s,
                    prompt_tokens=prompt_tokens,
                    target_output_tokens=target_output_tokens,
                ),
                endpoint_index=-1,
            )
        )
    return tuple(requests)
