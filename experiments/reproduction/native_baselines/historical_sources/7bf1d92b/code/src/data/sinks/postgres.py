"""PostgreSQL result sinks for completion and embedding operators."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable, Literal

import numpy as np


WritebackMode = Literal["none", "json_text", "pgvector"]


EMBEDDING_JSON_UPSERT = """
    INSERT INTO document_embeddings (doc_id, tenant_id, category, embedding_json)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (doc_id) DO UPDATE
    SET tenant_id = EXCLUDED.tenant_id,
        category = EXCLUDED.category,
        embedding_json = EXCLUDED.embedding_json,
        updated_at = CURRENT_TIMESTAMP
    """

EMBEDDING_VECTOR_UPSERT = """
    INSERT INTO document_embeddings (doc_id, tenant_id, category, embedding_json, embedding_vector)
    VALUES (%s, %s, %s, '[]', %s::vector)
    ON CONFLICT (doc_id) DO UPDATE
    SET tenant_id = EXCLUDED.tenant_id,
        category = EXCLUDED.category,
        embedding_json = EXCLUDED.embedding_json,
        embedding_vector = EXCLUDED.embedding_vector,
        updated_at = CURRENT_TIMESTAMP
    """

COMPLETION_UPSERT = """
    INSERT INTO document_completions (doc_id, tenant_id, category, completion_text, completion_json)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (doc_id) DO UPDATE
    SET tenant_id = EXCLUDED.tenant_id,
        category = EXCLUDED.category,
        completion_text = EXCLUDED.completion_text,
        completion_json = EXCLUDED.completion_json,
        updated_at = CURRENT_TIMESTAMP
    """


@dataclass(frozen=True)
class PostgresWritePlan:
    """Normalized rows and SQL, without transaction ownership."""

    statement: str
    rows: tuple[tuple[object, ...], ...]


def vector_to_pg_literal(vector: np.ndarray) -> str:
    return json.dumps(vector.tolist(), separators=(",", ":"))


def batched_rows(rows: list[tuple], batch_rows: int) -> Iterable[list[tuple]]:
    if batch_rows <= 0:
        yield rows
        return
    for start in range(0, len(rows), batch_rows):
        yield rows[start : start + batch_rows]


def prepare_embedding_write(
    results: list[dict],
    writeback_mode: WritebackMode,
) -> PostgresWritePlan | None:
    """Normalize engine results into an embedding upsert plan."""

    if writeback_mode == "none":
        return None
    rows: list[tuple[object, ...]] = []
    for result in results:
        vectors = result["embedding"]
        for index, doc_id in enumerate(result["doc_id"]):
            if writeback_mode not in {"json_text", "pgvector"}:
                raise ValueError(
                    f"Unsupported writeback mode: {writeback_mode}"
                )
            rows.append(
                (
                    doc_id,
                    result["tenant_id"][index],
                    result["category"][index],
                    vector_to_pg_literal(vectors[index]),
                )
            )
    statement = (
        EMBEDDING_JSON_UPSERT
        if writeback_mode == "json_text"
        else EMBEDDING_VECTOR_UPSERT
    )
    return PostgresWritePlan(statement=statement, rows=tuple(rows))


def prepare_completion_write(
    results: list[dict],
    writeback_mode: WritebackMode,
) -> PostgresWritePlan | None:
    """Normalize engine results into a completion upsert plan."""

    if writeback_mode == "none":
        return None
    if writeback_mode == "pgvector":
        raise ValueError("pgvector writeback is only valid for embedding results")
    rows: list[tuple[object, ...]] = []
    for result in results:
        outputs = result["output_text"]
        for index, doc_id in enumerate(result["doc_id"]):
            output_text = str(outputs[index])
            rows.append(
                (
                    doc_id,
                    result["tenant_id"][index],
                    result["category"][index],
                    output_text,
                    json.dumps(
                        {"text": output_text},
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                )
            )
    return PostgresWritePlan(statement=COMPLETION_UPSERT, rows=tuple(rows))


def execute_write_plan(
    conn,
    plan: PostgresWritePlan | None,
    write_batch_rows: int,
) -> int:
    """Execute one prepared plan without committing its connection."""

    if plan is None:
        return 0
    with conn.cursor() as cur:
        for chunk in batched_rows(list(plan.rows), write_batch_rows):
            cur.executemany(plan.statement, chunk)
    return len(plan.rows)


def write_embeddings(conn, results: list[dict], writeback_mode: WritebackMode, write_batch_rows: int) -> int:
    """Compatibility wrapper that preserves the historical implicit commit."""

    plan = prepare_embedding_write(results, writeback_mode)
    written = execute_write_plan(conn, plan, write_batch_rows)
    if plan is None:
        return written
    conn.commit()
    return written


def write_completions(conn, results: list[dict], writeback_mode: WritebackMode, write_batch_rows: int) -> int:
    """Compatibility wrapper that preserves the historical implicit commit."""

    plan = prepare_completion_write(results, writeback_mode)
    written = execute_write_plan(conn, plan, write_batch_rows)
    if plan is None:
        return written
    conn.commit()
    return written
