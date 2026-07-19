"""Result sinks for AI operator profiling."""

from __future__ import annotations

import json
from typing import Iterable, Literal

import numpy as np


WritebackMode = Literal["none", "json_text", "pgvector"]


def vector_to_pg_literal(vector: np.ndarray) -> str:
    return json.dumps(vector.tolist(), separators=(",", ":"))


def batched_rows(rows: list[tuple], batch_rows: int) -> Iterable[list[tuple]]:
    if batch_rows <= 0:
        yield rows
        return
    for start in range(0, len(rows), batch_rows):
        yield rows[start : start + batch_rows]


def write_embeddings(conn, results: list[dict], writeback_mode: WritebackMode, write_batch_rows: int) -> int:
    if writeback_mode == "none":
        return 0
    rows = []
    for result in results:
        vectors = result["embedding"]
        for i, doc_id in enumerate(result["doc_id"]):
            if writeback_mode in {"json_text", "pgvector"}:
                rows.append(
                    (
                        doc_id,
                        result["tenant_id"][i],
                        result["category"][i],
                        vector_to_pg_literal(vectors[i]),
                    )
                )
            else:
                raise ValueError(f"Unsupported writeback mode: {writeback_mode}")
    with conn.cursor() as cur:
        if writeback_mode == "json_text":
            statement = """
                INSERT INTO document_embeddings (doc_id, tenant_id, category, embedding_json)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (doc_id) DO UPDATE
                SET tenant_id = EXCLUDED.tenant_id,
                    category = EXCLUDED.category,
                    embedding_json = EXCLUDED.embedding_json,
                    updated_at = CURRENT_TIMESTAMP
                """
        else:
            statement = """
                INSERT INTO document_embeddings (doc_id, tenant_id, category, embedding_json, embedding_vector)
                VALUES (%s, %s, %s, '[]', %s::vector)
                ON CONFLICT (doc_id) DO UPDATE
                SET tenant_id = EXCLUDED.tenant_id,
                    category = EXCLUDED.category,
                    embedding_json = EXCLUDED.embedding_json,
                    embedding_vector = EXCLUDED.embedding_vector,
                    updated_at = CURRENT_TIMESTAMP
                """
        for chunk in batched_rows(rows, write_batch_rows):
            cur.executemany(statement, chunk)
    conn.commit()
    return len(rows)


def write_completions(conn, results: list[dict], writeback_mode: WritebackMode, write_batch_rows: int) -> int:
    if writeback_mode == "none":
        return 0
    if writeback_mode == "pgvector":
        raise ValueError("pgvector writeback is only valid for embedding results")
    rows = []
    for result in results:
        outputs = result["output_text"]
        for i, doc_id in enumerate(result["doc_id"]):
            output_text = str(outputs[i])
            rows.append(
                (
                    doc_id,
                    result["tenant_id"][i],
                    result["category"][i],
                    output_text,
                    json.dumps({"text": output_text}, ensure_ascii=False, separators=(",", ":")),
                )
            )
    with conn.cursor() as cur:
        statement = """
            INSERT INTO document_completions (doc_id, tenant_id, category, completion_text, completion_json)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (doc_id) DO UPDATE
            SET tenant_id = EXCLUDED.tenant_id,
                category = EXCLUDED.category,
                completion_text = EXCLUDED.completion_text,
                completion_json = EXCLUDED.completion_json,
                updated_at = CURRENT_TIMESTAMP
            """
        for chunk in batched_rows(rows, write_batch_rows):
            cur.executemany(statement, chunk)
    conn.commit()
    return len(rows)
