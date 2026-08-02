"""Profile a pgai SQL embedding surface.

This script is intentionally separate from postgres_ai_operator_profile.py so the
job-table simulated trigger and the real SQL trigger surface stay easy to
compare. Results are feasibility evidence unless the caller writes them to a
motivation result directory and documents the experiment boundary.
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import psycopg


SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS ai CASCADE;

CREATE TABLE IF NOT EXISTS pgai_profile_documents (
  doc_id BIGINT PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  text TEXT NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pgai_profile_embeddings (
  doc_id BIGINT PRIMARY KEY REFERENCES pgai_profile_documents(doc_id),
  tenant_id INTEGER NOT NULL,
  category TEXT NOT NULL,
  embedding vector(384),
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile pgai SQL embedding calls.")
    parser.add_argument(
        "--database-url",
        default="postgresql://postgres:postgres@localhost:5433/postgres",
    )
    parser.add_argument("--setup", action="store_true")
    parser.add_argument("--seed-rows", type=int, default=0)
    parser.add_argument("--total-rows", type=int, default=1024)
    parser.add_argument("--sql-batch-rows", type=int, default=256)
    parser.add_argument("--embedding-model", default="all-minilm")
    parser.add_argument("--warmup-runs", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--experiment-id", default="pgai_sql_manual")
    parser.add_argument("--output", default="feasibility/results/pgai_sql_profile.csv")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url)


def setup_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def database_metadata(conn: psycopg.Connection) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        server_version = str(cur.fetchone()[0])
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        vector_row = cur.fetchone()
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'ai'")
        ai_row = cur.fetchone()
    return {
        "server_version": server_version,
        "pgvector_version": str(vector_row[0]) if vector_row else "not_installed",
        "pgai_version": str(ai_row[0]) if ai_row else "not_installed",
    }


def seed_documents(conn: psycopg.Connection, target_rows: int) -> None:
    if target_rows <= 0:
        return
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pgai_profile_documents")
        existing = int(cur.fetchone()[0])
        if existing >= target_rows:
            return
        rows = []
        for doc_id in range(existing + 1, target_rows + 1):
            rows.append(
                (
                    doc_id,
                    doc_id % 8,
                    f"category_{doc_id % 5}",
                    (
                        f"Document {doc_id} for pgai SQL embedding profile. "
                        "This text is deterministic and used to validate the "
                        "database SQL trigger surface."
                    ),
                )
            )
        cur.executemany(
            """
            INSERT INTO pgai_profile_documents (doc_id, tenant_id, category, text)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (doc_id) DO NOTHING
            """,
            rows,
        )
    conn.commit()


def run_sql_batch(
    conn: psycopg.Connection,
    model: str,
    limit: int,
    offset: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH src AS (
              SELECT doc_id, tenant_id, category, text
              FROM pgai_profile_documents
              ORDER BY doc_id
              LIMIT %s OFFSET %s
            ),
            upserted AS (
              INSERT INTO pgai_profile_embeddings
                (doc_id, tenant_id, category, embedding, updated_at)
              SELECT
                doc_id,
                tenant_id,
                category,
                ai.ollama_embed(%s, text)::vector(384),
                CURRENT_TIMESTAMP
              FROM src
              ON CONFLICT (doc_id) DO UPDATE
              SET tenant_id = EXCLUDED.tenant_id,
                  category = EXCLUDED.category,
                  embedding = EXCLUDED.embedding,
                  updated_at = CURRENT_TIMESTAMP
              RETURNING doc_id
            )
            SELECT COUNT(*) FROM upserted
            """,
            (limit, offset, model),
        )
        written_rows = int(cur.fetchone()[0])
    conn.commit()
    return written_rows


def run_once(args: argparse.Namespace, phase: str, repeat_index: int) -> dict:
    if args.dry_run:
        return {
            "status": "dry_run",
            "experiment_id": args.experiment_id,
            "phase": phase,
            "repeat_index": repeat_index,
            "database_trigger": "pgai_sql",
            "embedding_model": args.embedding_model,
            "total_rows": args.total_rows,
            "sql_batch_rows": args.sql_batch_rows,
        }

    conn = connect(args.database_url)
    try:
        if args.setup:
            setup_schema(conn)
            seed_documents(conn, args.seed_rows)
        metadata = database_metadata(conn)

        e2e_start = time.perf_counter()
        processed_rows = 0
        sql_statements = 0
        sql_embed_writeback_s = 0.0

        while processed_rows < args.total_rows:
            batch_rows = min(args.sql_batch_rows, args.total_rows - processed_rows)
            batch_start = time.perf_counter()
            written = run_sql_batch(conn, args.embedding_model, batch_rows, processed_rows)
            sql_embed_writeback_s += time.perf_counter() - batch_start
            if written == 0:
                break
            processed_rows += written
            sql_statements += 1

        e2e_s = time.perf_counter() - e2e_start
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*), MIN(vector_dims(embedding)), MAX(vector_dims(embedding))
                FROM pgai_profile_embeddings
                WHERE doc_id BETWEEN 1 AND %s
                """,
                (processed_rows,),
            )
            count_rows, min_dims, max_dims = cur.fetchone()

        return {
            "status": "ok",
            "experiment_id": args.experiment_id,
            "phase": phase,
            "repeat_index": repeat_index,
            **metadata,
            "database_trigger": "pgai_sql",
            "executor": "sql",
            "strategy": "coalesced",
            "model_backend": "ollama",
            "embedding_model": args.embedding_model,
            "total_rows": processed_rows,
            "written_rows": int(count_rows),
            "sql_batch_rows": args.sql_batch_rows,
            "embedding_dim": int(max_dims or 0),
            "min_embedding_dim": int(min_dims or 0),
            "sql_statements": sql_statements,
            "ai_function_calls": processed_rows,
            "sql_embed_writeback_s": round(sql_embed_writeback_s, 6),
            "e2e_s": round(e2e_s, 6),
            "rows_per_s": round(processed_rows / e2e_s, 3) if e2e_s else 0.0,
            "boundary_note": "SQL embedding and pgvector writeback are not separated",
        }
    finally:
        conn.close()


def append_metrics(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    args = parse_args()
    phases = [("warmup", i) for i in range(args.warmup_runs)]
    phases.extend(("formal", i) for i in range(args.repeats))
    for phase, repeat_index in phases:
        row = run_once(args, phase, repeat_index)
        append_metrics(Path(args.output), row)
        print(row)


if __name__ == "__main__":
    main()
