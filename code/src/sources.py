"""Data source backends for PostgreSQL-driven AI operator profiles."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa


SourceName = Literal["arrow_postgres", "daft_postgres"]
SourceOrder = Literal["doc_id", "arrival_time"]


@dataclass(frozen=True)
class SourceConfig:
    limit: int
    offset: int
    workload_name: str | None = None
    order: SourceOrder = "doc_id"


@dataclass(frozen=True)
class SourceBatch:
    table: pa.Table | None
    metrics: dict[str, float]


def _validate_config(config: SourceConfig) -> None:
    if config.limit <= 0:
        raise ValueError("limit must be positive")
    if config.offset < 0:
        raise ValueError("offset must be non-negative")
    if config.order not in ("doc_id", "arrival_time"):
        raise ValueError(f"unknown source order: {config.order}")


def _order_by_sql(order: SourceOrder) -> str:
    if order == "doc_id":
        return "doc_id"
    return "arrival_time_s NULLS LAST, doc_id"


def postgres_documents_query(config: SourceConfig) -> tuple[str, tuple[int, int]]:
    _validate_config(config)
    where_sql = ""
    params: tuple[object, ...]
    if config.workload_name:
        where_sql = "WHERE workload_name = %s"
        params = (config.workload_name, config.limit, config.offset)
    else:
        params = (config.limit, config.offset)
    return (
        f"""
        SELECT
            doc_id,
            tenant_id,
            category,
            text,
            workload_name,
            prompt_tokens,
            target_output_tokens,
            arrival_time_s,
            session_id,
            prefix_key
        FROM documents
        {where_sql}
        ORDER BY {_order_by_sql(config.order)}
        LIMIT %s OFFSET %s
        """,
        params,
    )


class PostgresArrowSource:
    name = "arrow_postgres"

    def fetch(self, conn, config: SourceConfig) -> SourceBatch:
        fetch_start = time.perf_counter()
        sql, params = postgres_documents_query(config)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        db_fetch_s = time.perf_counter() - fetch_start
        if not rows:
            return SourceBatch(table=None, metrics={"db_fetch_s": db_fetch_s, "arrow_build_s": 0.0})

        arrow_start = time.perf_counter()
        columns = list(zip(*rows, strict=True))
        table = pa.table(
            {
                "doc_id": pa.array(columns[0], type=pa.int64()),
                "tenant_id": pa.array(columns[1], type=pa.int32()),
                "category": pa.array(columns[2], type=pa.string()),
                "text": pa.array(columns[3], type=pa.string()),
                "workload_name": pa.array(columns[4], type=pa.string()),
                "prompt_tokens": pa.array(columns[5], type=pa.int32()),
                "target_output_tokens": pa.array(columns[6], type=pa.int32()),
                "arrival_time_s": pa.array(columns[7], type=pa.float64()),
                "session_id": pa.array(columns[8], type=pa.string()),
                "prefix_key": pa.array(columns[9], type=pa.string()),
            }
        )
        arrow_build_s = time.perf_counter() - arrow_start
        return SourceBatch(table=table, metrics={"db_fetch_s": db_fetch_s, "arrow_build_s": arrow_build_s})


def daft_sql_query(config: SourceConfig) -> str:
    _validate_config(config)
    where_sql = ""
    if config.workload_name:
        workload_name = config.workload_name.replace("'", "''")
        where_sql = f"WHERE workload_name = '{workload_name}' "
    return (
        "SELECT doc_id, tenant_id, category, text, workload_name, prompt_tokens, "
        "target_output_tokens, arrival_time_s, session_id, prefix_key "
        "FROM documents "
        f"{where_sql}"
        f"ORDER BY {_order_by_sql(config.order)} "
        f"LIMIT {config.limit} OFFSET {config.offset}"
    )


class DaftPostgresSource:
    name = "daft_postgres"

    def fetch(self, database_url: str, config: SourceConfig) -> SourceBatch:
        import daft

        sql = daft_sql_query(config)
        read_start = time.perf_counter()
        df = daft.read_sql(sql, database_url)
        table = df.to_arrow()
        read_s = time.perf_counter() - read_start
        if table.num_rows == 0:
            return SourceBatch(table=None, metrics={"db_fetch_s": read_s, "arrow_build_s": 0.0})
        return SourceBatch(table=table, metrics={"db_fetch_s": read_s, "arrow_build_s": 0.0})


def make_source(name: SourceName):
    if name == "arrow_postgres":
        return PostgresArrowSource()
    if name == "daft_postgres":
        return DaftPostgresSource()
    raise ValueError(f"Unknown source: {name}")
