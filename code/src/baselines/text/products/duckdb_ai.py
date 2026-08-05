"""DuckDB ``ai`` community-extension AI_COMPLETE adapter (native execution).

The DuckDB ``ai`` community extension provides SQL ``ai_complete`` that calls an
external OpenAI-compatible endpoint. This adapter treats DuckDB + the extension
as a database-product-native AI_COMPLETE baseline: it only materializes the
immutable ``ChatRequest`` shard into a DuckDB table and issues one set-oriented
``SELECT ai_complete(...)``. The extension owns HTTP call batching, retry,
caching and concurrency (``duckdb_ai_max_concurrent_requests``); no project
scheduling code is injected, matching the native-baseline policy in
``experiments/plans/baseline_reference.md``.

Two operational constraints are documented here so they are not re-discovered:

* The ``ai`` extension binary is built per DuckDB version. It exists for
  DuckDB 1.5.4 and is absent for 1.5.5, so the driver venv must pin
  ``duckdb==1.5.4`` and run ``INSTALL ai FROM community`` once.
* The default provider is ``ollama``; an OpenAI-compatible vLLM endpoint must be
  selected with ``SET duckdb_ai_provider = 'openai_compatible'`` plus a
  ``TYPE duckdb_ai`` secret carrying ``BASE_URL``.

Results must be labelled "community extension" baseline, not DuckDB-core native.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Iterable

from src.baselines.common.contracts import BaselineRequestResult, ChatRequest


@dataclass(frozen=True)
class DuckDBAiConfig:
    """Endpoint and generation controls for the DuckDB ``ai`` extension.

    endpoint_base_url:
        OpenAI-compatible root, e.g. ``http://127.0.0.1:8000/v1``. The
        ``openai_compatible`` provider appends ``/chat/completions``.
    model:
        Served model name accepted by the endpoint, e.g. ``qwen2.5-7b``.
    api_key:
        Sentinel key for the local endpoint (commonly ``"EMPTY"``).
    max_tokens:
        Per-row completion cap; must equal the shard's
        ``ChatRequest.max_output_tokens``.
    database_path:
        DuckDB connection target (default in-memory).
    """

    endpoint_base_url: str
    model: str
    api_key: str
    max_tokens: int
    database_path: str = ":memory:"

    def __post_init__(self) -> None:
        if not self.endpoint_base_url:
            raise ValueError("endpoint_base_url must be non-empty")
        if not self.model:
            raise ValueError("model must be non-empty")
        if self.max_tokens < 0:
            raise ValueError("max_tokens must be non-negative")


ConnectionFactory = Callable[[DuckDBAiConfig], object]


def _validate_requests(requests: tuple[ChatRequest, ...]) -> int:
    if not requests:
        raise ValueError("DuckDB-ai shard is empty")
    if len({request.endpoint_index for request in requests}) > 1:
        raise ValueError("DuckDB-ai adapter accepts one endpoint shard at a time")
    caps = {request.max_output_tokens for request in requests}
    if len(caps) > 1:
        raise ValueError("DuckDB-ai shard requires the same max_output_tokens")
    return next(iter(caps))


def _sql_literal(value: str) -> str:
    """Single-quoted SQL string literal with embedded single quotes doubled."""

    return "'" + value.replace("'", "''") + "'"


def _source_table(endpoint_index: int) -> str:
    name = f"duckdb_ai_source_ep{int(endpoint_index)}"
    if not name.replace("_", "").isalnum():
        raise ValueError(f"invalid source table identifier: {name!r}")
    return name


def _connect(config: DuckDBAiConfig):
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB-ai baseline requires the 'duckdb' package; pin duckdb==1.5.4 "
            "(the 'ai' community extension binary is not built for 1.5.5) and "
            "run INSTALL ai FROM community once in the driver venv"
        ) from exc
    connection = duckdb.connect(config.database_path)
    connection.execute("LOAD ai")
    return connection


def configure_ai_endpoint(connection, config: DuckDBAiConfig) -> None:
    """Point the ``ai`` extension at the OpenAI-compatible endpoint.

    ``openai_compatible`` requires ``BASE_URL``; vLLM exposes the
    ``/v1/chat/completions`` surface the extension calls. Values come from the
    experiment config, not user input, and are still single-quote escaped.
    """

    connection.execute("SET duckdb_ai_provider = 'openai_compatible'")
    connection.execute(f"SET duckdb_ai_model = {_sql_literal(config.model)}")
    connection.execute(
        "CREATE OR REPLACE SECRET duckdb_ai_endpoint "
        f"(TYPE duckdb_ai, AI_PROVIDER 'openai_compatible', "
        f"BASE_URL {_sql_literal(config.endpoint_base_url)}, "
        f"API_KEY {_sql_literal(config.api_key)})"
    )


def build_ai_complete_query(source_table: str, *, max_tokens: int) -> str:
    """Set-oriented SELECT letting the extension own batching and concurrency."""

    if not source_table.replace("_", "").isalnum():
        raise ValueError(f"invalid source table identifier: {source_table!r}")
    return (
        f"SELECT doc_id, ai_complete(prompt, max_tokens => {int(max_tokens)}, "
        "temperature => 0.0) AS output_text "
        f"FROM {source_table} ORDER BY doc_id"
    )


def run_duckdb_ai_complete(
    requests: Iterable[ChatRequest],
    config: DuckDBAiConfig,
    connection_factory: ConnectionFactory | None = None,
) -> tuple[BaselineRequestResult, ...]:
    """Execute one set-oriented DuckDB ``ai_complete`` over the shard."""

    materialized = tuple(requests)
    shard_cap = _validate_requests(materialized)
    if shard_cap != config.max_tokens:
        raise ValueError(
            f"config.max_tokens={config.max_tokens} does not match shard "
            f"max_output_tokens={shard_cap}"
        )
    endpoint_index = materialized[0].endpoint_index
    source_table = _source_table(endpoint_index)
    connection = (
        connection_factory(config)
        if connection_factory is not None
        else _connect(config)
    )
    submitted_at_s = time.time()
    try:
        configure_ai_endpoint(connection, config)
        connection.execute(
            f"CREATE OR REPLACE TABLE {source_table} "
            "(doc_id BIGINT, prompt VARCHAR)"
        )
        connection.executemany(
            f"INSERT INTO {source_table} VALUES (?, ?)",
            tuple((request.doc_id, request.prompt) for request in materialized),
        )
        query = build_ai_complete_query(source_table, max_tokens=config.max_tokens)
        rows = connection.execute(query).fetchall()
    finally:
        connection.close()
    completed_at_s = time.time()

    expected_ids = [request.doc_id for request in materialized]
    observed_ids = [int(row[0]) for row in rows]
    if (
        len(observed_ids) != len(expected_ids)
        or len(set(observed_ids)) != len(observed_ids)
        or set(observed_ids) != set(expected_ids)
    ):
        raise ValueError("DuckDB-ai result failed exactly-once validation")

    output_by_id = {int(row[0]): str(row[1]) for row in rows}
    return tuple(
        BaselineRequestResult(
            doc_id=request.doc_id,
            endpoint_index=request.endpoint_index,
            status="completed",
            error=None,
            submitted_at_s=submitted_at_s,
            started_at_s=submitted_at_s,
            completed_at_s=completed_at_s,
            input_tokens=request.prompt_tokens,
            output_tokens=0,
            output_text=output_by_id[request.doc_id],
            finish_reason=None,
        )
        for request in materialized
    )
