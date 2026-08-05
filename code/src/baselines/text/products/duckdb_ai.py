"""DuckDB ``ai`` community-extension AI_COMPLETE adapter (native execution).

The DuckDB ``ai`` community extension provides SQL ``ai_complete`` that calls an
external OpenAI-compatible endpoint. This adapter treats DuckDB + the extension
as a database-product-native AI_COMPLETE baseline: it only materializes the
immutable ``ChatRequest`` shard into a DuckDB table and issues one set-oriented
``SELECT ai_try_complete(...)``. The extension owns HTTP call execution and
concurrency; its retry/cache/rate controls are explicitly frozen by the
experiment contract. No project scheduling code is injected, matching the policy in
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
    max_concurrent_requests: int = 32
    response_cache: bool = False
    provider_prompt_cache_hints: bool = False
    retry_count: int = 0
    retry_backoff_ms: int = 0
    min_request_interval_ms: int = 0
    timeout_seconds: int = 120

    def __post_init__(self) -> None:
        if not self.endpoint_base_url:
            raise ValueError("endpoint_base_url must be non-empty")
        if not self.model:
            raise ValueError("model must be non-empty")
        if self.max_tokens < 0:
            raise ValueError("max_tokens must be non-negative")
        if not 1 <= self.max_concurrent_requests <= 64:
            raise ValueError("max_concurrent_requests must be in [1, 64]")
        if not 0 <= self.retry_count <= 10:
            raise ValueError("retry_count must be in [0, 10]")
        if not 0 <= self.retry_backoff_ms <= 60_000:
            raise ValueError("retry_backoff_ms must be in [0, 60000]")
        if not 0 <= self.min_request_interval_ms <= 60_000:
            raise ValueError("min_request_interval_ms must be in [0, 60000]")
        if self.timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")


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
    # Freeze extension-owned controls so a machine/default change cannot silently
    # alter the baseline. Response caching is off in the same-work comparison;
    # vLLM prefix caching is a separate server-side setting and remains enabled.
    connection.execute(
        f"SET duckdb_ai_max_concurrent_requests = {config.max_concurrent_requests}"
    )
    connection.execute(
        f"SET duckdb_ai_cache = {'true' if config.response_cache else 'false'}"
    )
    connection.execute(
        "SET duckdb_ai_prompt_cache = "
        f"{'true' if config.provider_prompt_cache_hints else 'false'}"
    )
    connection.execute(f"SET duckdb_ai_retry_count = {config.retry_count}")
    connection.execute(
        f"SET duckdb_ai_retry_backoff_ms = {config.retry_backoff_ms}"
    )
    connection.execute(
        "SET duckdb_ai_min_request_interval_ms = "
        f"{config.min_request_interval_ms}"
    )
    connection.execute(f"SET duckdb_ai_timeout_seconds = {config.timeout_seconds}")
    connection.execute(
        "CREATE OR REPLACE SECRET duckdb_ai_endpoint "
        f"(TYPE duckdb_ai, AI_PROVIDER 'openai_compatible', "
        f"BASE_URL {_sql_literal(config.endpoint_base_url)}, "
        f"API_KEY {_sql_literal(config.api_key)})"
    )


def inspect_duckdb_ai_runtime(
    config: DuckDBAiConfig,
    connection_factory: ConnectionFactory | None = None,
) -> dict[str, str]:
    """Read the actual DuckDB/extension identity used by this Python runtime."""

    connection = (
        connection_factory(config)
        if connection_factory is not None
        else _connect(config)
    )
    try:
        duckdb_version = str(connection.execute("SELECT version()").fetchone()[0])
        extension_row = connection.execute(
            "SELECT extension_version, installed_from FROM duckdb_extensions() "
            "WHERE extension_name = 'ai' AND loaded"
        ).fetchone()
    finally:
        connection.close()
    if extension_row is None:
        raise RuntimeError("DuckDB ai extension is not loaded in the selected runtime")
    return {
        "duckdb_version": duckdb_version,
        "duckdb_ai_extension_version": str(extension_row[0]),
        "duckdb_ai_extension_source": str(extension_row[1]),
    }


def build_ai_complete_query(source_table: str, *, max_tokens: int) -> str:
    """Set-oriented SELECT letting the extension own batching and concurrency.

    Uses ``ai_try_complete`` instead of ``ai_complete``: the ``ai`` extension
    treats ``finish_reason=length`` (max_tokens reached) as a hard error, so
    ``ai_complete`` would abort the whole shard whenever any row truncates --
    common under a fixed output cap on ShareGPT-style prompts. ``ai_try_complete``
    returns a struct ``{response, error}`` per row without raising; we preserve
    both fields. ``response`` is NULL for truncated rows. This is a real semantic
    difference from the vLLM-bench / bounded-http arms, which return the partial
    text on truncation: DuckDB-ai yields NULL instead. The exactly-once check
    (doc_id set) still holds because every input row produces one result row;
    truncated rows are recorded as failed, so the zero-failure validity gate
    fails closed instead of reporting a false successful baseline.
    """

    if not source_table.replace("_", "").isalnum():
        raise ValueError(f"invalid source table identifier: {source_table!r}")
    return (
        "WITH completed AS MATERIALIZED ("
        f"SELECT doc_id, ai_try_complete(prompt, max_tokens => {int(max_tokens)}, "
        "temperature => 0.0) AS result "
        f"FROM {source_table}) "
        "SELECT doc_id, result.response AS output_text, result.error AS output_error "
        "FROM completed ORDER BY doc_id"
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

    output_by_id = {
        int(row[0]): (
            str(row[1]) if row[1] is not None else None,
            str(row[2]) if row[2] is not None else None,
        )
        for row in rows
    }

    def build_result(request: ChatRequest) -> BaselineRequestResult:
        output_text, output_error = output_by_id[request.doc_id]
        error = output_error
        if output_text is None and error is None:
            error = "duckdb_ai returned NULL response without an error"
        return BaselineRequestResult(
            doc_id=request.doc_id,
            endpoint_index=request.endpoint_index,
            status="failed" if error is not None else "completed",
            error=error,
            submitted_at_s=submitted_at_s,
            started_at_s=submitted_at_s,
            completed_at_s=completed_at_s,
            input_tokens=request.prompt_tokens,
            output_tokens=0,
            output_text=output_text,
            finish_reason=None,
        )

    return tuple(build_result(request) for request in materialized)
