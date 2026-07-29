"""OceanBase AI_COMPLETE product baseline with bound SQL parameters."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Callable, Iterable

from .contracts import BaselineRequestResult, ChatRequest


_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True)
class SqlStatement:
    sql: str
    params: tuple[object, ...] = ()


@dataclass(frozen=True)
class OceanBaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    model_key: str
    model_name: str
    endpoint_url: str
    access_key: str
    parallel_degree: int
    source_table: str
    result_table: str
    register_model: bool = False

    @property
    def endpoint_name(self) -> str:
        return f"{self.model_key}_endpoint"


ConnectionFactory = Callable[[OceanBaseConfig], object]


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid SQL identifier: {value!r}")
    return value


def _parallel_degree(value: int) -> int:
    if isinstance(value, bool) or not 1 <= value <= 1024:
        raise ValueError("parallel_degree must be between 1 and 1024")
    return value


def build_register_model_sql(
    config: OceanBaseConfig,
) -> tuple[SqlStatement, SqlStatement]:
    """Build official DBMS_AI_SERVICE calls without interpolating values."""

    _identifier(config.model_key)
    _identifier(config.endpoint_name)
    model_payload = json.dumps(
        {
            "type": "completion",
            "model_name": config.model_name,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    endpoint_payload = json.dumps(
        {
            "ai_model_name": config.model_key,
            "url": config.endpoint_url,
            "access_key": config.access_key,
            "provider": "openai",
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return (
        SqlStatement(
            "CALL DBMS_AI_SERVICE.CREATE_AI_MODEL(%s, %s)",
            (config.model_key, model_payload),
        ),
        SqlStatement(
            "CALL DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s, %s)",
            (config.endpoint_name, endpoint_payload),
        ),
    )


def build_ai_complete_sql(
    source_table: str,
    result_table: str,
    *,
    model_key: str,
    max_tokens: int,
    parallel_degree: int,
) -> SqlStatement:
    source = _identifier(source_table)
    result = _identifier(result_table)
    degree = _parallel_degree(parallel_degree)
    if max_tokens < 0:
        raise ValueError("max_tokens must be non-negative")
    return SqlStatement(
        f"""
INSERT INTO {result} (doc_id, output_text, completed_at)
SELECT /*+ PARALLEL({degree}) */
    doc_id,
    AI_COMPLETE(
        %s,
        prompt,
        JSON_OBJECT('temperature', 0.0, 'max_tokens', %s)
    ),
    NOW(6)
FROM {source}
ORDER BY doc_id
""".strip(),
        (model_key, max_tokens),
    )


def _connect(config: OceanBaseConfig):
    try:
        import pymysql
    except ImportError as exc:
        raise RuntimeError(
            "OceanBase baseline requires the 'PyMySQL' package"
        ) from exc
    return pymysql.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
        autocommit=False,
        charset="utf8mb4",
    )


def _validate_requests(
    requests: tuple[ChatRequest, ...],
) -> int:
    if len({request.endpoint_index for request in requests}) > 1:
        raise ValueError(
            "OceanBase adapter accepts one endpoint shard at a time"
        )
    caps = {request.max_output_tokens for request in requests}
    if len(caps) > 1:
        raise ValueError(
            "OceanBase shard requires the same max_output_tokens"
        )
    return next(iter(caps), 0)


def run_oceanbase_ai_complete(
    requests: Iterable[ChatRequest],
    config: OceanBaseConfig,
    connection_factory: ConnectionFactory | None = None,
) -> tuple[BaselineRequestResult, ...]:
    """Execute one set-oriented OceanBase AI_COMPLETE statement."""

    materialized = tuple(requests)
    max_tokens = _validate_requests(materialized)
    source_table = _identifier(config.source_table)
    result_table = _identifier(config.result_table)
    connection = (
        connection_factory(config)
        if connection_factory is not None
        else _connect(config)
    )
    submitted_at_s = time.time()
    try:
        with connection.cursor() as cursor:
            if config.register_model:
                for statement in build_register_model_sql(config):
                    cursor.execute(statement.sql, statement.params)
            cursor.execute(
                f"""
CREATE TABLE IF NOT EXISTS {source_table} (
    doc_id BIGINT PRIMARY KEY,
    prompt LONGTEXT NOT NULL
)
""".strip()
            )
            cursor.execute(
                f"""
CREATE TABLE IF NOT EXISTS {result_table} (
    doc_id BIGINT PRIMARY KEY,
    output_text LONGTEXT NOT NULL,
    completed_at DATETIME(6) NOT NULL
)
""".strip()
            )
            cursor.execute(f"DELETE FROM {source_table}")
            cursor.execute(f"DELETE FROM {result_table}")
            cursor.executemany(
                f"INSERT INTO {source_table} (doc_id, prompt) "
                "VALUES (%s, %s)",
                (
                    (request.doc_id, request.prompt)
                    for request in materialized
                ),
            )
            statement = build_ai_complete_sql(
                source_table,
                result_table,
                model_key=config.model_key,
                max_tokens=max_tokens,
                parallel_degree=config.parallel_degree,
            )
            cursor.execute(statement.sql, statement.params)
            cursor.execute(
                f"SELECT doc_id, output_text FROM {result_table} "
                "ORDER BY doc_id"
            )
            rows = tuple(cursor.fetchall())
        expected_ids = [request.doc_id for request in materialized]
        observed_ids = [int(row[0]) for row in rows]
        if (
            len(observed_ids) != len(expected_ids)
            or len(set(observed_ids)) != len(observed_ids)
            or set(observed_ids) != set(expected_ids)
        ):
            raise ValueError(
                "OceanBase result failed exactly-once validation"
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    completed_at_s = time.time()
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
