"""Database product-native text AI operator adapters."""

from .duckdb_ai import (
    DuckDBAiConfig,
    inspect_duckdb_ai_runtime,
    run_duckdb_ai_complete,
)
from .oceanbase import (
    OceanBaseConfig,
    SqlStatement,
    build_ai_complete_sql,
    build_register_model_sql,
    run_oceanbase_ai_complete,
)

__all__ = [
    "DuckDBAiConfig",
    "OceanBaseConfig",
    "SqlStatement",
    "build_ai_complete_sql",
    "build_register_model_sql",
    "inspect_duckdb_ai_runtime",
    "run_duckdb_ai_complete",
    "run_oceanbase_ai_complete",
]
