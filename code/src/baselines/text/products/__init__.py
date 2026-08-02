"""Database product-native text AI operator adapters."""

from .oceanbase import (
    OceanBaseConfig,
    SqlStatement,
    build_ai_complete_sql,
    build_register_model_sql,
    run_oceanbase_ai_complete,
)

__all__ = [
    "OceanBaseConfig",
    "SqlStatement",
    "build_ai_complete_sql",
    "build_register_model_sql",
    "run_oceanbase_ai_complete",
]
