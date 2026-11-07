"""Result writeback adapters."""

from .postgres import (
    PostgresWritePlan,
    batched_rows,
    execute_write_plan,
    prepare_completion_write,
    prepare_embedding_write,
    vector_to_pg_literal,
    write_completions,
    write_embeddings,
)

__all__ = [
    "batched_rows",
    "execute_write_plan",
    "PostgresWritePlan",
    "prepare_completion_write",
    "prepare_embedding_write",
    "vector_to_pg_literal",
    "write_completions",
    "write_embeddings",
]
