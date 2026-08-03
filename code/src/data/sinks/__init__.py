"""Result writeback adapters."""

from .postgres import batched_rows, vector_to_pg_literal, write_completions, write_embeddings

__all__ = [
    "batched_rows",
    "vector_to_pg_literal",
    "write_completions",
    "write_embeddings",
]
