"""Compatibility imports for the original wire-v3 adapter interface."""
from .semantic_session import (
    CompletionAdapter,
    CompletionAdapterError,
    Completion as V3Completion,
    CompletionRequest as V3CompletionRequest,
    run_v3_session,
)

__all__ = ["CompletionAdapter", "CompletionAdapterError", "V3Completion", "V3CompletionRequest", "run_v3_session"]
