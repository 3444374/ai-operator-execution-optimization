"""Stable model-backend API split into completion and embedding modules."""

from .common import (
    CompletionBackendName,
    CompletionEndpointResult,
    CompletionPromptFormat,
    CompletionProtocol,
    EmbeddingBackendName,
    model_request_wall_time,
    normalize_completion_backend,
    normalize_embedding_backend,
    text_token_count,
)
from .completion import (
    CompatibleAsyncHTTPCompletionActor,
    CompatibleHTTPCompletionActor,
    FakeCompletionActor,
    OllamaCompletionActor,
    call_compatible_completion_endpoint,
    call_ollama_completion_endpoint,
    compatible_http_complete_batch,
    fake_complete_batch,
    format_completion_prompts,
    ollama_complete_batch,
    ollama_generate_url,
)
from .embedding import (
    CompatibleHTTPEmbeddingActor,
    FakeEmbeddingActor,
    call_compatible_embedding_endpoint,
    compatible_http_embed_batch,
    fake_embed_batch,
)

__all__ = [name for name in globals() if not name.startswith("_")]
