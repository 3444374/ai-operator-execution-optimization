"""Reusable image AI operator contracts and CLIP stage implementations."""

from .clip import (
    ClipImagePreprocessor,
    ClipTensorActor,
    FastClipImagePreprocessor,
    configure_torch_thread_pools,
    extract_clip_image_features,
    l2_normalize_embeddings,
)
from .contracts import (
    EmbeddingSemantics,
    ImageBatchTelemetry,
    ImageEmbeddingBackend,
    ImageEmbeddingBatch,
    ImageEmbeddingResult,
)
from .source import (
    DaftImageSource,
    ImageSourceConfig,
    image_documents_query,
    split_image_source_config,
)

__all__ = [
    "ClipImagePreprocessor",
    "ClipTensorActor",
    "DaftImageSource",
    "EmbeddingSemantics",
    "FastClipImagePreprocessor",
    "ImageEmbeddingBackend",
    "ImageEmbeddingBatch",
    "ImageEmbeddingResult",
    "ImageBatchTelemetry",
    "ImageSourceConfig",
    "configure_torch_thread_pools",
    "extract_clip_image_features",
    "image_documents_query",
    "l2_normalize_embeddings",
    "split_image_source_config",
]
