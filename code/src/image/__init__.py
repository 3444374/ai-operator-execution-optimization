"""Reusable image AI operator contracts and CLIP stage implementations."""

from .clip import (
    ClipImagePreprocessor,
    ClipTensorActor,
    extract_clip_image_features,
    l2_normalize_embeddings,
)
from .contracts import (
    EmbeddingSemantics,
    ImageEmbeddingBackend,
    ImageEmbeddingBatch,
    ImageEmbeddingResult,
)
from .source import DaftImageSource, ImageSourceConfig, image_documents_query

__all__ = [
    "ClipImagePreprocessor",
    "ClipTensorActor",
    "DaftImageSource",
    "EmbeddingSemantics",
    "ImageEmbeddingBackend",
    "ImageEmbeddingBatch",
    "ImageEmbeddingResult",
    "ImageSourceConfig",
    "extract_clip_image_features",
    "image_documents_query",
    "l2_normalize_embeddings",
]
