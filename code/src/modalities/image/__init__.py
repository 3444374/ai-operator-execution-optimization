"""Reusable image AI operator contracts and CLIP stage implementations."""

from .clip import (
    ClipImagePreprocessor,
    ClipTensorActor,
    FastClipImagePreprocessor,
    configure_torch_thread_pools,
    extract_clip_image_features,
    l2_normalize_embeddings,
    l2_normalize_numpy_embeddings,
)
from .contracts import (
    EmbeddingSemantics,
    ImageBatchTelemetry,
    ImageEmbeddingBackend,
    ImageEmbeddingBatch,
    ImageEmbeddingResult,
    ImageSourceConfig,
    build_image_runtime_snapshot,
    build_image_work_descriptor,
    image_work_calibration_signature,
    split_image_source_config,
)
from .staged import (
    build_encoded_image_block_descriptor,
    build_prepared_image_block_descriptor,
    image_model_signature,
    image_transform_signature,
)

__all__ = [
    "ClipImagePreprocessor",
    "ClipTensorActor",
    "EmbeddingSemantics",
    "FastClipImagePreprocessor",
    "ImageEmbeddingBackend",
    "ImageEmbeddingBatch",
    "ImageEmbeddingResult",
    "ImageBatchTelemetry",
    "ImageSourceConfig",
    "build_image_runtime_snapshot",
    "build_image_work_descriptor",
    "configure_torch_thread_pools",
    "extract_clip_image_features",
    "image_work_calibration_signature",
    "l2_normalize_embeddings",
    "l2_normalize_numpy_embeddings",
    "split_image_source_config",
    "build_encoded_image_block_descriptor",
    "build_prepared_image_block_descriptor",
    "image_model_signature",
    "image_transform_signature",
]
