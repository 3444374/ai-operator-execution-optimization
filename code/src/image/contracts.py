"""Typed, serving-engine-independent contracts for image embeddings."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, Protocol

import numpy as np


ImageInputKind = Literal["encoded_bytes", "preprocessed_tensor"]


@dataclass(frozen=True)
class EmbeddingSemantics:
    """The output contract required for comparable embedding experiments."""

    model_revision: str
    processor_revision: str
    dimension: int
    dtype: str = "float32"
    projected: bool = True
    normalized: bool = True

    def __post_init__(self) -> None:
        if not self.model_revision or not self.processor_revision:
            raise ValueError("model and processor revisions must be non-empty")
        if self.dimension <= 0:
            raise ValueError("embedding dimension must be positive")
        if not self.dtype:
            raise ValueError("embedding dtype must be non-empty")


@dataclass(frozen=True)
class ImageEmbeddingBatch:
    """One complete image batch submitted to a model backend."""

    doc_ids: tuple[str, ...]
    payload: object
    input_kind: ImageInputKind
    work_units: int
    work_unit: str

    def __post_init__(self) -> None:
        if not self.doc_ids or any(not item for item in self.doc_ids):
            raise ValueError("doc_ids must contain non-empty identifiers")
        if self.payload is None:
            raise ValueError("payload must not be None")
        if self.input_kind not in ("encoded_bytes", "preprocessed_tensor"):
            raise ValueError(f"unsupported image input kind: {self.input_kind}")
        if (
            not isinstance(self.work_units, int)
            or isinstance(self.work_units, bool)
            or self.work_units <= 0
        ):
            raise ValueError("work_units must be a positive integer")
        if not self.work_unit:
            raise ValueError("work_unit must be non-empty")


@dataclass(frozen=True)
class ImageEmbeddingResult:
    """Validated embeddings returned in the same order as the input rows."""

    doc_ids: tuple[str, ...]
    embeddings: np.ndarray
    semantics: EmbeddingSemantics
    service_s: float

    def __post_init__(self) -> None:
        if self.service_s < 0 or not math.isfinite(self.service_s):
            raise ValueError("service_s must be finite and non-negative")
        matrix = np.asarray(self.embeddings)
        expected = (len(self.doc_ids), self.semantics.dimension)
        if matrix.shape != expected:
            raise ValueError(
                f"embedding shape mismatch: expected {expected}, got {matrix.shape}"
            )
        if not np.isfinite(matrix).all():
            raise ValueError("embeddings must contain only finite values")


class ImageEmbeddingBackend(Protocol):
    """Minimal backend surface consumed by the image pipeline."""

    semantics: EmbeddingSemantics
    input_kind: ImageInputKind

    def embed(self, batch: ImageEmbeddingBatch) -> ImageEmbeddingResult:
        ...
