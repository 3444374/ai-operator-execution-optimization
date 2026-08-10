"""Typed, serving-engine-independent contracts for image embeddings."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Literal, Protocol

import numpy as np

from ...planning.work import (
    RuntimeStateSnapshot,
    StageStateSnapshot,
    StageWork,
    WorkDescriptor,
)


ImageInputKind = Literal["encoded_bytes", "preprocessed_tensor"]


def image_work_calibration_signature(
    *,
    model_revision: str,
    processor_revision: str,
    dtype: str,
    input_size: int = 224,
    embedding_dimension: int = 512,
) -> str:
    """Return a stable identity for comparable image stage-work estimates."""
    if not model_revision or not processor_revision or not dtype:
        raise ValueError("image work calibration identity fields must be non-empty")
    if input_size <= 0 or embedding_dimension <= 0:
        raise ValueError("image work dimensions must be positive")
    payload = {
        "dtype": dtype,
        "embedding_dimension": embedding_dimension,
        "input_size": input_size,
        "model_revision": model_revision,
        "processor_revision": processor_revision,
        "schema": "image-stage-work-v1",
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"image-stage-work-v1:{digest}"


def build_image_work_descriptor(
    *,
    row_count: int,
    encoded_bytes: int,
    model_revision: str,
    processor_revision: str,
    dtype: str,
    input_size: int = 224,
    embedding_dimension: int = 512,
) -> WorkDescriptor:
    """Describe source, prepare, model, and result demand for one image batch."""
    if row_count <= 0 or encoded_bytes < 0:
        raise ValueError("row_count must be positive and encoded_bytes non-negative")
    pixels = row_count * input_size * input_size
    return WorkDescriptor(
        stages=(
            StageWork("source", encoded_bytes, "encoded_bytes"),
            StageWork("prepare", pixels * 3, "tensor_values"),
            StageWork("model", pixels, "pixels"),
            StageWork("result", row_count * embedding_dimension * 4, "bytes"),
        ),
        primary_stage="model",
        calibration_signature=image_work_calibration_signature(
            model_revision=model_revision,
            processor_revision=processor_revision,
            dtype=dtype,
            input_size=input_size,
            embedding_dimension=embedding_dimension,
        ),
        locality_key=f"shape-{input_size}x{input_size}",
    )


def build_image_runtime_snapshot(
    *,
    ready: tuple[tuple[WorkDescriptor, float], ...],
    active: tuple[WorkDescriptor, ...],
    observed_at_s: float,
    calibration_signature: str,
    max_active_batches: int,
    batch_size: int,
    input_size: int = 224,
) -> RuntimeStateSnapshot:
    """Build an observe-only snapshot from scheduler-visible image work.

    Submitted batches reserve both prepare and model work because the driver
    cannot observe their internal hand-off without intrusive actor polling.
    The stages must therefore be interpreted independently, never summed.
    """
    if max_active_batches <= 0 or batch_size <= 0 or input_size <= 0:
        raise ValueError("snapshot capacity inputs must be positive")
    descriptors = tuple(item for item, _ready_at_s in ready) + active
    if any(item.calibration_signature != calibration_signature for item in descriptors):
        raise ValueError("snapshot work descriptors must share the calibration signature")
    oldest_age_s = (
        max(0.0, observed_at_s - min(ready_at_s for _item, ready_at_s in ready))
        if ready
        else 0.0
    )

    def total(items: tuple[WorkDescriptor, ...], stage: str) -> int:
        return sum(
            stage_work.units
            for item in items
            if (stage_work := item.for_stage(stage)) is not None
        )

    ready_descriptors = tuple(item for item, _ready_at_s in ready)
    model_capacity = max_active_batches * batch_size * input_size * input_size
    prepare_capacity = model_capacity * 3
    stages = tuple(
        StageStateSnapshot(
            stage=stage,
            active_work=total(active, stage),
            queued_work=total(ready_descriptors, stage),
            service_rate_units_s=None,
            oldest_queue_age_s=oldest_age_s,
            observed_at_s=observed_at_s,
            capacity_work=capacity,
        )
        for stage, capacity in (
            ("prepare", prepare_capacity),
            ("model", model_capacity),
        )
    )
    return RuntimeStateSnapshot(
        stages=stages,
        observed_at_s=observed_at_s,
        calibration_signature=calibration_signature,
    )


@dataclass(frozen=True)
class ImageBatchTelemetry:
    """Optional per-batch stage measurements carried across Ray actors.

    Byte counts are logical payload sizes, not hardware bus counters. H2D/D2H
    timings are populated only when the runner explicitly enables intrusive
    detailed stage timing.
    """

    preprocess_s: float = 0.0
    encoded_bytes: int = 0
    input_tensor_bytes: int = 0
    device_input_bytes: int = 0
    host_copy_s: float = 0.0
    h2d_s: float = 0.0
    forward_s: float = 0.0
    d2h_s: float = 0.0
    output_bytes: int = 0

    def __post_init__(self) -> None:
        times = (
            self.preprocess_s,
            self.host_copy_s,
            self.h2d_s,
            self.forward_s,
            self.d2h_s,
        )
        if any(value < 0 or not math.isfinite(value) for value in times):
            raise ValueError("telemetry times must be finite and non-negative")
        byte_counts = (
            self.encoded_bytes,
            self.input_tensor_bytes,
            self.device_input_bytes,
            self.output_bytes,
        )
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in byte_counts
        ):
            raise ValueError("telemetry byte counts must be non-negative integers")


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
    work_descriptor: WorkDescriptor | None = None
    telemetry: ImageBatchTelemetry = field(default_factory=ImageBatchTelemetry)

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
        if self.work_descriptor is not None:
            primary = self.work_descriptor.primary
            if primary.units != self.work_units or primary.unit != self.work_unit:
                raise ValueError(
                    "work descriptor primary demand must match legacy work fields"
                )


@dataclass(frozen=True)
class ImageEmbeddingResult:
    """Validated embeddings returned in the same order as the input rows."""

    doc_ids: tuple[str, ...]
    embeddings: np.ndarray
    semantics: EmbeddingSemantics
    service_s: float
    telemetry: ImageBatchTelemetry = field(default_factory=ImageBatchTelemetry)

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
