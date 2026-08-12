"""Build and validate image block descriptors for staged execution."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import numpy as np

from ...planning.blocks import StageBlockDescriptor
from .contracts import build_image_work_descriptor


def image_transform_signature(
    *,
    processor_revision: str,
    decoder: str,
    input_size: int,
    representation: str,
    layout: str,
    dtype: str,
) -> str:
    """Identify a byte-level preprocessing contract for reuse and validation."""
    payload = {
        "decoder": decoder,
        "dtype": dtype,
        "input_size": input_size,
        "layout": layout,
        "processor_revision": processor_revision,
        "representation": representation,
        "schema": "image-transform-v1",
    }
    return _signature("image-transform-v1", payload)


def image_model_signature(
    *,
    model_revision: str,
    processor_revision: str,
    dtype: str,
    embedding_dimension: int,
    normalized: bool,
) -> str:
    """Identify model/output semantics independently from physical block storage."""
    payload = {
        "dtype": dtype,
        "embedding_dimension": embedding_dimension,
        "model_revision": model_revision,
        "normalized": normalized,
        "processor_revision": processor_revision,
        "schema": "image-model-v1",
    }
    return _signature("image-model-v1", payload)


def build_encoded_image_block_descriptor(
    *,
    job_id: str,
    ordered_sequence: int,
    row_ids: tuple[str, ...],
    encoded_images: list[bytes],
    model_revision: str,
    processor_revision: str,
    model_dtype: str,
    created_at_s: float,
    input_size: int = 224,
    embedding_dimension: int = 512,
    prepared_dtype: str = "float32",
    prepared_representation: str = "prepared_fp32_nchw",
    decoder: str = "torchvision_decode_image_rgb",
) -> StageBlockDescriptor:
    """Describe one encoded source block and exact fixed-shape ready reservation."""
    if len(row_ids) != len(encoded_images):
        raise ValueError("encoded image count must match row_ids")
    if not encoded_images or any(
        not isinstance(item, bytes) or not item for item in encoded_images
    ):
        raise ValueError("encoded_images must contain non-empty byte strings")
    if input_size <= 0 or embedding_dimension <= 0:
        raise ValueError("image descriptor dimensions must be positive")
    try:
        element_bytes = np.dtype(prepared_dtype).itemsize
    except TypeError as error:
        raise ValueError(f"unsupported prepared dtype: {prepared_dtype}") from error
    encoded_bytes = sum(len(item) for item in encoded_images)
    ready_bytes = len(row_ids) * 3 * input_size * input_size * element_bytes
    digest = _encoded_digest(row_ids, encoded_images)
    transform_signature = image_transform_signature(
        processor_revision=processor_revision,
        decoder=decoder,
        input_size=input_size,
        representation=prepared_representation,
        layout="NCHW",
        dtype=prepared_dtype,
    )
    model_signature = image_model_signature(
        model_revision=model_revision,
        processor_revision=processor_revision,
        dtype=model_dtype,
        embedding_dimension=embedding_dimension,
        normalized=True,
    )
    work = build_image_work_descriptor(
        row_count=len(row_ids),
        encoded_bytes=encoded_bytes,
        model_revision=model_revision,
        processor_revision=processor_revision,
        dtype=model_dtype,
        input_size=input_size,
        embedding_dimension=embedding_dimension,
    )
    block_id = hashlib.sha256(
        f"{job_id}\0{ordered_sequence}\0{digest}".encode("utf-8")
    ).hexdigest()
    return StageBlockDescriptor(
        block_id=block_id,
        job_id=job_id,
        ordered_sequence=ordered_sequence,
        row_ids=row_ids,
        representation="encoded",
        shape=(len(row_ids),),
        layout="variable_binary",
        dtype="uint8_bytes",
        logical_bytes=encoded_bytes,
        physical_bytes=encoded_bytes,
        ready_bytes_estimate=ready_bytes,
        content_digest=digest,
        transform_signature=transform_signature,
        model_signature=model_signature,
        work=work,
        created_at_s=created_at_s,
    )


def build_prepared_image_block_descriptor(
    encoded: StageBlockDescriptor,
    tensor: np.ndarray,
    *,
    ready_at_s: float,
    representation: str = "prepared_fp32_nchw",
) -> StageBlockDescriptor:
    """Validate a packed NCHW tensor against its reserved source descriptor."""
    payload = np.asarray(tensor)
    if payload.ndim != 4 or payload.shape[0] != len(encoded.row_ids):
        raise ValueError("prepared image tensor must have NCHW shape matching row_ids")
    if payload.shape[1] != 3:
        raise ValueError("prepared image tensor must contain three RGB channels")
    if payload.shape[2] != payload.shape[3]:
        raise ValueError("prepared image tensor must use the declared square input shape")
    if int(payload.shape[0] * payload.shape[2] * payload.shape[3]) != encoded.model_work_units:
        raise ValueError("prepared image tensor shape does not match calibrated model work")
    if str(payload.dtype) != _representation_dtype(representation):
        raise ValueError("prepared representation and tensor dtype do not match")
    if not payload.flags.c_contiguous:
        raise ValueError("prepared image tensor must be C-contiguous")
    if int(payload.nbytes) > encoded.ready_bytes_estimate:
        raise ValueError("prepared image tensor exceeds its reserved ready bytes")
    return replace(
        encoded,
        representation=representation,
        shape=tuple(int(item) for item in payload.shape),
        layout="NCHW",
        dtype=str(payload.dtype),
        logical_bytes=int(payload.nbytes),
        physical_bytes=int(payload.nbytes),
        ready_at_s=ready_at_s,
    )


def _encoded_digest(row_ids: tuple[str, ...], encoded_images: list[bytes]) -> str:
    hasher = hashlib.sha256()
    for row_id, encoded in zip(row_ids, encoded_images, strict=True):
        row = row_id.encode("utf-8")
        hasher.update(len(row).to_bytes(8, byteorder="big"))
        hasher.update(row)
        hasher.update(len(encoded).to_bytes(8, byteorder="big"))
        hasher.update(encoded)
    return hasher.hexdigest()


def _signature(prefix: str, payload: dict[str, object]) -> str:
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:{digest}"


def _representation_dtype(representation: str) -> str:
    supported = {
        "prepared_fp32_nchw": "float32",
        "prepared_fp16_nchw": "float16",
        "prepared_u8_nchw": "uint8",
    }
    try:
        return supported[representation]
    except KeyError as error:
        raise ValueError(f"unsupported prepared representation: {representation}") from error
