"""Engine-independent descriptors for physical blocks in staged AI pipelines."""

from __future__ import annotations

import math
from dataclasses import dataclass

from .work import WorkDescriptor


@dataclass(frozen=True)
class StageBlockDescriptor:
    """Immutable identity, representation, work, and memory for one block."""

    block_id: str
    job_id: str
    ordered_sequence: int
    row_ids: tuple[str, ...]
    representation: str
    shape: tuple[int, ...]
    layout: str
    dtype: str
    logical_bytes: int
    physical_bytes: int
    ready_bytes_estimate: int
    content_digest: str
    transform_signature: str
    model_signature: str
    work: WorkDescriptor
    created_at_s: float
    ready_at_s: float | None = None
    retry_count: int = 0

    def __post_init__(self) -> None:
        strings = (
            self.block_id,
            self.job_id,
            self.representation,
            self.layout,
            self.dtype,
            self.content_digest,
            self.transform_signature,
            self.model_signature,
        )
        if any(not item for item in strings):
            raise ValueError("stage block string fields must be non-empty")
        if self.ordered_sequence < 0 or self.retry_count < 0:
            raise ValueError("sequence and retry_count must be non-negative")
        if not self.row_ids or any(not item for item in self.row_ids):
            raise ValueError("row_ids must contain non-empty identifiers")
        if len(self.row_ids) != len(set(self.row_ids)):
            raise ValueError("row_ids must be unique within a block")
        if any(value <= 0 for value in self.shape):
            raise ValueError("stage block shape dimensions must be positive")
        byte_counts = (
            self.logical_bytes,
            self.physical_bytes,
            self.ready_bytes_estimate,
        )
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in byte_counts
        ):
            raise ValueError("stage block byte counts must be non-negative integers")
        if not math.isfinite(self.created_at_s) or self.created_at_s < 0:
            raise ValueError("created_at_s must be finite and non-negative")
        if self.ready_at_s is not None and (
            not math.isfinite(self.ready_at_s) or self.ready_at_s < self.created_at_s
        ):
            raise ValueError("ready_at_s must not precede block creation")

    @property
    def model_work_units(self) -> int:
        stage = self.work.for_stage("model")
        if stage is None:
            raise ValueError("stage block work must include model demand")
        return stage.units

    @property
    def prepare_work_units(self) -> int:
        stage = self.work.for_stage("prepare")
        if stage is None:
            raise ValueError("stage block work must include prepare demand")
        return stage.units
