"""Shared execution and validation helpers for image embedding benchmarks."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from .contracts import ImageEmbeddingResult


@dataclass
class EmbeddingAudit:
    """Streaming exactly-once and embedding-semantics audit."""

    expected_doc_ids: frozenset[str]
    dimension: int
    seen_doc_ids: set[str] = field(default_factory=set)
    rows: int = 0
    checksum: float = 0.0
    max_norm_error: float = 0.0

    def add(self, doc_ids: tuple[str, ...], embeddings: Any) -> None:
        matrix = np.asarray(embeddings, dtype=np.float32)
        if matrix.shape != (len(doc_ids), self.dimension):
            raise ValueError(
                "embedding shape mismatch: "
                f"expected {(len(doc_ids), self.dimension)}, got {matrix.shape}"
            )
        if not np.isfinite(matrix).all():
            raise ValueError("embeddings contain non-finite values")
        duplicates = self.seen_doc_ids.intersection(doc_ids)
        if duplicates:
            raise ValueError(f"duplicate output doc_ids: {sorted(duplicates)[:5]}")
        self.seen_doc_ids.update(doc_ids)
        self.rows += len(doc_ids)
        self.checksum += float(matrix[:, 0].sum(dtype=np.float64))
        norm_error = np.abs(np.linalg.norm(matrix, axis=1) - 1.0)
        self.max_norm_error = max(self.max_norm_error, float(norm_error.max()))

    def add_result(self, result: ImageEmbeddingResult) -> None:
        self.add(result.doc_ids, result.embeddings)

    def finish(self) -> dict[str, object]:
        missing = self.expected_doc_ids.difference(self.seen_doc_ids)
        unexpected = self.seen_doc_ids.difference(self.expected_doc_ids)
        if missing or unexpected:
            raise ValueError(
                "exactly-once mismatch: "
                f"missing={len(missing)}, unexpected={len(unexpected)}"
            )
        if self.rows != len(self.expected_doc_ids):
            raise ValueError(
                f"row count mismatch: expected {len(self.expected_doc_ids)}, got {self.rows}"
            )
        return {
            "output_rows": self.rows,
            "embedding_checksum": self.checksum,
            "max_norm_error": self.max_norm_error,
            "exactly_once": True,
        }


@dataclass(frozen=True)
class ExecutionResult:
    """Comparable operator timing returned by every execution arm."""

    total_s: float
    first_output_s: float
    audit: dict[str, object]
    batch_service_s: tuple[float, ...] = ()

    def __post_init__(self) -> None:
        if self.total_s <= 0 or not math.isfinite(self.total_s):
            raise ValueError("total_s must be finite and positive")
        if self.first_output_s < 0 or self.first_output_s > self.total_s:
            raise ValueError("first_output_s must fall within the execution wall")


@dataclass(frozen=True)
class ProjectRayWorkerPool:
    """Persistent Ray worker handles shared by warmup and formal queries."""

    preprocessors: tuple[object, ...]
    gpu_actors: tuple[object, ...]


def stop_project_ray_worker_pool(worker_pool: ProjectRayWorkerPool) -> None:
    """Terminate one per-query worker pool after warmup or formal execution."""
    import ray

    for actor in (*worker_pool.preprocessors, *worker_pool.gpu_actors):
        ray.kill(actor, no_restart=True)


def build_project_ray_worker_pool(
    *,
    model_revision: str,
    processor_revision: str,
    cpu_workers: int,
    gpu_workers: int,
    dtype: str,
) -> ProjectRayWorkerPool:
    """Create persistent CPU preprocess and tensor-only GPU actors."""
    if min(cpu_workers, gpu_workers) <= 0:
        raise ValueError("worker counts must be positive")
    import ray

    from .clip import ClipTensorActor, FastClipImagePreprocessor
    from .contracts import ImageEmbeddingBatch

    @ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
    class CpuPreprocessActor:
        def __init__(self, revision: str):
            self._preprocessor = FastClipImagePreprocessor(revision)

        def ready(self) -> bool:
            return True

        def preprocess(
            self,
            doc_ids: tuple[str, ...],
            encoded_images: list[bytes],
        ) -> ImageEmbeddingBatch:
            pixels = self._preprocessor.preprocess(encoded_images)
            return ImageEmbeddingBatch(
                doc_ids=doc_ids,
                payload=pixels,
                input_kind="preprocessed_tensor",
                work_units=len(doc_ids) * int(pixels.shape[-2]) * int(pixels.shape[-1]),
                work_unit="pixels",
            )

    RemoteGpuActor = ray.remote(
        num_cpus=1,
        num_gpus=1,
        max_restarts=0,
        max_task_retries=0,
    )(ClipTensorActor)
    preprocessors = tuple(
        CpuPreprocessActor.remote(processor_revision) for _ in range(cpu_workers)
    )
    gpu_actors = tuple(
        RemoteGpuActor.remote(
            model_revision,
            processor_revision=processor_revision,
            dtype=dtype,
            normalize=True,
        )
        for _ in range(gpu_workers)
    )
    ray.get([actor.ready.remote() for actor in preprocessors])
    ray.get([actor.ready.remote() for actor in gpu_actors])
    return ProjectRayWorkerPool(
        preprocessors=preprocessors,
        gpu_actors=gpu_actors,
    )


def run_project_ray_pipeline(
    source_df,
    *,
    worker_pool: ProjectRayWorkerPool,
    expected_doc_ids: frozenset[str],
    batch_size: int,
    max_active_batches: int,
    embedding_dimension: int = 512,
) -> ExecutionResult:
    """Run a bounded Daft-source -> Ray CPU -> Ray GPU streaming pipeline."""
    if min(batch_size, max_active_batches) <= 0:
        raise ValueError("batch_size and max_active_batches must be positive")
    if not worker_pool.preprocessors or not worker_pool.gpu_actors:
        raise ValueError("worker_pool must contain CPU and GPU actors")
    if max_active_batches < len(worker_pool.gpu_actors):
        raise ValueError("max_active_batches must be at least the GPU worker count")

    import ray

    audit = EmbeddingAudit(
        expected_doc_ids=expected_doc_ids,
        dimension=embedding_dimension,
    )
    pending: dict[object, float] = {}
    cpu_position = 0
    gpu_position = 0
    first_output_s: float | None = None
    batch_service_s: list[float] = []
    started = time.perf_counter()

    def consume_one() -> None:
        nonlocal first_output_s
        ready, _ = ray.wait(list(pending), num_returns=1)
        reference = ready[0]
        submitted_at = pending.pop(reference)
        result = ray.get(reference)
        audit.add_result(result)
        batch_service_s.append(time.perf_counter() - submitted_at)
        if first_output_s is None:
            first_output_s = time.perf_counter() - started

    batches = source_df.into_batches(batch_size).to_arrow_iter(results_buffer_size=2)
    for record_batch in batches:
        doc_ids = tuple(str(item.as_py()) for item in record_batch["doc_id"])
        encoded = [item.as_py() for item in record_batch["image"]]
        preprocessor = worker_pool.preprocessors[
            cpu_position % len(worker_pool.preprocessors)
        ]
        gpu_actor = worker_pool.gpu_actors[gpu_position % len(worker_pool.gpu_actors)]
        cpu_position += 1
        gpu_position += 1
        preprocessed = preprocessor.preprocess.remote(doc_ids, encoded)
        submitted_at = time.perf_counter()
        output = gpu_actor.embed.remote(preprocessed)
        pending[output] = submitted_at
        if len(pending) >= max_active_batches:
            consume_one()

    while pending:
        consume_one()

    total_s = time.perf_counter() - started
    return ExecutionResult(
        total_s=total_s,
        first_output_s=first_output_s or total_s,
        audit=audit.finish(),
        batch_service_s=tuple(batch_service_s),
    )
