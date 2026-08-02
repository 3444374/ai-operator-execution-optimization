"""Shared execution and validation helpers for image embedding benchmarks."""

from __future__ import annotations

import hashlib
import math
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from .contracts import ImageBatchTelemetry, ImageEmbeddingResult


@dataclass
class EmbeddingAudit:
    """Streaming exactly-once and embedding-semantics audit."""

    expected_doc_ids: frozenset[str]
    dimension: int
    seen_doc_ids: set[str] = field(default_factory=set)
    rows: int = 0
    checksum: float = 0.0
    all_values_checksum: float = 0.0
    rounded_digest_xor: int = 0
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
        self.all_values_checksum += float(matrix.sum(dtype=np.float64))
        for doc_id, row in zip(doc_ids, matrix, strict=True):
            rounded = np.round(row, decimals=5).astype("<f4", copy=False)
            digest = hashlib.blake2b(
                doc_id.encode("utf-8") + b"\0" + rounded.tobytes(),
                digest_size=16,
            ).digest()
            self.rounded_digest_xor ^= int.from_bytes(digest, byteorder="big")
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
            "embedding_sum_all": self.all_values_checksum,
            "embedding_digest_xor_rounded5": f"{self.rounded_digest_xor:032x}",
            "max_norm_error": self.max_norm_error,
            "exactly_once": True,
        }


@dataclass(frozen=True)
class ExecutionResult:
    """Comparable operator timing returned by every execution arm."""

    total_s: float
    first_output_s: float
    audit: dict[str, object]
    batch_completion_wall_s: tuple[float, ...] = ()
    batch_actor_service_s: tuple[float, ...] = ()
    batch_unattributed_wait_s: tuple[float, ...] = ()
    batch_preprocess_s: tuple[float, ...] = ()
    batch_host_copy_s: tuple[float, ...] = ()
    batch_h2d_s: tuple[float, ...] = ()
    batch_forward_s: tuple[float, ...] = ()
    batch_d2h_s: tuple[float, ...] = ()
    batch_source_next_s: tuple[float, ...] = ()
    batch_driver_materialize_s: tuple[float, ...] = ()
    batch_submit_s: tuple[float, ...] = ()
    encoded_bytes: int = 0
    input_tensor_bytes: int = 0
    device_input_bytes: int = 0
    output_bytes: int = 0
    submitted_batches: int = 0
    pending_batches_peak: int = 0
    engine_stats: str = ""

    def __post_init__(self) -> None:
        if self.total_s <= 0 or not math.isfinite(self.total_s):
            raise ValueError("total_s must be finite and positive")
        if self.first_output_s < 0 or self.first_output_s > self.total_s:
            raise ValueError("first_output_s must fall within the execution wall")
        if min(
            self.encoded_bytes,
            self.input_tensor_bytes,
            self.device_input_bytes,
            self.output_bytes,
        ) < 0:
            raise ValueError("execution byte counts must be non-negative")
        if self.submitted_batches < 0 or self.pending_batches_peak < 0:
            raise ValueError("execution batch counts must be non-negative")
        if self.pending_batches_peak > self.submitted_batches:
            raise ValueError("pending batch peak cannot exceed submitted batches")

    @property
    def batch_service_s(self) -> tuple[float, ...]:
        """Deprecated alias for submission-to-result completion wall time."""
        return self.batch_completion_wall_s


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
    detailed_stage_timing: bool = False,
    torch_intraop_threads: int = 1,
    torch_interop_threads: int = 1,
) -> ProjectRayWorkerPool:
    """Create persistent CPU preprocess and tensor-only GPU actors."""
    if min(cpu_workers, gpu_workers) <= 0:
        raise ValueError("worker counts must be positive")
    import ray

    from .clip import ClipTensorActor, FastClipImagePreprocessor
    from .contracts import ImageEmbeddingBatch

    @ray.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
    class CpuPreprocessActor:
        def __init__(
            self,
            revision: str,
            intraop_threads: int,
            interop_threads: int,
        ):
            self._preprocessor = FastClipImagePreprocessor(
                revision,
                torch_intraop_threads=intraop_threads,
                torch_interop_threads=interop_threads,
            )

        def ready(self) -> dict[str, int]:
            import torch

            return {
                "torch_intraop_threads": torch.get_num_threads(),
                "torch_interop_threads": torch.get_num_interop_threads(),
            }

        def preprocess(
            self,
            doc_ids: tuple[str, ...],
            encoded_images: list[bytes],
        ) -> ImageEmbeddingBatch:
            started = time.perf_counter()
            pixels = self._preprocessor.preprocess(encoded_images)
            preprocess_s = time.perf_counter() - started
            return ImageEmbeddingBatch(
                doc_ids=doc_ids,
                payload=pixels,
                input_kind="preprocessed_tensor",
                work_units=len(doc_ids) * int(pixels.shape[-2]) * int(pixels.shape[-1]),
                work_unit="pixels",
                telemetry=ImageBatchTelemetry(
                    preprocess_s=preprocess_s,
                    encoded_bytes=sum(len(item) for item in encoded_images),
                    input_tensor_bytes=int(pixels.nbytes),
                ),
            )

    RemoteGpuActor = ray.remote(
        num_cpus=1,
        num_gpus=1,
        max_restarts=0,
        max_task_retries=0,
    )(ClipTensorActor)
    preprocessors = tuple(
        CpuPreprocessActor.remote(
            processor_revision,
            torch_intraop_threads,
            torch_interop_threads,
        )
        for _ in range(cpu_workers)
    )
    gpu_actors = tuple(
        RemoteGpuActor.remote(
            model_revision,
            processor_revision=processor_revision,
            dtype=dtype,
            normalize=True,
            detailed_stage_timing=detailed_stage_timing,
            torch_intraop_threads=torch_intraop_threads,
            torch_interop_threads=torch_interop_threads,
        )
        for _ in range(gpu_workers)
    )
    readiness = ray.get([actor.ready.remote() for actor in preprocessors])
    readiness.extend(ray.get([actor.ready.remote() for actor in gpu_actors]))
    expected_threads = {
        "torch_intraop_threads": torch_intraop_threads,
        "torch_interop_threads": torch_interop_threads,
    }
    mismatches = [
        item
        for item in readiness
        if any(item.get(key) != value for key, value in expected_threads.items())
    ]
    if mismatches:
        raise RuntimeError(
            "Ray worker Torch thread contract mismatch: "
            f"expected={expected_threads}, observed={mismatches}"
        )
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
    batch_completion_wall_s: list[float] = []
    batch_actor_service_s: list[float] = []
    batch_unattributed_wait_s: list[float] = []
    batch_preprocess_s: list[float] = []
    batch_host_copy_s: list[float] = []
    batch_h2d_s: list[float] = []
    batch_forward_s: list[float] = []
    batch_d2h_s: list[float] = []
    batch_source_next_s: list[float] = []
    batch_driver_materialize_s: list[float] = []
    batch_submit_s: list[float] = []
    encoded_bytes = 0
    input_tensor_bytes = 0
    device_input_bytes = 0
    output_bytes = 0
    submitted_batches = 0
    pending_batches_peak = 0
    started = time.perf_counter()

    def consume_one() -> None:
        nonlocal first_output_s
        nonlocal encoded_bytes, input_tensor_bytes, device_input_bytes, output_bytes
        ready, _ = ray.wait(list(pending), num_returns=1)
        reference = ready[0]
        submitted_at = pending.pop(reference)
        result = ray.get(reference)
        audit.add_result(result)
        completion_wall_s = time.perf_counter() - submitted_at
        batch_completion_wall_s.append(completion_wall_s)
        batch_actor_service_s.append(result.service_s)
        telemetry = result.telemetry
        batch_preprocess_s.append(telemetry.preprocess_s)
        batch_unattributed_wait_s.append(
            max(0.0, completion_wall_s - telemetry.preprocess_s - result.service_s)
        )
        batch_host_copy_s.append(telemetry.host_copy_s)
        if telemetry.h2d_s > 0:
            batch_h2d_s.append(telemetry.h2d_s)
        if telemetry.forward_s > 0:
            batch_forward_s.append(telemetry.forward_s)
        if telemetry.d2h_s > 0:
            batch_d2h_s.append(telemetry.d2h_s)
        encoded_bytes += telemetry.encoded_bytes
        input_tensor_bytes += telemetry.input_tensor_bytes
        device_input_bytes += telemetry.device_input_bytes
        output_bytes += telemetry.output_bytes
        if first_output_s is None:
            first_output_s = time.perf_counter() - started

    batches = iter(source_df.into_batches(batch_size).to_arrow_iter(results_buffer_size=2))
    while True:
        source_next_started = time.perf_counter()
        try:
            record_batch = next(batches)
        except StopIteration:
            break
        batch_source_next_s.append(time.perf_counter() - source_next_started)

        materialize_started = time.perf_counter()
        doc_ids = tuple(str(item.as_py()) for item in record_batch["doc_id"])
        encoded = [item.as_py() for item in record_batch["image"]]
        batch_driver_materialize_s.append(time.perf_counter() - materialize_started)
        preprocessor = worker_pool.preprocessors[
            cpu_position % len(worker_pool.preprocessors)
        ]
        gpu_actor = worker_pool.gpu_actors[gpu_position % len(worker_pool.gpu_actors)]
        cpu_position += 1
        gpu_position += 1
        submit_started = time.perf_counter()
        preprocessed = preprocessor.preprocess.remote(doc_ids, encoded)
        submitted_at = time.perf_counter()
        output = gpu_actor.embed.remote(preprocessed)
        batch_submit_s.append(time.perf_counter() - submit_started)
        pending[output] = submitted_at
        submitted_batches += 1
        pending_batches_peak = max(pending_batches_peak, len(pending))
        if len(pending) >= max_active_batches:
            consume_one()

    while pending:
        consume_one()

    total_s = time.perf_counter() - started
    return ExecutionResult(
        total_s=total_s,
        first_output_s=first_output_s or total_s,
        audit=audit.finish(),
        batch_completion_wall_s=tuple(batch_completion_wall_s),
        batch_actor_service_s=tuple(batch_actor_service_s),
        batch_unattributed_wait_s=tuple(batch_unattributed_wait_s),
        batch_preprocess_s=tuple(batch_preprocess_s),
        batch_host_copy_s=tuple(batch_host_copy_s),
        batch_h2d_s=tuple(batch_h2d_s),
        batch_forward_s=tuple(batch_forward_s),
        batch_d2h_s=tuple(batch_d2h_s),
        batch_source_next_s=tuple(batch_source_next_s),
        batch_driver_materialize_s=tuple(batch_driver_materialize_s),
        batch_submit_s=tuple(batch_submit_s),
        encoded_bytes=encoded_bytes,
        input_tensor_bytes=input_tensor_bytes,
        device_input_bytes=device_input_bytes,
        output_bytes=output_bytes,
        submitted_batches=submitted_batches,
        pending_batches_peak=pending_batches_peak,
    )
