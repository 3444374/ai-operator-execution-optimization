"""Fused and explicitly staged Daft ``@daft.cls`` CLIP baselines."""

from __future__ import annotations

import time

import numpy as np

from .execution import EmbeddingAudit, ExecutionResult


def build_daft_clip_embedder(
    *,
    model_revision: str,
    processor_revision: str,
    batch_size: int,
    model_workers: int,
    gpus_per_worker: float,
    dtype: str,
    embedding_dimension: int = 512,
):
    """Build the strong Daft-native persistent GPU UDF baseline.

    The UDF owns decode, preprocessing, transfer, and CLIP forward. This is the
    official Daft execution boundary; unlike the project pipeline it does not
    expose an intermediate tensor batch to an external scheduler.
    """
    if batch_size <= 0 or model_workers <= 0 or gpus_per_worker <= 0:
        raise ValueError("batch size, model workers, and GPU share must be positive")
    if gpus_per_worker > 1:
        raise ValueError("each Daft CLIP worker may use at most one GPU")

    import daft

    return_dtype = daft.DataType.fixed_size_list(
        daft.DataType.float32(),
        embedding_dimension,
    )

    @daft.cls(
        cpus=1,
        gpus=gpus_per_worker,
        max_concurrency=model_workers,
        max_retries=0,
    )
    class DaftClipEmbedder:
        def __init__(
            self,
            model_path: str,
            processor_path: str,
            model_dtype: str,
        ) -> None:
            from .clip import ClipTensorActor, FastClipImagePreprocessor

            self._preprocessor = FastClipImagePreprocessor(processor_path)
            self._actor = ClipTensorActor(
                model_path,
                processor_revision=processor_path,
                dtype=model_dtype,
                normalize=True,
            )

        @daft.method.batch(return_dtype=return_dtype, batch_size=batch_size)
        def embed(self, encoded_images):
            from .contracts import ImageEmbeddingBatch

            encoded = encoded_images.to_pylist()
            pixels = self._preprocessor.preprocess(encoded)
            batch = ImageEmbeddingBatch(
                doc_ids=tuple(str(index) for index in range(len(encoded))),
                payload=pixels,
                input_kind="preprocessed_tensor",
                work_units=len(encoded) * int(pixels.shape[-2]) * int(pixels.shape[-1]),
                work_unit="pixels",
            )
            return self._actor.embed(batch).embeddings

    return DaftClipEmbedder(model_revision, processor_revision, dtype)


def build_daft_staged_clip_pipeline(
    *,
    model_revision: str,
    processor_revision: str,
    batch_size: int,
    cpu_workers: int,
    model_workers: int,
    gpus_per_worker: float,
    dtype: str,
    embedding_dimension: int = 512,
):
    """Build Daft CPU-preprocess and tensor-only GPU stages.

    Unlike :func:`build_daft_clip_embedder`, this strong baseline gives Daft
    explicit CPU and GPU operator boundaries so its Ray runner can overlap
    preprocessing with model execution.  It mirrors the staged pipeline
    recommended by Daft/Ray rather than reserving a GPU while decoding JPEGs.
    """
    if min(batch_size, cpu_workers, model_workers) <= 0 or gpus_per_worker <= 0:
        raise ValueError("batch size, worker counts, and GPU share must be positive")
    if gpus_per_worker > 1:
        raise ValueError("each Daft CLIP worker may use at most one GPU")

    import daft

    pixel_dtype = daft.DataType.tensor(
        daft.DataType.float32(),
        (3, 224, 224),
    )
    embedding_dtype = daft.DataType.fixed_size_list(
        daft.DataType.float32(),
        embedding_dimension,
    )

    @daft.cls(cpus=1, max_concurrency=cpu_workers, max_retries=0)
    class DaftClipPreprocessor:
        def __init__(self, processor_path: str) -> None:
            from .clip import FastClipImagePreprocessor

            self._preprocessor = FastClipImagePreprocessor(processor_path)

        @daft.method.batch(return_dtype=pixel_dtype, batch_size=batch_size)
        def preprocess(self, encoded_images):
            return self._preprocessor.preprocess(encoded_images.to_pylist())

    @daft.cls(
        cpus=1,
        gpus=gpus_per_worker,
        max_concurrency=model_workers,
        max_retries=0,
    )
    class DaftClipTensorEmbedder:
        def __init__(
            self,
            model_path: str,
            processor_path: str,
            model_dtype: str,
        ) -> None:
            from .clip import ClipTensorActor

            self._actor = ClipTensorActor(
                model_path,
                processor_revision=processor_path,
                dtype=model_dtype,
                normalize=True,
            )

        @daft.method.batch(return_dtype=embedding_dtype, batch_size=batch_size)
        def embed(self, pixel_values):
            from .contracts import ImageEmbeddingBatch

            pixels = pixel_values.to_arrow().to_numpy_ndarray()
            rows = int(pixels.shape[0])
            batch = ImageEmbeddingBatch(
                doc_ids=tuple(str(index) for index in range(rows)),
                payload=pixels,
                input_kind="preprocessed_tensor",
                work_units=rows * int(pixels.shape[-2]) * int(pixels.shape[-1]),
                work_unit="pixels",
            )
            return self._actor.embed(batch).embeddings

    return (
        DaftClipPreprocessor(processor_revision),
        DaftClipTensorEmbedder(model_revision, processor_revision, dtype),
    )


def run_daft_clip_baseline(
    source_df,
    *,
    embedder,
    expected_doc_ids: frozenset[str],
    embedding_dimension: int = 512,
) -> ExecutionResult:
    """Stream one Daft UDF query and validate every output row."""
    audit = EmbeddingAudit(
        expected_doc_ids=expected_doc_ids,
        dimension=embedding_dimension,
    )
    query = source_df.with_column("embedding", embedder.embed(source_df["image"]))
    query = query.select("doc_id", "embedding")
    started = time.perf_counter()
    first_output_s: float | None = None
    for record_batch in query.to_arrow_iter(results_buffer_size=2):
        doc_ids = tuple(str(item.as_py()) for item in record_batch["doc_id"])
        embeddings = np.asarray(record_batch["embedding"].to_pylist(), dtype=np.float32)
        audit.add(doc_ids, embeddings)
        if first_output_s is None:
            first_output_s = time.perf_counter() - started
    total_s = time.perf_counter() - started
    return ExecutionResult(
        total_s=total_s,
        first_output_s=first_output_s or total_s,
        audit=audit.finish(),
    )


def run_daft_staged_clip_baseline(
    source_df,
    *,
    preprocessor,
    embedder,
    expected_doc_ids: frozenset[str],
    embedding_dimension: int = 512,
) -> ExecutionResult:
    """Stream one Daft query with explicit CPU and GPU operator stages."""
    audit = EmbeddingAudit(
        expected_doc_ids=expected_doc_ids,
        dimension=embedding_dimension,
    )
    query = source_df.with_column(
        "pixel_values",
        preprocessor.preprocess(source_df["image"]),
    )
    query = query.with_column(
        "embedding",
        embedder.embed(query["pixel_values"]),
    ).select("doc_id", "embedding")
    started = time.perf_counter()
    first_output_s: float | None = None
    for record_batch in query.to_arrow_iter(results_buffer_size=2):
        doc_ids = tuple(str(item.as_py()) for item in record_batch["doc_id"])
        embeddings = np.asarray(record_batch["embedding"].to_pylist(), dtype=np.float32)
        audit.add(doc_ids, embeddings)
        if first_output_s is None:
            first_output_s = time.perf_counter() - started
    total_s = time.perf_counter() - started
    return ExecutionResult(
        total_s=total_s,
        first_output_s=first_output_s or total_s,
        audit=audit.finish(),
    )
