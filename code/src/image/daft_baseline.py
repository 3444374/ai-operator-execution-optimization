"""Official-style Daft ``@daft.cls`` CLIP baseline implementation."""

from __future__ import annotations

import time

import numpy as np

from .execution import EmbeddingAudit, ExecutionResult


def build_daft_clip_embedder(
    *,
    model_revision: str,
    processor_revision: str,
    batch_size: int,
    gpu_workers: int,
    dtype: str,
    embedding_dimension: int = 512,
):
    """Build the strong Daft-native persistent GPU UDF baseline.

    The UDF owns decode, preprocessing, transfer, and CLIP forward. This is the
    official Daft execution boundary; unlike the project pipeline it does not
    expose an intermediate tensor batch to an external scheduler.
    """
    if batch_size <= 0 or gpu_workers <= 0:
        raise ValueError("batch_size and gpu_workers must be positive")

    import daft

    return_dtype = daft.DataType.fixed_size_list(
        daft.DataType.float32(),
        embedding_dimension,
    )

    @daft.cls(cpus=1, gpus=1, max_concurrency=gpu_workers, max_retries=0)
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


def run_daft_clip_baseline(
    source_df,
    *,
    embedder,
    expected_doc_ids: frozenset[str],
    partitions: int,
    embedding_dimension: int = 512,
) -> ExecutionResult:
    """Stream one Daft UDF query and validate every output row."""
    if partitions <= 0:
        raise ValueError("partitions must be positive")
    audit = EmbeddingAudit(
        expected_doc_ids=expected_doc_ids,
        dimension=embedding_dimension,
    )
    source_df = source_df.repartition(partitions)
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
