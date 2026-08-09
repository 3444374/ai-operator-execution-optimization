"""Ray Data-native SQL -> CPU ``map_batches`` -> GPU actor baseline."""

from __future__ import annotations

import functools
import time

import numpy as np

from src.modalities.image.execution import EmbeddingAudit, ExecutionResult
from src.modalities.image.source import (
    ImageSourceConfig,
    image_documents_query,
    split_image_source_config,
)


class RayDataClipPreprocessor:
    """Stateful CPU ``map_batches`` callable for encoded PostgreSQL images."""

    def __init__(
        self,
        processor_revision: str,
        torch_intraop_threads: int,
        torch_interop_threads: int,
    ) -> None:
        from src.modalities.image.clip import FastClipImagePreprocessor

        self._preprocessor = FastClipImagePreprocessor(
            processor_revision,
            torch_intraop_threads=torch_intraop_threads,
            torch_interop_threads=torch_interop_threads,
        )

    def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        encoded = [bytes(item) for item in batch["image"]]
        return {
            "doc_id": np.asarray(batch["doc_id"]),
            "pixel_values": self._preprocessor.preprocess(encoded),
        }


class RayDataClipPredictor:
    """Stateful GPU ``map_batches`` callable accepting only pixel tensors."""

    def __init__(
        self,
        model_revision: str,
        processor_revision: str,
        dtype: str,
        torch_intraop_threads: int,
        torch_interop_threads: int,
    ) -> None:
        from src.modalities.image.clip import ClipTensorActor

        self._actor = ClipTensorActor(
            model_revision,
            processor_revision=processor_revision,
            dtype=dtype,
            normalize=True,
            torch_intraop_threads=torch_intraop_threads,
            torch_interop_threads=torch_interop_threads,
        )

    def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        from src.modalities.image.contracts import ImageEmbeddingBatch

        pixels = np.ascontiguousarray(batch["pixel_values"], dtype=np.float32)
        doc_ids = tuple(str(item) for item in batch["doc_id"])
        request = ImageEmbeddingBatch(
            doc_ids=doc_ids,
            payload=pixels,
            input_kind="preprocessed_tensor",
            work_units=len(doc_ids) * int(pixels.shape[-2]) * int(pixels.shape[-1]),
            work_unit="pixels",
        )
        result = self._actor.embed(request)
        return {
            "doc_id": np.asarray(batch["doc_id"]),
            "embedding": result.embeddings,
        }


def build_ray_data_clip_pipeline(
    *,
    database_url: str,
    source_config: ImageSourceConfig,
    source_shards: int,
    processor_revision: str,
    model_revision: str,
    dtype: str,
    batch_size: int,
    cpu_workers: int,
    gpu_workers: int,
    autoscaling_actor_pools: bool = False,
    torch_intraop_threads: int = 1,
    torch_interop_threads: int = 1,
):
    """Return one lazy Ray Data staged inference pipeline.

    The SQL reader, CPU callable actors, GPU actor pool, backpressure, and task
    dispatch all remain inside Ray Data. The adapter defines only workload
    UDFs and native worker/batch configuration; it does not implement project
    admission or in-flight scheduling.
    """
    if not database_url:
        raise ValueError("database_url must be non-empty")
    if min(
        source_config.limit,
        source_shards,
        batch_size,
        cpu_workers,
        gpu_workers,
    ) <= 0:
        raise ValueError("row, shard, batch, and worker values must be positive")

    import psycopg
    import ray.data

    connection_factory = functools.partial(psycopg.connect, database_url)
    # Ray SQL's hash sharding does not support this ordered LIMIT/OFFSET query
    # reliably across PostgreSQL types.  Use the exact same non-overlapping
    # contiguous ranges as the Daft source and union their lazy read tasks.
    source_datasets = [
        ray.data.read_sql(
            image_documents_query(shard),
            connection_factory,
            override_num_blocks=1,
            concurrency=1,
            num_cpus=1,
        )
        for shard in split_image_source_config(source_config, source_shards)
    ]
    dataset = source_datasets[0]
    for shard_dataset in source_datasets[1:]:
        dataset = dataset.union(shard_dataset)
    cpu_pool = (
        ray.data.ActorPoolStrategy(min_size=1, max_size=cpu_workers)
        if autoscaling_actor_pools
        else ray.data.ActorPoolStrategy(size=cpu_workers)
    )
    dataset = dataset.map_batches(
        RayDataClipPreprocessor,
        fn_constructor_kwargs={
            "processor_revision": processor_revision,
            "torch_intraop_threads": torch_intraop_threads,
            "torch_interop_threads": torch_interop_threads,
        },
        batch_size=batch_size,
        batch_format="numpy",
        compute=cpu_pool,
        num_cpus=1,
        zero_copy_batch=True,
    )
    gpu_pool = (
        ray.data.ActorPoolStrategy(min_size=1, max_size=gpu_workers)
        if autoscaling_actor_pools
        else ray.data.ActorPoolStrategy(size=gpu_workers)
    )
    return dataset.map_batches(
        RayDataClipPredictor,
        fn_constructor_kwargs={
            "model_revision": model_revision,
            "processor_revision": processor_revision,
            "dtype": dtype,
            "torch_intraop_threads": torch_intraop_threads,
            "torch_interop_threads": torch_interop_threads,
        },
        batch_size=batch_size,
        batch_format="numpy",
        compute=gpu_pool,
        num_cpus=1,
        num_gpus=1,
        zero_copy_batch=True,
    )


def run_ray_data_clip_baseline(
    dataset,
    *,
    expected_doc_ids: frozenset[str],
    embedding_dimension: int = 512,
) -> ExecutionResult:
    """Execute a lazy Ray Data pipeline and validate streamed output batches."""
    audit = EmbeddingAudit(
        expected_doc_ids=expected_doc_ids,
        dimension=embedding_dimension,
    )
    started = time.perf_counter()
    first_output_s: float | None = None
    for record_batch in dataset.iter_batches(
        batch_format="pyarrow",
        prefetch_batches=2,
    ):
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
        engine_stats=dataset.stats(),
    )
