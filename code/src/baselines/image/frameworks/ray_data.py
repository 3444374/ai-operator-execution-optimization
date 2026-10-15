"""Ray Data-native SQL/files -> CPU ``map_batches`` -> GPU actor baseline."""

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
from src.modalities.image.file_source import ImageFileManifest, file_doc_id


def _bind_file_rows(batch, *, rows_by_path, pass_index, passes):
    """Attach fixed identities to native binary-reader output without decoding."""
    from pathlib import Path

    ids = []
    for path, encoded in zip(batch["path"], batch["bytes"], strict=True):
        key = str(Path(str(path)).resolve())
        if key not in rows_by_path:
            raise ValueError("native file reader returned an unexpected path")
        doc_id, size = rows_by_path[key]
        if len(encoded) != size:
            raise ValueError("native file reader returned a changed encoded size")
        ids.append(file_doc_id(doc_id, pass_index, passes))
    return {"doc_id": np.asarray(ids), "image": np.asarray(batch["bytes"], dtype=object)}


def _read_ray_image_files(manifest, config, source_shards):
    import ray.data

    entries = manifest.select(config)
    rows_by_path = {str(manifest.root / entry.path): (entry.doc_id, entry.image_bytes) for entry in entries}
    datasets = []
    for pass_index in range(1, config.dataset_passes + 1):
        dataset = ray.data.read_binary_files(
            list(rows_by_path), include_paths=True, ignore_missing_paths=False,
            shuffle=None, override_num_blocks=source_shards, concurrency=source_shards, num_cpus=1,
        )
        dataset.context.execution_options.preserve_order = True
        dataset = dataset.map_batches(
            functools.partial(_bind_file_rows, rows_by_path=rows_by_path,
                              pass_index=pass_index, passes=config.dataset_passes),
            batch_format="numpy", num_cpus=1,
        )
        datasets.append(dataset)
    dataset = datasets[0]
    for other in datasets[1:]:
        dataset = dataset.union(other)
    return dataset


class RayDataClipPreprocessor:
    """Stateful CPU ``map_batches`` callable for encoded JPEG images."""

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
        output = {
            "doc_id": np.asarray(batch["doc_id"]),
            "embedding": result.embeddings,
        }
        probe = self._actor._reuse_probe
        if probe is not None:
            # Scalar identity/counters travel in the native returned batch, so
            # graph shutdown need not flush a worker-side per-request writer.
            observed = probe.snapshot()
            for name, value in (("uid", observed["model_uid"]), ("pid", observed["pid"]),
                                ("embed_calls", observed["image_returns"]),
                                ("rows", observed["image_returned_rows"])):
                output["_semloom_model_" + name] = np.full(len(doc_ids), value)
        return output


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
    file_manifest: ImageFileManifest | None = None,
):
    """Return one lazy Ray Data staged inference pipeline.

    The native source, CPU callable actors, GPU actor pool, backpressure, and task
    dispatch all remain inside Ray Data. The adapter defines only workload
    UDFs and native worker/batch configuration; it does not implement project
    admission or in-flight scheduling.
    """
    if not database_url and file_manifest is None:
        raise ValueError("database_url must be non-empty")
    if min(
        source_config.limit,
        source_shards,
        batch_size,
        cpu_workers,
        gpu_workers,
    ) <= 0:
        raise ValueError("row, shard, batch, and worker values must be positive")

    import ray.data

    if file_manifest is not None:
        dataset = _read_ray_image_files(file_manifest, source_config, source_shards)
    else:
        import psycopg

        connection_factory = functools.partial(psycopg.connect, database_url)
        # Use the same non-overlapping ordered SQL ranges as the Daft source.
        source_datasets = [
            ray.data.read_sql(
                image_documents_query(shard), connection_factory,
                override_num_blocks=1, concurrency=1, num_cpus=1,
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
