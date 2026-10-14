"""Diagnostic metadata only: delegate original kernels/consumer without mutation.

No added model call, dtype/batch change, GPU transfer, or synchronization.
CPU fingerprints read existing encoded/pixel/output values. Timings are observed
only in this capture diagnostic and never enter the original performance matrix.
"""
import hashlib
import json
import os
from pathlib import Path
import threading
import time

import numpy as np

_lock = threading.Lock()
_installed = False
_original_preprocess = None
_original_embed = None


def record(kind, **fields):
    directory = os.environ.get("SEMLOOM_CLIP_TRACE_DIR")
    if not directory:
        return
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    item = {"kind": kind, "pid": os.getpid(), "time_ns": time.time_ns(), **fields}
    with _lock:
        with (directory / f"worker-{os.getpid()}.jsonl").open("a") as handle:
            handle.write(json.dumps(item, separators=(",", ":")) + "\n")


def digest(value, dtype):
    return hashlib.sha256(np.asarray(value, dtype=dtype).tobytes(order="C")).hexdigest()


def observed_preprocess(self, encoded_images):
    values = _original_preprocess(self, encoded_images)
    record("cpu_preprocess", rows=len(encoded_images), shape=list(values.shape), dtype=str(values.dtype),
           items=[{"encoded_sha256": hashlib.sha256(encoded).hexdigest(),
                   "encoded_bytes": len(encoded), "pixel_fp32_sha256": digest(row, "<f4")}
                  for encoded, row in zip(encoded_images, values, strict=True)])
    return values


def observed_embed(self, batch):
    payload = np.asarray(batch.payload)
    before = [{"doc_id": str(doc_id), "pixel_fp32_sha256": digest(row, "<f4"),
               "cpu_cast_fp16_sha256": digest(row, "<f2")}
              for doc_id, row in zip(batch.doc_ids, payload, strict=True)]
    started = time.time_ns()
    result = _original_embed(self, batch)
    record("gpu_model", started_ns=started, rows=len(batch.doc_ids), shape=list(payload.shape),
           input_dtype=str(payload.dtype), model_dtype=str(self._dtype),
           output_shape=list(result.embeddings.shape), output_dtype=str(result.embeddings.dtype),
           doc_ids=[str(value) for value in batch.doc_ids], input_fingerprints=before,
           output_fp32_sha256=[digest(row, "<f4") for row in result.embeddings])
    return result


def install_kernel_observers():
    global _installed, _original_preprocess, _original_embed
    if _installed:
        return
    from src.modalities.image.clip import FastClipImagePreprocessor, ClipTensorActor
    _original_preprocess = FastClipImagePreprocessor.preprocess
    _original_embed = ClipTensorActor.embed
    FastClipImagePreprocessor.preprocess = observed_preprocess
    ClipTensorActor.embed = observed_embed
    _installed = True
    record("observer_installed", implementation="delegating CPU-resident fingerprints; no algorithm change")


class ColumnObserver:
    def __init__(self, column, rows):
        self.column, self.rows = column, rows

    def to_pylist(self):
        started = time.perf_counter_ns()
        result = self.column.to_pylist()
        conversion_s = (time.perf_counter_ns() - started) / 1e9
        # The original np.asarray(float32) remains unmodified and unmeasured;
        # this only times vendor to_pylist once and returns its exact objects.
        record("arrow_to_pylist", rows=self.rows, seconds=conversion_s,
               container_type=type(result).__module__ + "." + type(result).__name__,
               first_item_type=(type(result[0]).__module__ + "." + type(result[0]).__name__) if result else None,
               first_item_shape=list(getattr(result[0], "shape", ())) if result else None)
        return result


class BatchObserver:
    def __init__(self, batch):
        self.batch = batch
        column = batch["embedding"]
        arrow_type = column.type
        record("arrow_schema", rows=batch.num_rows, schema=str(batch.schema),
               embedding_type=str(arrow_type), extension_name=getattr(arrow_type, "extension_name", None),
               storage_type=str(getattr(arrow_type, "storage_type", None)),
               array_class=type(column).__module__ + "." + type(column).__name__,
               embedding_chunks=getattr(column, "num_chunks", None))

    def __getitem__(self, name):
        value = self.batch[name]
        return ColumnObserver(value, self.batch.num_rows) if name == "embedding" else value


class DatasetObserver:
    def __init__(self, dataset):
        self.dataset = dataset

    def iter_batches(self, **kwargs):
        for batch in self.dataset.iter_batches(**kwargs):
            yield BatchObserver(batch)

    def stats(self):
        return self.dataset.stats()


def observe_cli(cli):
    original_execute = cli._ImageExecution.execute
    original_build = cli.build_ray_data_clip_pipeline
    original_setup = cli._load_image_run_setup

    def setup(*args, **kwargs):
        resolved = original_setup(*args, **kwargs)
        # Supply only the observer output directory to the existing Ray worker
        # environment; never include the parent lease token in Ray job metadata.
        resolved.worker_runtime_env["env_vars"]["SEMLOOM_CLIP_TRACE_DIR"] = os.environ["SEMLOOM_CLIP_TRACE_DIR"]
        return resolved

    def execute(self, limit, expected_ids, **kwargs):
        stage = "formal" if limit == 257 else "warmup"
        record("driver_boundary", event="start", stage=stage, expected_rows=limit,
               expected_ids_sha256=hashlib.sha256(("\n".join(sorted(expected_ids))+"\n").encode()).hexdigest())
        try:
            return original_execute(self, limit, expected_ids, **kwargs)
        finally:
            record("driver_boundary", event="end", stage=stage, expected_rows=limit)

    def build(**kwargs):
        return DatasetObserver(original_build(**kwargs))

    cli._ImageExecution.execute = execute
    cli.build_ray_data_clip_pipeline = build
    cli._load_image_run_setup = setup

    def restore():
        cli._ImageExecution.execute = original_execute
        cli.build_ray_data_clip_pipeline = original_build
        cli._load_image_run_setup = original_setup

    return restore
