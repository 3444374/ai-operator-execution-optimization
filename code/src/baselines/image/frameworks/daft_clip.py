"""Local CLIP in Daft's native graph, for RGB RM batches or encoded file inputs.

The stateful UDF is project-authored (``diagnostic_reference``); Daft owns its
batching, resource placement and worker lifetime. No HTTP or SemLoom scheduler
is involved. Each call builds and fully consumes a fresh graph; Daft may retain
the stateful model instance across graphs using the same class wrapper.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import nullcontext
import hashlib
from importlib import metadata
import math
import os
from pathlib import Path
import threading
import time
from typing import TYPE_CHECKING

import numpy as np

from src.modalities.image.clip import ClipTensorActor
from src.modalities.image.contracts import (
    EmbeddingSemantics, ImageEmbeddingBatch, ImageEmbeddingResult,
)

if TYPE_CHECKING:
    from PIL import Image


DAFT_VERSION = "0.7.21"


def _span(operation):
    # Reuse the optional timing layer when integrated; no writer when disabled.
    if not os.environ.get("SEMLOOM_DETAILED_TIMING_DIR"):
        return nullcontext()
    from src.experiments.execution_timing import timing_span

    return timing_span(operation, backend="daft-image")


def _load_processor(path):
    from transformers import CLIPImageProcessor

    # This explicit class uses the same slow PIL resize/crop/normalization as
    # ClipImagePreprocessor, with decode supplied by Daft instead of PIL.
    return CLIPImageProcessor.from_pretrained(path, local_files_only=True)


def _build_embedder(*, model_path, processor_path, dtype, batch_size,
                    model_workers, gpus_per_worker, device, torch_threads):
    import daft

    actor_type, load_processor = ClipTensorActor, _load_processor

    @daft.cls(cpus=1, gpus=gpus_per_worker, use_process=True,
              max_concurrency=model_workers, max_retries=0, on_error="raise")
    class NativeClip:
        def __init__(self):
            with _span("daft_image.model_load"):
                self.processor = load_processor(processor_path)
                self.actor = actor_type(
                    model_path, processor_revision=processor_path, device=device,
                    dtype=dtype, normalize=True, local_files_only=True,
                    detailed_stage_timing=False, torch_intraop_threads=torch_threads,
                    torch_interop_threads=torch_threads,
                )
                if self.actor.semantics.dimension != 512:
                    raise ValueError("Daft CLIP requires a 512-dimensional projection")

        @daft.method.batch(
            return_dtype=daft.DataType.fixed_size_list(daft.DataType.float32(), 512),
            batch_size=batch_size, max_retries=0, on_error="raise",
        )
        def embed(self, decoded_images, doc_ids):
            with _span("daft_image.preprocess"):
                pixels = np.ascontiguousarray(self.processor(
                    images=decoded_images.to_pylist(), return_tensors="np",
                    input_data_format="channels_last",
                )["pixel_values"], dtype=np.float32)
            ids = tuple(doc_ids.to_pylist())
            batch = ImageEmbeddingBatch(
                doc_ids=ids, payload=pixels, input_kind="preprocessed_tensor",
                work_units=len(ids) * int(pixels.shape[-2]) * int(pixels.shape[-1]),
                work_unit="pixels",
            )
            try:
                with _span("daft_image.model_embed"):
                    return self.actor.embed(batch).embeddings
            except BaseException as error:
                # Confirm our own device work before allowing this worker to fail.
                try:
                    self.actor.synchronize()
                except BaseException as cleanup:
                    from src.experiments.execution_timing import is_timing_error
                    if is_timing_error(error) and not is_timing_error(cleanup):
                        cleanup.add_note("Prior image timing failure: " + type(error).__name__)
                        raise cleanup from error
                    error.add_note("CLIP synchronize also failed: " + type(cleanup).__name__)
                raise

    return NativeClip()


def _digest(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _arrow_embedding_matrix(column):
    """Read native fixed float32 vectors without creating Python float lists.

    Child NumPy views retain their Arrow buffers. The existing final assembly
    and ID reorder still produce the caller-owned result matrix.
    """
    import pyarrow as pa

    if (not pa.types.is_fixed_size_list(column.type) or column.type.list_size != 512
            or not pa.types.is_float32(column.type.value_type)):
        raise ValueError("Daft CLIP Arrow vectors must be fixed-size float32 lists of width 512")
    chunks = column.chunks if isinstance(column, pa.ChunkedArray) else (column,)
    matrices = []
    for chunk in chunks:
        if chunk.null_count:
            raise ValueError("Daft CLIP Arrow vectors must not contain null rows")
        # FixedSizeListArray.values ignores the parent's row offset; slicing it
        # explicitly also respects any independent offset on the child array.
        values = chunk.values.slice(chunk.offset * 512, len(chunk) * 512)
        if values.null_count:
            raise ValueError("Daft CLIP Arrow vectors must not contain null values")
        matrices.append(values.to_numpy(zero_copy_only=True).reshape(len(chunk), 512))
    if not matrices:
        return np.empty((0, 512), dtype=np.float32)
    return matrices[0] if len(matrices) == 1 else np.concatenate(matrices)


class DaftClipExecutor:
    """Bounded synchronous image batch interface; inference is a native graph.

    ``max_batch_rows`` limits caller input/result materialization, not Daft's
    internal buffers. Models load in the Daft-owned UDF execution context; the
    native engine chooses placement, which must be observed for each setting.
    Successful calls exhaust the graph before returning. A failed native graph
    poisons this object because the SDK exposes no public worker-join handle.
    """

    input_kind = "rgb_pil"

    def __init__(self, *, model_path: str, processor_path: str | None = None,
                 dtype: str = "float32", batch_size: int = 32,
                 model_workers: int = 1, gpus_per_worker: float = 1,
                 device: str = "cuda", torch_threads: int = 1,
                 max_batch_rows: int = 4096, input_kind: str = "rgb_pil"):
        if input_kind not in ("rgb_pil", "encoded_bytes"):
            raise ValueError("Daft CLIP input_kind must be rgb_pil or encoded_bytes")
        self.input_kind = input_kind
        if any(type(v) is not int or v <= 0 for v in
               (batch_size, model_workers, torch_threads, max_batch_rows)):
            raise ValueError("batch size, worker count, threads and row limit must be positive integers")
        if batch_size > max_batch_rows:
            raise ValueError("native batch size must not exceed caller row limit")
        if dtype not in ("float32", "float16", "bfloat16"):
            raise ValueError("unsupported CLIP compute dtype")
        if (isinstance(gpus_per_worker, bool) or not isinstance(gpus_per_worker, (int, float))
                or not math.isfinite(gpus_per_worker) or not 0 <= gpus_per_worker <= 1):
            raise ValueError("GPU share must be finite and between zero and one")
        if not ((device == "cpu" and gpus_per_worker == 0)
                or (device == "cuda" and gpus_per_worker > 0)):
            raise ValueError("CPU requires zero GPUs; CUDA requires a positive native GPU resource")
        import daft
        from daft.runners import get_or_create_runner

        if daft.__version__ != DAFT_VERSION or get_or_create_runner().name != "native":
            raise RuntimeError("Daft CLIP requires Daft 0.7.21 with its native runner")
        model = Path(model_path).resolve(strict=True)
        processor = Path(processor_path or model_path).resolve(strict=True)
        weights = model / "model.safetensors"
        if not weights.exists():
            weights = model / "pytorch_model.bin"
        artifacts = {
            "weights": _digest(weights), "model_config": _digest(model / "config.json"),
            "processor_config": _digest(processor / "preprocessor_config.json"),
        }
        self.semantics = EmbeddingSemantics(
            model_revision=str(model), processor_revision=str(processor), dimension=512,
        )
        versions = {}
        for package in ("daft", "torch", "transformers", "pyarrow", "numpy", "Pillow"):
            try:
                versions[package] = metadata.version(package)
            except metadata.PackageNotFoundError:
                versions[package] = None
        self.identity = dict(
            scheduler_owner="daft_native", implementation="diagnostic_reference",
            model_revision=self.semantics.model_revision,
            processor_revision=self.semantics.processor_revision, artifacts_sha256=artifacts,
            compute_dtype=dtype, output_dtype="float32", dimension=512, projected=True,
            normalized=True, processor="CLIPImageProcessor", decode="Daft RGB",
            batch_size=batch_size, model_workers=model_workers, gpus_per_worker=gpus_per_worker,
            device=device, use_process_requested=True, max_retries=0, max_batch_rows=max_batch_rows,
            torch_threads=torch_threads, software=versions, model_lifetime="daft_owned_stateful_instance",
            input_kind=input_kind,
        )
        if input_kind == "rgb_pil":
            self.identity.update(
                input_kind=self.input_kind,
                input_adapter="PIL RGB -> HWC uint8 -> Daft Image[RGB]",
                decode="none (caller-decoded RGB)",
            )
        self._embedder = _build_embedder(
            model_path=str(model), processor_path=str(processor), dtype=dtype,
            batch_size=batch_size, model_workers=model_workers,
            gpus_per_worker=gpus_per_worker, device=device, torch_threads=torch_threads,
        )
        self._lock = threading.Lock()
        self._closed = False
        self._unconfirmed_work = False

    def embed_batch(self, doc_ids: tuple[str, ...],
                    images: Sequence[bytes | Image.Image]) -> ImageEmbeddingResult:
        if not isinstance(doc_ids, tuple) or any(type(i) is not str or not i for i in doc_ids):
            raise ValueError("doc_ids must be a tuple of non-empty strings")
        if len(set(doc_ids)) != len(doc_ids):
            raise ValueError("doc_ids must be unique")
        if self.input_kind == "rgb_pil":
            from PIL import Image

            if len(images) != len(doc_ids):
                raise ValueError("images must contain one image per ID")
            if len(doc_ids) > self.identity["max_batch_rows"]:
                raise ValueError("caller batch exceeds max_batch_rows")
            # Validate the whole caller batch before any native submission, so a
            # closed/corrupt suffix cannot execute an otherwise valid prefix.
            for image in images:
                if (not isinstance(image, Image.Image) or image.mode != "RGB"
                        or image.width <= 0 or image.height <= 0):
                    raise ValueError("native images must be non-empty RGB PIL images")
                image.load()
        else:
            if len(images) != len(doc_ids) or any(type(i) is not bytes or not i for i in images):
                raise ValueError("images must be non-empty encoded bytes, one per ID")
            if len(doc_ids) > self.identity["max_batch_rows"]:
                raise ValueError("caller batch exceeds max_batch_rows")
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("Daft CLIP already has an active batch")
        try:
            if self._closed or self._unconfirmed_work:
                raise RuntimeError("Daft CLIP is closed or has unconfirmed native work")
            if not doc_ids:
                return ImageEmbeddingResult(doc_ids, np.empty((0, 512), dtype=np.float32),
                                            self.semantics, 0)
            with _span("daft_image.batch"):
                return self._execute(doc_ids, images)
        finally:
            self._lock.release()

    def _execute(self, doc_ids, images):
        import daft
        from daft.functions import decode_image

        started = time.perf_counter()
        payloads = ([np.asarray(image) for image in images]
                    if self.input_kind == "rgb_pil" else list(images))
        frame = daft.from_pydict({"doc_id": list(doc_ids), "image": payloads})
        frame = frame.into_batches(self.identity["batch_size"])
        if self.input_kind == "rgb_pil":
            frame = frame.with_column("_rgb", frame["image"].cast(daft.DataType.image("RGB")))
        else:
            frame = frame.with_column("_rgb", decode_image(frame["image"], on_error="raise",
                                                           mode=daft.ImageMode.RGB))
        query = frame.with_column("embedding", self._embedder.embed(frame["_rgb"], frame["doc_id"]))
        query = query.select("doc_id", "embedding")
        iterator = None
        self._unconfirmed_work = True
        ids, chunks = [], []
        try:
            iterator = query.to_arrow_iter()
            with _span("daft_image.graph_consume"):
                for batch in iterator:
                    ids.extend(batch["doc_id"].to_pylist())
                    chunks.append(_arrow_embedding_matrix(batch["embedding"]))
        except BaseException as error:
            try:
                if iterator is not None:
                    iterator.close()
            except BaseException as cleanup:
                from src.experiments.execution_timing import is_timing_error
                if is_timing_error(error) and not is_timing_error(cleanup):
                    cleanup.add_note("Prior image timing failure: " + type(error).__name__)
                    raise cleanup from error
                error.add_note("Daft iterator close also failed: " + type(cleanup).__name__)
            raise
        else:
            iterator.close()
            self._unconfirmed_work = False
        matrix = np.concatenate(chunks) if chunks else np.empty((0, 512), dtype=np.float32)
        if len(ids) != len(doc_ids) or len(set(ids)) != len(ids) or set(ids) != set(doc_ids):
            raise ValueError("Daft CLIP output ID mismatch or duplicate")
        if matrix.shape != (len(doc_ids), 512) or not np.isfinite(matrix).all():
            raise ValueError("Daft CLIP output must contain complete finite 512-dimensional vectors")
        if not np.allclose(np.linalg.norm(matrix, axis=1), 1, atol=1e-5, rtol=0):
            raise ValueError("Daft CLIP output must be unit normalized in float32")
        positions = {identifier: index for index, identifier in enumerate(ids)}
        matrix = matrix[[positions[identifier] for identifier in doc_ids]]
        return ImageEmbeddingResult(doc_ids, matrix, self.semantics, time.perf_counter() - started)

    def close(self):
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("cannot close Daft CLIP while a batch is active")
        try:
            if self._unconfirmed_work:
                raise RuntimeError("native graph failed; worker exit is unconfirmed")
            self._embedder = None
            self._closed = True
        finally:
            self._lock.release()
