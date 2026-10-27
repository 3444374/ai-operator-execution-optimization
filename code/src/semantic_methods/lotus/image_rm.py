"""Public LOTUS retrieval-model adapter for complete image embeddings."""
from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass, field
import hashlib
import io
import os
import time
from typing import Protocol, Sequence

import numpy as np
from PIL import Image

from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult


class ImageBatchExecutor(Protocol):
    """The executor owns batching/lifetime; absent input_kind means encoded bytes."""
    semantics: EmbeddingSemantics

    def embed_batch(self, doc_ids: tuple[str, ...], images: Sequence[bytes | Image.Image]) -> ImageEmbeddingResult:
        ...

    def close(self) -> None:
        ...


def timing_span(operation, **fields):
    if not os.environ.get("SEMLOOM_DETAILED_TIMING_DIR"):
        return nullcontext()
    # Reuse the optional timing slice when installed on the integration branch.
    from src.observability.execution_timing import timing_span as existing_span
    return existing_span(operation, **fields)


def image_bytes(image: Image.Image) -> bytes:
    """Lossless current-pixel fallback for executors that require encoded bytes."""
    if not isinstance(image, Image.Image) or image.mode != "RGB":
        raise TypeError("LOTUS image embedding requires RGB PIL images")
    with io.BytesIO() as stream:
        image.save(stream, format="PNG", compress_level=1)
        return stream.getvalue()


def checked_matrix(matrix, rows, semantics):
    matrix = np.asarray(matrix)
    if (matrix.dtype != np.float32 or matrix.shape != (rows, semantics.dimension)
            or not np.isfinite(matrix).all()):
        raise ValueError("embedding output must be complete finite float32 vectors")
    if semantics.dtype != "float32" or not semantics.projected or not semantics.normalized:
        raise ValueError("LOTUS CLIP retrieval requires projected normalized float32 vectors")
    if not np.allclose(np.linalg.norm(matrix, axis=1), 1, atol=2e-5, rtol=0):
        raise ValueError("embedding output is not L2 normalized")
    return np.ascontiguousarray(matrix)


@dataclass
class RMObservation:
    calls: list[dict] = field(default_factory=list)
    capture_vectors: bool = False
    vectors: dict[str, np.ndarray] = field(default_factory=dict)
    _released_calls: int = field(default=0, init=False, repr=False)
    _released_image_rows: int = field(default=0, init=False, repr=False)

    @property
    def call_count(self):
        return self._released_calls + len(self.calls)

    def retain(self, matrix):
        if self.capture_vectors:
            key = "call_" + str(self.call_count)
            if key in self.vectors:
                raise ValueError("RM vector evidence already exists for this call")
            self.vectors[key] = matrix.copy()

    def release_exported(self, *, calls=(), vectors=None):
        """Release only an explicitly persisted call prefix or vector snapshot.

        The caller confirms the write succeeded before calling this method.
        Default retention includes failed calls and all unexported evidence.
        """
        calls, vectors = tuple(calls), {} if vectors is None else dict(vectors)
        if len(calls) > len(self.calls) or any(
                left is not right for left, right in zip(calls, self.calls)):
            raise ValueError("exported RM calls must match a retained prefix")
        if any(key not in self.vectors or self.vectors[key] is not value
               for key, value in vectors.items()):
            raise ValueError("exported RM vectors must match retained identities")
        if calls:
            image_rows = sum(record["image_rows"] for record in calls)
            self._released_calls += len(calls)
            self._released_image_rows += image_rows
            self.calls = self.calls[len(calls):]
        for key in vectors:
            del self.vectors[key]

    @property
    def image_rows(self):
        return self._released_image_rows + sum(x["image_rows"] for x in self.calls)


def make_image_rm(executor: ImageBatchExecutor, text_rm, *, max_rows=5000,
                  max_output_bytes=32 * 1024 * 1024, observation=None,
                  source_images=None):
    """Replace only RM._embed; native RM query conversion and FAISS stay intact.

    A sem_index call requires a complete N×D matrix. This explicitly bounded
    collection is distinct from the existing streaming benchmark's capture.
    """
    input_kind = getattr(executor, "input_kind", "encoded_bytes")
    if input_kind not in ("encoded_bytes", "rgb_pil"):
        raise ValueError("LOTUS image executor input_kind must be encoded_bytes or rgb_pil")
    if source_images is not None:
        from collections.abc import Mapping
        if input_kind != "encoded_bytes" or not isinstance(source_images, Mapping):
            raise ValueError("source images require an encoded-byte executor and an identity mapping")
    from lotus.models.rm import RM

    if (type(max_rows) is not int or max_rows < 1 or type(max_output_bytes) is not int
            or max_output_bytes < 1):
        raise ValueError("RM collection limits must be positive integers")
    semantics = executor.semantics
    if semantics.dimension != 512:
        raise ValueError("this native CLIP workload requires 512 dimensions")
    observation = observation if observation is not None else RMObservation()

    class ImageRM(RM):
        def _embed(self, docs):
            if not docs or len(docs) > max_rows or len(docs) * 512 * 4 > max_output_bytes:
                raise ValueError("RM input exceeds the declared matrix collection size")
            if any(not isinstance(x, (Image.Image, str)) for x in docs):
                raise TypeError("RM inputs must be PIL images or text queries")
            started = time.monotonic_ns()
            images = [i for i, x in enumerate(docs) if isinstance(x, Image.Image)]
            texts = [i for i, x in enumerate(docs) if isinstance(x, str)]
            record = dict(call_index=observation.call_count, image_rows=len(images), text_rows=len(texts),
                          input_kind=input_kind, status="failed")
            try:
                matrix = np.empty((len(docs), 512), dtype=np.float32)
                if images:
                    ids = tuple(str(i) for i in images)
                    with timing_span("lotus.rm.image_embed", scope="rm_call"):
                        # Check every image before preparing or submitting a prefix.
                        for i in images:
                            if docs[i].mode != "RGB" or min(docs[i].size) <= 0:
                                raise TypeError("LOTUS image embedding requires nonempty RGB PIL images")
                            docs[i].load()
                        if input_kind == "rgb_pil":
                            record.update(image_encoding="not_performed", png_compress_level=None,
                                image_encode_wall_ns=None, image_encode_cpu_ns=None,
                                encoded_total_bytes=0, encoded_peak_single_bytes=0)
                            payload = [docs[i] for i in images]
                        else:
                            payload, reused, fallback = [], 0, 0
                            record.update(image_encode_wall_ns=None, image_encode_cpu_ns=None)
                            for i in images:
                                image = docs[i]
                                source = None if source_images is None else source_images.get(id(image))
                                if (type(source) is tuple and len(source) == 4 and source[0] is image
                                        and type(source[1]) is bytes and source[1]
                                        and type(source[2]) is tuple and len(source[2]) == 2
                                        and all(type(value) is int for value in source[2])
                                        and source[2] == image.size and type(source[3]) is str
                                        and source[3] == hashlib.sha256(image.tobytes()).hexdigest()):
                                    payload.append(source[1])
                                    reused += 1
                                else:
                                    encode_at, cpu_at = time.monotonic_ns(), time.process_time_ns()
                                    try:
                                        payload.append(image_bytes(image))
                                        fallback += 1
                                    finally:
                                        record["image_encode_wall_ns"] = (record["image_encode_wall_ns"] or 0) + time.monotonic_ns() - encode_at
                                        record["image_encode_cpu_ns"] = (record["image_encode_cpu_ns"] or 0) + time.process_time_ns() - cpu_at
                            record.update(image_encoding=("source_bytes_reused" if not fallback
                                else "lossless_png_fallback" if not reused else "mixed_source_and_png"),
                                source_reused_rows=reused, png_fallback_rows=fallback,
                                png_compress_level=1 if fallback else None)
                            record.update(encoded_total_bytes=sum(len(x) for x in payload),
                                          encoded_peak_single_bytes=max(len(x) for x in payload))
                        execute_at = time.monotonic_ns()
                        try:
                            result = executor.embed_batch(ids, payload)
                        finally:
                            record["executor_elapsed_ns"] = time.monotonic_ns() - execute_at
                    if result.semantics != semantics:
                        raise ValueError("executor changed its embedding semantics")
                    if (len(set(result.doc_ids)) != len(ids) or set(result.doc_ids) != set(ids)):
                        raise ValueError("executor image IDs are duplicated, missing or unexpected")
                    values = checked_matrix(result.embeddings, len(ids), semantics)
                    if result.doc_ids == ids and not texts:
                        matrix[:] = values
                    else:
                        position = dict(zip(ids, images, strict=True))
                        matrix[[position[doc_id] for doc_id in result.doc_ids]] = values
                if texts:
                    with timing_span("lotus.rm.text_embed", scope="rm_call"):
                        matrix[texts] = checked_matrix(text_rm([docs[i] for i in texts]), len(texts), semantics)
                observation.retain(matrix)
                record["status"] = "succeeded"
                # Each image/text partition was checked before scattering into
                # this owned float32 matrix; a second full norm scan adds no check.
                return matrix
            finally:
                record["elapsed_ns"] = time.monotonic_ns() - started
                observation.calls.append(record)

    rm = ImageRM()
    rm.observation = observation
    return rm


def observe_stock_rm(stock, *, observation=None):
    """Delegate all embedding mathematics and native batching to the stock RM."""
    from lotus.models.rm import RM
    observation = observation if observation is not None else RMObservation()
    semantics = EmbeddingSemantics("stock-clip", "stock-processor", 512)

    class ObservedStockRM(RM):
        def _embed(self, docs):
            started = time.monotonic_ns()
            record = dict(call_index=observation.call_count, image_rows=sum(isinstance(x, Image.Image) for x in docs),
                          text_rows=sum(isinstance(x, str) for x in docs), status="failed")
            try:
                with timing_span("lotus.rm.stock_embed", scope="rm_call"):
                    result = stock(docs)
                result = checked_matrix(result, len(docs), semantics)
                observation.retain(result)
                record["status"] = "succeeded"
                return result
            finally:
                record["elapsed_ns"] = time.monotonic_ns() - started
                observation.calls.append(record)

    rm = ObservedStockRM()
    rm.observation = observation
    return rm
