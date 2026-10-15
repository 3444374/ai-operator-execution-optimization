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
    from src.experiments.execution_timing import timing_span as existing_span
    return existing_span(operation, **fields)


def validate_png_compress_level(level):
    if level is not None and (type(level) is not int or not -1 <= level <= 9):
        raise ValueError("PNG compression level must be None or an integer from -1 to 9")


def image_bytes(image: Image.Image, *, png_compress_level=None) -> bytes:
    """Losslessly transport the exact RGB pixels supplied to native LOTUS."""
    if not isinstance(image, Image.Image) or image.mode != "RGB":
        raise TypeError("LOTUS image embedding requires RGB PIL images")
    validate_png_compress_level(png_compress_level)
    with io.BytesIO() as stream:
        options = {} if png_compress_level is None else {"compress_level": png_compress_level}
        image.save(stream, format="PNG", **options)
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

    def retain(self, matrix):
        if self.capture_vectors:
            self.vectors["call_" + str(len(self.calls))] = matrix.copy()

    @property
    def image_rows(self):
        return sum(x["image_rows"] for x in self.calls)


def make_image_rm(executor: ImageBatchExecutor, text_rm, *, max_rows=5000,
                  max_output_bytes=32 * 1024 * 1024, observation=None, png_compress_level=None,
                  source_images=None):
    """Replace only RM._embed; native RM query conversion and FAISS stay intact.

    A sem_index call requires a complete N×D matrix. This explicitly bounded
    collection is distinct from the existing streaming benchmark's capture.
    """
    validate_png_compress_level(png_compress_level)
    input_kind = getattr(executor, "input_kind", "encoded_bytes")
    if input_kind not in ("encoded_bytes", "rgb_pil"):
        raise ValueError("LOTUS image executor input_kind must be encoded_bytes or rgb_pil")
    if source_images is not None:
        from collections.abc import Mapping
        if input_kind != "encoded_bytes" or not isinstance(source_images, Mapping):
            raise ValueError("source images require an encoded-byte executor and an identity mapping")
        input_kind = "source_encoded_bytes"
    if input_kind != "encoded_bytes" and png_compress_level is not None:
        raise ValueError("native image input does not accept PNG compression settings")
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
            record = dict(call_index=len(observation.calls), image_rows=len(images), text_rows=len(texts),
                          input_kind=input_kind, status="failed")
            try:
                matrix = np.empty((len(docs), 512), dtype=np.float32)
                if images:
                    ids = tuple(str(i) for i in images)
                    with timing_span("lotus.rm.image_embed", scope="rm_call"):
                        if input_kind in ("rgb_pil", "source_encoded_bytes"):
                            for i in images:
                                if docs[i].mode != "RGB" or min(docs[i].size) <= 0:
                                    raise TypeError("LOTUS image embedding requires nonempty RGB PIL images")
                                docs[i].load()
                            record.update(image_encoding="not_performed", png_compress_level=None,
                                image_encode_wall_ns=None, image_encode_cpu_ns=None,
                                encoded_total_bytes=0, encoded_peak_single_bytes=0)
                            payload = []
                            for i in images:
                                if input_kind == "rgb_pil":
                                    payload.append(docs[i])
                                else:
                                    source = source_images.get(id(docs[i]))
                                    if (not isinstance(source, tuple) or len(source) != 4 or source[0] is not docs[i]
                                            or type(source[1]) is not bytes or not source[1]
                                            or source[2] != docs[i].size
                                            or source[3] != hashlib.sha256(docs[i].tobytes()).hexdigest()):
                                        raise ValueError("image is missing its identical validated source bytes")
                                    payload.append(source[1])
                            if input_kind == "source_encoded_bytes":
                                record.update(image_encoding="source_bytes_reused",
                                    encoded_total_bytes=sum(len(x) for x in payload),
                                    encoded_peak_single_bytes=max(len(x) for x in payload))
                        else:
                            encode_at, cpu_at = time.monotonic_ns(), time.process_time_ns()
                            try:
                                payload = [image_bytes(docs[i], png_compress_level=png_compress_level) for i in images]
                            finally:
                                record.update(png_compress_level=png_compress_level,
                                    image_encode_wall_ns=time.monotonic_ns() - encode_at,
                                    image_encode_cpu_ns=time.process_time_ns() - cpu_at)
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
                    position = {doc_id: i for i, doc_id in enumerate(result.doc_ids)}
                    matrix[images] = values[[position[x] for x in ids]]
                if texts:
                    with timing_span("lotus.rm.text_embed", scope="rm_call"):
                        matrix[texts] = checked_matrix(text_rm([docs[i] for i in texts]), len(texts), semantics)
                record["status"] = "succeeded"
                observation.retain(matrix)
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
            record = dict(call_index=len(observation.calls), image_rows=sum(isinstance(x, Image.Image) for x in docs),
                          text_rows=sum(isinstance(x, str) for x in docs), status="failed")
            try:
                with timing_span("lotus.rm.stock_embed", scope="rm_call"):
                    result = stock(docs)
                result = checked_matrix(result, len(docs), semantics)
                record["status"] = "succeeded"
                observation.retain(result)
                return result
            finally:
                record["elapsed_ns"] = time.monotonic_ns() - started
                observation.calls.append(record)

    rm = ObservedStockRM()
    rm.observation = observation
    return rm
