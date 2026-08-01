"""Reusable CLIP preprocessing and tensor-only GPU actor stages."""

from __future__ import annotations

import io
import os
import time
from collections.abc import Sequence

import numpy as np

from .contracts import (
    EmbeddingSemantics,
    ImageEmbeddingBatch,
    ImageEmbeddingResult,
)


def decode_rgb_image(encoded: bytes):
    """Decode one encoded image into an eagerly materialized RGB PIL image."""
    from PIL import Image

    with Image.open(io.BytesIO(encoded)) as image:
        return image.convert("RGB")


def extract_clip_image_features(model_output):
    """Return the projected 2-D CLIP image embedding tensor across HF versions."""
    features = (
        model_output.pooler_output
        if hasattr(model_output, "pooler_output")
        else model_output
    )
    if getattr(features, "ndim", None) != 2:
        raise ValueError("CLIP image features must be a two-dimensional tensor")
    return features


def l2_normalize_embeddings(features, *, epsilon: float = 1e-12):
    """L2-normalize an embedding tensor without changing row order."""
    norms = features.norm(dim=-1, keepdim=True).clamp_min(epsilon)
    return features / norms


class ClipImagePreprocessor:
    """CPU-only encoded-image to contiguous CLIP pixel tensor adapter."""

    def __init__(self, processor_revision: str):
        from transformers import CLIPProcessor

        self.processor_revision = processor_revision
        self.processor = CLIPProcessor.from_pretrained(processor_revision)

    def preprocess(self, encoded_images: Sequence[bytes]) -> np.ndarray:
        if not encoded_images:
            raise ValueError("encoded_images must not be empty")
        images = [decode_rgb_image(item) for item in encoded_images]
        pixel_values = self.processor(
            images=images,
            return_tensors="np",
        )["pixel_values"]
        return np.ascontiguousarray(pixel_values, dtype=np.float32)


class FastClipImagePreprocessor:
    """CPU CLIP preprocessing using torchvision tensor decode end to end.

    This is the production candidate selected by the interleaved preprocessing
    profile. It keeps encoded JPEG bytes on the source boundary, avoids a PIL
    round trip, and returns the same contiguous float32 tensor contract as
    :class:`ClipImagePreprocessor`.
    """

    def __init__(self, processor_revision: str):
        from transformers import AutoImageProcessor

        self.processor_revision = processor_revision
        self.processor = AutoImageProcessor.from_pretrained(
            processor_revision,
            backend="torchvision",
        )

    def preprocess(self, encoded_images: Sequence[bytes]) -> np.ndarray:
        if not encoded_images:
            raise ValueError("encoded_images must not be empty")

        import torch
        from torchvision.io import ImageReadMode, decode_image

        images = [
            decode_image(
                torch.frombuffer(bytearray(item), dtype=torch.uint8),
                mode=ImageReadMode.RGB,
            )
            for item in encoded_images
        ]
        pixel_values = self.processor(
            images=images,
            return_tensors="pt",
        )["pixel_values"]
        return np.ascontiguousarray(pixel_values.numpy(), dtype=np.float32)


class ClipTensorActor:
    """GPU-resident CLIP actor accepting only preprocessed pixel tensors.

    The caller owns batching and CPU preprocessing. Ray can wrap this class with
    ``ray.remote(num_gpus=1)``; keeping the class undecorated makes its contract
    unit-testable and avoids importing Ray in model code.
    """

    input_kind = "preprocessed_tensor"

    def __init__(
        self,
        model_revision: str,
        *,
        processor_revision: str | None = None,
        device: str = "cuda",
        dtype: str = "float16",
        normalize: bool = True,
    ) -> None:
        import torch
        from transformers import CLIPModel

        dtype_by_name = {
            "float16": torch.float16,
            "float32": torch.float32,
            "bfloat16": torch.bfloat16,
        }
        if dtype not in dtype_by_name:
            raise ValueError(f"unsupported CLIP dtype: {dtype}")
        self._torch = torch
        self._device = device
        self._dtype = dtype_by_name[dtype]
        self._normalize = normalize
        self._model = CLIPModel.from_pretrained(model_revision).eval()
        self._model = self._model.to(device=device, dtype=self._dtype)
        projection_dim = int(self._model.config.projection_dim)
        self.semantics = EmbeddingSemantics(
            model_revision=model_revision,
            processor_revision=processor_revision or model_revision,
            dimension=projection_dim,
            dtype="float32",
            projected=True,
            normalized=normalize,
        )

    def ready(self) -> dict[str, object]:
        return {
            "actor_worker_pid": os.getpid(),
            "actor_type": type(self).__name__,
            "input_kind": self.input_kind,
            "embedding_dimension": self.semantics.dimension,
            "normalized": self.semantics.normalized,
        }

    def embed(self, batch: ImageEmbeddingBatch) -> ImageEmbeddingResult:
        if batch.input_kind != self.input_kind:
            raise ValueError(
                f"ClipTensorActor requires {self.input_kind}, got {batch.input_kind}"
            )
        started = time.perf_counter()
        payload = np.asarray(batch.payload)
        # Ray's zero-copy object-store view is intentionally read-only. Torch
        # warns even though CLIP never mutates the input, so take ownership at
        # this device-transfer boundary instead of relying on undefined write
        # behavior in a future model implementation.
        if not payload.flags.writeable:
            payload = payload.copy()
        pixel_values = self._torch.as_tensor(
            payload,
            dtype=self._dtype,
            device=self._device,
        )
        if pixel_values.ndim != 4 or pixel_values.shape[0] != len(batch.doc_ids):
            raise ValueError("pixel tensor must have shape (rows, channels, height, width)")
        with self._torch.inference_mode():
            output = self._model.get_image_features(pixel_values=pixel_values)
            # Normalize in float32 so ``normalized=True`` is an accurate output
            # contract rather than an approximate float16 property.
            features = extract_clip_image_features(output).float()
            if self._normalize:
                features = l2_normalize_embeddings(features)
        embeddings = features.cpu().numpy()
        return ImageEmbeddingResult(
            doc_ids=batch.doc_ids,
            embeddings=embeddings,
            semantics=self.semantics,
            service_s=time.perf_counter() - started,
        )
