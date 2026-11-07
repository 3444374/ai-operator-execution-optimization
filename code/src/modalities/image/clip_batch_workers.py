"""Shared CLIP batch workers; callers own graph scheduling and lifecycle."""
from __future__ import annotations
import numpy as np


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
        allowed_gpu_uuids=None,
        preprocessing_identity=None,
    ) -> None:
        from src.modalities.image.clip import ClipTensorActor

        self._preprocessing_identity = preprocessing_identity
        options = {} if allowed_gpu_uuids is None else {"allowed_gpu_uuids": allowed_gpu_uuids}
        self._actor = ClipTensorActor(
            model_revision,
            processor_revision=processor_revision,
            dtype=dtype,
            normalize=True,
            torch_intraop_threads=torch_intraop_threads,
            torch_interop_threads=torch_interop_threads,
            **options,
        )

    def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        from src.modalities.image.contracts import ImageEmbeddingBatch

        if self._preprocessing_identity is not None:
            import json
            identities = batch.get("_semloom_preprocessing_identity")
            if identities is None or len(identities) != len(batch["doc_id"]) or any(
                    json.loads(str(value)) != self._preprocessing_identity for value in identities):
                raise ValueError("Ray CLIP batch preprocessing identity differs from its owner")
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



class RayDataClipImagePredictor(RayDataClipPredictor):
    """Default native RGB arrays -> the same slow processor -> CLIP in one UDF."""

    def __init__(self, model_revision, processor_revision, dtype,
                 torch_intraop_threads, torch_interop_threads, allowed_gpu_uuids=None,
                 preprocessing_identity=None):
        from src.modalities.image.clip import ClipImagePreprocessor
        self._preprocessor = ClipImagePreprocessor(processor_revision, local_files_only=True)
        if preprocessing_identity is not None:
            from src.modalities.image.worker_identity import validate_preprocessing_identity, worker_identity, record_worker_identity
            identity = validate_preprocessing_identity(self._preprocessor.preprocessing_identity())
            if identity != preprocessing_identity:
                raise ValueError("Ray fused CLIP preprocessor differs from its owner")
            record_worker_identity(worker_identity("clip_fused_prepare", preprocessing_identity=identity))
        super().__init__(model_revision, processor_revision, dtype,
                         torch_intraop_threads, torch_interop_threads,
                         allowed_gpu_uuids=allowed_gpu_uuids, preprocessing_identity=preprocessing_identity)

    def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        images = [np.asarray(image) for image in batch["image"]]
        if any(image.dtype != np.uint8 or image.ndim != 3 or image.shape[-1] != 3
               or min(image.shape[:2]) <= 0 for image in images):
            raise ValueError("native Ray image arrays must be uint8 HWC RGB")
        prepared = {"doc_id": batch["doc_id"], "pixel_values": self._preprocessor.preprocess_decoded(images)}
        if self._preprocessing_identity is not None:
            import json
            prepared["_semloom_preprocessing_identity"] = np.full(len(images), json.dumps(self._preprocessing_identity, sort_keys=True))
        return super().__call__(prepared)

