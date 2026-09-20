"""Typed single-image stages and the synchronous embedding reference."""

from __future__ import annotations

import io
import time

import numpy as np

from ...execution_provider.semantic_image import MAX_IMAGE_PIXELS, SemanticImagePlan
from .contracts import ImageBatchTelemetry, ImageEmbeddingBatch
from .staged import build_prepared_image_block_descriptor


class ImageComputationUnknown(RuntimeError):
    """The worker returned without confirmation that its device work ended."""


def check_encoded_image(plan: SemanticImagePlan, encoded: bytes) -> None:
    """Inspect dimensions before a decoder allocates RGB pixels."""
    from PIL import Image

    plan.validate_input(encoded)
    with Image.open(io.BytesIO(encoded)) as image:
        if image.format not in ("JPEG", "PNG") or getattr(image, "n_frames", 1) != 1:
            raise ValueError("the image profile accepts single-frame JPEG and PNG only")
        width, height = image.size
        if width < 1 or height < 1 or width * height > MAX_IMAGE_PIXELS:
            raise ValueError("decoded image dimensions exceed the pixel limit")


class ImagePrepareStage:
    """CPU worker; no model weights and no GPU dependency."""

    def __init__(self, plan: SemanticImagePlan, *, preprocessor=None):
        self.plan = plan
        if preprocessor is None:
            from .clip import ClipImagePreprocessor
            preprocessor = ClipImagePreprocessor(
                plan.processor_id, revision=plan.processor_revision, local_files_only=True)
        self._preprocessor = preprocessor

    def ready(self):
        return {"semantic_digest": self.plan.digest, "stage": "prepare"}

    def prepare(self, doc_ids: tuple[str, ...], encoded_images: list[bytes]):
        if len(doc_ids) != 1 or len(encoded_images) != 1:
            raise ValueError("image operator stages accept exactly one row")
        started = time.monotonic()
        check_encoded_image(self.plan, encoded_images[0])
        pixels = self._preprocessor.preprocess(encoded_images)
        if (not isinstance(pixels, np.ndarray)
                or pixels.shape != (1, 3, self.plan.input_size, self.plan.input_size)
                or pixels.dtype != np.float32 or not pixels.flags.c_contiguous
                or not np.isfinite(pixels).all()):
            raise ValueError("processor output does not match the image profile")
        return ImageEmbeddingBatch(
            doc_ids, pixels, "preprocessed_tensor", self.plan.input_size ** 2, "pixels",
            telemetry=ImageBatchTelemetry(
                preprocess_s=time.monotonic() - started,
                encoded_bytes=len(encoded_images[0]), input_tensor_bytes=pixels.nbytes),
        )

    def preprocess_staged(self, descriptor, encoded_images):
        batch = self.prepare(descriptor.row_ids, encoded_images)
        prepared = build_prepared_image_block_descriptor(
            descriptor, batch.payload,
            # Cross-host monotonic clocks are incomparable. The engine assigns
            # its actual ready observation after receiving this descriptor.
            ready_at_s=descriptor.created_at_s,
        )
        return prepared, batch


class ImageModelStage:
    """Persistent tensor-only CLIP worker, loaded from pinned cached assets."""

    def __init__(self, plan: SemanticImagePlan, *, device="cuda", model=None):
        self.plan = plan
        if model is not None and device != "cpu" and not callable(getattr(model, "synchronize", None)):
            raise ValueError("a GPU image model must expose device completion confirmation")
        if model is None:
            from .clip import ClipTensorActor
            model = ClipTensorActor(
                plan.model_id, processor_revision=plan.processor_id, device=device,
                dtype=plan.dtype, normalize=True, model_commit=plan.model_revision,
                processor_commit=plan.processor_revision, local_files_only=True,
            )
            if (int(model._model.config.vision_config.image_size) != plan.input_size
                    or int(model._model.config.projection_dim) != plan.dimension):
                raise ValueError("cached CLIP model shape does not match the database plan")
        self._model = model

    def ready(self):
        self._synchronize()
        return {"semantic_digest": self.plan.digest, "stage": "model"}

    def _synchronize(self):
        synchronize = getattr(self._model, "synchronize", None)
        if synchronize is not None:
            try:
                synchronize()
            except Exception as error:
                raise ImageComputationUnknown("image device completion could not be confirmed") from error

    def embed(self, batch):
        if (batch.input_kind != "preprocessed_tensor" or len(batch.doc_ids) != 1
                or batch.payload.shape != (1, 3, self.plan.input_size, self.plan.input_size)
                or batch.payload.dtype != np.float32):
            raise ValueError("model input does not match the image profile")
        try:
            result = self._model.embed(batch)
        finally:
            self._synchronize()
        if (result.doc_ids != batch.doc_ids
                or result.semantics.model_revision != self.plan.model_revision
                or result.semantics.processor_revision != self.plan.processor_revision
                or result.semantics.dimension != self.plan.dimension
                or result.semantics.dtype != "float32"
                or not result.semantics.projected or not result.semantics.normalized):
            raise ValueError("model output identity does not match the image profile")
        self.plan.encode_result(result.embeddings[0])
        return result


class SynchronousImageReference:
    """One input -> CPU prepare -> model -> checked float4, without Ray or core."""

    def __init__(self, plan: SemanticImagePlan, prepare: ImagePrepareStage, model: ImageModelStage):
        if prepare.plan != plan or model.plan != plan:
            raise ValueError("reference stages must use the same semantic plan")
        self.plan, self._prepare, self._model = plan, prepare, model

    def embed(self, encoded: bytes | None) -> bytes | None:
        if encoded is None:
            return None
        batch = self._prepare.prepare(("reference",), [encoded])
        result = self._model.embed(batch)
        return self.plan.encode_result(result.embeddings[0])


def build_image_worker_pool(plan: SemanticImagePlan, *, cpu_workers: int, gpu_workers: int,
                            cpu_threads: int, startup_timeout_s: float, ray_api=None):
    """Service-owned actors; construction failure cleans up only this new pool."""
    from .execution import SemLoomRayWorkerPool

    if any(type(value) is not int or value < 1 for value in (cpu_workers, gpu_workers, cpu_threads)):
        raise ValueError("image worker counts and CPU threads must be positive")
    if not np.isfinite(startup_timeout_s) or startup_timeout_s <= 0:
        raise ValueError("image worker startup timeout must be finite and positive")
    if ray_api is None:
        import ray as ray_api

    class Cpu(ImagePrepareStage):
        def __init__(self, selected_plan):
            from .clip import configure_torch_thread_pools
            configure_torch_thread_pools(cpu_threads, 1)
            super().__init__(selected_plan)

    class Gpu(ImageModelStage):
        def __init__(self, selected_plan):
            from .clip import configure_torch_thread_pools
            configure_torch_thread_pools(cpu_threads, 1)
            super().__init__(selected_plan)

    cpu = ray_api.remote(num_cpus=cpu_threads, max_restarts=0, max_task_retries=0)(Cpu)
    gpu = ray_api.remote(num_cpus=cpu_threads, num_gpus=1, max_restarts=0, max_task_retries=0)(Gpu)
    actors = []
    try:
        preprocessors = []
        for _ in range(cpu_workers):
            actor = cpu.remote(plan)
            actors.append(actor)
            preprocessors.append(actor)
        models = []
        for _ in range(gpu_workers):
            actor = gpu.remote(plan)
            actors.append(actor)
            models.append(actor)
        ready = ray_api.get([actor.ready.remote() for actor in actors], timeout=startup_timeout_s)
        if any(value.get("semantic_digest") != plan.digest for value in ready):
            raise ValueError("image worker semantic identity mismatch")
        return SemLoomRayWorkerPool(tuple(preprocessors), tuple(models))
    except BaseException:
        for actor in actors:
            ray_api.kill(actor, no_restart=True)
        raise
