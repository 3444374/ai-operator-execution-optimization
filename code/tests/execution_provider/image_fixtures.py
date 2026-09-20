"""Zero-weight image fixtures. Their model identities never represent real CLIP."""

import io
import time
from pathlib import Path

import numpy as np
from PIL import Image

from src.execution_provider.semantic_image import SemanticImagePlan
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult
from src.modalities.image.operator import ImageModelStage, ImagePrepareStage, SynchronousImageReference


PLAN = SemanticImagePlan("fixture/rgb-projection", "a" * 40, "fixture/pillow-rgb",
                         "b" * 40, "float32", 3, 2)


class FixturePreprocessor:
    def preprocess(self, images):
        with Image.open(io.BytesIO(images[0])) as value:
            pixels = np.asarray(value.convert("RGB").resize((2, 2)), dtype=np.float32)
        return pixels.transpose(2, 0, 1)[None].copy()


class FixtureModel:
    def __init__(self, *, delay_s=0, entered_path=None, release_path=None):
        self.delay_s, self.entered_path, self.release_path = delay_s, entered_path, release_path

    def embed(self, batch):
        if self.entered_path:
            Path(self.entered_path).write_text("entered\n")
        if self.release_path:
            deadline = time.monotonic() + 10
            while not Path(self.release_path).exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("fixture release deadline expired")
                time.sleep(0.01)
        time.sleep(self.delay_s)
        vectors = batch.payload.mean(axis=(2, 3)).astype(np.float32)
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        vectors /= np.maximum(norms, np.float32(1e-12))
        return ImageEmbeddingResult(batch.doc_ids, vectors,
            EmbeddingSemantics(PLAN.model_revision, PLAN.processor_revision, PLAN.dimension), 0)


def reference(**kwargs):
    return SynchronousImageReference(PLAN,
        ImagePrepareStage(PLAN, preprocessor=FixturePreprocessor()),
        ImageModelStage(PLAN, device="cpu", model=FixtureModel(**kwargs)))


def ray_workers(ray_api, **kwargs):
    """Real Ray CPU actors stand in for both physical stages; no GPU is requested."""
    from src.modalities.image.execution import SemLoomRayWorkerPool

    @ray_api.remote(num_cpus=1, max_restarts=0, max_task_retries=0)
    class Prepare(ImagePrepareStage):
        def __init__(self):
            super().__init__(PLAN, preprocessor=FixturePreprocessor())

    @ray_api.remote(num_cpus=1, num_gpus=0, max_restarts=0, max_task_retries=0)
    class Model(ImageModelStage):
        def __init__(self):
            super().__init__(PLAN, device="cpu", model=FixtureModel(**kwargs))

    cpu, model = Prepare.remote(), Model.remote()
    ray_api.get([cpu.ready.remote(), model.ready.remote()])
    return SemLoomRayWorkerPool((cpu,), (model,))
