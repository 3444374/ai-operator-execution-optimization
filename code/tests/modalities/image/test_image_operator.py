import io
import struct
import sys
import tempfile
import unittest
import zlib
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
from PIL import Image

from src.modalities.image.operator import (
    ImageComputationUnknown, ImagePrepareStage, ImageModelStage, SynchronousImageReference,
    check_encoded_image,
)
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult
from tests.modalities.image.test_incremental_image_backend import PLAN


def encoded_png(color="red"):
    output = io.BytesIO()
    Image.new("RGB", (2, 2), color).save(output, format="PNG")
    return output.getvalue()


class ImageOperatorTest(unittest.TestCase):
    def test_real_decode_and_reference_null_zero_work(self):
        calls = []

        def preprocess(images):
            calls.append("prepare")
            with Image.open(io.BytesIO(images[0])) as decoded:
                return np.asarray(decoded.convert("RGB"), dtype=np.float32).transpose(2, 0, 1)[None].copy()

        def embed(batch):
            calls.append("model")
            return ImageEmbeddingResult(batch.doc_ids, np.array([[1, 0]], np.float32),
                EmbeddingSemantics(PLAN.model_revision, PLAN.processor_revision, 2), 0)

        prepare = ImagePrepareStage(PLAN, preprocessor=SimpleNamespace(preprocess=preprocess))
        model = ImageModelStage(PLAN, device="cpu", model=SimpleNamespace(embed=embed))
        reference = SynchronousImageReference(PLAN, prepare, model)
        self.assertIsNone(reference.embed(None))
        self.assertEqual(calls, [])
        self.assertEqual(reference.embed(encoded_png()), PLAN.encode_result([1, 0]))
        self.assertEqual(calls, ["prepare", "model"])

    def test_corrupt_unsupported_and_oversized_headers_fail_before_preprocess(self):
        for encoded in (b"", b"not an image"):
            with self.assertRaises((ValueError, OSError)):
                check_encoded_image(PLAN, encoded)
        output = io.BytesIO()
        Image.new("RGB", (2, 2)).save(output, format="GIF")
        with self.assertRaises(ValueError):
            check_encoded_image(PLAN, output.getvalue())

    def test_nonfinite_and_wrong_shape_processor_outputs_are_rejected(self):
        for pixels in (np.zeros((1, 3, 3, 3), np.float32), np.full((1, 3, 2, 2), np.nan, np.float32)):
            prepare = ImagePrepareStage(PLAN, preprocessor=SimpleNamespace(preprocess=lambda _: pixels))
            with self.assertRaises(ValueError):
                prepare.prepare(("one",), [encoded_png()])

    def test_oversized_png_header_is_rejected_before_pixel_decode(self):
        def chunk(kind, payload):
            return struct.pack("!I", len(payload)) + kind + payload + struct.pack("!I", zlib.crc32(kind + payload))

        header = struct.pack("!IIBBBBB", 8192, 4096, 8, 2, 0, 0, 0)
        encoded = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(b"IDAT", b"x") + chunk(b"IEND", b"")
        with self.assertRaisesRegex(ValueError, "pixel limit"):
            check_encoded_image(PLAN, encoded)

    def test_worker_factory_pins_resources_and_cleans_only_its_failed_startup(self):
        from src.modalities.image.operator import build_image_worker_pool

        class Ray:
            def __init__(self, fail=False):
                self.options, self.actors, self.killed = [], [], []
                self.fail = fail

            def remote(self, **options):
                self.options.append(options)

                def decorate(cls):
                    def remote(plan):
                        actor = SimpleNamespace(ready=SimpleNamespace(remote=lambda: {"semantic_digest": plan.digest}))
                        self.actors.append(actor)
                        return actor
                    return SimpleNamespace(remote=remote)
                return decorate

            def get(self, values, *, timeout):
                assert timeout == 2
                if self.fail:
                    raise TimeoutError("controlled startup failure")
                return values

            def kill(self, actor, *, no_restart):
                assert no_restart
                self.killed.append(actor)

        ray = Ray()
        pool = build_image_worker_pool(PLAN, cpu_workers=1, gpu_workers=1, cpu_threads=2,
                                      startup_timeout_s=2, ray_api=ray)
        self.assertEqual(len(pool.preprocessors), 1)
        self.assertEqual(ray.options, [dict(num_cpus=2, max_restarts=0, max_task_retries=0),
            dict(num_cpus=2, num_gpus=1, max_restarts=0, max_task_retries=0)])
        self.assertEqual(ray.killed, [])
        failed = Ray(fail=True)
        with self.assertRaises(TimeoutError):
            build_image_worker_pool(PLAN, cpu_workers=1, gpu_workers=1, cpu_threads=2,
                                    startup_timeout_s=2, ray_api=failed)
        self.assertEqual(failed.killed, failed.actors)

    def test_cached_model_and_processor_receive_exact_revisions_without_download(self):
        from src.modalities.image.clip import ClipImagePreprocessor, ClipTensorActor

        loads = []

        class Model:
            config = SimpleNamespace(projection_dim=3)
            def eval(self):
                return self
            def to(self, **kwargs):
                loads.append(("placement", kwargs))
                return self

        def load_model(name, **kwargs):
            loads.append((name, kwargs))
            return Model()

        def load_processor(name, **kwargs):
            loads.append((name, kwargs))
            return object()

        transformers = SimpleNamespace(CLIPModel=SimpleNamespace(from_pretrained=load_model),
            CLIPProcessor=SimpleNamespace(from_pretrained=load_processor))
        torch = SimpleNamespace(float16="float16", float32="float32", bfloat16="bfloat16")
        with patch.dict(sys.modules, {"transformers": transformers, "torch": torch}):
            ClipImagePreprocessor(PLAN.processor_id, revision=PLAN.processor_revision, local_files_only=True)
            model = ClipTensorActor(PLAN.model_id, model_commit=PLAN.model_revision,
                processor_revision=PLAN.processor_id, processor_commit=PLAN.processor_revision,
                dtype=PLAN.dtype, local_files_only=True)
        self.assertEqual(loads, [
            (PLAN.processor_id, dict(revision=PLAN.processor_revision, local_files_only=True)),
            (PLAN.model_id, dict(revision=PLAN.model_revision, local_files_only=True)),
            ("placement", dict(device="cuda", dtype=PLAN.dtype))])
        self.assertEqual(model.semantics.model_revision, PLAN.model_revision)
        self.assertEqual(model.semantics.processor_revision, PLAN.processor_revision)
        with tempfile.TemporaryDirectory() as path:
            with self.assertRaises(ValueError):
                ClipTensorActor(path, model_commit=PLAN.model_revision)
            with self.assertRaises(ValueError):
                ClipImagePreprocessor(path, revision=PLAN.processor_revision)

    def test_actor_completion_confirmation_checks_device_after_model_error(self):
        calls = []

        def fail(batch):
            calls.append("model")
            raise ValueError("controlled model error")

        stage = ImageModelStage(PLAN, model=SimpleNamespace(embed=fail, synchronize=lambda: calls.append("sync")))
        batch = SimpleNamespace(input_kind="preprocessed_tensor", doc_ids=("one",),
                                payload=np.zeros((1, 3, 2, 2), dtype=np.float32))
        with self.assertRaises(ValueError):
            stage.embed(batch)
        self.assertEqual(calls, ["model", "sync"])
        stage.ready()
        self.assertEqual(calls, ["model", "sync", "sync"])

        def unknown():
            raise RuntimeError("device did not confirm completion")

        stage = ImageModelStage(PLAN, model=SimpleNamespace(embed=fail, synchronize=unknown))
        with self.assertRaises(ImageComputationUnknown):
            stage.embed(batch)
        with self.assertRaises(ImageComputationUnknown):
            stage.ready()
