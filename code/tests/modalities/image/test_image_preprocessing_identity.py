"""CPU identity fixtures; no CLIP weights, downloads, Ray or GPU required."""

from __future__ import annotations

import io
import sys
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
from PIL import Image, __version__ as pillow_version

from src.modalities.image.clip import ClipImagePreprocessor, FastClipImagePreprocessor
from src.modalities.image.contracts import image_work_calibration_signature
from src.modalities.image.staged import (
    build_encoded_image_block_descriptor,
    build_prepared_image_block_descriptor,
    image_transform_signature,
)
from src.scheduling.runtime.stage_broker import BoundedStageBroker, StageBrokerLimits


class _ImageProcessor:
    def to_dict(self):
        return {"do_center_crop": True, "crop_size": {"height": 2, "width": 2},
                "rescale_factor": 1 / 255}


class _SlowProcessor:
    def __init__(self):
        self.image_processor = _ImageProcessor()
        self.images = []

    def __call__(self, *, images, return_tensors):
        assert return_tensors == "np"
        self.images = images
        return {"pixel_values": np.stack([
            np.asarray(image, dtype=np.float32).transpose(2, 0, 1) / 255
            for image in images
        ])}


class ImagePreprocessingIdentityTest(unittest.TestCase):
    def slow(self):
        processor = _SlowProcessor()
        transformers = SimpleNamespace(__version__="fixture-transformers",
            CLIPProcessor=SimpleNamespace(from_pretrained=lambda *_args, **_kwargs: processor))
        with patch.dict(sys.modules, {"transformers": transformers}):
            preprocessor = ClipImagePreprocessor("processor", revision="processor-commit",
                                                 local_files_only=True)
            identity = preprocessor.preprocessing_identity()
        return preprocessor, processor, identity

    def descriptor(self, identity=None, **changes):
        arguments = dict(job_id="identity-job", ordered_sequence=0, row_ids=("one",),
            encoded_images=[b"image"], model_revision="model", processor_revision="processor",
            model_dtype="float32", created_at_s=1.0, input_size=2,
            preprocessing_identity=identity)
        arguments.update(changes)
        return build_encoded_image_block_descriptor(**arguments)

    def test_slow_publishes_actual_pil_decoder_and_leaves_pixels_unchanged(self):
        preprocessor, processor, identity = self.slow()
        output = io.BytesIO()
        image = Image.fromarray(np.arange(12, dtype=np.uint8).reshape(2, 2, 3))
        image.save(output, format="PNG")
        actual = preprocessor.preprocess([output.getvalue()])
        expected = preprocessor.preprocess_decoded([image])
        np.testing.assert_array_equal(actual, expected)
        self.assertTrue(all(isinstance(value, Image.Image) and value.mode == "RGB"
                            for value in processor.images))
        self.assertEqual(identity["decoder"], "pillow_convert_rgb")
        self.assertEqual(identity["decoder_version"], pillow_version)
        self.assertEqual(identity["processor_revision"], "processor-commit")
        self.assertIn("fixture-transformers", identity["processor_version"])
        self.assertEqual((identity["layout"], identity["dtype"], identity["input_size"]),
                         ("NCHW", "float32", 2))

    def test_fast_identity_keeps_the_torchvision_decoder_and_its_versions(self):
        transformers = SimpleNamespace(__version__="fixture-transformers",
            AutoImageProcessor=SimpleNamespace(from_pretrained=lambda *_args, **_kwargs: _ImageProcessor()))
        with patch.dict(sys.modules, {"transformers": transformers,
                "torch": SimpleNamespace(__version__="fixture-torch"),
                "torchvision": SimpleNamespace(__version__="fixture-torchvision")}):
            preprocessor = FastClipImagePreprocessor("processor")
            identity = preprocessor.preprocessing_identity()
        self.assertEqual(identity["decoder"], "torchvision_decode_image_rgb")
        self.assertIn("fixture-torchvision", identity["decoder_version"])
        self.assertIn("fixture-torch", identity["decoder_version"])
        self.assertEqual(identity["execution_variant"], "clip_fast_torchvision")
        self.assertNotEqual(image_transform_signature(**identity),
                            image_transform_signature(**self.slow()[2]))

    def test_loaded_processor_configuration_and_resolved_revision_are_identified(self):
        preprocessor, processor, first = self.slow()
        configuration = {**processor.image_processor.to_dict(), "rescale_factor": 1 / 127}
        processor.image_processor.to_dict = lambda: configuration
        processor.image_processor._commit_hash = "resolved-processor-commit"
        with patch.dict(sys.modules, {"transformers": SimpleNamespace(__version__="fixture-transformers")}):
            second = preprocessor.preprocessing_identity()
        self.assertEqual(second["input_size"], first["input_size"])
        self.assertEqual(second["decoder"], first["decoder"])
        self.assertEqual(second["processor_revision"], "resolved-processor-commit")
        self.assertNotEqual(second["processor_version"], first["processor_version"])
        self.assertNotEqual(image_transform_signature(**first), image_transform_signature(**second))

    def test_same_shape_different_decoder_cannot_complete_a_source_block(self):
        identity = self.slow()[2]
        descriptor = self.descriptor(identity)
        pixels = np.zeros((1, 3, 2, 2), dtype=np.float32)
        prepared = build_prepared_image_block_descriptor(descriptor, pixels, ready_at_s=2,
                                                         preprocessing_identity=identity)
        self.assertEqual(prepared.transform_signature, image_transform_signature(**identity))
        other = {**identity, "decoder": "torchvision_decode_image_rgb"}
        with self.assertRaisesRegex(ValueError, "preprocessor identity"):
            build_prepared_image_block_descriptor(descriptor, pixels, ready_at_s=2,
                                                  preprocessing_identity=other)
        broker = BoundedStageBroker(StageBrokerLimits(encoded_bytes=100, ready_bytes=100,
            ready_work=100, prepare_inflight=1, model_inflight=1))
        broker.enqueue_encoded(descriptor)
        lease = broker.lease_prepare(now_s=1.5)
        forged = replace(prepared, transform_signature=image_transform_signature(**other))
        with self.assertRaisesRegex(ValueError, "immutable block identity"):
            broker.complete_prepare(lease.lease_id, forged, now_s=2)
        self.assertEqual(broker.snapshot().prepare_inflight, 1)
        broker.complete_prepare(lease.lease_id, prepared, now_s=2)

    def test_decoder_variant_and_loaded_configuration_split_work_calibration(self):
        identity = self.slow()[2]
        descriptor = self.descriptor(identity)
        for changes in ({"decoder": "another-decoder"}, {"execution_variant": "another-variant"},
                        {"decoder_version": "another-version"}, {"processor_version": "another-config"}):
            other = self.descriptor({**identity, **changes}, row_ids=("two",), ordered_sequence=1)
            self.assertEqual(descriptor.shape, other.shape)
            self.assertNotEqual(descriptor.work.calibration_signature, other.work.calibration_signature)
            broker = BoundedStageBroker(StageBrokerLimits(encoded_bytes=100, ready_bytes=100,
                ready_work=100, prepare_inflight=1, model_inflight=1))
            broker.enqueue_encoded(descriptor)
            with self.assertRaisesRegex(ValueError, "calibration signature"):
                broker.enqueue_encoded(other)

    def test_work_signature_preserves_legacy_source_and_separates_explicit_paths(self):
        common = dict(model_revision="model", processor_revision="processor", dtype="float32")
        legacy = image_work_calibration_signature(**common)
        first = image_work_calibration_signature(**common, decoder="pillow", execution_variant="staged")
        second = image_work_calibration_signature(**common, decoder="torchvision", execution_variant="staged")
        third = image_work_calibration_signature(**common, decoder="pillow", execution_variant="fused")
        self.assertTrue(legacy.startswith("image-stage-work-v1:"))
        self.assertTrue(first.startswith("image-stage-work-v2:"))
        self.assertEqual(len({legacy, first, second, third}), 4)
        implicit = self.descriptor()
        self.assertEqual(implicit.transform_signature, image_transform_signature(
            processor_revision="processor", decoder="torchvision_decode_image_rgb", input_size=2,
            representation="prepared_fp32_nchw", layout="NCHW", dtype="float32"))
        self.assertTrue(implicit.work.calibration_signature.startswith("image-stage-work-v2:"))

    def test_declared_size_layout_dtype_and_missing_versions_are_rejected(self):
        identity = self.slow()[2]
        for changes in ({"input_size": 3}, {"layout": "NHWC"}, {"dtype": "float16"},
                        {"representation": "prepared_fp16_nchw"}):
            with self.assertRaisesRegex(ValueError, "ready representation"):
                self.descriptor({**identity, **changes})
        with self.assertRaisesRegex(ValueError, "requires versions"):
            self.descriptor({key: value for key, value in identity.items() if key != "decoder_version"})

    def test_slow_worker_ready_and_completion_use_the_loaded_processor_identity(self):
        from src.semantic_methods.lotus.image_backend import SlowClipPrepare

        preprocessor, _processor, identity = self.slow()
        worker = SlowClipPrepare.__new__(SlowClipPrepare)
        worker.processor = preprocessor
        worker.identity = identity
        image = Image.fromarray(np.arange(12, dtype=np.uint8).reshape(2, 2, 3))
        output = io.BytesIO()
        image.save(output, format="PNG")
        encoded = [output.getvalue()]
        with patch.dict(sys.modules, {"transformers": SimpleNamespace(__version__="fixture-transformers")}):
            ready = worker.ready()
            self.assertEqual(ready["preprocessing_identity"], identity)
            descriptor = self.descriptor(ready["preprocessing_identity"], encoded_images=encoded)
            prepared, batch = worker.preprocess_staged(descriptor, encoded)
            np.testing.assert_array_equal(batch.payload, preprocessor.preprocess_decoded([image]))
            self.assertEqual(prepared.transform_signature, image_transform_signature(**identity))
            wrong = self.descriptor({**identity, "decoder": "torchvision_decode_image_rgb"},
                                    encoded_images=encoded)
            with self.assertRaisesRegex(ValueError, "preprocessor identity"):
                worker.preprocess_staged(wrong, encoded)

    def test_hse_inherits_pool_identity_and_rejects_conflicting_override(self):
        from src.modalities.image.execution import ProjectRayWorkerPool
        from src.modalities.image.staged_execution import run_project_ray_hse_pipeline
        from tests.modalities.image.test_staged_image_execution import (
            _CpuActor, _FakeRay, _GpuActor, _RecordBatch, _Scalar, _Source, _fake_ray_module,
        )

        descriptors = []
        class Cpu(_CpuActor):
            def _preprocess(self, descriptor, encoded):
                descriptors.append(descriptor)
                return super()._preprocess(descriptor, encoded)

        identity = self.slow()[2]
        pool = ProjectRayWorkerPool((Cpu(),), (_GpuActor(),), preprocessing_identity=identity)
        arguments = dict(worker_pool=pool, expected_doc_ids=frozenset({"one"}), batch_size=1,
            max_active_batches=1, encoded_block_bytes_upper_bound=5,
            limits=StageBrokerLimits(encoded_bytes=100, ready_bytes=100, ready_work=100,
                prepare_inflight=1, model_inflight=1), model_revision="model",
            processor_revision="processor", model_dtype="float32", input_size=2,
            embedding_dimension=2)
        source = _Source([_RecordBatch(doc_id=[_Scalar("one")], image=[_Scalar(b"image")])])
        with _fake_ray_module(_FakeRay()):
            result = run_project_ray_hse_pipeline(source, **arguments)
        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(descriptors[0].transform_signature, image_transform_signature(**identity))
        self.assertTrue(descriptors[0].work.calibration_signature.startswith("image-stage-work-v2:"))
        with self.assertRaisesRegex(ValueError, "does not match the worker pool"):
            run_project_ray_hse_pipeline(_Source([]), **arguments,
                preprocessing_identity={**identity, "decoder": "another-decoder"})

    def test_lotus_startup_requires_actual_slow_ready_identity(self):
        from src.semantic_methods.lotus.image_backend import SemLoomImageExecutor

        identity = {**self.slow()[2], "input_size": 224}
        for advertised in (identity, None):
            handles = [SimpleNamespace(ready=SimpleNamespace(remote=lambda: None)) for _ in range(2)]
            factories = iter(handles)
            killed = []
            ray = SimpleNamespace(is_initialized=lambda: True,
                remote=lambda **_options: lambda _cls: SimpleNamespace(remote=lambda *_args, **_kwargs: next(factories)),
                get=lambda _refs: [{"preprocessing_identity": advertised}, {}],
                kill=lambda actor, **_kwargs: killed.append(actor))
            with patch.dict(sys.modules, {"ray": ray}):
                if advertised is None:
                    with self.assertRaisesRegex(ValueError, "actual preprocessing identity"):
                        SemLoomImageExecutor(model="model")
                    self.assertEqual(killed, handles)
                else:
                    executor = SemLoomImageExecutor(model="model")
                    self.assertEqual(executor.pool.preprocessing_identity, identity)

    def test_versioned_identity_rejects_missing_core_fields(self):
        identity = self.slow()[2]
        for name in ("decoder", "processor_revision", "representation", "layout", "dtype"):
            with self.subTest(field=name), self.assertRaisesRegex(ValueError, "identity fields"):
                image_transform_signature(**{**identity, name: ""})
        for size in (0, True, 2.0):
            with self.subTest(size=size), self.assertRaisesRegex(ValueError, "identity fields"):
                image_transform_signature(**{**identity, "input_size": size})

    def test_invalid_pool_identity_is_rejected_before_opening_even_an_empty_source(self):
        from src.modalities.image.execution import ProjectRayWorkerPool
        from src.modalities.image.staged_execution import run_project_ray_hse_pipeline

        class Source:
            def into_batches(self, _size):
                raise AssertionError("invalid identity reached source opening")

        identity = self.slow()[2]
        for changes in ({"dtype": "float16"}, {"input_size": 3}, {"layout": "NHWC"},
                        {"decoder": ""}):
            pool = ProjectRayWorkerPool((object(),), (object(),),
                preprocessing_identity={**identity, **changes})
            with self.subTest(changes=changes), self.assertRaisesRegex(ValueError, "identity"):
                run_project_ray_hse_pipeline(Source(), worker_pool=pool,
                    expected_doc_ids=frozenset(), batch_size=1, max_active_batches=1,
                    encoded_block_bytes_upper_bound=5,
                    limits=StageBrokerLimits(100, 100, 100, 1, 1), model_revision="model",
                    processor_revision="processor", model_dtype="float32", input_size=2,
                    embedding_dimension=2)

    def test_wrong_decoder_is_rejected_before_slow_or_fast_decode(self):
        from src.modalities.image import execution
        from src.semantic_methods.lotus.image_backend import SlowClipPrepare
        from tests.modalities.image.test_image_worker_startup import _FakeRay

        identity = self.slow()[2]
        descriptor = self.descriptor({**identity, "decoder": "another-decoder"})
        def forbidden_decode(_encoded):
            raise AssertionError("wrong identity reached decoding")
        preprocessor = SimpleNamespace(preprocessing_identity=lambda: identity,
                                       preprocess=forbidden_decode)
        slow = SlowClipPrepare.__new__(SlowClipPrepare)
        slow.processor = preprocessor
        classes, ray = [], _FakeRay()
        original_remote = ray.remote
        def remote(**options):
            wrap = original_remote(**options)
            def capture(cls):
                classes.append(cls)
                return wrap(cls)
            return capture
        ray.remote = remote
        with patch.dict(sys.modules, {"ray": ray}):
            execution.build_project_ray_worker_pool(model_revision="fixture",
                processor_revision="fixture", cpu_workers=1, gpu_workers=1, dtype="float32")
        fast = classes[0].__new__(classes[0])
        fast._preprocessor = preprocessor
        for worker in (slow, fast):
            with self.subTest(worker=type(worker).__name__), self.assertRaisesRegex(ValueError, "identity"):
                worker.preprocess_staged(descriptor, [b"not an image"])


if __name__ == "__main__":
    unittest.main()
