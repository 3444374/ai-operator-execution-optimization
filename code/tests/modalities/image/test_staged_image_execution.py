from __future__ import annotations

import sys
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

from src.modalities.image.contracts import (
    EmbeddingSemantics,
    ImageBatchTelemetry,
    ImageEmbeddingBatch,
    ImageEmbeddingResult,
)
from src.modalities.image.execution import ProjectRayWorkerPool
from src.modalities.image.staged import build_prepared_image_block_descriptor
from src.modalities.image.staged_execution import run_project_ray_hse_pipeline
from src.scheduling.runtime.stage_broker import StageBrokerLimits


class _Scalar:
    def __init__(self, value):
        self._value = value

    def as_py(self):
        return self._value


class _RecordBatch(dict):
    pass


class _Source:
    def __init__(self, batches):
        self._batches = batches

    def into_batches(self, _batch_size):
        return self

    def to_arrow_iter(self, *, results_buffer_size):
        assert results_buffer_size == 2
        return iter(self._batches)


class _Ref:
    def __init__(self, value):
        self.value = value


class _FakeRay:
    def __init__(self):
        self.get_types = []

    def wait(self, refs, *, num_returns):
        assert num_returns == 1
        return refs[:1], refs[1:]

    def get(self, ref):
        self.get_types.append(type(ref.value).__name__)
        return ref.value


class _ReverseFakeRay(_FakeRay):
    def wait(self, refs, *, num_returns):
        assert num_returns == 1
        return refs[-1:], refs[:-1]


class _OptionsMethod:
    def __init__(self, callback):
        self._callback = callback
        self._num_returns = 1

    def options(self, *, num_returns):
        self._num_returns = num_returns
        return self

    def remote(self, *args):
        value = self._callback(*args)
        if self._num_returns == 2:
            return tuple(_Ref(item) for item in value)
        return _Ref(value)


class _CpuActor:
    def __init__(self, actor_id=None, call_log=None):
        self._actor_id = actor_id
        self._call_log = call_log
        self.preprocess_staged = _OptionsMethod(self._preprocess)

    def _preprocess(self, descriptor, encoded):
        if self._call_log is not None:
            self._call_log.append(self._actor_id)
        side = 2
        pixels = np.zeros((len(encoded), 3, side, side), dtype=np.float32)
        prepared = build_prepared_image_block_descriptor(
            descriptor,
            pixels,
            ready_at_s=descriptor.created_at_s,
        )
        batch = ImageEmbeddingBatch(
            doc_ids=descriptor.row_ids,
            payload=pixels,
            input_kind="preprocessed_tensor",
            work_units=descriptor.model_work_units,
            work_unit="pixels",
            work_descriptor=descriptor.work,
            telemetry=ImageBatchTelemetry(
                preprocess_s=0.01,
                encoded_bytes=descriptor.physical_bytes,
                input_tensor_bytes=pixels.nbytes,
            ),
        )
        return prepared, batch


class _GpuActor:
    def __init__(self):
        self.embed = _OptionsMethod(self._embed)

    @staticmethod
    def _embed(batch_ref):
        batch = batch_ref.value if isinstance(batch_ref, _Ref) else batch_ref
        rows = len(batch.doc_ids)
        embeddings = np.zeros((rows, 2), dtype=np.float32)
        embeddings[:, 0] = 1.0
        return ImageEmbeddingResult(
            doc_ids=batch.doc_ids,
            embeddings=embeddings,
            semantics=EmbeddingSemantics(
                model_revision="model",
                processor_revision="processor",
                dimension=2,
            ),
            service_s=0.02,
            telemetry=ImageBatchTelemetry(
                preprocess_s=batch.telemetry.preprocess_s,
                encoded_bytes=batch.telemetry.encoded_bytes,
                input_tensor_bytes=batch.telemetry.input_tensor_bytes,
                device_input_bytes=batch.telemetry.input_tensor_bytes // 2,
                output_bytes=embeddings.nbytes,
            ),
        )


@contextmanager
def _fake_ray_module(fake_ray):
    module = SimpleNamespace(wait=fake_ray.wait, get=fake_ray.get)
    with patch.dict(sys.modules, {"ray": module}):
        yield


class StagedImageExecutionTest(unittest.TestCase):
    def test_driver_reads_descriptors_and_results_but_not_prepared_payload(self) -> None:
        batches = [
            _RecordBatch(
                doc_id=[_Scalar("a"), _Scalar("b")],
                image=[_Scalar(b"a"), _Scalar(b"b")],
            ),
            _RecordBatch(
                doc_id=[_Scalar("c")],
                image=[_Scalar(b"c")],
            ),
        ]
        fake_ray = _FakeRay()
        with _fake_ray_module(fake_ray):
            result = run_project_ray_hse_pipeline(
                _Source(batches),
                worker_pool=ProjectRayWorkerPool(
                    preprocessors=(_CpuActor(),),
                    gpu_actors=(_GpuActor(),),
                ),
                expected_doc_ids=frozenset({"a", "b", "c"}),
                batch_size=2,
                max_active_batches=2,
                encoded_block_bytes_upper_bound=2,
                limits=StageBrokerLimits(
                    encoded_bytes=32,
                    ready_bytes=2 * 3 * 2 * 2 * 4,
                    ready_work=2 * 2 * 2,
                    prepare_inflight=1,
                    model_inflight=1,
                ),
                model_revision="model",
                processor_revision="processor",
                model_dtype="float16",
                input_size=2,
                embedding_dimension=2,
            )

        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(result.submitted_batches, 2)
        self.assertEqual(result.execution_mode, "hse_static")
        self.assertLessEqual(result.ready_bytes_peak, 2 * 3 * 2 * 2 * 4)
        self.assertNotIn("ImageEmbeddingBatch", fake_ray.get_types)
        self.assertEqual(fake_ray.get_types.count("StageBlockDescriptor"), 2)
        self.assertEqual(fake_ray.get_types.count("ImageEmbeddingResult"), 2)

    def test_refuses_more_model_leases_than_gpu_actors(self) -> None:
        with self.assertRaisesRegex(ValueError, "GPU actor count"):
            run_project_ray_hse_pipeline(
                _Source([]),
                worker_pool=ProjectRayWorkerPool(
                    preprocessors=(_CpuActor(),),
                    gpu_actors=(_GpuActor(),),
                ),
                expected_doc_ids=frozenset({"a"}),
                batch_size=1,
                max_active_batches=2,
                encoded_block_bytes_upper_bound=1,
                limits=StageBrokerLimits(
                    encoded_bytes=16,
                    ready_bytes=48,
                    ready_work=4,
                    prepare_inflight=1,
                    model_inflight=2,
                ),
                model_revision="model",
                processor_revision="processor",
                model_dtype="float16",
                input_size=2,
                embedding_dimension=2,
            )

    def test_refuses_source_lookahead_larger_than_encoded_capacity(self) -> None:
        with self.assertRaisesRegex(BufferError, "worst-case HSE source block"):
            run_project_ray_hse_pipeline(
                _Source([]),
                worker_pool=ProjectRayWorkerPool(
                    preprocessors=(_CpuActor(),),
                    gpu_actors=(_GpuActor(),),
                ),
                expected_doc_ids=frozenset(),
                batch_size=2,
                max_active_batches=2,
                encoded_block_bytes_upper_bound=17,
                limits=StageBrokerLimits(
                    encoded_bytes=16,
                    ready_bytes=96,
                    ready_work=8,
                    prepare_inflight=1,
                    model_inflight=1,
                ),
                model_revision="model",
                processor_revision="processor",
                model_dtype="float16",
                input_size=2,
                embedding_dimension=2,
            )

    def test_reuses_the_actor_that_actually_completed(self) -> None:
        batches = [
            _RecordBatch(doc_id=[_Scalar(row)], image=[_Scalar(row.encode())])
            for row in ("a", "b", "c")
        ]
        calls = []
        fake_ray = _ReverseFakeRay()
        with _fake_ray_module(fake_ray):
            result = run_project_ray_hse_pipeline(
                _Source(batches),
                worker_pool=ProjectRayWorkerPool(
                    preprocessors=(
                        _CpuActor(actor_id=0, call_log=calls),
                        _CpuActor(actor_id=1, call_log=calls),
                    ),
                    gpu_actors=(_GpuActor(),),
                ),
                expected_doc_ids=frozenset({"a", "b", "c"}),
                batch_size=1,
                max_active_batches=3,
                encoded_block_bytes_upper_bound=1,
                limits=StageBrokerLimits(
                    encoded_bytes=3,
                    ready_bytes=3 * 3 * 2 * 2 * 4,
                    ready_work=3 * 2 * 2,
                    prepare_inflight=2,
                    model_inflight=1,
                ),
                model_revision="model",
                processor_revision="processor",
                model_dtype="float16",
                input_size=2,
                embedding_dimension=2,
            )

        self.assertTrue(result.audit["exactly_once"])
        self.assertEqual(calls[:3], [0, 1, 1])


if __name__ == "__main__":
    unittest.main()
