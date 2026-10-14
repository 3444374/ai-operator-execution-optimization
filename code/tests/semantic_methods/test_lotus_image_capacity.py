"""Fixed byte capacities go to the existing HSE broker, without resizing inputs."""
import sys
import threading
from types import SimpleNamespace

import numpy as np
import pytest

from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult, ImageEmbeddingBatch
from src.modalities.image.execution import SemLoomRayWorkerPool
from src.modalities.image.staged import build_prepared_image_block_descriptor
from src.semantic_methods.lotus.image_backend import SemLoomImageExecutor
from tests.modalities.image.test_staged_image_execution import (
    _Scalar, _RecordBatch, _Source, _FakeRay, _CpuActor, _GpuActor,
)


class Gpu512(_GpuActor):
    @staticmethod
    def _embed(ref):
        old = _GpuActor._embed(ref)
        matrix = np.zeros((len(old.doc_ids), 512), np.float32)
        matrix[:, 0] = 1
        return ImageEmbeddingResult(old.doc_ids, matrix, EmbeddingSemantics("model", "processor", 512),
                                    old.service_s, telemetry=old.telemetry)


class Cpu224(_CpuActor):
    def _preprocess(self, descriptor, encoded):
        pixels = np.zeros((len(encoded), 3, 224, 224), np.float32)
        prepared = build_prepared_image_block_descriptor(descriptor, pixels,
                                                         ready_at_s=descriptor.created_at_s)
        return prepared, ImageEmbeddingBatch(descriptor.row_ids, pixels, "preprocessed_tensor",
                                            descriptor.model_work_units, "pixels", work_descriptor=descriptor.work)


@pytest.mark.parametrize("capacity,upper,passing,expected_capacity,expected_upper", [
    (None, None, True, 8, 4), (4, 4, True, 4, 4),
    (3, 4, False, None, None), (4, 3, False, None, None),
])
def test_fixed_capacity_and_block_bound_use_real_hse_rejection(
        monkeypatch, capacity, upper, passing, expected_capacity, expected_upper):
    fake = _FakeRay()
    monkeypatch.setitem(sys.modules, "ray", SimpleNamespace(wait=fake.wait, get=fake.get))
    def source(data):
        return _Source([_RecordBatch(doc_id=[_Scalar(x) for x in data["doc_id"]],
                                    image=[_Scalar(x) for x in data["image"]])])
    monkeypatch.setitem(sys.modules, "daft", SimpleNamespace(from_pydict=source))
    executor = SemLoomImageExecutor.__new__(SemLoomImageExecutor)
    executor._lock = threading.Lock()
    executor._closed = executor._unconfirmed_work = False
    executor.batch_size, executor.window, executor.hse = 2, 2, True
    executor.model, executor.processor, executor.dtype = "model", "processor", "float32"
    executor.semantics = EmbeddingSemantics("model", "processor", 512)
    executor.pool = SemLoomRayWorkerPool((Cpu224(),), (Gpu512(),))
    executor.encoded_bytes_limit, executor.encoded_block_bytes_upper_bound = capacity, upper
    executor.execution_observations = []
    if passing:
        result = executor.embed_batch(("a", "b"), [b"aa", b"bb"])
        assert result.doc_ids == ("a", "b") and result.embeddings.shape == (2, 512)
        record = executor.execution_observations[0]
        assert record["encoded_bytes_limit"] == expected_capacity
        assert record["encoded_block_bytes_upper_bound"] == expected_upper
        assert record["encoded_bytes_peak"] <= expected_capacity
        assert not executor._unconfirmed_work
    else:
        with pytest.raises(BufferError):
            executor.embed_batch(("a", "b"), [b"aa", b"bb"])
        assert not executor.execution_observations
        assert executor._unconfirmed_work and executor.pool is not None
        with pytest.raises(RuntimeError, match="worker exit is unconfirmed"):
            executor.embed_batch(("a", "b"), [b"aa", b"bb"])


@pytest.mark.parametrize("invalid", [True, 0, -1, 1.0, "4"])
def test_capacity_settings_reject_invalid_before_actor_start(invalid):
    with pytest.raises(ValueError, match="positive integers"):
        SemLoomImageExecutor(model="unused", encoded_bytes_limit=invalid)
