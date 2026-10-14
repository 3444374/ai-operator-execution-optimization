from __future__ import annotations

import asyncio
import contextlib
import unittest
from unittest.mock import patch

import numpy as np

from src.modalities.image.clip import ClipTensorActor
from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingBatch
from src.modalities.image.reusable_clip import InputCompletionUnknown, ReusableClipTensorActor


class _Tensor:
    def __init__(self, values):
        self.values = np.asarray(values)

    @property
    def ndim(self):
        return self.values.ndim

    @property
    def shape(self):
        return self.values.shape

    def numel(self):
        return self.values.size

    def element_size(self):
        return self.values.dtype.itemsize

    def float(self):
        return _Tensor(self.values.astype(np.float32))

    def norm(self, dim, keepdim):
        return _Tensor(np.linalg.norm(self.values, axis=dim, keepdims=keepdim))

    def clamp_min(self, value):
        return _Tensor(np.maximum(self.values, value))

    def __truediv__(self, other):
        return _Tensor(self.values / other.values)

    def cpu(self):
        return self

    def numpy(self):
        return self.values


class _Torch:
    """Count conversion and fence calls; this does not run CUDA or a model."""
    def __init__(self):
        self.cuda = self
        self.conversions = self.fences = 0
        self.fail_fence = self.fail_conversion = False

    def as_tensor(self, values, dtype, device):
        self.conversions += 1
        if self.fail_conversion:
            raise RuntimeError("conversion failed after submission")
        return _Tensor(np.array(values, dtype=dtype, copy=True) if device.startswith("cuda")
                       else np.asarray(values, dtype=dtype))

    def inference_mode(self):
        return contextlib.nullcontext()

    def synchronize(self, _device=None):
        self.fences += 1
        if self.fail_fence:
            raise RuntimeError("device fence unavailable")


class _Model:
    def __init__(self):
        self.calls = 0
        self.failure = None

    def get_image_features(self, *, pixel_values):
        self.calls += 1
        if self.failure:
            pixel_values.values[:] = 99
            raise self.failure
        values = pixel_values.values.reshape(pixel_values.shape[0], -1).astype(np.float32)
        return _Tensor(np.stack((values.sum(axis=1), values[:, 0], np.ones(values.shape[0])), axis=1))


def _initialize(actor, _revision, **kwargs):
    actor._device = kwargs.get("device", "cuda")
    actor._dtype = np.float32 if kwargs.get("dtype") == "float32" else np.float16
    actor._torch, actor._model = _Torch(), _Model()
    actor._normalize = True
    actor._detailed_stage_timing = kwargs.get("detailed_stage_timing", False)
    actor.semantics = EmbeddingSemantics("fixed-model", "fixed-processor", 3)


class ReusableClipTests(unittest.TestCase):
    def actor(self, budget=96, **kwargs):
        with patch.object(ClipTensorActor, "__init__", _initialize):
            return ReusableClipTensorActor("fixed-model", input_cache_bytes=budget,
                                           dtype="float32", **kwargs)

    def batch(self, pixels=None, ids=("first", "second")):
        pixels = np.arange(24, dtype=np.float32).reshape(2, 3, 2, 2) if pixels is None else pixels
        return ImageEmbeddingBatch(ids, pixels, "preprocessed_tensor", 8, "pixels")

    def test_same_contents_new_rows_skip_conversion_and_keep_full_forward(self):
        actor = self.actor()
        first = actor.embed(self.batch())
        second = actor.embed(self.batch(ids=("first#pass=2", "second#pass=2")))
        self.assertEqual((actor._torch.conversions, actor._model.calls), (1, 2))
        np.testing.assert_array_equal(first.embeddings, second.embeddings)
        self.assertEqual(second.doc_ids, ("first#pass=2", "second#pass=2"))
        self.assertEqual(second.embeddings.shape, (2, 3))
        self.assertEqual(second.embeddings.dtype, np.float32)
        np.testing.assert_allclose(np.linalg.norm(second.embeddings, axis=1), 1.0, rtol=1e-6)
        observation = actor.input_reuse_observation()
        self.assertTrue(observation.transfer.reuse_available)
        self.assertEqual((observation.transfer.host_reload_bytes,
                          observation.transfer.device_copy_bytes), (0, 0))
        self.assertIsNone(observation.h2d_s)
        self.assertEqual(second.telemetry.device_input_bytes, 96)
        self.assertEqual(actor.input_residency_snapshot().held_references, 0)

    def test_default_zero_avoids_hashing_and_matches_original_actor(self):
        actor = self.actor(0)
        reference = ClipTensorActor.__new__(ClipTensorActor)
        _initialize(reference, "fixed-model", dtype="float32")
        with patch("src.modalities.image.reusable_clip.prepared_tensor_key",
                   side_effect=AssertionError("disabled reuse must not hash")):
            first, second = actor.embed(self.batch()), actor.embed(self.batch())
        expected = reference.embed(self.batch())
        np.testing.assert_array_equal(first.embeddings, expected.embeddings)
        np.testing.assert_array_equal(second.embeddings, expected.embeddings)
        self.assertEqual((actor._torch.conversions, actor._model.calls, actor._torch.fences), (2, 2, 0))
        self.assertEqual(actor.input_residency_snapshot().resident_bytes, 0)
        self.assertIsNone(actor.input_reuse_observation())

    def test_changed_input_and_processor_version_miss_and_evict(self):
        actor = self.actor()
        actor.embed(self.batch())
        changed = self.batch().payload.copy()
        changed[0, 0, 0, 0] += 10
        actor.embed(self.batch(changed))
        self.assertFalse(actor.input_reuse_observation().transfer.reuse_available)
        actor.semantics = EmbeddingSemantics("fixed-model", "processor-v2", 3)
        actor.embed(self.batch(changed))
        self.assertEqual(actor._torch.conversions, 3)
        snapshot = actor.input_residency_snapshot()
        self.assertEqual((snapshot.resident_bytes, snapshot.logical_bytes_peak, snapshot.evictions), (96, 96, 2))

    def test_oversize_input_uses_original_full_output_without_retention(self):
        actor = self.actor(1)
        for _ in range(2):
            result = actor.embed(self.batch())
            self.assertEqual(result.embeddings.shape, (2, 3))
            self.assertEqual(actor.input_residency_snapshot().resident_bytes, 0)
        self.assertEqual(actor._torch.conversions, 2)

    def test_noncontiguous_hash_copy_and_readonly_host_copy_are_observed(self):
        actor = self.actor()
        pixels = self.batch().payload[:, :, :, ::-1]
        pixels.flags.writeable = False
        actor.embed(self.batch(pixels))
        observation = actor.input_reuse_observation()
        self.assertEqual((observation.identity_copy_bytes,
                          observation.host_ownership_copy_bytes), (96, 96))
        self.assertGreaterEqual(observation.identity_s, 0)
        self.assertEqual((observation.transfer.host_reload_bytes,
                          observation.transfer.device_copy_bytes), (96, 96))

    def test_cpu_retention_does_not_alias_callers_mutable_payload(self):
        actor = self.actor(device="cpu")
        original = self.batch().payload.copy()
        pixels = original.copy()
        result = actor.embed(self.batch(pixels))
        pixels[:] = 99
        reused = actor.embed(self.batch(original))
        np.testing.assert_array_equal(result.embeddings, reused.embeddings)
        self.assertEqual(actor._torch.conversions, 1)

    def test_forward_failure_discards_input_only_after_successful_fence(self):
        actor = self.actor()
        actor._model.failure = RuntimeError("forward failed")
        with self.assertRaisesRegex(RuntimeError, "forward"):
            actor.embed(self.batch())
        self.assertEqual((actor._torch.fences, actor.input_residency_snapshot().resident_bytes), (1, 0))
        actor._model.failure = None
        actor.embed(self.batch())
        self.assertEqual(actor._torch.conversions, 2)

    def test_cancelled_forward_and_failed_fence_keep_owner_reference(self):
        actor = self.actor()
        actor._model.failure = asyncio.CancelledError()
        actor._torch.fail_fence = True
        with self.assertRaises(asyncio.CancelledError):
            actor.embed(self.batch())
        actor.clear_input_residency()
        snapshot = actor.input_residency_snapshot()
        self.assertEqual((snapshot.resident_bytes, snapshot.unknown_references), (96, 1))
        with self.assertRaises(InputCompletionUnknown):
            actor.embed(self.batch())
        unrelated = self.actor()
        unrelated.synchronize()
        self.assertEqual(actor.input_residency_snapshot().resident_bytes, 96)
        with self.assertRaisesRegex(RuntimeError, "fence"):
            actor.synchronize()
        self.assertEqual(actor.input_residency_snapshot().unknown_references, 1)
        actor._torch.fail_fence = False
        actor.synchronize()
        self.assertEqual((actor.input_residency_snapshot().resident_bytes,
                          actor.input_residency_snapshot().held_references), (0, 0))

    def test_successful_forward_but_failed_fence_is_unknown(self):
        actor = self.actor()
        actor._torch.fail_fence = True
        with self.assertRaises(InputCompletionUnknown):
            actor.embed(self.batch())
        self.assertEqual(actor.input_residency_snapshot().unknown_references, 1)
        actor._torch.fail_fence = False
        actor.synchronize()
        actor.embed(self.batch())
        self.assertEqual(actor._torch.conversions, 2)

    def test_conversion_exception_holds_prepublication_bytes_until_fence(self):
        actor = self.actor()
        actor._torch.fail_conversion = actor._torch.fail_fence = True
        with self.assertRaisesRegex(RuntimeError, "conversion"):
            actor.embed(self.batch())
        self.assertEqual(actor.input_residency_snapshot().reserved_bytes, 96)
        with self.assertRaises(InputCompletionUnknown):
            actor.embed(self.batch())
        actor._torch.fail_fence = actor._torch.fail_conversion = False
        actor.synchronize()
        self.assertEqual(actor.input_residency_snapshot().reserved_bytes, 0)
        actor.embed(self.batch())

    def test_actor_service_time_includes_its_completion_fence(self):
        actor = self.actor()
        clock = [0.0]
        def fence(_device=None):
            clock[0] += 2.0
        actor._torch.synchronize = fence
        with patch("src.modalities.image.reusable_clip.time.perf_counter", lambda: clock[0]):
            result = actor.embed(self.batch())
        self.assertEqual(result.service_s, 2.0)
        self.assertEqual(actor.input_reuse_observation().settle_s, 2.0)

    def test_cancelled_completion_fence_keeps_input_unknown_until_later_confirmation(self):
        for interruption in (asyncio.CancelledError(), KeyboardInterrupt()):
            with self.subTest(interruption=type(interruption).__name__):
                actor = self.actor()
                with patch.object(actor._torch, "synchronize", side_effect=interruption):
                    with self.assertRaises(type(interruption)):
                        actor.embed(self.batch())
                snapshot = actor.input_residency_snapshot()
                self.assertEqual((snapshot.resident_bytes, snapshot.unknown_references), (96, 1))
                self.assertFalse(snapshot.entries[0].reusable)
                with self.assertRaises(InputCompletionUnknown):
                    actor.embed(self.batch())
                actor.synchronize()
                self.assertEqual(actor.input_residency_snapshot().held_references, 0)
                actor.embed(self.batch())
                self.assertEqual(actor.input_residency_snapshot().held_references, 0)


if __name__ == "__main__":
    unittest.main()
