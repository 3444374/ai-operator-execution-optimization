"""Already-observed CUDA UUID receipts; no real CUDA/NVML access."""

import os
import asyncio
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from src.infrastructure.gpu_allocation import current_cuda_gpu_identity, cuda_visible_gpu_uuids


GPU = "GPU-12345678-abcd-1234-abcd-123456789012"
OTHER = "GPU-abcdef01-abcd-1234-abcd-123456789012"


class ModelGpuIdentityTests(unittest.TestCase):
    def test_actual_cuda_identity_is_canonical_and_requires_one_existing_device(self):
        cuda = SimpleNamespace(is_initialized=lambda: True, device_count=lambda: 1,
                               get_device_properties=lambda index: SimpleNamespace(uuid=GPU[4:].upper()))
        with patch.dict("sys.modules", torch=SimpleNamespace(cuda=cuda)):
            value = current_cuda_gpu_identity()
        self.assertEqual(value["gpu_uuids"], [GPU])

    def test_observation_never_initializes_cuda_for_a_driver_or_actor(self):
        cuda = SimpleNamespace(is_initialized=lambda: False, device_count=Mock(), get_device_properties=Mock())
        with patch.dict("sys.modules", torch=SimpleNamespace(cuda=cuda)):
            value = current_cuda_gpu_identity()
        self.assertIsNone(value["gpu_uuids"])
        cuda.device_count.assert_not_called()
        cuda.get_device_properties.assert_not_called()

    def test_ambiguous_device_count_or_malformed_UUID_remains_unknown(self):
        for count, raw in ((0, GPU), (2, GPU), (1, "GPU-short"), (1, "MIG-" + GPU[4:])):
            cuda = SimpleNamespace(is_initialized=lambda: True, device_count=lambda: count,
                get_device_properties=lambda index: SimpleNamespace(uuid=raw))
            with self.subTest(count=count, raw=raw), patch.dict("sys.modules", torch=SimpleNamespace(cuda=cuda)):
                self.assertIsNone(current_cuda_gpu_identity()["gpu_uuids"])

    def test_numeric_mask_uses_actual_UUID_receipt_without_nvml_ordinal_guessing(self):
        with patch.dict(os.environ, {}, clear=True), patch.dict("sys.modules", pynvml=None):
            self.assertEqual(cuda_visible_gpu_uuids("1", assigned_gpu_uuids=[GPU]), (GPU,))
            self.assertEqual(cuda_visible_gpu_uuids("0", assigned_gpu_uuids=[OTHER]), (OTHER,))

    def test_actual_identity_cannot_widen_inherited_UUID_mask(self):
        with patch.dict(os.environ, dict(NVIDIA_VISIBLE_DEVICES=OTHER), clear=True):
            with self.assertRaises(ValueError):
                cuda_visible_gpu_uuids("1", assigned_gpu_uuids=[GPU])
        with patch.dict(os.environ, {}, clear=True):
            for visibility, assigned in ((GPU, [OTHER]), ("0,1", [GPU]), ("0", ["GPU-short"]),
                                         ("0", []), ("0", [GPU, OTHER])):
                with self.subTest(visibility=visibility, assigned=assigned), self.assertRaises(ValueError):
                    cuda_visible_gpu_uuids(visibility, assigned_gpu_uuids=assigned)

    def test_actor_passes_private_model_identity_to_the_existing_sampler_once(self):
        from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
        actor = object.__new__(AsyncCoreModelActor)
        actor._resource_sources = None
        actor._resource_lock = threading.Lock()
        actor.engine = SimpleNamespace(ready_report=dict(engine_uid="e" * 32,
            native_gpu_identity=dict(status="observed_actual_CUDA_device", gpu_uuids=[GPU])))
        sampler = Mock(return_value=dict(values={}, sources={}))
        sampler.gpu_ids = (GPU,)
        with patch("src.observability.records_telemetry.MetricSources.from_cuda_visibility", return_value=sampler) as build:
            first, second = actor._resource_snapshot(1), actor._resource_snapshot(1)
        build.assert_called_once_with(assigned_gpu_uuids=[GPU])
        self.assertEqual(first["gpu_uuids"], [GPU])
        self.assertEqual(second["engine_uid"], "e" * 32)

    def test_async_readiness_keeps_optional_identity_without_engine_config_changes(self):
        from src.serving.backends.vllm_async_engine import VllmAsyncEngine
        engine = object.__new__(VllmAsyncEngine)
        engine._closed, engine._feedback, engine._reserved_model_calls = False, None, 0
        engine.engine_uid = "e" * 32
        engine.spec = SimpleNamespace(max_model_calls=4, enable_mfu_metrics=False)
        identity = dict(status="observed_actual_CUDA_device", gpu_uuids=[GPU])
        engine._gpu_identity = identity
        with patch("src.infrastructure.gpu_allocation.current_cuda_gpu_identity", side_effect=AssertionError("API process must not probe CUDA")) as observe:
            first, second = engine.ready(), engine.ready()
        observe.assert_not_called()
        self.assertEqual(first["native_gpu_identity"], identity)
        self.assertEqual(second["native_gpu_identity"], identity)


class NativeIdentityRpcTests(unittest.IsolatedAsyncioTestCase):
    def engine(self, reply):
        from src.serving.backends.vllm_async_engine import VllmAsyncEngine
        engine = object.__new__(VllmAsyncEngine)
        engine._loop = asyncio.get_running_loop()
        engine._feedback = object()
        engine._llm = SimpleNamespace(collective_rpc=AsyncMock(return_value=reply))
        return engine

    async def test_existing_worker_RPC_carries_one_actual_UUID_without_API_process_CUDA(self):
        engine = self.engine(None)
        cuda = SimpleNamespace(is_initialized=lambda: True, device_count=lambda: 1,
            get_device_properties=Mock(return_value=SimpleNamespace(uuid=GPU[4:])))
        async def worker_rpc(method, *, timeout):
            self.assertEqual(timeout, 1)
            return [method(SimpleNamespace())]
        engine._llm.collective_rpc.side_effect = worker_rpc
        with patch.dict(os.environ, dict(CUDA_VISIBLE_DEVICES="1"), clear=True), \
                patch.dict("sys.modules", torch=SimpleNamespace(cuda=cuda), pynvml=None):
            await engine.observe_gpu_identity()
        self.assertEqual(engine._gpu_identity["gpu_uuids"], [GPU])
        engine._llm.collective_rpc.assert_awaited_once()
        cuda.get_device_properties.assert_called_once_with(0)

    async def test_feedback_off_has_no_native_observation_RPC(self):
        engine = self.engine(None)
        engine._feedback = None
        await engine.observe_gpu_identity()
        engine._llm.collective_rpc.assert_not_called()
        self.assertIsNone(engine._gpu_identity["gpu_uuids"])

    async def test_missing_ambiguous_and_wrong_identity_never_fall_back_to_host_ordinal(self):
        for reply in (None, [], [{}, {}], [dict(status="observed_actual_CUDA_device")],
                [dict(status="observed_actual_CUDA_device", gpu_uuids=["GPU-short"])],
                [dict(status="unavailable", gpu_uuids=[GPU])]):
            engine = self.engine(reply)
            with self.subTest(reply=reply), patch.dict(os.environ, dict(CUDA_VISIBLE_DEVICES="0"), clear=True), \
                    patch("src.infrastructure.gpu_allocation._single_cuda_gpu_uuid", side_effect=AssertionError("no guessed fallback")):
                await engine.observe_gpu_identity()
            self.assertIsNone(engine._gpu_identity["gpu_uuids"])

    async def test_rpc_timeout_stays_optional_and_cancellation_propagates(self):
        engine = self.engine(None)
        engine._llm.collective_rpc.side_effect = TimeoutError("fixture")
        await engine.observe_gpu_identity()
        self.assertIsNone(engine._gpu_identity["gpu_uuids"])
        engine._llm.collective_rpc.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await engine.observe_gpu_identity()
