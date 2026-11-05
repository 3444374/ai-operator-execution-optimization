"""Already-observed CUDA UUID receipts; no real CUDA/NVML access."""

import os
import asyncio
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from src.infrastructure.gpu_allocation import (
    current_cuda_gpu_identity, cuda_visible_gpu_uuids,
    SemLoomGpuIdentityWorkerExtension, VLLM_GPU_IDENTITY_METHOD,
)


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

    def test_worker_receipt_provenance_is_distinct_from_prior_visibility_resolution(self):
        from src.observability.records_telemetry import MetricSources
        with patch.dict(os.environ, {}, clear=True), patch.dict("sys.modules", pynvml=None):
            native = MetricSources.from_cuda_visibility("1", assigned_gpu_uuids=[GPU])
            prior = MetricSources.from_cuda_visibility(GPU)
        self.assertEqual(native.gpu_binding_source, "native_worker_receipt")
        self.assertEqual(prior.gpu_binding_source, "cuda_visibility_without_worker_receipt")

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
        from src.execution_provider.adapters.records_async_model import AsyncCoreModelActor
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
        from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
        engine.spec = EngineSpec('/tmp/CPU-context', '/tmp/CPU-model-python', '/tmp/CPU-driver-python', max_model_calls=4)
        engine.generation = GenerationSpec()
        engine._context_observed = None
        engine._llm = SimpleNamespace(vllm_config=SimpleNamespace(model_config=SimpleNamespace(max_model_len=32768)))
        identity = dict(status="observed_actual_CUDA_device", gpu_uuids=[GPU])
        engine._gpu_identity = identity
        with patch("src.infrastructure.gpu_allocation.current_cuda_gpu_identity", side_effect=AssertionError("API process must not probe CUDA")) as observe:
            first, second = engine.ready(), engine.ready()
        observe.assert_not_called()
        self.assertEqual(first["native_gpu_identity"], identity)
        self.assertEqual(first["model_context"]["max_model_len"], 32768)
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
            self.assertEqual(method, VLLM_GPU_IDENTITY_METHOD)
            return [getattr(SemLoomGpuIdentityWorkerExtension(), method)()]
        engine._llm.collective_rpc.side_effect = worker_rpc
        with patch.dict(os.environ, dict(CUDA_VISIBLE_DEVICES="1"), clear=True), \
                patch.dict("sys.modules", torch=SimpleNamespace(cuda=cuda), pynvml=None):
            await engine.observe_gpu_identity()
        self.assertEqual(engine._gpu_identity["gpu_uuids"], [GPU])
        engine._llm.collective_rpc.assert_awaited_once()
        cuda.get_device_properties.assert_called_once_with(0)

    async def test_new_RPC_uses_only_primitive_wire_arguments_and_retains_default_security(self):
        engine = self.engine([dict(status="observed_actual_CUDA_device", gpu_uuids=[GPU])])
        before = os.environ.get("VLLM_ALLOW_INSECURE_SERIALIZATION")
        with patch.dict(os.environ, dict(CUDA_VISIBLE_DEVICES="0")):
            await engine.observe_gpu_identity()
        args, kwargs = engine._llm.collective_rpc.call_args
        self.assertEqual(args, (VLLM_GPU_IDENTITY_METHOD,))
        self.assertEqual(kwargs, dict(timeout=1.0))
        self.assertEqual(os.environ.get("VLLM_ALLOW_INSECURE_SERIALIZATION"), before)

    def test_model_worker_extension_is_a_named_no_argument_read_only_method(self):
        result = dict(status="unavailable:fixture", gpu_uuids=None)
        with patch("src.infrastructure.gpu_allocation.current_cuda_gpu_identity", return_value=result) as read:
            self.assertEqual(SemLoomGpuIdentityWorkerExtension().semloom_gpu_identity(), result)
        read.assert_called_once_with()

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
