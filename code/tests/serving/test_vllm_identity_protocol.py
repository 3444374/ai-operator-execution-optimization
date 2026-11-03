"""Opt-in real MessagePack and fixed vLLM source protocol checks; zero model/Ray/CUDA."""

import asyncio
import ast
import importlib.util
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.infrastructure.gpu_allocation import VLLM_GPU_IDENTITY_METHOD, VLLM_GPU_IDENTITY_EXTENSION
from src.serving.backends.vllm_async_engine import VllmAsyncEngine
from tests.serving.vllm_rpc_source_fixture import (
    SOURCE_HASHES, pinned_encoder, pinned_worker, verified_source,
)


GPU = "GPU-12345678-abcd-1234-abcd-123456789012"


class PinnedIdentityProtocolTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        root = os.environ.get("VLLM_0251_SOURCE_ROOT")
        if not root:
            raise unittest.SkipTest("set VLLM_0251_SOURCE_ROOT to the verified official fixed-tag source")
        if importlib.util.find_spec("msgspec") is None:
            raise RuntimeError("explicit source probe requires real msgspec")
        cls.root = Path(root)
        for name in SOURCE_HASHES:
            verified_source(cls.root, name)

    def test_default_encoder_rejects_old_function_RPC_before_worker_execution(self):
        codec = pinned_encoder(self.root)
        self.assertFalse(codec.insecure)
        body = Mock()

        def old_function(worker):
            return body()

        with self.assertRaisesRegex(TypeError, "is not serializable"):
            codec.encoder.encode((0, 1, "collective_rpc", (old_function, 1., (), {})))
        body.assert_not_called()
        self.assertIsNone(codec.encoder.aux_buffers)

    async def test_named_RPC_round_trips_default_codec_and_upstream_worker_extension(self):
        codec, worker, requests = pinned_encoder(self.root), pinned_worker(self.root), []
        cuda = SimpleNamespace(is_initialized=lambda: True, device_count=lambda: 1,
            get_device_properties=Mock(return_value=SimpleNamespace(uuid=GPU[4:])))

        async def collective_rpc(method, timeout=None, args=(), kwargs=None):
            encoded = codec.encoder.encode((0, 1, "collective_rpc", (method, timeout, args, kwargs)))
            frame = codec.decode(encoded[0])
            requests.append(frame)
            method, _, args, kwargs = frame[3]
            result = [codec.run_method(worker, method, args, kwargs or {})]
            reply = codec.encoder.encode(codec.utility(result))
            return codec.decode(reply[0])[1]

        engine = object.__new__(VllmAsyncEngine)
        engine._loop, engine._feedback = asyncio.get_running_loop(), object()
        engine._llm = SimpleNamespace(collective_rpc=collective_rpc)
        with patch.dict(os.environ, dict(CUDA_VISIBLE_DEVICES="1"), clear=True), \
                patch.dict("sys.modules", torch=SimpleNamespace(cuda=cuda), pynvml=None):
            await engine.observe_gpu_identity()
        self.assertEqual(engine._gpu_identity["gpu_uuids"], [GPU])
        self.assertEqual(requests[0][3][0], VLLM_GPU_IDENTITY_METHOD)
        self.assertFalse(codec.insecure)
        cuda.get_device_properties.assert_called_once_with(0)

    def test_fixed_worker_extension_API_rejects_method_name_collision(self):
        with self.assertRaisesRegex(AssertionError, "conflicts"):
            pinned_worker(self.root, collision=True)

    def test_source_probe_rejects_a_changed_version_or_changed_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "vllm/v1/serial_utils.py"
            path.parent.mkdir(parents=True)
            path.write_text("# another source\n")
            with self.assertRaisesRegex(ValueError, "source differs"):
                verified_source(Path(directory), "vllm/v1/serial_utils.py")

    def test_fixed_engine_args_forward_extension_qualified_name_without_code_objects(self):
        _, tree = verified_source(self.root, "vllm/engine/arg_utils.py")
        args = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "EngineArgs")
        self.assertTrue(any(isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                            and node.target.id == "worker_extension_cls" for node in args.body))
        call = next(node for node in ast.walk(args) if isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name) and node.func.id == "ParallelConfig")
        value = next(keyword.value for keyword in call.keywords if keyword.arg == "worker_extension_cls")
        self.assertEqual(ast.unparse(value), "self.worker_extension_cls")
        self.assertIsInstance(VLLM_GPU_IDENTITY_EXTENSION, str)
