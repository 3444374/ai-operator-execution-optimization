"""Actual local IPC carries startup device metadata from an explicit CPU fixture."""

import asyncio
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.async_engine_worker import serve
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.serving.async_engine_fixture import FixtureEngine


GPU = "GPU-12345678-abcd-1234-abcd-123456789012"


class IdentityFixture(FixtureEngine):
    async def observe_gpu_identity(self):
        self.observed = True

    def ready(self):
        return dict(super().ready(), fixture_observation_ran=getattr(self, "observed", False),
            native_gpu_identity=dict(status="CPU_fixture", gpu_uuids=[GPU]))


class IdentityProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, str(Path(__file__).resolve()), *map(str, descriptors)]


class AllocationIdentityIpcTests(unittest.TestCase):
    def test_startup_observation_precedes_ready_and_keeps_CPU_fixture_identity(self):
        with patch.object(EngineSpec, "validate_driver_environment", return_value={}):
            owner = IdentityProcess(EngineSpec("/tmp/CPU-fixture", sys.executable, sys.executable),
                                    GenerationSpec(), limits=AsyncEngineLimits(max_inflight=8), startup_timeout_s=5)
        try:
            self.assertTrue(owner.ready_report["fixture_observation_ran"])
            self.assertFalse(owner.ready_report["physical_gpu"])
            self.assertEqual(owner.ready_report["inference_calls"], 0)
            self.assertEqual(owner.ready_report["native_gpu_identity"]["gpu_uuids"], [GPU])
            self.assertEqual(owner.close(timeout=3)["status"], "closed")
        finally:
            owner.close(timeout=3)


if __name__ == "__main__":
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=IdentityFixture))
