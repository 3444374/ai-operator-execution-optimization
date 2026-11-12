"""Startup readback distinguishes requested numerical mode from worker state."""
import asyncio
import os
from types import ModuleType, SimpleNamespace
import sys
import unittest
from unittest.mock import patch

from src.observability.metrics.vllm import worker_execution_profile
from src.serving.backends.vllm_async_engine import VllmAsyncEngine


class NativeExecutionProfileTests(unittest.TestCase):
    def test_worker_state_is_not_inferred_from_requested_flag(self):
        vllm = ModuleType('vllm')
        vllm.envs = SimpleNamespace(VLLM_BATCH_INVARIANT=True)
        layers = ModuleType('vllm.model_executor.layers')
        layers.batch_invariant = SimpleNamespace(_batch_invariant_MODE=False)
        with patch.dict(sys.modules, {'vllm': vllm, 'vllm.model_executor.layers': layers}):
            observed = worker_execution_profile()
        self.assertTrue(observed['batch_invariant_requested'])
        self.assertFalse(observed['batch_invariant_active'])

    def observe(self, reply=None, error=None, requested=True):
        async def run():
            engine = VllmAsyncEngine.__new__(VllmAsyncEngine)
            engine._loop, engine._feedback = asyncio.get_running_loop(), None
            calls = []
            async def rpc(method, *, timeout):
                calls.append(method)
                if error is not None:
                    raise error
                return reply
            engine._llm = SimpleNamespace(collective_rpc=rpc)
            with patch.dict(os.environ, {}, clear=True):
                if requested:
                    os.environ['VLLM_BATCH_INVARIANT'] = '1'
                await engine.observe_gpu_identity()
            return engine, calls
        return asyncio.run(run())

    def test_explicit_profile_is_observed_without_periodic_feedback(self):
        reply = dict(status='observed_worker_execution_profile', pid=123,
                     batch_invariant_requested=True, batch_invariant_active=True)
        engine, calls = self.observe([reply])
        self.assertEqual(engine._execution_profile, reply)
        self.assertEqual(calls, ['semloom_execution_profile'])
        self.assertEqual(engine._gpu_identity['status'], 'unavailable:not_requested')

    def test_failed_readback_stays_unavailable(self):
        engine, _ = self.observe(error=TimeoutError())
        self.assertEqual(engine._execution_profile, {'status': 'unavailable:TimeoutError'})

    def test_default_does_not_add_a_worker_rpc(self):
        engine, calls = self.observe(requested=False)
        self.assertEqual(calls, [])
        self.assertEqual(engine._execution_profile, {'status': 'unavailable:not_requested'})
