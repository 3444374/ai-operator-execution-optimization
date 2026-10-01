"""Async pre-dispatch accounting must settle before any remote method starts."""
import asyncio
import unittest

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport, _Block, _Row
from tests.execution_provider.test_ray_map_transport import FakeRay, task


class AsyncGuardTransportTests(unittest.IsolatedAsyncioTestCase):
    def transport(self, guard, sent):
        async def execute(table, index, template):
            sent.append(template.key)
            return template.key, b'ok', 1, 2
        return RayMapTransport(FixedModelConfig('http://localhost/fixture', 'model', 1000), 4,
            physical=RayMapConfig('fixture-cluster', 1, 2, 1024, 2048),
            before_request=guard, ray_api=FakeRay(execute))

    async def test_async_guard_finishes_before_rpc_and_cancelled_row_never_dispatches(self):
        for cancelled in (False, True):
            with self.subTest(cancelled=cancelled):
                entered, release, sent = asyncio.Event(), asyncio.Event(), []
                async def guard(request):
                    entered.set()
                    await release.wait()
                transport = self.transport(guard, sent)
                request = task(0)
                row = _Row(request, asyncio.get_running_loop().create_future())
                transport.rows[request.key] = row
                transport.blocks[0] = _Block(None, 64, {request.key})
                transport.used_bytes = 64
                operation = asyncio.create_task(transport._run_row(row, 0, 0))
                try:
                    await asyncio.sleep(0)
                    self.assertEqual(sent, [], 'RPC started before accounting completed')
                    self.assertTrue(entered.is_set(), 'guard was not awaited')
                    self.assertEqual(transport.used_bytes, 64)
                    if cancelled:
                        transport.cancel_pending(request.key)
                    release.set()
                    await operation
                    self.assertEqual(await row.future, b'' if cancelled else b'ok')
                    self.assertEqual(len(sent), 0 if cancelled else 1)
                    self.assertEqual(transport.used_bytes, 0)
                finally:
                    release.set()
                    await operation
                    await transport.close()

    async def test_async_budget_failure_does_not_dispatch_or_leave_objects(self):
        sent = []
        async def guard(request):
            await asyncio.sleep(0)
            raise ValueError('fixture accounting failed')
        transport = self.transport(guard, sent)
        request = task(0)
        row = _Row(request, asyncio.get_running_loop().create_future())
        transport.blocks[0] = _Block(None, 64, {request.key})
        transport.used_bytes = 64
        try:
            await transport._run_row(row, 0, 0)
            self.assertIn(b'MODEL_UNAVAILABLE', await row.future)
            self.assertEqual(sent, [])
            self.assertEqual(transport.used_bytes, 0)
        finally:
            await transport.close()

    async def test_cancelled_delivery_future_suppresses_pending_rpc(self):
        entered, release, sent = asyncio.Event(), asyncio.Event(), []
        async def guard(request):
            entered.set()
            await release.wait()
        transport = self.transport(guard, sent)
        request = task(0)
        row = _Row(request, asyncio.get_running_loop().create_future())
        transport.blocks[0] = _Block(None, 64, {request.key})
        transport.used_bytes = 64
        operation = asyncio.create_task(transport._run_row(row, 0, 0))
        try:
            await entered.wait()
            row.future.cancel()
            release.set()
            await operation
            self.assertEqual(sent, [])
            self.assertEqual(transport.used_bytes, 0)
        finally:
            release.set()
            await operation
            await transport.close()
