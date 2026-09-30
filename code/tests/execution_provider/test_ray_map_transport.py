"""Daft windows and borrowed Ray calls keep independent row lifecycles."""

import asyncio
import importlib.util
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport
from src.scheduling.core.session_contract import BackendTask, OfferedTask, SessionSpec, TaskKey


def task(sequence, payload=b'payload'):
    return BackendTask(TaskKey(0, sequence), SessionSpec('job', 'flow', 'fixture'),
                       OfferedTask(sequence, payload, 1, 100))


class FakeRay:
    def __init__(self, execute):
        self.execute = execute
        self.puts, self.killed = [], []

    def is_initialized(self):
        return True

    def get_runtime_context(self):
        return SimpleNamespace(gcs_address='fixture-cluster')

    def remote(self, **options):
        async def close():
            return True
        actor = SimpleNamespace(ready=SimpleNamespace(remote=lambda: True),
                                execute=SimpleNamespace(remote=self.execute),
                                close=SimpleNamespace(remote=close))
        return lambda cls: SimpleNamespace(remote=lambda *_: actor)

    def get(self, value, **kwargs):
        return value

    def put(self, table):
        self.puts.append(table.num_rows)
        return table

    def kill(self, actor, **kwargs):
        self.killed.append(actor)


class RayMapStartupTests(unittest.TestCase):
    def test_startup_durations_use_local_clock_and_preserve_connection_ownership(self):
        for borrowed in (False, True):
            with self.subTest(borrowed=borrowed):
                tick=[0];events=[];ray=FakeRay(None)
                ray.is_initialized=lambda: borrowed
                def connect(**kwargs):tick[0]+=2_000_000_000
                def context():
                    tick[0]+=2_000_000_000
                    return SimpleNamespace(gcs_address='fixture-cluster')
                ray.init=connect;ray.get_runtime_context=context;ray.shutdown=Mock()
                remote=ray.remote
                def create(**options):
                    tick[0]+=3_000_000_000
                    return remote(**options)
                def ready(value, **kwargs):
                    tick[0]+=5_000_000_000
                    return value
                ray.remote=create;ray.get=ready
                with patch('src.execution_provider.adapters.ray_map_transport.time.monotonic_ns',
                           side_effect=lambda:tick[0]):
                    transport=RayMapTransport(FixedModelConfig('http://localhost/fixture','model',1000),4,
                        events.append,physical=RayMapConfig('fixture-cluster',1,2,1024,2048),ray_api=ray)
                    transport.abort_startup()
                self.assertEqual([event['stage'] for event in events],
                                 ['library_import','driver_connect','actor_create','actor_ready'])
                self.assertEqual([event['elapsed_seconds'] for event in events],[0,2,3,5])
                self.assertTrue(all(event['status']=='completed' for event in events))
                self.assertEqual(ray.shutdown.call_count,0 if borrowed else 1)
                self.assertEqual(len(ray.killed),1)

    def test_backend_start_failure_releases_already_created_transport(self):
        from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
        transport = SimpleNamespace(execute=Mock(), close=Mock(), abort_startup=Mock())
        with patch('src.execution_provider.adapters.incremental_execution.BoundedAsyncBackend',
                   side_effect=RuntimeError('fixture backend startup failure')):
            with self.assertRaisesRegex(RuntimeError, 'startup failure'):
                build_fixed_model_execution(FixedModelConfig('http://localhost/fixture', 'model', 1000),
                                            transport_factory=lambda *_: transport)
        transport.abort_startup.assert_called_once_with()

    def test_partial_actor_pool_creation_reclaims_the_first_actor(self):
        ray = FakeRay(None)
        events=[]
        actor = SimpleNamespace()
        create = Mock(side_effect=[actor, RuntimeError('fixture actor creation failure')])
        ray.remote = lambda **options: lambda cls: SimpleNamespace(remote=create)
        with self.assertRaisesRegex(RuntimeError, 'actor creation failure'):
            RayMapTransport(FixedModelConfig('http://localhost/fixture', 'model', 1000), 4, events.append,
                physical=RayMapConfig('fixture-cluster', 2, 2, 1024, 2048), ray_api=ray)
        self.assertEqual(ray.killed, [actor])
        self.assertEqual(events[-1]['stage'],'actor_create')
        self.assertEqual(events[-1]['status'],'failed')
        self.assertNotIn('actor_ready',[event['stage'] for event in events])

    def test_different_borrowed_cluster_is_rejected_before_actor_creation(self):
        ray = FakeRay(None)
        ray.remote = Mock(side_effect=AssertionError('must not allocate on the wrong cluster'))
        with self.assertRaisesRegex(ValueError, 'declared GCS address'):
            RayMapTransport(FixedModelConfig('http://localhost/fixture', 'model', 1000), 4,
                physical=RayMapConfig('another-cluster', 1, 2, 1024, 2048), ray_api=ray)
        ray.remote.assert_not_called()
        self.assertEqual(ray.killed, [])


@unittest.skipUnless(importlib.util.find_spec('daft') and importlib.util.find_spec('pyarrow'),
                     'Daft and Arrow are required for the actual batch adapter')
class RayMapTransportTests(unittest.IsolatedAsyncioTestCase):
    def transport(self, execute, events, guard=None, *, window=1024, objects=2048):
        ray = FakeRay(execute)
        transport = RayMapTransport(FixedModelConfig('http://localhost/fixture', 'model', 1000),
            4, events.append, physical=RayMapConfig('fixture-cluster', 1, 2, window, objects),
            before_request=guard, ray_api=ray)
        return transport, ray

    async def test_fast_row_returns_while_same_batch_slow_row_holds_object(self):
        release, started = asyncio.Event(), asyncio.Event()
        async def execute(table, index, template):
            self.assertEqual(template.task.payload, b'')
            if template.key.sequence == 0:
                started.set()
                await release.wait()
            return template.key, table['payload'][index].as_py(), 1, 2
        events = []
        transport, ray = self.transport(execute, events)
        slow = asyncio.create_task(transport.execute(task(0, b'slow'), 'model'))
        fast = asyncio.create_task(transport.execute(task(1, b'fast'), 'model'))
        try:
            self.assertEqual(await asyncio.wait_for(asyncio.shield(fast), 10), b'fast')
            self.assertTrue(started.is_set())
            self.assertFalse(slow.done())
            self.assertGreater(transport.used_bytes, 0)
            self.assertEqual(ray.puts, [2])
        finally:
            release.set()
            await asyncio.gather(slow, fast)
            await transport.close()
        self.assertEqual(transport.used_bytes, 0)
        self.assertEqual(len(ray.killed), 1)
        self.assertEqual(sum(event.get('stage')=='first_payload' for event in events),1)
        self.assertEqual(sum(event['event']=='ray_first_submit' for event in events),1)

    async def test_cancel_before_remote_send_and_guard_failure_spend_no_http(self):
        sent, guarded, events = [], [], []
        async def execute(table, index, template):
            sent.append(template.key)
            return template.key, b'ok', 1, 2
        def guard(request):
            guarded.append(request.key.sequence)
            raise ValueError('fixture exhausted budget')
        transport, _ = self.transport(execute, events, guard)
        cancelled = asyncio.create_task(transport.execute(task(0), 'model'))
        rejected = asyncio.create_task(transport.execute(task(1), 'model'))
        await asyncio.sleep(0)
        transport.cancel_pending(TaskKey(0, 0))
        self.assertEqual(await cancelled, b'')
        self.assertIn(b'MODEL_UNAVAILABLE', await rejected)
        await transport.close()
        self.assertEqual(sent, [])
        self.assertEqual(guarded, [1])
        self.assertEqual(transport.used_bytes, 0)

    async def test_cancel_after_send_waits_for_the_actual_result(self):
        release, started, events = asyncio.Event(), asyncio.Event(), []
        async def execute(table, index, template):
            started.set()
            await release.wait()
            return template.key, b'ok', 1, 2
        transport, _ = self.transport(execute, events)
        pending = asyncio.create_task(transport.execute(task(0), 'model'))
        try:
            await asyncio.wait_for(started.wait(), 10)
            transport.cancel_pending(TaskKey(0, 0))
            self.assertFalse(pending.done())
            self.assertGreater(transport.used_bytes, 0)
        finally:
            release.set()
            self.assertEqual(await pending, b'ok')
            await transport.close()

    async def test_remote_error_retains_unknown_object_and_fails_clean_close(self):
        async def execute(table, index, template):
            raise OSError('fixture remote outcome unknown')
        transport, ray = self.transport(execute, [])
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            await transport.execute(task(0), 'model')
        self.assertGreater(transport.used_bytes, 0)
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            await transport.close()
        self.assertEqual(len(ray.killed), 1)

    async def test_object_limit_waits_for_release_without_overspending(self):
        release, entered, events = asyncio.Event(), asyncio.Event(), []
        async def execute(table, index, template):
            if template.key.sequence < 2:
                entered.set()
                await release.wait()
            return template.key, b'ok', 1, 2
        transport, ray = self.transport(execute, events, window=62, objects=70)
        tasks = [asyncio.create_task(transport.execute(task(i), 'model')) for i in range(4)]
        try:
            await asyncio.wait_for(entered.wait(), 10)
            self.assertEqual(ray.puts, [2])
            self.assertLessEqual(transport.used_bytes, 70)
        finally:
            release.set()
            self.assertEqual(await asyncio.gather(*tasks), [b'ok'] * 4)
            await transport.close()
        self.assertEqual(ray.puts, [2, 2])
        self.assertTrue(all(e['object_bytes'] <= 70 for e in events))
