"""Only an identified actor reply before HTTP can settle a failed payload member."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.execution_provider.adapters.ray_map_transport import (
    RayMapConfig, RayMapTransport, _Block, _HttpActor, _RemoteFailure, _Row,
)
from src.scheduling.core.session_contract import BackendTask, OfferedTask, SessionSpec, TaskKey
from tests.execution_provider.test_ray_map_transport import FakeRay
from tests.execution_provider.test_ray_map_worker_pool import NamedRay, physical, CONFIG


def request(sequence=0, session_id=0):
    return BackendTask(TaskKey(session_id, sequence), SessionSpec('job', 'flow', 'fixture'),
                       OfferedTask(sequence, b'input', 1, 1024))


def table(*keys):
    values = {'session_id': [key.session_id for key in keys],
              'sequence': [key.sequence for key in keys], 'payload': [b'input'] * len(keys)}
    return {name: [SimpleNamespace(as_py=lambda value=value: value) for value in column]
            for name, column in values.items()}


class PayloadFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_confirmed_payload_failure_settles_optional_preparation_once(self):
        from tests.execution_provider.test_map_preparation import batches, make_transport, wait_until
        from tests.execution_provider.test_ray_map_transport import task
        actor = _HttpActor(CONFIG, 2)
        calls, events = [], []
        async def execute(*args):
            calls.append(args)
            raise AssertionError('invalid payload must not enter HTTP')
        actor.transport.execute = execute
        transport, preparation, ray = make_transport(actor.execute, events=events)
        put = ray.put
        def corrupt(actual):
            put(actual)
            return table(TaskKey(99, 0))
        ray.put = corrupt
        try:
            with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
                self.assertEqual(preparation.try_prepare((task(0),)), 1)
                await asyncio.to_thread(wait_until, lambda: preparation.is_ready(task(0).key))
            with self.assertRaisesRegex(RuntimeError, '^confirmed task failure'):
                await transport.execute(task(0), 'model')
            self.assertEqual(transport.used_bytes, 0)
            self.assertEqual(preparation.snapshot()['stages']['model_inflight'], 0)
            self.assertTrue(preparation.release(task(0).key))
            self.assertTrue(preparation.release(task(0).key))
            self.assertEqual(preparation.snapshot()['held_tasks'], 0)
            self.assertEqual(calls, [])
            self.assertEqual(transport.unknown, set())
            self.assertEqual(sum(e['event'] == 'ray_block_released' for e in events), 1)
        finally:
            await transport.close()
            await actor.close()

    async def test_payload_identity_failure_releases_only_its_member_without_http(self):
        actor = _HttpActor(CONFIG, 2)
        entered, finish, calls, events = asyncio.Event(), asyncio.Event(), [], []
        async def execute(actual, endpoint):
            calls.append(actual.key)
            entered.set()
            await finish.wait()
            return b'ok'
        actor.transport.execute = execute
        bad, peer = request(), request(1)
        ray = FakeRay(actor.execute)
        transport = RayMapTransport(CONFIG, 2, events.append,
            physical=RayMapConfig('fixture-cluster', 1, 2, 1024, 2048), ray_api=ray)
        transport.blocks[0] = _Block(table(TaskKey(99, 0), peer.key), 64, {bad.key, peer.key})
        transport.used_bytes = 64
        row = _Row(bad, asyncio.get_running_loop().create_future())
        other = _Row(peer, asyncio.get_running_loop().create_future())
        operation = asyncio.create_task(transport._run_row(other, 0, 1))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            await transport._run_row(row, 0, 0)
            with self.assertRaisesRegex(RuntimeError, '^confirmed task failure') as failure:
                await row.future
            self.assertEqual(failure.exception.key, bad.key)
            self.assertEqual(transport.unknown, set())
            self.assertEqual(transport.blocks[0].members, {peer.key})
            self.assertEqual(transport.used_bytes, 64)
            self.assertEqual(calls, [peer.key])
            event = next(e for e in events if e['event'] == 'ray_execution_error')
            self.assertEqual((event['stage'], event['remote_outcome']), ('payload', 'not_sent'))
            self.assertEqual(failure.exception.reason, event['reason'])
            self.assertEqual(event['reason']['exception_type'], 'builtins.ValueError')
            self.assertFalse(other.future.done())
        finally:
            finish.set()
            await operation
            self.assertEqual(await other.future, b'ok')
            if not row.future.cancelled():
                row.future.exception()
            try:
                await transport.close()
            finally:
                await actor.close()
        self.assertEqual(transport.used_bytes, 0)
        self.assertEqual(sum(e['event'] == 'ray_block_released' for e in events), 1)

    async def test_wrong_failure_identity_or_stage_cannot_release_object(self):
        for key, stage in ((TaskKey(99, 0), 'payload'), (TaskKey(0, 0), 'unknown')):
            with self.subTest(key=key, stage=stage):
                async def execute(*_):
                    return _RemoteFailure(key, stage, {'exception_type': 'builtins.ValueError'})
                transport = RayMapTransport(CONFIG, 1, physical=RayMapConfig(
                    'fixture-cluster', 1, 1, 1024, 2048), ray_api=FakeRay(execute))
                actual = request()
                transport.blocks[0] = _Block(None, 64, {actual.key})
                transport.used_bytes = 64
                row = _Row(actual, asyncio.get_running_loop().create_future())
                await transport._run_row(row, 0, 0)
                with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                    await row.future
                self.assertEqual(transport.unknown, {actual.key})
                self.assertEqual(transport.used_bytes, 64)
                with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
                    await transport.close()

    async def test_payload_failure_allows_next_query_to_claim_the_same_worker(self):
        ray, calls = NamedRay(), []
        actor = _HttpActor(CONFIG, 4)
        async def execute(actual, endpoint):
            calls.append(actual.key)
            return b'ok'
        actor.transport.execute = execute
        async def remote(batch, index, template, owner):
            self.assertEqual(ray.owners['fixture-0'], owner)
            return await actor.execute(batch, index, template)
        ray.named['fixture-0'].execute.remote = remote
        owners = []
        try:
            for rejected in (True, False):
                transport = RayMapTransport(CONFIG, 4, physical=physical(), ray_api=ray)
                owners.append(transport.worker_owner)
                actual = request()
                transport.blocks[0] = _Block(table(TaskKey(99, 0) if rejected else actual.key),
                                              64, {actual.key})
                transport.used_bytes = 64
                row = _Row(actual, asyncio.get_running_loop().create_future())
                try:
                    await transport._run_row(row, 0, 0)
                    if rejected:
                        with self.assertRaisesRegex(RuntimeError, '^confirmed task failure'):
                            await row.future
                    else:
                        self.assertEqual(await row.future, b'ok')
                    self.assertEqual(transport.used_bytes, 0)
                finally:
                    if not row.future.cancelled():
                        row.future.exception()
                    await transport.close()
        finally:
            await actor.close()
        self.assertNotEqual(*owners)
        self.assertEqual([entry[1] for entry in ray.releases], owners)
        self.assertEqual(calls, [request().key])
        self.assertEqual(ray.owners, {})
        self.assertEqual(ray.killed, [])

    async def test_cancelling_rpc_await_cannot_confirm_remote_completion(self):
        entered, finish = asyncio.Event(), asyncio.Event()
        async def execute(*_):
            entered.set()
            await finish.wait()
        ray = NamedRay()
        ray.named['fixture-0'].execute.remote = execute
        transport = RayMapTransport(CONFIG, 4, physical=physical(), ray_api=ray)
        actual = request()
        transport.blocks[0] = _Block(None, 64, {actual.key})
        transport.used_bytes = 64
        row = _Row(actual, asyncio.get_running_loop().create_future())
        operation = asyncio.create_task(transport._run_row(row, 0, 0))
        await asyncio.wait_for(entered.wait(), 1)
        operation.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await operation
        self.assertEqual(transport.used_bytes, 64)
        self.assertEqual(transport.unknown, {actual.key})
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            await transport.close()
        self.assertEqual(ray.releases, [])
        self.assertEqual(ray.owners['fixture-0'], transport.worker_owner)

    async def test_finished_consumer_future_keeps_object_until_actual_reply(self):
        entered, finish = asyncio.Event(), asyncio.Event()
        async def execute(_table, _index, template):
            entered.set()
            await finish.wait()
            return template.key, b'ok', 1, 2
        transport = RayMapTransport(CONFIG, 1, physical=RayMapConfig(
            'fixture-cluster', 1, 1, 1024, 2048), ray_api=FakeRay(execute))
        actual = request()
        transport.blocks[0] = _Block(None, 64, {actual.key})
        transport.used_bytes = 64
        row = _Row(actual, asyncio.get_running_loop().create_future())
        operation = asyncio.create_task(transport._run_row(row, 0, 0))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            row.future.cancel()
            transport.cancel_pending(actual.key)
            await asyncio.sleep(0)
            self.assertFalse(operation.done())
            self.assertEqual(transport.used_bytes, 64)
            self.assertEqual(transport.blocks[0].members, {actual.key})
        finally:
            finish.set()
            await operation
            await transport.close()
        self.assertTrue(row.future.cancelled())
        self.assertEqual(transport.used_bytes, 0)
