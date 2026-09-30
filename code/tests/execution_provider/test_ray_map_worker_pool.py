"""Service workers preserve exclusive query ownership and uncertain outcomes."""

import asyncio
from dataclasses import replace
import importlib.util
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import (
    RayMapConfig, RayMapTransport, _Block, _HttpActor, _RemoteFailure, _Row,
    _model_identity, owned_map_worker_pool,
)
from tests.execution_provider.test_ray_map_transport import FakeRay, task


CONFIG = FixedModelConfig('http://localhost/fixture', 'model', 1000)
OWNER = 'a' * 32


class NamedRay(FakeRay):
    def __init__(self, count=1):
        super().__init__(None)
        self.named, self.owners = {}, {}
        self.claims, self.releases, self.executions = [], [], []
        self.close_calls = []
        for index in range(count):
            name = f'fixture-{index}'
            def claim(owner, identity, capacity, name=name):
                self.claims.append((name, owner, identity, capacity))
                if identity != _model_identity(CONFIG) or capacity != 4:
                    raise ValueError('model or capacity differs')
                if name in self.owners:
                    raise RuntimeError('worker already has a query owner')
                self.owners[name] = owner
                return True
            def release(owner, name=name):
                if self.owners.get(name) != owner:
                    raise RuntimeError('wrong query owner')
                self.releases.append((name, owner))
                del self.owners[name]
                return True
            async def execute(table, index, template, owner, name=name):
                if self.owners.get(name) != owner:
                    raise ValueError('wrong query owner')
                self.executions.append((name, owner, template.key))
                return template.key, b'ok', 1, 2
            self.named[name] = SimpleNamespace(
                ready=SimpleNamespace(remote=lambda: True),
                claim=SimpleNamespace(remote=claim), release=SimpleNamespace(remote=release),
                execute=SimpleNamespace(remote=execute),
                close=SimpleNamespace(remote=lambda name=name: self.close_calls.append(name) or True))

    def get_actor(self, name, *, namespace):
        if namespace != 'semloom-map':
            raise ValueError('wrong namespace')
        return self.named[name]


def physical(workers=1):
    return RayMapConfig('fixture-cluster', workers, 2, 1024, 2048, 'fixture')


class WorkerOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_model_capacity_and_query_identity_are_checked_before_claim(self):
        actor = _HttpActor(CONFIG, 4, managed=True)
        try:
            for owner, identity, capacity in ((None, _model_identity(CONFIG), 4),
                    (OWNER, _model_identity(replace(CONFIG, model_id='other')), 4),
                    (OWNER, _model_identity(CONFIG), 8)):
                with self.subTest(owner=owner, capacity=capacity):
                    with self.assertRaises(ValueError):
                        await actor.claim(owner, identity, capacity)
                    self.assertIsNone(actor.owner)
            await actor.claim(OWNER, _model_identity(CONFIG), 4)
            with self.assertRaisesRegex(RuntimeError, 'query owner'):
                await actor.claim('b' * 32, _model_identity(CONFIG), 4)
            with self.assertRaises(RuntimeError):
                await actor.release('b' * 32)
            with self.assertRaises(RuntimeError):
                await actor.close()
            await actor.release(OWNER)
        finally:
            await actor.close()

    async def test_wrong_owner_spends_no_http_and_active_owner_cannot_be_released(self):
        actor = _HttpActor(CONFIG, 4, managed=True)
        request = task(0)
        table = {name: [SimpleNamespace(as_py=lambda value=value: value)]
                 for name, value in (('session_id', 0), ('sequence', 0), ('payload', b'input'))}
        entered, finish = asyncio.Event(), asyncio.Event()
        async def execute(actual, endpoint):
            self.assertEqual(actual.task.payload, b'input')
            self.assertEqual(endpoint, 'model')
            entered.set()
            await finish.wait()
            return b'ok'
        actor.transport.execute = Mock(side_effect=execute)
        await actor.claim(OWNER, _model_identity(CONFIG), 4)
        outcome = await actor.execute(table, 0, request, 'b' * 32)
        self.assertIsInstance(outcome, _RemoteFailure)
        actor.transport.execute.assert_not_called()
        self.assertEqual(actor.active, 0)
        operation = asyncio.create_task(actor.execute(table, 0, request, OWNER))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            with self.assertRaises(RuntimeError):
                await actor.release(OWNER)
        finally:
            finish.set()
            self.assertEqual((await operation)[1], b'ok')
            await actor.release(OWNER)
            await actor.close()


class BorrowedWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_sequential_queries_reuse_worker_without_closing_or_killing_it(self):
        ray, events, owners = NamedRay(), [], []
        for _ in range(2):
            transport = RayMapTransport(CONFIG, 4, events.append, physical=physical(), ray_api=ray)
            owners.append(transport.worker_owner)
            request = task(0)
            row = _Row(request, asyncio.get_running_loop().create_future())
            transport.blocks[0] = _Block(None, 64, {request.key})
            transport.used_bytes = 64
            await transport._run_row(row, 0, 0)
            self.assertEqual(await row.future, b'ok')
            await transport.close()
        self.assertNotEqual(*owners)
        self.assertEqual([e[1] for e in ray.executions], owners)
        self.assertEqual([e[1] for e in ray.releases], owners)
        self.assertEqual(ray.owners, {})
        self.assertEqual(ray.killed, [])
        self.assertEqual(ray.close_calls, [])
        self.assertEqual(sum(e.get('stage') == 'actor_bind' for e in events), 2)

    async def test_unknown_execution_keeps_lease_and_payload_charge_for_service_owner(self):
        ray = NamedRay()
        async def fail(*args):
            raise OSError('fixture remote outcome unknown')
        ray.named['fixture-0'].execute.remote = fail
        transport = RayMapTransport(CONFIG, 4, physical=physical(), ray_api=ray)
        request = task(0)
        row = _Row(request, asyncio.get_running_loop().create_future())
        transport.blocks[0] = _Block(None, 64, {request.key})
        transport.used_bytes = 64
        await transport._run_row(row, 0, 0)
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            await row.future
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            await transport.close()
        self.assertEqual(transport.used_bytes, 64)
        self.assertEqual(ray.owners['fixture-0'], transport.worker_owner)
        self.assertEqual(ray.releases, [])
        self.assertEqual(ray.killed, [])
        with self.assertRaisesRegex(RuntimeError, 'query owner'):
            RayMapTransport(CONFIG, 4, physical=physical(), ray_api=ray)


@unittest.skipUnless(importlib.util.find_spec('daft') and importlib.util.find_spec('pyarrow'),
                     'Daft and Arrow are required for the actual batch adapter')
class BorrowedWorkerCancelTests(unittest.IsolatedAsyncioTestCase):
    async def test_unsent_cancel_and_budget_failure_release_lease_without_http(self):
        ray, guarded = NamedRay(), []
        def guard(request):
            guarded.append(request.key.sequence)
            raise ValueError('fixture exhausted budget')
        transport = RayMapTransport(CONFIG, 4, physical=physical(), before_request=guard, ray_api=ray)
        cancelled = asyncio.create_task(transport.execute(task(0), 'model'))
        rejected = asyncio.create_task(transport.execute(task(1), 'model'))
        await asyncio.sleep(0)
        transport.cancel_pending(task(0).key)
        self.assertEqual(await cancelled, b'')
        self.assertIn(b'MODEL_UNAVAILABLE', await rejected)
        await transport.close()
        self.assertEqual(guarded, [1])
        self.assertEqual(ray.executions, [])
        self.assertEqual(ray.owners, {})
        self.assertEqual(transport.used_bytes, 0)
        self.assertEqual(ray.killed, [])

    async def test_sent_cancel_holds_lease_until_confirmed_result(self):
        ray, entered, finish = NamedRay(), asyncio.Event(), asyncio.Event()
        async def execute(table, index, template, owner):
            self.assertEqual(ray.owners['fixture-0'], owner)
            entered.set()
            await finish.wait()
            return template.key, b'ok', 1, 2
        ray.named['fixture-0'].execute.remote = execute
        transport = RayMapTransport(CONFIG, 4, physical=physical(), ray_api=ray)
        result = asyncio.create_task(transport.execute(task(0), 'model'))
        try:
            await asyncio.wait_for(entered.wait(), 10)
            transport.cancel_pending(task(0).key)
            self.assertFalse(result.done())
            self.assertGreater(transport.used_bytes, 0)
            self.assertEqual(ray.releases, [])
        finally:
            finish.set()
            self.assertEqual(await result, b'ok')
            await transport.close()
        self.assertEqual(ray.owners, {})
        self.assertEqual(ray.killed, [])


class PoolStartupTests(unittest.TestCase):
    def test_partial_borrow_releases_confirmed_claims_and_disconnects_owned_driver(self):
        ray = NamedRay()
        ray.is_initialized = lambda: False
        ray.init = Mock()
        ray.shutdown = Mock()
        with self.assertRaises(KeyError):
            RayMapTransport(CONFIG, 4, physical=physical(2), ray_api=ray)
        self.assertEqual(len(ray.releases), 1)
        self.assertEqual(ray.owners, {})
        self.assertEqual(ray.killed, [])
        ray.shutdown.assert_called_once_with()

    def test_wrong_model_or_capacity_cannot_reconfigure_existing_workers(self):
        for config, capacity in ((replace(CONFIG, model_id='other'), 4), (CONFIG, 8)):
            with self.subTest(capacity=capacity):
                ray = NamedRay()
                with self.assertRaises(ValueError):
                    RayMapTransport(config, capacity, physical=physical(), ray_api=ray)
                self.assertEqual(ray.owners, {})
                self.assertEqual(ray.releases, [])
                self.assertEqual(ray.killed, [])

    def test_service_partial_creation_only_reclaims_its_own_actor(self):
        ray = NamedRay(2)
        own = ray.named['fixture-0']
        create = Mock(side_effect=[own, ValueError('worker name already exists')])
        builder = SimpleNamespace(options=lambda **kwargs: SimpleNamespace(remote=create))
        ray.remote = lambda **kwargs: lambda cls: builder
        with self.assertRaisesRegex(ValueError, 'already exists'):
            with owned_map_worker_pool(ray, CONFIG, 4, physical(2)):
                self.fail('service cannot start with a conflicting worker')
        self.assertEqual(ray.close_calls, ['fixture-0'])
        self.assertEqual(ray.killed, [own])

    def test_service_cleanup_attempts_all_workers_even_when_one_kill_fails(self):
        ray = NamedRay(2)
        handles = list(ray.named.values())
        create = Mock(side_effect=handles)
        builder = SimpleNamespace(options=lambda **kwargs: SimpleNamespace(remote=create))
        ray.remote = lambda **kwargs: lambda cls: builder
        ray.kill = Mock(side_effect=[OSError('fixture cleanup failure'), None])
        with self.assertRaisesRegex(RuntimeError, 'cleanup could not be confirmed'):
            with owned_map_worker_pool(ray, CONFIG, 4, physical(2)):
                pass
        self.assertEqual(ray.kill.call_count, 2)
        self.assertEqual(ray.close_calls, ['fixture-0', 'fixture-1'])

    def test_service_requires_declared_cluster_and_valid_worker_pool_name(self):
        for name in ('', 'path/name', 'a' * 49, 12):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    replace(physical(), worker_pool=name)
        ray = NamedRay()
        ray.remote = Mock()
        with self.assertRaises(ValueError):
            with owned_map_worker_pool(ray, CONFIG, 4, replace(physical(), address='other-cluster')):
                self.fail('must not allocate on another cluster')
        ray.remote.assert_not_called()
