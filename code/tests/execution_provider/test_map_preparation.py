"""Single-Job staged Map tests with explicit table and actor substitutes."""

import asyncio
from dataclasses import replace
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport
from src.experiments.map_observation_probe import SyntheticTable, WORK
from src.scheduling.core.session_contract import SessionLimits, SessionSpec, TaskInfo
from src.scheduling.runtime.stage_broker import StageBrokerLimits
from tests.execution_provider.test_ray_map_transport import FakeRay, task


def batches(rows, limits, *, batch_rows, backend):
    yield SyntheticTable(rows)


def wait_until(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError('fixture preparation did not settle')
        time.sleep(.001)


def make_transport(execute, *, max_tasks=8, ready_bytes=1024, encoded_bytes=1024,
                   item_bytes=64, guard=None, events=None):
    ray = FakeRay(execute)
    physical = RayMapConfig('fixture-cluster', 1, 2, 128, 1024,
        payload_backend='arrow', preparation=StageBrokerLimits(encoded_bytes, ready_bytes, 8, 1, max_tasks))
    transport = RayMapTransport(FixedModelConfig('http://localhost/fixture', 'model', 1000),
        2, events.append if events is not None else None, physical=physical,
        before_request=guard, ray_api=ray)
    limits = SessionLimits(max_tasks, 1024, 1024, 2, 2, max_tasks, item_bytes, 100, 64, 8, 2, .01)
    preparation = transport.prepare_inputs(limits, lambda: None)
    return transport, preparation, ray


class MapPreparationTests(unittest.IsolatedAsyncioTestCase):
    async def test_shared_prepared_object_keeps_slow_row_after_fast_result_and_ack(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def execute(table, index, template):
            if template.key.sequence == 0:
                started.set()
                await release.wait()
            return template.key, table['payload'][index].as_py(), 1, 2
        transport, preparation, ray = make_transport(execute)
        with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
            self.assertEqual(preparation.try_prepare((task(0, b'slow'), task(1, b'fast'))), 2)
            await asyncio.to_thread(wait_until, lambda: preparation.is_ready(task(0).key))
        slow = asyncio.create_task(transport.execute(task(0, b'slow'), 'model'))
        fast = asyncio.create_task(transport.execute(task(1, b'fast'), 'model'))
        try:
            self.assertEqual(await asyncio.wait_for(asyncio.shield(fast), 2), b'fast')
            self.assertTrue(started.is_set())
            self.assertTrue(preparation.release(task(1).key))
            self.assertGreater(transport.used_bytes, 0)
            self.assertEqual(ray.puts, [2])
        finally:
            release.set()
            await asyncio.gather(slow, fast)
            self.assertTrue(preparation.release(task(0).key))
            await transport.close()
        self.assertEqual(preparation.snapshot()['stages']['ready_held_bytes'], 0)

    async def test_cancel_during_materialization_waits_for_worker_without_rpc(self):
        entered, release = threading.Event(), threading.Event()
        def blocked(rows, limits, **kwargs):
            entered.set()
            if not release.wait(3):
                raise AssertionError('fixture worker not released')
            yield SyntheticTable(rows)
        transport, preparation, ray = make_transport(None)
        try:
            with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', blocked):
                preparation.try_prepare((task(0),))
                self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                self.assertFalse(preparation.release(task(0).key))
                self.assertEqual(preparation.snapshot()['stages']['prepare_inflight'], 1)
                release.set()
                await asyncio.to_thread(wait_until, lambda: preparation.is_ready(task(0).key))
                self.assertTrue(preparation.release(task(0).key))
            self.assertFalse(ray.puts)
        finally:
            release.set()
            await transport.close()

    async def test_ready_cancel_and_guard_failure_do_not_send_model_work(self):
        calls = []
        async def execute(*args):
            calls.append(args)
            raise AssertionError('cancelled or denied row must not send')
        def deny(_):
            raise RuntimeError('fixture budget denied')
        transport, preparation, _ = make_transport(execute, guard=deny)
        with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
            preparation.try_prepare((task(0), task(1)))
            await asyncio.to_thread(wait_until, lambda: preparation.is_ready(task(0).key))
        self.assertTrue(preparation.release(task(0).key))
        self.assertIn(b'MODEL_UNAVAILABLE', await transport.execute(task(1), 'model'))
        self.assertTrue(preparation.release(task(1).key))
        self.assertFalse(calls)
        await transport.close()

    async def test_cancel_while_async_guard_waits_retains_reservation_until_settled(self):
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []
        async def guard(_):
            entered.set()
            await release.wait()
        async def execute(*args):
            calls.append(args)
        transport, preparation, _ = make_transport(execute, guard=guard)
        with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
            preparation.try_prepare((task(0),))
            await asyncio.to_thread(wait_until, lambda: preparation.is_ready(task(0).key))
        operation = asyncio.create_task(transport.execute(task(0), 'model'))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            transport.cancel_pending(task(0).key)
            self.assertFalse(preparation.release(task(0).key))
            release.set()
            self.assertEqual(await operation, b'')
            self.assertTrue(preparation.release(task(0).key))
            self.assertFalse(calls)
        finally:
            release.set()
            await transport.close()

    async def test_unknown_remote_outcome_keeps_broker_work_bytes_and_input_owner(self):
        async def execute(*args):
            raise RuntimeError('fixture reply lost')
        transport, preparation, _ = make_transport(execute)
        with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
            preparation.try_prepare((task(0),))
            await asyncio.to_thread(wait_until, lambda: preparation.is_ready(task(0).key))
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            await transport.execute(task(0), 'model')
        self.assertFalse(preparation.release(task(0).key))
        snapshot = preparation.snapshot()
        self.assertEqual(snapshot['stages']['model_inflight'], 1)
        self.assertEqual(snapshot['stages']['ready_held_work'], 1)
        self.assertGreater(snapshot['object_bytes'], 0)
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            await transport.close()

    async def test_overestimated_backing_buffer_fails_locally_without_put_or_model(self):
        class Oversized(SyntheticTable):
            def get_total_buffer_size(self):
                return super().get_total_buffer_size() + 1
        def oversized(rows, limits, **kwargs):
            yield Oversized(rows)
        transport, preparation, ray = make_transport(None)
        with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', oversized):
            preparation.try_prepare((task(0),))
            await asyncio.to_thread(wait_until, lambda: preparation.is_ready(task(0).key))
        self.assertIn(b'MODEL_UNAVAILABLE', await transport.execute(task(0), 'model'))
        self.assertTrue(preparation.release(task(0).key))
        self.assertFalse(ray.puts)
        self.assertFalse(transport.unknown)
        await transport.close()

    async def test_altered_payload_cannot_use_prepared_identity(self):
        transport, preparation, _ = make_transport(None)
        with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
            preparation.try_prepare((task(0),))
            await asyncio.to_thread(wait_until, lambda: preparation.is_ready(task(0).key))
        altered = replace(task(0), task=replace(task(0).task, payload=b'changed'))
        with self.assertRaisesRegex(ValueError, 'identity differs'):
            await transport.execute(altered, 'model')
        self.assertTrue(preparation.release(task(0).key))
        await transport.close()

    async def test_ready_byte_pressure_stops_preparation_and_cancelled_queue_can_drain(self):
        transport, preparation, _ = make_transport(None, ready_bytes=128, item_bytes=16)
        with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
            for i in range(4):
                await asyncio.to_thread(wait_until,
                    lambda: preparation.try_prepare((task(i, b'1234567890'),)) == 1)
            await asyncio.to_thread(wait_until,
                lambda: preparation.snapshot()['stages']['ready_queued'] == 2)
            snapshot = preparation.snapshot()
            self.assertEqual(snapshot['stages']['encoded_queued'], 2)
            self.assertLessEqual(snapshot['stages']['ready_held_bytes'], 128)
            for i in range(4):
                await asyncio.to_thread(wait_until, lambda: preparation.release(task(i).key))
            await asyncio.to_thread(wait_until, lambda: not preparation.running)
        await transport.close()

    async def test_borrowing_checks_row_byte_work_and_configuration_limits(self):
        transport, preparation, _ = make_transport(None, max_tasks=2, encoded_bytes=88)
        with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
            self.assertEqual(preparation.try_prepare((task(0), task(1), task(2))), 2)
            self.assertEqual(preparation.try_prepare((task(2),)), 0)
            await asyncio.to_thread(wait_until, lambda: preparation.is_ready(task(0).key))
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                preparation.try_prepare((task(0),))
            self.assertTrue(preparation.release(task(0).key))
            self.assertTrue(preparation.release(task(1).key))
        await transport.close()


class MapPreparationCompositionTests(unittest.TestCase):
    def test_real_core_and_async_backend_keep_requests_work_results_and_references_bounded(self):
        calls, active, peak = [], [0], [0]
        async def execute(table, index, template):
            active[0] += 1
            peak[0] = max(peak[0], active[0])
            try:
                payload = table['payload'][index].as_py()
                calls.append((template.key.sequence, payload))
                await asyncio.sleep(.002)
                return template.key, payload, 1, 2
            finally:
                active[0] -= 1
        ray = FakeRay(execute)
        physical = RayMapConfig('fixture-cluster', 1, 2, 2**21, 2**22,
            payload_backend='arrow', preparation=StageBrokerLimits(2**21, 2**22, 8, 1, 8))
        transports = []
        def transport_factory(*args):
            result = RayMapTransport(*args, physical=physical, ray_api=ray)
            transports.append(result)
            return result
        with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
            execution = build_fixed_model_execution(
                FixedModelConfig('http://localhost/fixture', 'model', 1000), max_tasks=8,
                max_active_requests=2, transport_factory=transport_factory,
                preparation_factory=lambda transport, limits, notify: transport.prepare_inputs(limits, notify))
            engine = execution.engine
            job, session, _ = execution.open_job('fixture', SessionSpec('fixture', 'flow', 'fixture'))
            offered = [replace(task(i).task, payload=f'body-{i}'.encode(),
                               info=TaskInfo('fixture', i, 'model', WORK)) for i in range(8)]
            session.offer(offered)
            session.seal()
            results, preparing_ahead = {}, False
            deadline = time.monotonic() + 3
            try:
                while len(results) < 8 and time.monotonic() < deadline:
                    engine.advance()
                    usage = engine.capacity.usage()
                    self.assertLessEqual(usage.active_requests, 2)
                    self.assertLessEqual(usage.active_work, 2)
                    stage = transports[0].preparation.snapshot()
                    self.assertLessEqual(stage['stages']['ready_held_bytes'], 2**22)
                    preparing_ahead |= stage['held_tasks'] > usage.active_requests
                    result = session.advance(8)
                    self.assertIsNone(result.error)
                    for delivery in result.deliveries:
                        self.assertNotIn(delivery.key.sequence, results)
                        results[delivery.key.sequence] = delivery.result
                    if result.deliveries:
                        session.release([d.lease_id for d in result.deliveries])
                    else:
                        engine.wake.wait(engine.wake.generation, .005)
                self.assertEqual(results, {i: offered[i].payload for i in range(8)})
                self.assertEqual(sorted(calls), [(i, offered[i].payload) for i in range(8)])
                self.assertTrue(preparing_ahead)
                self.assertLessEqual(peak[0], 2)
                self.assertEqual(engine.capacity.usage().held_tasks, 0)
            finally:
                session.close()
                engine.close_job(job)
                for _ in range(20):
                    engine.advance()
                    if execution.close():
                        break
                    time.sleep(.005)
                else:
                    self.fail('fixture staged execution did not close')

    def test_multi_job_composition_is_rejected_before_transport_start(self):
        with self.assertRaisesRegex(ValueError, 'one Job'):
            build_fixed_model_execution(FixedModelConfig('http://localhost/fixture', 'model', 1000),
                max_jobs=2, preparation_factory=lambda *_: None)
