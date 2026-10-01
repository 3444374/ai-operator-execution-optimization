"""Timing keeps clock ownership, row identity and unknown-outcome accounting."""
import asyncio
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import (
    RayMapConfig, RayMapTransport, _Block, _RemoteResult, _Row, _clock_domain,
)
from src.experiments.buffered_events import compact_event
from tests.execution_provider.test_ray_map_transport import FakeRay, task


class RayTimingTests(unittest.IsolatedAsyncioTestCase):
    def transport(self, execute=None, observer=None):
        return RayMapTransport(FixedModelConfig('http://localhost/fixture', 'model', 1000), 4,
            observer, physical=RayMapConfig('fixture-cluster', 1, 2, 1024, 2048),
            ray_api=FakeRay(execute))

    async def test_thread_work_separates_queue_execution_and_loop_resume_even_on_failure(self):
        for failed in (False, True):
            with self.subTest(failed=failed):
                events = []
                transport = self.transport(observer=events.append)
                def work():
                    if failed:
                        raise ValueError('fixture work failure')
                    return 'payload'
                try:
                    with patch('src.execution_provider.adapters.ray_map_transport.time.monotonic_ns',
                               side_effect=[100, 130, 230, 270]):
                        if failed:
                            with self.assertRaisesRegex(ValueError, 'fixture work failure'):
                                await transport._work(work, stage='payload_next', key={'session_id': 0, 'sequence': 7})
                        else:
                            self.assertEqual(await transport._work(work, stage='payload_next',
                                key={'session_id': 0, 'sequence': 7}), 'payload')
                    event = [e for e in events if e['event'] == 'ray_work'][0]
                    self.assertEqual((event['queue_ns'], event['work_ns'], event['resume_ns']), (30, 100, 40))
                    self.assertEqual(event['elapsed_ns'], 170)
                    self.assertEqual(event['status'], 'failed' if failed else 'completed')
                    self.assertEqual(compact_event(event)['key']['sequence'], 7)
                    self.assertEqual(compact_event(event)['work_ns'], 100)
                finally:
                    await transport.close()

    async def test_without_observer_work_uses_no_timing_wrapper(self):
        transport = self.transport()
        try:
            with patch('src.execution_provider.adapters.ray_map_transport.time.monotonic_ns',
                       side_effect=AssertionError('diagnostic clock must not run')):
                self.assertEqual(await transport._work(lambda: 17, stage='object_put'), 17)
        finally:
            await transport.close()

    async def test_worker_before_and_after_require_matching_clock_and_sum_to_rpc(self):
        for domain in ('a' * 64, 'b' * 64, None):
            with self.subTest(domain=domain):
                async def execute(*args):
                    return _RemoteResult(task(0).key, b'ok', 140, 190, domain)
                events = []
                transport = self.transport(execute, events.append)
                transport.clock_domain = 'a' * 64
                transport.first_submit = False
                request = task(0)
                row = _Row(request, asyncio.get_running_loop().create_future())
                transport.blocks[0] = _Block(None, 64, {request.key})
                transport.used_bytes = 64
                try:
                    with patch('src.execution_provider.adapters.ray_map_transport.time.monotonic_ns',
                               side_effect=[100, 130, 210]):
                        await transport._run_row(row, 0, 0)
                    self.assertEqual(await row.future, b'ok')
                    event = [e for e in events if e['event'] == 'ray_http_completed'][0]
                    self.assertEqual(event['worker_elapsed_ns'], 50)
                    self.assertEqual(event['submit_elapsed_ns'], 30)
                    self.assertEqual(event['await_elapsed_ns'], 80)
                    if domain == 'a' * 64:
                        self.assertTrue(event['shared_clock'])
                        self.assertEqual((event['before_worker_ns'], event['after_worker_ns']), (40, 20))
                        self.assertEqual(event['rpc_elapsed_ns'],
                            event['before_worker_ns'] + event['worker_elapsed_ns'] + event['after_worker_ns'])
                    else:
                        self.assertFalse(event['shared_clock'])
                        self.assertIsNone(event['before_worker_ns'])
                        self.assertIsNone(event['after_worker_ns'])
                    self.assertEqual(compact_event(event)['rpc_elapsed_ns'], 110)
                    self.assertEqual(transport.used_bytes, 0)
                finally:
                    await transport.close()

    async def test_noncausal_same_clock_reply_remains_unknown_and_charged(self):
        async def execute(*args):
            return _RemoteResult(task(0).key, b'ok', 80, 190, 'a' * 64)
        events = []
        transport = self.transport(execute, events.append)
        transport.clock_domain = 'a' * 64
        transport.first_submit = False
        request = task(0)
        row = _Row(request, asyncio.get_running_loop().create_future())
        transport.blocks[0] = _Block(None, 64, {request.key})
        transport.used_bytes = 64
        with patch('src.execution_provider.adapters.ray_map_transport.time.monotonic_ns',
                   side_effect=[100, 130, 210]):
            await transport._run_row(row, 0, 0)
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            await row.future
        self.assertEqual(transport.unknown, {request.key})
        self.assertEqual(transport.used_bytes, 64)
        self.assertEqual(events[-1]['stage'], 'result_validation')
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            await transport.close()

    def test_unavailable_linux_clock_is_reported_without_guessing_identity(self):
        with patch('src.execution_provider.adapters.ray_map_transport.Path.read_bytes', side_effect=OSError):
            self.assertIsNone(_clock_domain())
