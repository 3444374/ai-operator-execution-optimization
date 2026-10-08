"""Return callback and fixture safety without Ray, vendors or a network connection."""
import asyncio
from concurrent.futures import Future
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.experiments.map_observation_probe import SyntheticTable
from src.scheduling.core.session_contract import TaskKey


path = Path(__file__).resolve().parents[3]/'experiments/results/diagnostics/map_queued_preparation_20261008/raw/run_ray_return_wait.py'
spec = importlib.util.spec_from_file_location('ray_return_probe', path)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class RayReturnWaitTests(unittest.TestCase):
    def test_full_fixture_input_identity_and_actor_activity(self):
        async def run():
            actor = probe.SleepActor(FixedModelConfig('http://localhost/unused', 'fixture', 1000), 1)
            template = SimpleNamespace(key=TaskKey('fixture', 0), task=SimpleNamespace(payload=b''))
            table = SyntheticTable((('fixture', 0, probe.payload(0)),))
            result = await actor.execute(table, 0, template)
            self.assertEqual(result.result, probe.response(0))
            self.assertEqual(result.key, template.key)
            state = await actor.snapshot()
            self.assertEqual((state['active'], state['peak'], len(state['calls'])), (0, 1, 1))
            with self.assertRaisesRegex(AssertionError, 'exactly-once'):
                await actor.execute(table, 0, template)
            self.assertTrue(await actor.close())
        with patch('socket.socket.connect', side_effect=AssertionError('unit network forbidden')):
            asyncio.run(run())

    def test_callback_precedes_receive_and_accounting_precedes_rpc(self):
        async def run():
            future, issued, ready, called = Future(), [], [], []
            actor = SimpleNamespace(execute=SimpleNamespace(remote=lambda *args:
                called.append(args) or SimpleNamespace(future=lambda:future)))
            charged = {}
            proxy = probe.ExecuteProxy(actor, charged, issued, ready)
            template = SimpleNamespace(key=TaskKey('fixture', 0))
            with self.assertRaisesRegex(AssertionError, 'accounting'):
                proxy.remote(None, 0, template)
            self.assertFalse(called)
            charged[0] = 1
            waiting = proxy.remote(None, 0, template)
            future.set_result(b'complete')
            self.assertEqual(await waiting, b'complete')
            self.assertEqual([s for s, _ in ready], [0])
            self.assertLessEqual(ready[0][1], time.monotonic_ns())
            with self.assertRaisesRegex(AssertionError, 'duplicated'):
                proxy.remote(None, 0, template)
            self.assertEqual(len(called), 1)
        asyncio.run(run())

    def test_segments_add_up_and_wrong_row_or_clock_is_rejected(self):
        receipt = dict(event='ray_http_completed', key=dict(sequence=0), shared_clock=True,
            worker_ended_ns=100, received_ns=300, after_worker_ns=200)
        guard = dict(event='remote_request_guard', key=dict(sequence=0), elapsed_ns=50)
        events = [receipt, guard]
        worker = dict(active=0, peak=1)
        result = probe.analyze(events, [(0, 200)], worker, 1)
        self.assertEqual(result['worker_end_to_callback']['sum_seconds'] +
                         result['callback_to_receive']['sum_seconds'], result['worker_end_to_receive']['sum_seconds'])
        with self.assertRaisesRegex(AssertionError, 'population'):
            probe.analyze(events, [(1, 200)], worker, 1)
        with self.assertRaisesRegex(AssertionError, 'chronology'):
            probe.analyze(events, [(0, 400)], worker, 1)


if __name__ == '__main__':
    unittest.main()
