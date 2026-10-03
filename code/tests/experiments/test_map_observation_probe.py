"""No-model diagnosis preserves accounting, event identity and thread ownership."""
import asyncio
import gzip
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.experiments.map_observation_probe import (
    SyntheticRay, SyntheticTable, payload, response, run_case,
)
from src.scheduling.core.session import SchedulingSession
from tests.execution_provider.test_ray_map_transport import task


class ObservationProbeTests(unittest.TestCase):
    def test_unknown_mapped_registration_closes_handles_during_failed_core_cleanup(self):
        from src.experiments.attempt_ledger import BudgetError
        from src.experiments.mapped_request_budget import _DIRTY
        def dirty_guard(task, client, *args):
            with client._locked():
                client._write_header(client._header()[4], _DIRTY)
            raise BudgetError('fixture registration outcome unknown')
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'mapped-unknown'
            with patch('src.experiments.map_observation_probe._guard_remote_request', dirty_guard):
                with self.assertRaisesRegex(RuntimeError, 'evidence retained'):
                    run_case(output, 'mapped', 'buffered', rows=8, capacity=4, batch_rows=2)
            result = json.loads((output / 'result.json').read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertTrue(result['mapped_handles_closed'])
            self.assertTrue(result['transport_drained'] and result['synthetic_worker_stopped'])
            self.assertIsNone(result['accounted_calls'])

    def test_guard_without_confirmed_registration_keeps_uncertain_transport_records(self):
        from src.experiments.mapped_request_budget import _DIRTY
        def invalid_guard(task, client, *args):
            with client._locked():
                client._write_header(client._header()[4], _DIRTY)
            # Deliberately broken guard: returns before a confirmed registration.
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'invalid-guard'
            with patch('src.experiments.map_observation_probe._guard_remote_request', invalid_guard):
                with self.assertRaisesRegex(RuntimeError, 'evidence retained'):
                    run_case(output, 'mapped', 'buffered', rows=8, capacity=4, batch_rows=2)
            result = json.loads((output / 'result.json').read_text())
            self.assertTrue(result['mapped_handles_closed'] and result['synthetic_worker_stopped'])
            self.assertFalse(result['transport_drained'])
            self.assertEqual(result['synthetic_calls'], 0)

    def test_mapped_fixture_counter_preserves_row_and_resource_lifecycle(self):
        with tempfile.TemporaryDirectory() as root:
            result = run_case(Path(root) / 'mapped', 'mapped', 'buffered', rows=8, capacity=4, batch_rows=2)
            self.assertEqual((result['synthetic_calls'], result['accounted_calls']), (8, 8))
            self.assertTrue(result['mapped_handles_closed'] and result['transport_drained'])
            self.assertTrue(result['resources_drained'] and result['exactly_once'])
            self.assertFalse(result['cleanup_errors'])

    def test_both_guards_and_recorders_complete_each_row_and_dispose_resources(self):
        with tempfile.TemporaryDirectory() as root:
            for accounting in ('durable', 'memory'):
                for recording in ('memory', 'buffered'):
                    with self.subTest(accounting=accounting, recording=recording):
                        output = Path(root) / f'{accounting}-{recording}'
                        release = SchedulingSession.release
                        def release_deliveries(session, leases):
                            self.assertTrue(leases, 'empty releases must not wake the fixture driver')
                            return release(session, leases)
                        with patch.object(SchedulingSession, 'release', release_deliveries):
                            result = run_case(output, accounting, recording, rows=8, capacity=4, batch_rows=2)
                        self.assertEqual(result['status'], 'passed')
                        self.assertEqual((result['synthetic_calls'], result['accounted_calls']), (8, 8))
                        self.assertEqual((result['http_requests'], result['model_requests'], result['pg_queries']), (0, 0, 0))
                        self.assertTrue(result['exactly_once'] and result['transport_drained'])
                        self.assertTrue(result['synthetic_worker_stopped'] and result['actor_disposed'])
                        self.assertLessEqual(result['core_active_peak'], 4)
                        self.assertLessEqual(result['synthetic_worker_peak'], 4)
                        self.assertEqual(result['submitted_to_rpc']['count'], 8)
                        self.assertFalse(result['cleanup_errors'])
                        with gzip.open(output / 'events.jsonl.gz', 'rt') as stream:
                            events = [json.loads(line) for line in stream]
                        self.assertEqual(sum(e['event'] == 'submitted' for e in events), 8)
                        if recording == 'buffered':
                            self.assertEqual(result['writer']['accepted_events'], result['writer']['written_events'])
                            self.assertTrue(result['writer']['closed'])
                        self.assertEqual(result['durable_per_call_history'], accounting == 'durable')

    def test_guard_failure_sends_no_call_and_preserves_failed_history(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'failed'
            with patch('src.experiments.map_observation_probe._guard_remote_request',
                       side_effect=RuntimeError('fixture accounting failure')):
                with self.assertRaisesRegex(RuntimeError, 'evidence retained'):
                    run_case(output, 'durable', 'buffered', rows=8, capacity=4, batch_rows=2)
            result = json.loads((output / 'result.json').read_text())
            self.assertEqual(result['status'], 'failed')
            self.assertEqual(result['synthetic_calls'], 0)
            self.assertTrue(result['synthetic_worker_stopped'] and result['actor_disposed'])
            self.assertTrue(result['transport_drained'])
            self.assertFalse(result['cleanup_errors'])
            self.assertTrue((output / 'events.jsonl.gz').is_file())

    def test_existing_directory_is_refused_without_overwriting(self):
        with tempfile.TemporaryDirectory() as root:
            sentinel = Path(root) / 'original'
            sentinel.write_text('kept')
            with self.assertRaises(FileExistsError):
                run_case(root, 'memory', 'memory', rows=8, capacity=4, batch_rows=2)
            self.assertEqual(sentinel.read_text(), 'kept')

    def test_failure_accounting_includes_calls_that_settle_during_cleanup(self):
        sent = threading.Event()
        submit = SyntheticRay._submit
        def observed_submit(ray, *arguments):
            call = submit(ray, *arguments)
            sent.set()
            return call
        def failed_consumer(*_):
            self.assertTrue(sent.wait(2), 'fixture call never reached its independent worker')
            raise RuntimeError('fixture consumer stops after dispatch')
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'failed-after-dispatch'
            with patch.object(SyntheticRay, '_submit', observed_submit), \
                 patch.object(SchedulingSession, 'advance', failed_consumer):
                with self.assertRaisesRegex(RuntimeError, 'evidence retained'):
                    run_case(output, 'memory', 'memory', rows=8, capacity=4, batch_rows=2)
            result = json.loads((output / 'result.json').read_text())
            with gzip.open(output / 'events.jsonl.gz', 'rt') as stream:
                events = [json.loads(line) for line in stream]
            self.assertGreaterEqual(result['synthetic_calls'], 1)
            self.assertEqual(result['synthetic_calls'], sum(e['event'] == 'ray_http_completed' for e in events))
            self.assertEqual(result['accounted_calls'], sum(e['event'] == 'fixture_request_observed' for e in events))
            self.assertLessEqual(result['calls_when_failure_observed'], result['synthetic_calls'])
            self.assertTrue(result['transport_drained'] and result['synthetic_worker_stopped'])
            self.assertFalse(result['cleanup_errors'])


class WorkerThreadTests(unittest.IsolatedAsyncioTestCase):
    async def test_driver_blocking_does_not_extend_worker_service_duration(self):
        ray = SyntheticRay({0: 1})
        try:
            call = ray.actor.execute.remote(SyntheticTable([(0, 0, payload(0))]), 0, task(0))
            # Block this loop after the worker starts; its own loop still completes.
            until = time.monotonic() + 2
            while ray.active == 0 and time.monotonic() < until:
                await asyncio.sleep(.001)
            self.assertEqual(ray.active, 1)
            time.sleep(.05)
            self.assertEqual(ray.active, 0)
            self.assertEqual(ray.completed, [0])
            reply = await call
            self.assertEqual(reply.result, response(0))
        finally:
            ray.close()

    async def test_unaccounted_call_is_refused_before_worker(self):
        ray = SyntheticRay({})
        try:
            with self.assertRaisesRegex(AssertionError, 'preceded its accounting'):
                ray.actor.execute.remote(SyntheticTable([(0, 0, payload(0))]), 0, task(0))
            self.assertFalse(ray.issued or ray.completed or ray.active)
        finally:
            ray.close()


if __name__ == '__main__':
    unittest.main()
