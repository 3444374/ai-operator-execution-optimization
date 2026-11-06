"""Local probes use the time that actually remains in their suite."""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.experiments import map_observation_probe, mapped_request_probe


class ProbeSuiteDeadlineTests(unittest.TestCase):
    def run_suite(self, probe, first_seconds):
        clock = SimpleNamespace(now=100.0)
        calls = []

        def run_case(output, mode, variant, *args, **kwargs):
            calls.append(kwargs.get('deadline'))
            if len(calls) == 1:
                clock.now += first_seconds
            rows = kwargs.get('rows', 1024)
            result = dict(accounting=mode, recording=variant, rows=rows, synthetic_calls=rows,
                          mode=mode, workers=variant, received_grants=rows,
                          grant_seconds=0, component_total_seconds=0, query_seconds=0,
                          accounting_setup_seconds=0, preparation_windows=0,
                          core_active_mean=0, synthetic_worker_active_mean=0)
            if probe is mapped_request_probe:
                result['repeat'] = args[0]
            return result

        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'probe'
            with patch.object(probe, 'time', SimpleNamespace(monotonic=lambda: clock.now)), \
                 patch.object(probe, 'run_case', side_effect=run_case), redirect_stdout(io.StringIO()):
                result = probe.run_probe(output)
            return result, calls

    def test_next_case_uses_remaining_suite_time_instead_of_reserving_a_whole_case(self):
        for probe, first_seconds, expected_runs in (
            (map_observation_probe, 245, 20),
            (mapped_request_probe, 275, 16),
        ):
            with self.subTest(probe=probe.__name__):
                result, deadlines = self.run_suite(probe, first_seconds)
                self.assertEqual(result['status'], 'passed')
                self.assertEqual(len(result['runs']), expected_runs)
                self.assertEqual(deadlines, [400.0] * expected_runs)

    def test_exhausted_total_stops_before_another_case_and_preserves_the_summary(self):
        for probe in (map_observation_probe, mapped_request_probe):
            with self.subTest(probe=probe.__name__), tempfile.TemporaryDirectory() as root:
                clock = SimpleNamespace(now=100.0)
                calls = []

                def first_case(*args, **kwargs):
                    calls.append(kwargs.get('deadline'))
                    clock.now = 400.0
                    result = dict(accounting='durable', recording='memory', rows=16,
                                  synthetic_calls=16, mode='durable', workers=1,
                                  received_grants=16, grant_seconds=0, component_total_seconds=0,
                                  query_seconds=0, accounting_setup_seconds=0, preparation_windows=0,
                                  core_active_mean=0, synthetic_worker_active_mean=0)
                    if probe is mapped_request_probe:
                        result['repeat'] = 0
                    return result

                output = Path(root) / 'expired'
                with patch.object(probe, 'time', SimpleNamespace(monotonic=lambda: clock.now)), \
                     patch.object(probe, 'run_case', side_effect=first_case), redirect_stdout(io.StringIO()):
                    with self.assertRaises(TimeoutError):
                        probe.run_probe(output)
                self.assertEqual(len(calls), 1)
                summary = json.loads((output / 'summary.json').read_text())
                self.assertEqual((summary['status'], summary['error_type']), ('failed', 'TimeoutError'))
                self.assertEqual(len(summary['runs']), 1)


class ProbeCaseDeadlineTests(unittest.TestCase):
    def test_observation_expiry_after_dispatch_stops_intake_and_preserves_settled_calls(self):
        dispatched = threading.Event()
        submit = map_observation_probe.SyntheticRay._submit

        def observed_submit(ray, *args):
            future = submit(ray, *args)
            dispatched.set()
            return future

        clock = SimpleNamespace(monotonic=lambda: time.monotonic() + (10 if dispatched.is_set() else 0),
                                monotonic_ns=time.monotonic_ns, time=time.time, sleep=time.sleep)
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'observation'
            until = time.monotonic() + 1
            wall_until = time.time() + 1
            with patch.object(map_observation_probe, 'time', clock), \
                 patch.object(map_observation_probe.SyntheticRay, '_submit', observed_submit):
                with self.assertRaisesRegex(RuntimeError, 'evidence retained'):
                    map_observation_probe.run_case(output, 'memory', 'buffered', rows=8,
                                                   capacity=4, batch_rows=2, deadline=until)
            result = json.loads((output / 'result.json').read_text())
            self.assertEqual(result['error_type'], 'TimeoutError')
            self.assertGreater(result['synthetic_calls'], 0)
            self.assertEqual(result['accounted_calls'], result['synthetic_calls'])
            self.assertTrue(result['transport_drained'] and result['synthetic_worker_stopped'])
            self.assertTrue(result['writer']['closed'])
            self.assertFalse(result['cleanup_errors'])
            with sqlite3.connect(output / 'ledger.sqlite') as connection:
                allocated, saved_until = connection.execute('SELECT allocated,deadline FROM budget').fetchone()
            self.assertEqual(allocated, 8)
            self.assertAlmostEqual(saved_until, wall_until, delta=.1)

    def test_expired_observation_case_records_failure_and_closes_writer_before_worker_start(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'expired'
            with patch.object(map_observation_probe, 'SyntheticRay') as worker:
                with self.assertRaisesRegex(RuntimeError, 'evidence retained'):
                    map_observation_probe.run_case(output, 'memory', 'buffered', rows=8,
                                                   capacity=4, batch_rows=2, deadline=time.monotonic() - 1)
            worker.assert_not_called()
            result = json.loads((output / 'result.json').read_text())
            self.assertEqual((result['status'], result['error_type']), ('failed', 'TimeoutError'))
            self.assertEqual((result['synthetic_calls'], result['accounted_calls']), (0, 0))
            self.assertTrue(result['writer']['closed'] and result['synthetic_worker_stopped'])
            self.assertFalse(result['cleanup_errors'])

    def test_component_workers_wait_only_until_the_shared_deadline(self):
        start = Mock()
        start.wait.return_value = False
        connection = Mock()
        with patch.object(mapped_request_probe, 'time', SimpleNamespace(monotonic=lambda: 99.75)):
            mapped_request_probe._worker(object(), 'durable', (), start, connection, 100.0)
        start.wait.assert_called_once_with(.25)
        self.assertEqual(connection.send.call_args_list[-1].args[0]['error_type'], 'TimeoutError')
        connection.close.assert_called_once_with()

    def test_late_start_signal_does_not_authorize_another_component_grant(self):
        resource, start, connection = Mock(), Mock(), Mock()
        start.wait.return_value = True
        with patch.object(mapped_request_probe, 'time', SimpleNamespace(monotonic=lambda: 100.0)):
            mapped_request_probe._worker(resource, 'durable', ((0, 'digest'),), start, connection, 100.0)
        resource.reserve.assert_not_called()
        self.assertEqual(connection.send.call_args_list[-1].args[0]['grants'], [])
        self.assertEqual(connection.send.call_args_list[-1].args[0]['error_type'], 'TimeoutError')
        connection.close.assert_called_once_with()

    def test_component_shared_deadline_preserves_process_and_accounting_lifecycle(self):
        with tempfile.TemporaryDirectory() as root:
            for mode in ('durable', 'mapped'):
                with self.subTest(mode=mode), patch.object(mapped_request_probe, 'ROWS', 8):
                    output = Path(root) / mode
                    result = mapped_request_probe.run_case(output, mode, 2, 0,
                                                           deadline=time.monotonic() + 10)
                    self.assertEqual((result['status'], result['received_grants']), ('passed', 8))
                    self.assertTrue(result['workers_stopped'] and result['unique_prefix'])
                    self.assertEqual(result['worker_exits'], [0, 0])
                    self.assertIsNone(result['close_error'])

    def test_component_expiry_stops_workers_without_refunding_the_unit(self):
        expired = threading.Event()
        clock = SimpleNamespace(monotonic=lambda: time.monotonic() + (10 if expired.is_set() else 0),
                                time=time.time)

        def expire_wait(connections, seconds):
            expired.set()
            return []

        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'component'
            with patch.object(mapped_request_probe, 'ROWS', 8), \
                 patch.object(mapped_request_probe, 'time', clock), \
                 patch.object(mapped_request_probe, 'wait', expire_wait):
                with self.assertRaisesRegex(RuntimeError, 'evidence retained'):
                    mapped_request_probe.run_case(output, 'mapped', 1, 0, deadline=time.monotonic() + 1)
            result = json.loads((output / 'result.json').read_text())
            self.assertEqual((result['status'], result['error_type']), ('failed', 'TimeoutError'))
            self.assertTrue(result['workers_stopped'])
            self.assertTrue(all(code is not None for code in result['worker_exits']))
            self.assertIsNone(result['close_error'])
            with sqlite3.connect(output / 'ledger.sqlite') as connection:
                allocated = connection.execute('SELECT allocated FROM budget').fetchone()[0]
            self.assertEqual(allocated, 8)


if __name__ == '__main__':
    unittest.main()
