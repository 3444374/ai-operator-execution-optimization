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

from src.experiments import mapped_request_probe


class ProbeSuiteDeadlineTests(unittest.TestCase):
    def run_suite(self, first_seconds):
        probe = mapped_request_probe
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
            result['repeat'] = args[0]
            return result

        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'probe'
            with patch.object(probe, 'time', SimpleNamespace(monotonic=lambda: clock.now)), \
                 patch.object(probe, 'run_case', side_effect=run_case), redirect_stdout(io.StringIO()):
                result = probe.run_probe(output)
            return result, calls

    def test_next_case_uses_remaining_suite_time_instead_of_reserving_a_whole_case(self):
        result, deadlines = self.run_suite(275)
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(len(result['runs']), 16)
        self.assertEqual(deadlines, [400.0] * 16)

    def test_exhausted_total_stops_before_another_case_and_preserves_the_summary(self):
        probe = mapped_request_probe
        with tempfile.TemporaryDirectory() as root:
            clock = SimpleNamespace(now=100.0)
            calls = []

            def first_case(*args, **kwargs):
                calls.append(kwargs.get('deadline'))
                clock.now = 400.0
                return dict(accounting='durable', recording='memory', rows=16,
                    synthetic_calls=16, mode='durable', workers=1, repeat=0,
                    received_grants=16, grant_seconds=0, component_total_seconds=0,
                    query_seconds=0, accounting_setup_seconds=0, preparation_windows=0,
                    core_active_mean=0, synthetic_worker_active_mean=0)

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
