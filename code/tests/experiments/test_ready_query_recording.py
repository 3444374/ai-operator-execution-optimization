from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from src.experiments.postgresql.ready_query_recording import (
    record_prepared_execution, record_prepared_pg_query)


class ReadyQueryRecordingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 1000

    def clock(self):
        self.now += 100
        return self.now

    def test_submission_excludes_recorder_setup_and_preparation(self):
        entered = []
        @contextmanager
        def rows():
            entered.append(self.clock())
            yield iter([('a', 'POSITIVE'), ('b', 'NEGATIVE')])
        result = record_prepared_execution(self.root/'success', rows,
            backend_ready_ns=1000, preparation_started_ns=100,
            max_rows=2, max_result_bytes=4096, clock=self.clock)
        self.assertLess(result['t_release_ns'], result['t_submit_ns'])
        self.assertLess(result['t_submit_ns'], entered[0])
        self.assertLessEqual(entered[0], result['t_first_row_ns'])
        self.assertEqual(result['ready_query_seconds'],
            (result['t_query_terminal_ns']-result['t_submit_ns'])/1e9)
        self.assertEqual(result['preparation_seconds'], 900/1e9)
        saved = json.loads((self.root/'success/execution.json').read_text())
        self.assertNotIn('t_submit_ns', saved, 'retain the original recorder artifact')
        saved.update(json.loads((self.root/'success/ready-timing.json').read_text()))
        self.assertEqual(saved, result)

    def test_native_failure_keeps_partial_rows_and_submission_timing(self):
        def stream():
            yield 'a', 'POSITIVE'
            raise RuntimeError('native query failed')
        @contextmanager
        def rows():
            yield stream()
        with self.assertRaisesRegex(RuntimeError, 'native query failed'):
            record_prepared_execution(self.root/'failure', rows,
                backend_ready_ns=1000, max_rows=2, max_result_bytes=4096, clock=self.clock)
        saved = json.loads((self.root/'failure/execution.json').read_text())
        saved.update(json.loads((self.root/'failure/ready-timing.json').read_text()))
        self.assertEqual(saved['status'], 'failed')
        self.assertEqual(saved['recorded_rows'], 1)
        self.assertTrue(saved['partial_results_are_provisional'])
        self.assertIsNotNone(saved['ready_query_seconds'])
        self.assertEqual(saved['query_error']['message'], 'native query failed')

    def test_context_entry_failure_keeps_real_submission_and_error(self):
        @contextmanager
        def rows():
            raise LookupError('submission rejected')
            yield
        with self.assertRaisesRegex(LookupError, 'submission rejected'):
            record_prepared_execution(self.root/'entry', rows,
                backend_ready_ns=1000, max_rows=2, max_result_bytes=4096, clock=self.clock)
        saved = json.loads((self.root/'entry/execution.json').read_text())
        saved.update(json.loads((self.root/'entry/ready-timing.json').read_text()))
        self.assertIsNotNone(saved['t_submit_ns'])
        self.assertEqual(saved['query_error']['type'], 'LookupError')

    def test_pg_cursor_exists_before_ready_and_statement_is_not_preexecuted(self):
        events = []
        test = self
        class Cursor:
            def __enter__(self):
                events.append(('cursor_open', test.clock()))
                return self
            def __exit__(self, *_):
                events.append(('cursor_close', test.clock()))
            def stream(self, query):
                events.append((query, test.clock()))
                yield 'a', 'POSITIVE'
        class Connection:
            def cursor(self): return Cursor()
        result = record_prepared_pg_query(Connection(), 'SELECT native_map', self.root/'pg',
            max_rows=2, max_result_bytes=4096, clock=self.clock)
        self.assertEqual([event[0] for event in events],
                         ['cursor_open', 'SELECT native_map', 'cursor_close'])
        self.assertLess(events[0][1], result['t_backend_ready_ns'])
        self.assertLess(result['t_submit_ns'], events[1][1])
        self.assertLess(result['t_query_terminal_ns'], events[-1][1])

    def test_invalid_preparation_timestamp_does_not_submit(self):
        @contextmanager
        def rows():
            self.fail('invalid preparation must not submit a query')
            yield
        for ready, started in ((0, None), (100, 101), (True, None)):
            with self.subTest(ready=ready), self.assertRaises(ValueError):
                record_prepared_execution(self.root/'invalid', rows,
                    backend_ready_ns=ready, preparation_started_ns=started,
                    max_rows=2, max_result_bytes=4096, clock=self.clock)

    def test_timing_write_failure_does_not_replace_native_query_failure(self):
        @contextmanager
        def rows():
            raise RuntimeError('original native failure')
            yield
        with patch('src.experiments.postgresql.ready_query_recording.write_private_json',
                   side_effect=OSError('timing write failed')):
            with self.assertRaisesRegex(RuntimeError, 'original native failure'):
                record_prepared_execution(self.root/'secondary', rows,
                    backend_ready_ns=1000, max_rows=2, max_result_bytes=4096, clock=self.clock)
        saved=json.loads((self.root/'secondary/execution.json').read_text())
        self.assertEqual(saved['query_error']['message'],'original native failure')

    def test_timing_write_failure_rejects_an_otherwise_completed_query(self):
        @contextmanager
        def rows():
            yield iter([('a','POSITIVE')])
        with patch('src.experiments.postgresql.ready_query_recording.write_private_json',
                   side_effect=OSError('timing write failed')):
            with self.assertRaisesRegex(OSError,'timing write failed'):
                record_prepared_execution(self.root/'writefail', rows,
                    backend_ready_ns=1000,max_rows=2,max_result_bytes=4096,clock=self.clock)

    def test_pg_cursor_cleanup_failure_preserves_original_query_error(self):
        class Cursor:
            def __enter__(self): return self
            def __exit__(self,*_): raise OSError('cursor cleanup failed')
            def stream(self,query):
                yield 'a','POSITIVE'
                raise RuntimeError('original SQL error')
        class Connection:
            def cursor(self): return Cursor()
        with self.assertRaisesRegex(RuntimeError,'original SQL error'):
            record_prepared_pg_query(Connection(),'SELECT native_map',self.root/'pg-errors',
                max_rows=2,max_result_bytes=4096,clock=self.clock)
        saved=json.loads((self.root/'pg-errors/execution.json').read_text())
        self.assertEqual(saved['query_error']['message'],'original SQL error')
        self.assertEqual(saved['cleanup_error']['message'],'cursor cleanup failed')


if __name__ == '__main__':
    unittest.main()
