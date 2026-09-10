"""Event callbacks avoid file I/O while keeping bounded, complete ordered output."""
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from src.experiments.buffered_events import BufferedEvents, EventRecordingError, compact_event


class BufferedEventTests(unittest.TestCase):
    def test_order_durability_and_no_writes_on_callback_thread(self):
        caller = threading.get_ident()
        actual_write, actual_fsync = os.write, os.fsync
        def write(*args):
            self.assertNotEqual(threading.get_ident(), caller)
            return actual_write(*args)
        def fsync(*args):
            self.assertNotEqual(threading.get_ident(), caller)
            return actual_fsync(*args)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'events'
            with patch('os.write', side_effect=write), patch('os.fsync', side_effect=fsync):
                with BufferedEvents(path, max_events=2000) as recorder:
                    for i in range(1000):
                        recorder.record({'sequence': i, 'text': '完整原文'})
            values = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([v['sequence'] for v in values], list(range(1000)))
            self.assertEqual(recorder.snapshot()['written_events'], 1000)
            self.assertEqual(recorder.snapshot()['pending_bytes'], 0)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_overflow_is_explicit_and_does_not_append_a_partial_event(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'events'
            recorder = BufferedEvents(path, max_pending_bytes=100)
            recorder.record({'event': 'small'})
            with self.assertRaises(EventRecordingError):
                recorder.record({'text': 'x' * 200})
            with self.assertRaises(EventRecordingError):
                recorder.close()
            self.assertEqual(len(path.read_text().splitlines()), 1)

    def test_write_failure_is_not_reported_as_a_successful_close(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch('os.write', side_effect=OSError('injected writer failure')):
                recorder = BufferedEvents(Path(directory) / 'events', batch_bytes=1)
                recorder.record({'event': 'one'})
                with self.assertRaises(EventRecordingError):
                    recorder.close()
            self.assertEqual(recorder.snapshot()['error_type'], 'OSError')

    def test_compact_events_retain_hashes_without_input_or_output_text(self):
        value = compact_event({'event': 'completion', 'raw_output': 'answer', 'body': {'private': 'input'},
                               'sequence': 0, 'payload_digest': 'a' * 64})
        self.assertNotIn('raw_output', value)
        self.assertNotIn('body', value)
        self.assertEqual(len(value['raw_output_sha256']), 64)


if __name__ == '__main__':
    unittest.main()
