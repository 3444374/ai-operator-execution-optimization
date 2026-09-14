"""Exercise late timeout observation without yielding the event loop."""
import asyncio
from contextlib import asynccontextmanager, contextmanager
import hashlib
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from src.experiments.postgresql import map_query_recording as recorder
from src.experiments.postgresql.map_direct import DirectMap


TIMEOUT = .02
BLOCK = .08


class AsyncRecordingDeadlineTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)/'query'

    @contextmanager
    def slow_write(self, last=1):
        original = recorder._Recording.open_results
        def open_results(recording):
            original(recording)
            stream = recording.stream
            class Writer:
                calls = 0
                def write(self, value):
                    self.calls += 1
                    result = stream.write(value)
                    if self.calls == last:
                        time.sleep(BLOCK)
                    return result
                def flush(self):
                    return stream.flush()
            recording.stream = Writer()
        with patch.object(recorder._Recording, 'open_results', open_results):
            yield

    def run_timeout(self, source, expected_rows):
        with self.assertRaises(TimeoutError):
            asyncio.run(recorder.record_async_execution(self.root, source, max_rows=10,
                max_result_bytes=4096, query_timeout_s=TIMEOUT))
        record = json.loads((self.root/'execution.json').read_text())
        self.assertEqual(record['status'], 'failed')
        self.assertEqual(record['query_error']['type'], 'TimeoutError')
        self.assertEqual(record['recorded_rows'], expected_rows)
        data = (self.root/'results.jsonl').read_bytes()
        self.assertEqual(len(data.splitlines()), expected_rows)
        self.assertEqual(hashlib.sha256(data).hexdigest(), record['results_sha256'])
        self.assertGreaterEqual(record['t_stream_cleanup_ns'], record['t_query_terminal_ns'])
        self.assertGreaterEqual(record['t_query_terminal_ns'], record['t_cancel_triggered_ns'])
        deadline = record['async_deadline']
        self.assertEqual(deadline['timeout_s'], TIMEOUT)
        self.assertGreaterEqual(deadline['observed_s'], deadline['deadline_s'])
        self.assertEqual(deadline['observed_ns'], record['t_cancel_triggered_ns'])
        with self.assertRaises(ValueError):
            recorder.evaluate_recording(self.root, lambda rows: list(rows))
        return record

    def test_synchronous_source_work_cannot_finish_successfully_after_deadline(self):
        @asynccontextmanager
        async def source():
            async def rows():
                time.sleep(BLOCK)
                yield ('a', 'late')
            yield rows()
        self.run_timeout(source, 0)

    def test_synchronous_eof_and_context_entry_are_checked(self):
        for phase in ('entry', 'eof'):
            with self.subTest(phase=phase):
                self.root = self.root.parent/phase
                @asynccontextmanager
                async def source():
                    if phase == 'entry': time.sleep(BLOCK)
                    async def rows():
                        if phase == 'eof': time.sleep(BLOCK)
                        if False: yield
                    yield rows()
                self.run_timeout(source, 0)

    def test_final_synchronous_write_retains_rows_and_times_out_before_cleanup(self):
        @asynccontextmanager
        async def source():
            async def rows():
                for i in range(3):
                    yield (str(i), 'answer')
            try: yield rows()
            finally: await asyncio.sleep(.03)
        with self.slow_write(last=3):
            record = self.run_timeout(source, 3)
        self.assertGreater(record['inline_recording_seconds'], .06)
        self.assertEqual(record['async_deadline']['detection'], 'absolute_check')
        self.assertGreater(record['async_deadline']['observed_s']-record['async_deadline']['deadline_s'], .04)
        self.assertGreater(record['t_stream_cleanup_ns']-record['t_query_terminal_ns'], 20_000_000)

    def test_actual_direct_iterator_cannot_cancel_an_overdue_timeout_after_last_write(self):
        direct = object.__new__(DirectMap)
        direct.concurrency = 1
        async def one(sequence, row, session):
            return row['source_example_id'], 'answer'
        direct._one = one
        inputs = [dict(source_example_id='a')]
        with self.slow_write():
            self.run_timeout(lambda: direct.rows(inputs, 1), 1)

    def test_cooperative_wait_and_success_with_slow_cleanup_keep_their_meaning(self):
        @asynccontextmanager
        async def waiting():
            async def rows():
                await asyncio.sleep(BLOCK)
                yield ('a', 'late')
            yield rows()
        self.assertEqual(self.run_timeout(waiting, 0)['async_deadline']['detection'], 'timeout_callback')
        self.root = self.root.parent/'success'
        @asynccontextmanager
        async def quick():
            async def rows(): yield ('a', 'answer')
            try: yield rows()
            finally: time.sleep(BLOCK)
        record = asyncio.run(recorder.record_async_execution(self.root, quick, max_rows=1,
            max_result_bytes=4096, query_timeout_s=TIMEOUT))
        self.assertEqual(record['status'], 'completed')
        self.assertIsNone(record['t_cancel_triggered_ns'])
        self.assertIsNone(record['async_deadline']['observed_s'])

    def test_hot_iterator_checks_deadline_between_rows_without_an_await(self):
        async def run():
            now = [0.0]
            @asynccontextmanager
            async def source():
                async def rows():
                    for i in range(10):
                        now[0] += .008
                        yield (str(i), 'answer')
                yield rows()
            with patch.object(asyncio.get_running_loop(), 'time', lambda: now[0]):
                with self.assertRaises(TimeoutError):
                    await recorder.record_async_execution(self.root, source, max_rows=10,
                        max_result_bytes=4096, query_timeout_s=TIMEOUT)
        asyncio.run(run())
        record = json.loads((self.root/'execution.json').read_text())
        self.assertEqual(record['recorded_rows'], 2)
        self.assertEqual(record['async_deadline']['detection'], 'absolute_check')
