import csv
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from src.baselines.text.products import sema


FAKE_BINARY = r'''
import csv, json, os, re, sys, time
from pathlib import Path

root = Path(__file__).parent
mode = MODE
rows = []
pending = ''
with (root / 'fake-events.jsonl').open('a') as log:
    def event(value):
        log.write(json.dumps(value) + '\n')
        log.flush()
    def emit(row):
        csv.writer(sys.stdout).writerow(row)
        sys.stdout.flush()
    event({'event': 'start', 'pid': os.getpid(), 'args': sys.argv[1:]})
    for line in sys.stdin:
        pending += line
        if not line.rstrip().endswith(';'):
            continue
        sql, pending = pending.strip(), ''
        if sql.startswith('CREATE TABLE') and mode == 'prepare_error':
            sys.stderr.write('fixture preparation error\n')
            sys.exit(7)
        if sql.startswith('COPY input'):
            source = re.search(r"COPY input FROM '((?:[^']|'')*)'", sql).group(1)
            with open(source.replace("''", "'"), newline='') as stream:
                rows = list(csv.DictReader(stream))
            event({'event': 'copied', 'rows': len(rows)})
        elif sql.startswith('SELECT row_id,s'):
            event({'event': 'query', 'pid': os.getpid()})
            if mode == 'query_error':
                sys.stderr.write('fixture query error\n')
                sys.exit(9)
            if mode == 'hang':
                time.sleep(30)
            if mode == 'bad_schema':
                emit(('unexpected', 'three', 'fields'))
            else:
                for index, row in enumerate(rows):
                    emit((row['row_id'], 'POSITIVE\nquoted "label"'))
                    if mode == 'partial' and index == 0:
                        time.sleep(30)
        elif sql.startswith("SELECT '__sema_complete__'"):
            values = [value.replace("''", "'") for value in
                      re.findall(r"'((?:[^']|'')*)'", sql)]
            if 'COUNT(*)' in sql:
                values.append(str(len(rows)))
            event({'event': 'marker', 'pid': os.getpid()})
            emit(values)
'''


class SemaPreparedTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='sema-prepared-test-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.plan = SimpleNamespace(instruction='Classify the review.')
        self.model = SimpleNamespace(endpoint_url='http://localhost:18080/v1/chat/completions',
                                     model_id='fixture-model', bearer_token=None, timeout_ms=1000)
        self.values = ({'source_example_id': 'row-a', 'input_text': 'same "quoted" review\nline'},
                       {'source_example_id': 'row-b', 'input_text': 'same "quoted" review\nline'})

    def binary(self, mode='normal'):
        binary = self.root / 'fake-sema'
        source = '#!' + sys.executable + '\n' + FAKE_BINARY.replace('MODE', repr(mode))
        binary.write_text(source)
        binary.chmod(0o700)
        return binary

    def events(self):
        return [json.loads(line) for line in (self.root / 'fake-events.jsonl').read_text().splitlines()]

    def prepared(self, binary):
        expected_hash = hashlib.sha256(binary.read_bytes()).hexdigest()
        patch = mock.patch.object(sema, 'BINARY_SHA256', expected_hash)
        self.addCleanup(patch.stop)
        patch.start()
        return sema.prepare_projection(self.values, self.plan, self.model,
                                       binary=binary, root=self.root, num_threads=2)

    def assert_stopped(self, session):
        self.assertIsNotNone(session.returncode)
        with self.assertRaises(ProcessLookupError):
            os.kill(session.pid, 0)
        self.assertTrue((self.root / 'sema-prepared-stdout.txt').is_file())
        self.assertTrue((self.root / 'sema-prepared-stderr.txt').is_file())

    def test_ready_imports_duplicate_values_without_executing_the_projection(self):
        with self.prepared(self.binary()) as session:
            self.assertEqual([e['event'] for e in self.events()], ['start', 'copied', 'marker'])
            self.assertIsNone(session.returncode)
            first = list(session.execute())
            second = list(session.execute())
            expected = [('row-a', 'POSITIVE\nquoted "label"'),
                        ('row-b', 'POSITIVE\nquoted "label"')]
            self.assertEqual(first, expected)
            self.assertEqual(second, expected)
            self.assertEqual({e['pid'] for e in self.events() if 'pid' in e}, {session.pid})
            self.assertEqual(sum(e['event'] == 'query' for e in self.events()), 2)
        self.assert_stopped(session)
        self.assertEqual(session.returncode, 0)
        with (self.root / 'sema-source.csv').open(newline='') as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual([r['review_text'] for r in rows], [v['input_text'] for v in self.values])

    def test_pinned_binary_is_checked_before_any_process_or_source_file(self):
        with self.assertRaisesRegex(ValueError, 'pinned author artifact'):
            with sema.prepare_projection(self.values, self.plan, self.model,
                                         binary=self.binary(), root=self.root):
                self.fail('unverified binary must not become ready')
        self.assertFalse((self.root / 'sema-source.csv').exists())
        self.assertFalse((self.root / 'fake-events.jsonl').exists())

    def test_same_author_session_replaces_the_raw_relation_before_a_later_select(self):
        with self.prepared(self.binary()) as session:
            first=list(session.execute())
            root=self.root/'replacement'
            root.mkdir()
            session.replace_source(self.values[:1],root,self.model.endpoint_url)
            second=list(session.execute())
            self.assertEqual(first[:1],second)
            self.assertEqual({e['pid'] for e in self.events() if 'pid' in e},{session.pid})
            self.assertEqual([e['rows'] for e in self.events() if e['event']=='copied'],[2,1])
            self.assertIsNone(session.returncode)
        self.assert_stopped(session)

    def test_preparation_error_stops_before_any_projection(self):
        with self.assertRaisesRegex(RuntimeError, 'completion marker; exit=7'):
            with self.prepared(self.binary('prepare_error')):
                self.fail('failed preparation must not yield a session')
        self.assertEqual(sum(e['event'] == 'query' for e in self.events()), 0)
        self.assertIn('fixture preparation error',
                      (self.root / 'sema-prepared-stderr.txt').read_text())

    def test_native_error_stops_the_session_without_retrying(self):
        with self.prepared(self.binary('query_error')) as session:
            with self.assertRaisesRegex(RuntimeError, 'completion marker; exit=9'):
                list(session.execute())
            self.assert_stopped(session)
            with self.assertRaisesRegex(RuntimeError, 'not ready'):
                list(session.execute())
        self.assertEqual(sum(e['event'] == 'query' for e in self.events()), 1)
        self.assertIn('fixture query error', (self.root / 'sema-prepared-stderr.txt').read_text())

    def test_closing_a_partial_iterator_stops_the_native_process(self):
        with self.prepared(self.binary('partial')) as session:
            stream = session.execute()
            self.assertEqual(next(stream)[0], 'row-a')
            with self.assertRaisesRegex(RuntimeError, 'active projection'):
                next(session.execute())
            stream.close()
            self.assert_stopped(session)
        self.assertEqual(sum(e['event'] == 'query' for e in self.events()), 1)

    def test_completion_timeout_retains_output_and_stops_process(self):
        with self.prepared(self.binary('hang')) as session:
            session._timeout_seconds = 0.1
            started = time.monotonic()
            with self.assertRaisesRegex(TimeoutError, 'completion deadline'):
                list(session.execute())
            self.assertLess(time.monotonic() - started, 2)
            self.assert_stopped(session)

    def test_proxy_thread_can_cancel_without_joining_the_reader(self):
        with self.prepared(self.binary('hang')) as session:
            errors = []
            def consume():
                try:
                    list(session.execute())
                except BaseException as error:
                    errors.append(error)
            reader = threading.Thread(target=consume)
            reader.start()
            deadline = time.monotonic() + 2
            while not any(e['event'] == 'query' for e in self.events()):
                if time.monotonic() > deadline:
                    self.fail('fixture reader did not submit its query')
                time.sleep(0.01)
            started = time.monotonic()
            session.cancel()
            self.assertLess(time.monotonic() - started, 0.5)
            reader.join(timeout=2)
            self.assertFalse(reader.is_alive())
            self.assertEqual(len(errors), 1)
            self.assertIsInstance(errors[0], RuntimeError)
            self.assertRegex(str(errors[0]), 'canceled|completion marker')
            self.assert_stopped(session)
            session.cancel()

    def test_unexpected_output_schema_stops_before_reporting_completion(self):
        with self.prepared(self.binary('bad_schema')) as session:
            with self.assertRaisesRegex(ValueError, 'unexpected CSV schema'):
                list(session.execute())
            self.assert_stopped(session)

    def test_original_one_shot_interface_keeps_its_flags_and_result_rows(self):
        binary = self.binary()
        with mock.patch.object(sema, 'BINARY_SHA256', hashlib.sha256(binary.read_bytes()).hexdigest()):
            rows = list(sema.run_projection(self.values, self.plan, self.model,
                                           binary=binary, root=self.root, num_threads=2))
        self.assertEqual(rows, [['row-a', 'POSITIVE\nquoted "label"'],
                                ['row-b', 'POSITIVE\nquoted "label"']])
        self.assertEqual(self.events()[0]['args'], ['-csv', '-noheader'])
        self.assertTrue((self.root / 'sema-stdout.txt').is_file())
        self.assertTrue((self.root / 'sema-stderr.txt').is_file())


if __name__ == '__main__':
    unittest.main()
