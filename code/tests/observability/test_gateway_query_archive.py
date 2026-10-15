"""Query export/reclamation checks; synthetic rows never claim HTTP/model evidence."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.client import HTTPConnection
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock
from urllib.parse import urlsplit

from src.experiments.postgresql import persistent_native_adapter_query as persistent
from src.observability import request_gateway as module


class HistoryRows(dict):
    scans = 0

    def values(self):
        self.scans += 1
        return super().values()


class GatewayQueryArchiveTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    @contextmanager
    def gateway(self, **kwargs):
        gateway = module.ObservationGateway(
            routes=(module.GatewayRoute('owner', 'model', 'http://127.0.0.1:1'),),
            trace_path=self.root / 'owner-trace.jsonl', **kwargs)
        gateway._trace_rows = HistoryRows()
        with gateway:
            yield gateway

    @staticmethod
    def on_loop(gateway, action):
        async def run():
            return action()
        return asyncio.run_coroutine_threadsafe(run(), gateway._loop).result(3)

    def add_rows(self, gateway, query_id, first, count=1, *, terminal=True):
        def add():
            for request_id in range(first, first + count):
                row = dict(gateway_request_id=request_id, query_id=query_id,
                    request_terminal_monotonic_ns=1 if terminal else None,
                    errors=[], status='completed' if terminal else 'incomplete')
                gateway._track_request(row)
                gateway._request_settled(row)
        self.on_loop(gateway, add)

    @staticmethod
    def owner(gateway):
        owner = persistent._ArmOwner.__new__(persistent._ArmOwner)
        owner.binding = owner.service = owner.execution = None
        owner.gateway = gateway
        owner.group = SimpleNamespace(model=SimpleNamespace(timeout_ms=10), poisoned=False)
        return owner

    def query(self, owner, unit_id):
        root = self.root / unit_id
        root.mkdir()
        return owner.query(unit_id, root, lambda *_: None, lambda *_: None, lambda *_: None)

    def test_64_queries_copy_only_120_rows_and_release_both_indexes(self):
        copied = []
        dumps = json.dumps

        def observe_copy(value, *args, **kwargs):
            if isinstance(value, list):
                copied.append(len(value))
            return dumps(value, *args, **kwargs)

        with self.gateway() as gateway:
            # Unknown, already terminal evidence must survive named-query reclamation.
            self.add_rows(gateway, None, -1)
            with mock.patch.object(module.json, 'dumps', side_effect=observe_copy):
                for number in range(64):
                    query_id = str(number)
                    self.add_rows(gateway, query_id, 120 * number, 120)
                    self.assertEqual(len(gateway._trace_rows), 121)
                    rows = gateway.snapshot(query_id=query_id)
                    self.assertEqual(len(rows), 120)
                    gateway.archive_query(query_id, rows)
                    self.assertEqual(len(gateway._trace_rows), 1)
                    self.assertEqual(set(gateway._query_rows), {None})
                    self.assertFalse(gateway._pending_rows)
            self.assertEqual(copied, [120] * 64)
            self.assertEqual(gateway._trace_rows.scans, 0)
            self.assertEqual(gateway._archived_rows, 7680)
            self.assertEqual(gateway.snapshot()[0]['query_id'], None)
        archived = [json.loads(line) for line in (self.root / 'owner-trace.jsonl').read_text().splitlines()]
        self.assertEqual(len(archived), 7681)
        self.assertEqual(len({row['gateway_request_id'] for row in archived}), 7681)

    def test_default_snapshot_remains_full_and_detached_until_explicit_archive(self):
        with self.gateway() as gateway:
            self.add_rows(gateway, 'first', 0)
            self.add_rows(gateway, 'second', 1)
            rows = gateway.snapshot()
            self.assertEqual([row['query_id'] for row in rows], ['first', 'second'])
            rows[0]['errors'].append({'error_type': 'caller mutation'})
            self.assertEqual(gateway.snapshot(query_id='first')[0]['errors'], [])
            with self.assertRaisesRegex(RuntimeError, 'changed after snapshot'):
                gateway.archive_query('first', rows[:1])
            gateway.archive_query('first', gateway.snapshot(query_id='first'))
            self.assertEqual([row['query_id'] for row in gateway.snapshot()], ['second'])

    def test_pending_unknown_or_previous_query_blocks_handoff_and_reclamation(self):
        for pending_query in (None, 'previous'):
            with self.subTest(query_id=pending_query), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                with self.gateway() as gateway:
                    self.add_rows(gateway, 'current', 0)
                    self.add_rows(gateway, pending_query, 1, terminal=False)
                    with self.assertRaises(TimeoutError):
                        gateway.snapshot(.01, query_id='current')
                    rows = [dict(gateway._trace_rows[0])]
                    with self.assertRaisesRegex(RuntimeError, 'remain unsettled'):
                        gateway.archive_query('current', rows)
                    self.assertEqual(len(gateway._trace_rows), 2)
                    def settle():
                        row = gateway._trace_rows[1]
                        row['request_terminal_monotonic_ns'] = 2
                        gateway._request_settled(row)
                    self.on_loop(gateway, settle)
                    gateway.archive_query('current', gateway.snapshot(query_id='current'))
                    self.assertEqual(gateway.snapshot()[0]['query_id'], pending_query)

    def test_late_request_invalidates_empty_snapshot_without_losing_evidence(self):
        with self.gateway() as gateway:
            rows = gateway.snapshot(query_id='late')
            self.assertEqual(rows, [])
            self.add_rows(gateway, 'late', 0)
            with self.assertRaisesRegex(RuntimeError, 'changed after snapshot'):
                gateway.archive_query('late', rows)
            self.assertEqual(len(gateway.snapshot(query_id='late')), 1)
            self.assertEqual(gateway._archived_rows, 0)

    def test_archive_fsync_failure_retains_rows_and_stop_can_persist_them(self):
        with self.gateway() as gateway:
            self.add_rows(gateway, 'failed', 0)
            rows = gateway.snapshot(query_id='failed')
            failure = OSError('fixture fsync')
            with mock.patch.object(module.os, 'fsync', side_effect=failure):
                with self.assertRaises(OSError) as caught:
                    gateway.archive_query('failed', rows)
            self.assertIs(caught.exception, failure)
            self.assertEqual(gateway.snapshot(query_id='failed'), rows)
            self.assertEqual(gateway._archived_rows, 0)
        stored = [json.loads(line) for line in (self.root / 'owner-trace.jsonl').read_text().splitlines()]
        self.assertEqual(stored, rows)

    def test_owner_archives_after_file_commit_and_handles_empty_query(self):
        with self.gateway() as gateway:
            owner = self.owner(gateway)
            for number, count in enumerate((2, 0, 3)):
                query_id = 'query-' + str(number)
                with self.query(owner, query_id):
                    self.add_rows(gateway, query_id, number * 3, count)
                stored = (self.root / query_id / 'http-trace.jsonl').read_text().splitlines()
                self.assertEqual(len(stored), count)
                self.assertFalse(gateway._trace_rows)
                self.assertFalse(owner.group.poisoned)
            self.assertEqual(gateway._archived_rows, 5)
        self.assertEqual(len((self.root / 'owner-trace.jsonl').read_text().splitlines()), 5)

    def test_owner_archive_failure_follows_committed_export_and_keeps_query_error_primary(self):
        with self.gateway() as gateway:
            owner = self.owner(gateway)
            primary = RuntimeError('query failed')
            with mock.patch.object(gateway, '_write_trace_rows', side_effect=OSError('owner archive failed')):
                with self.assertRaises(RuntimeError) as caught:
                    with self.query(owner, 'failed'):
                        self.add_rows(gateway, 'failed', 0)
                        raise primary
            self.assertIs(caught.exception, primary)
            self.assertTrue(owner.group.poisoned)
            self.assertEqual(len((self.root / 'failed' / 'http-trace.jsonl').read_text().splitlines()), 1)
            self.assertEqual(len(gateway.snapshot()), 1)
            self.assertEqual(gateway._archived_rows, 0)
        self.assertEqual(len((self.root / 'owner-trace.jsonl').read_text().splitlines()), 1)

    def test_timeout_cancels_queued_archive_without_late_reclamation(self):
        with self.gateway() as gateway:
            self.add_rows(gateway, 'queued', 0)
            rows = gateway.snapshot(query_id='queued')
            entered, release = threading.Event(), threading.Event()
            def block_loop():
                entered.set()
                release.wait(3)
            gateway._loop.call_soon_threadsafe(block_loop)
            self.assertTrue(entered.wait(3))
            try:
                with self.assertRaises(TimeoutError):
                    gateway.archive_query('queued', rows, timeout_s=.01)
            finally:
                release.set()
            self.assertEqual(gateway.snapshot(), rows)
            self.assertEqual(gateway._archived_rows, 0)

    def test_export_commit_failure_never_acknowledges_rows(self):
        with self.gateway() as gateway:
            owner = self.owner(gateway)
            original = persistent.open_private_text
            @contextmanager
            def fail_commit(path):
                with original(path) as stream:
                    yield stream
                raise OSError('export commit failed')
            with mock.patch.object(persistent, 'open_private_text', side_effect=fail_commit):
                with self.assertRaisesRegex(OSError, 'export commit failed'):
                    with self.query(owner, 'failed'):
                        self.add_rows(gateway, 'failed', 0)
            self.assertEqual(len(gateway.snapshot()), 1)
            self.assertEqual(gateway._archived_rows, 0)
            self.assertTrue(owner.group.poisoned)

    def test_in_progress_archive_timeout_reclaims_only_after_successful_persistence(self):
        with self.gateway() as gateway:
            self.add_rows(gateway, 'writing', 0)
            rows = gateway.snapshot(query_id='writing')
            entered, release = threading.Event(), threading.Event()
            write = gateway._write_trace_rows
            def blocked_write(stream, selected):
                entered.set()
                release.wait(3)
                write(stream, selected)
            with mock.patch.object(gateway, '_write_trace_rows', side_effect=blocked_write):
                with ThreadPoolExecutor(max_workers=1) as pool:
                    call = pool.submit(gateway.archive_query, 'writing', rows, .05)
                    try:
                        self.assertTrue(entered.wait(3))
                        with self.assertRaises(TimeoutError):
                            call.result(3)
                        self.assertEqual(len(gateway._trace_rows), 1)
                        self.assertEqual(gateway._archived_rows, 0)
                    finally:
                        release.set()
                    self.assertEqual(gateway.snapshot(), [])
                    self.assertEqual(gateway._archived_rows, 1)
            stored = [json.loads(line) for line in (self.root / 'owner-trace.jsonl').read_text().splitlines()]
            self.assertEqual(stored, rows)

    def test_final_archive_failure_preserves_direct_context_error_and_cancellation(self):
        for primary in (ValueError('query failed'), asyncio.CancelledError()):
            with self.subTest(primary=type(primary).__name__), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                failure = OSError('final owner trace failed')
                with mock.patch.object(module.ObservationGateway, '_write_trace_rows', side_effect=failure):
                    with self.assertRaises(type(primary)) as caught:
                        with self.gateway() as gateway:
                            self.add_rows(gateway, 'failed', 0)
                            raise primary
                self.assertIs(caught.exception, primary)
                self.assertTrue(any('OSError' in note for note in primary.__notes__))
                self.assertEqual(len(gateway._trace_rows), 1)

    def test_failed_or_cancelled_export_keeps_original_error_and_unpersisted_rows(self):
        for primary in (RuntimeError('query failed'), asyncio.CancelledError()):
            with self.subTest(primary=type(primary).__name__), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                with self.gateway() as gateway:
                    owner = self.owner(gateway)
                    with mock.patch.object(persistent, 'open_private_text', side_effect=OSError('write failed')):
                        with self.assertRaises(type(primary)) as caught:
                            with self.query(owner, 'failed'):
                                self.add_rows(gateway, 'failed', 0)
                                raise primary
                    self.assertIs(caught.exception, primary)
                    self.assertTrue(primary.__notes__)
                    self.assertTrue(owner.group.poisoned)
                    self.assertIsNone(owner.binding)
                    self.assertEqual(len(gateway.snapshot(query_id='failed')), 1)
                    self.assertEqual(gateway._archived_rows, 0)
                self.assertEqual(len((self.root / 'owner-trace.jsonl').read_text().splitlines()), 1)

    def test_cancellation_during_export_never_acknowledges_rows(self):
        with self.gateway() as gateway:
            owner = self.owner(gateway)
            cancel = asyncio.CancelledError()
            with mock.patch.object(persistent, 'open_private_text', side_effect=cancel):
                with self.assertRaises(asyncio.CancelledError) as caught:
                    with self.query(owner, 'cancelled'):
                        self.add_rows(gateway, 'cancelled', 0)
            self.assertIs(caught.exception, cancel)
            self.assertEqual(len(gateway.snapshot()), 1)
            self.assertEqual(gateway._archived_rows, 0)
            self.assertTrue(owner.group.poisoned)

    def test_unknown_request_cannot_use_a_subsequent_query_callbacks(self):
        query_id = [None]
        before, after = mock.Mock(), mock.Mock()
        with self.gateway(query_identity=lambda: query_id[0], before_forward=before,
                          after_forward=after) as gateway:
            tracked = threading.Event()
            track = gateway._track_request
            def observed(row):
                track(row)
                tracked.set()
            with mock.patch.object(gateway, '_track_request', side_effect=observed):
                url = urlsplit(gateway.endpoint_url('owner', 'model'))
                connection = HTTPConnection(url.hostname, url.port, timeout=3)
                try:
                    connection.putrequest('POST', url.path)
                    connection.putheader('Content-Length', '2')
                    connection.endheaders()
                    self.assertTrue(tracked.wait(3))
                    query_id[0] = 'next-query'
                    connection.send(b'{}')
                    response = connection.getresponse()
                    self.assertEqual(response.status, 502)
                    response.read()
                finally:
                    connection.close()
            self.assertEqual(gateway.snapshot(query_id='next-query'), [])
            gateway.archive_query('next-query', [])
            row = gateway.snapshot()[0]
            self.assertIsNone(row['query_id'])
            self.assertFalse(row['forwarded'])
            self.assertEqual(row['error_phase'], 'before_forward')
        before.assert_not_called()
        after.assert_not_called()

    def test_concurrent_requests_keep_their_captured_query_identity(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from urllib import request

        gate, received = threading.Event(), threading.Event()
        class Upstream(BaseHTTPRequestHandler):
            count = 0
            lock = threading.Lock()

            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                with self.lock:
                    type(self).count += 1
                    if type(self).count >= 2:
                        received.set()
                gate.wait(3)
                self.send_response(200)
                self.send_header('Content-Length', '2')
                self.end_headers()
                self.wfile.write(b'{}')

            def log_message(self, *_):
                pass

        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        query_id = ['first']
        try:
            with self.gateway(query_identity=lambda: query_id[0]) as gateway:
                gateway._routes[('owner', 'model')] = module.GatewayRoute('owner', 'model',
                    'http://127.0.0.1:' + str(upstream.server_port) + '/v1/chat/completions')
                def send():
                    with request.urlopen(request.Request(gateway.endpoint_url('owner', 'model'), data=b'{}'),
                                         timeout=3) as response:
                        return response.read()
                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(send) for _ in range(2)]
                    try:
                        self.assertTrue(received.wait(3))
                        with self.assertRaises(TimeoutError):
                            gateway.snapshot(.01, query_id='first')
                    finally:
                        gate.set()
                    for future in futures:
                        future.result(3)
                rows = gateway.snapshot(query_id='first')
                self.assertEqual(len(rows), 2)
                self.assertTrue(all(row['query_id'] == 'first' for row in rows))
                gateway.archive_query('first', rows)
                query_id[0] = 'second'
                send()
                rows = gateway.snapshot(query_id='second')
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['query_id'], 'second')
                gateway.archive_query('second', rows)
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(3)
