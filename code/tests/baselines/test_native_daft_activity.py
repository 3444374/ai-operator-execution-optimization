"""Daft HTTP overlap and graph-loop clients; actual Daft checks are opt-in."""
import asyncio
import base64
from contextlib import contextmanager
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.util import find_spec
import json
import os
import threading
import time
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import native_calls as native
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from tests.baselines.test_native_calls import call, prepare_native_test_runtime, record_fixture
from tests.baselines.test_native_http_lifecycle import Session


@contextmanager
def activity_server():
    """Echo independent calls, holding the first replies until HTTP overlaps."""
    requests, events = [], []
    lock, overlap = threading.Lock(), threading.Event()
    activity = dict(active=0, peak=0)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_POST(self):
            payload = self.rfile.read(int(self.headers['Content-Length']))
            status = 429 if json.loads(payload)['index'] % 6 == 0 else 200
            with lock:
                sequence = len(requests)
                requests.append(dict(sequence=sequence, path=self.path,
                    raw_body=base64.b64encode(payload).decode(),
                    repeated_headers=self.headers.get_all('X-Input'), status=status))
                activity['active'] += 1
                activity['peak'] = max(activity['peak'], activity['active'])
                events.append(dict(sequence=sequence, event='start',
                    monotonic_ns=time.monotonic_ns(), active=activity['active']))
                if activity['active'] > 1:
                    overlap.set()
            try:
                if sequence < 2:
                    overlap.wait(2)
                time.sleep(.02)
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with lock:
                    activity['active'] -= 1
                    events.append(dict(sequence=sequence, event='end',
                        monotonic_ns=time.monotonic_ns(), active=activity['active']))

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}/complete?query=1', requests, events, activity, overlap
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        if thread.is_alive():
            raise RuntimeError('Daft activity fixture did not stop')
        record_fixture('daft-http-service', dict(requests=requests, events=events,
            active_after_close=activity['active'], peak_active=activity['peak'],
            observation_scope='localhost handlers through complete response write'))


class DaftGraphLoopClientTests(unittest.TestCase):
    """Exercise client ownership only, using the existing HTTP session stand-in."""
    def test_one_control_uses_and_closes_a_distinct_client_on_each_live_loop(self):
        control = native._GraphControl()
        created, owners, failures = [], [], []
        lock = threading.Lock()
        ready = [threading.Event(), threading.Event()]

        def factory():
            client = native._HttpClient(asyncio.get_running_loop(), Session())
            with lock:
                created.append(client)
            return client

        def worker(index):
            async def run():
                loop = asyncio.get_running_loop()
                released = loop.create_future()
                with lock:
                    owners.append((loop, released))
                for row in range(3):
                    source = native.NativeCall(f'loop:{index}:{row}', index * 3 + row,
                        json.dumps(dict(index=index, row=row)).encode(),
                        'http://localhost/fixture', (), 1)
                    returned = await native._request(source, control)
                    self.assertEqual((returned.call_id, returned.row_id), (source.call_id, source.row_id))
                    self.assertEqual(returned.response.body, source.payload)
                ready[index].set()
                await released
                await asyncio.wrap_future(control._client_closes[loop])
            try:
                asyncio.run(run())
            except BaseException as error:
                failures.append(error)

        threads = [threading.Thread(target=worker, args=(index,)) for index in range(2)]
        with patch.object(native, '_new_http_client', factory):
            try:
                for thread in threads:
                    thread.start()
                self.assertTrue(all(event.wait(3) for event in ready), failures)
                self.assertEqual(len(created), 2)
                self.assertTrue(all(not client.closed for client in created))
            finally:
                control.stop()
                for loop, released in owners:
                    if not loop.is_closed():
                        loop.call_soon_threadsafe(released.set_result, None)
                for thread in threads:
                    if thread.ident is not None:
                        thread.join(3)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(failures, [])
        control.wait_clients_closed(0)
        self.assertEqual(len(control._clients), 2)
        self.assertTrue(all(client.closed and client.loop.is_closed() for client in created))
        self.assertEqual([len(client.session.requests) for client in created], [3, 3])
        self.assertEqual([client.session.closes for client in created], [1, 1])


DAFT_ENABLED = os.environ.get('SEMLOOM_NATIVE_CALL_TEST') == '1' and find_spec('daft') is not None


@unittest.skipUnless(DAFT_ENABLED, 'actual Daft 0.7.21 activity needs SEMLOOM_NATIVE_CALL_TEST=1 and daft')
class DaftHttpActivityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import daft
        if daft.__version__ != '0.7.21':
            raise RuntimeError('HTTP activity tests require pinned Daft 0.7.21')
        prepare_native_test_runtime()

    def setUp(self):
        os.environ['SEMLOOM_NATIVE_CALL_CASE'] = self.id()

    def test_prepared_calls_overlap_with_greedy_consumption_and_keep_full_results(self):
        controls = []
        control_factory = native._GraphControl

        def capture_control():
            control = control_factory()
            controls.append(control)
            return control

        options = NativeGraphOptions(concurrency=4)
        with activity_server() as (endpoint, requests, events, activity, overlap):
            sources = tuple(replace(call(endpoint, index),
                payload=json.dumps(dict(index=index, text='duplicate')).encode()) for index in range(24))
            prepared_ns = time.monotonic_ns()
            executor = native.NativeCallExecutor('daft', options)
            with patch.object(native, '_GraphControl', capture_control):
                outcomes = tuple(executor.iter_batch(sources))
            consumed_ns = time.monotonic_ns()
        expected = {source.call_id: source for source in sources}
        self.assertEqual({outcome.call_id for outcome in outcomes}, set(expected))
        for outcome in outcomes:
            source = expected[outcome.call_id]
            self.assertEqual(outcome.row_id, source.row_id)
            self.assertEqual(outcome.response.body, source.payload)
            self.assertEqual(outcome.response.status_code, 429 if source.row_id % 6 == 0 else 200)
            self.assertEqual(outcome.transport_error, '')
        self.assertEqual(len(requests), len(sources))
        self.assertEqual(sorted(base64.b64decode(row['raw_body']) for row in requests),
                         sorted(source.payload for source in sources))
        self.assertTrue(all(row['repeated_headers'] == ['one', 'two'] for row in requests))
        self.assertTrue(overlap.is_set(), 'prepared input and a greedy consumer did not produce overlapping HTTP')
        self.assertLessEqual(activity['peak'], options.concurrency)
        self.assertEqual(activity['active'], 0)
        self.assertEqual(len(events), 2 * len(sources))
        self.assertTrue(all(event['active'] >= 0 for event in events))
        self.assertEqual(events[-1]['active'], 0)
        self.assertEqual(activity['peak'], max(event['active'] for event in events))
        for sequence in range(len(sources)):
            start, end = [event for event in events if event['sequence'] == sequence]
            self.assertEqual((start['event'], end['event']), ('start', 'end'))
            self.assertLessEqual(prepared_ns, start['monotonic_ns'])
            self.assertLessEqual(start['monotonic_ns'], end['monotonic_ns'])
        report = executor.last_close_report
        self.assertTrue(report['clean'])
        self.assertTrue(report['graph_consumer_stopped'])
        self.assertEqual(report['native_graph_parameters'], dict(batch_size=1, max_concurrency=3))
        self.assertEqual(report['http_pool_scope'], 'graph_event_loop')
        self.assertGreater(report['http_pool_count'], 0)
        self.assertTrue(report['http_pools_closed'])
        self.assertEqual(len(controls), 1)
        self.assertTrue(all(client.closed for client in controls[0]._clients.values()))
        self.assertEqual(native._DAFT_CONTROLS, {})
        record_fixture('daft-http-activity', dict(report=report,
            prepared_input_rows=len(sources), prepared_ns=prepared_ns, consumed_ns=consumed_ns,
            source_preparation='complete before iteration', result_consumption='without deliberate waits',
            observed_active_counts=sorted({event['active'] for event in events}),
            peak_active=activity['peak'], actual_posts=len(requests)))


if __name__ == '__main__':
    unittest.main()
