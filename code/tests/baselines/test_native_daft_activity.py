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

    def test_minimal_official_async_udf_keeps_complete_map_calls_and_responses(self):
        import aiohttp
        import daft
        from src.execution_provider.semantic_map import SemanticMapPlan
        from src.experiments.postgresql.native_adapter_query import prepare_fixed_calls
        from tests.baselines.test_native_calls import complete_server

        prepared = prepare_fixed_calls(tuple(dict(row_id=str(i), text='duplicate fixture text')
            for i in range(16)), SemanticMapPlan('fixture instruction', 'fixture', 16), lambda _: None)
        body = b'{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":2,"completion_tokens":1}}'
        options = NativeGraphOptions(concurrency=4)
        with complete_server(body=body, delay=.01) as (endpoint, calls, _):
            frame = daft.from_pydict(dict(row_id=[row['row_id'] for row in prepared],
                payload=[row['payload'].decode() for row in prepared]))
            started = time.perf_counter()

            @daft.func.batch(return_dtype=daft.DataType.string(), use_process=False,
                            batch_size=1, max_concurrency=options.concurrency - 1,
                            max_retries=0, on_error='raise')
            async def complete(payloads):
                if len(payloads) != 1:
                    raise ValueError('fixture expects one complete call per native batch')
                async with aiohttp.ClientSession(auto_decompress=False,
                        timeout=aiohttp.ClientTimeout(total=5)) as session:
                    async with session.post(endpoint, data=payloads.to_pylist()[0].encode(),
                            headers={'Content-Type': 'application/json', 'Accept-Encoding': 'identity'},
                            allow_redirects=False) as response:
                        full = native.FullModelResponse(response.status,
                            tuple((name.decode('ascii'), value.decode('latin-1'))
                                  for name, value in response.raw_headers), await response.read(),
                            f'HTTP/{response.version.major}.{response.version.minor}')
                return daft.Series.from_pylist([base64.b64encode(native.encode_full_response(full)).decode()])

            result = frame.with_column('response', complete(daft.col('payload'))).select('row_id', 'response')
            iterator = iter(result.iter_rows(results_buffer_size=1))
            try:
                returned = list(iterator)
            finally:
                iterator.close()
            elapsed = time.perf_counter() - started
            self.assertEqual(len(returned), len(prepared))
            self.assertEqual({row['row_id'] for row in returned}, {row['row_id'] for row in prepared})
            responses = [native.decode_full_response(base64.b64decode(row['response'], validate=True))
                         for row in returned]
            self.assertTrue(all(response.status_code == 200 and response.body == body
                                and response.http_version == 'HTTP/1.1' for response in responses))
            self.assertTrue(all([value for name, value in response.headers if name.lower() == 'x-repeated']
                                == ['one', 'two'] for response in responses))
            self.assertEqual(sorted(payload for _, payload, _ in calls),
                             sorted(row['payload'] for row in prepared))
            self.assertEqual((len(calls), calls.active), (16, 0))
            record_fixture('minimal-official-daft-http', dict(actual_posts=len(calls),
                peak_active=calls.peak, batch_size=1, max_concurrency=options.concurrency - 1,
                execution_seconds=elapsed, complete_response_bytes_preserved=True,
                session_scope='one aiohttp session per native UDF batch',
                timing_scope='async UDF definition, graph application, full iterator consumption and close; source/runtime preparation excluded',
                observation_scope='localhost server handlers through complete response write'))

    def test_sdk_execution_failure_keeps_primary_when_iterator_close_also_fails(self):
        import daft
        from tests.baselines.test_native_call_detailed_timing import observations
        from tests.baselines.test_native_calls import complete_server
        original = daft.DataFrame.iter_rows
        class ClosingIterator:
            def __init__(self, rows):
                self.rows = iter(rows)
            def __iter__(self):
                return self
            def __next__(self):
                return next(self.rows)
            def close(self):
                self.rows.close()
                raise OSError('fixture SDK iterator close')
        for enabled in (False, True):
            with self.subTest(enabled=enabled), observations(enabled), \
                    complete_server(body=b'oversized') as (endpoint, requests, _), \
                    patch.object(daft.DataFrame, 'iter_rows',
                        lambda frame, **kwargs: ClosingIterator(original(frame, **kwargs))):
                executor = native.NativeCallExecutor('daft')
                with self.assertRaises(Exception) as caught:
                    list(executor.iter_batch((call(endpoint, maximum=2),)))
                self.assertNotIsInstance(caught.exception, OSError)
                self.assertIn('byte allowance', str(caught.exception))
                self.assertIn('OSError', '\n'.join(getattr(caught.exception, '__notes__', ())))
                self.assertFalse(native._DAFT_CONTROLS)
                self.assertFalse(executor.last_close_report['reuse_allowed'])
                record_fixture('daft-sdk-cleanup', dict(timing_enabled=enabled,
                    primary_type=type(caught.exception).__name__,
                    secondary_notes=getattr(caught.exception, '__notes__', ()),
                    actual_posts=len(requests), controls_after_close=len(native._DAFT_CONTROLS),
                    report=executor.last_close_report))

    def test_prepared_calls_overlap_with_greedy_consumption_and_keep_full_results(self):
        controls = []
        control_factory = native._GraphControl
        client_factory, client_close = native._new_http_client, native._HttpClient.close
        created, create_seconds, close_seconds = [], [], []

        def measured_client():
            started = time.perf_counter()
            client = client_factory()
            create_seconds.append(time.perf_counter() - started)
            created.append(client)
            return client

        async def measured_close(client):
            started = time.perf_counter()
            try:
                await client_close(client)
            finally:
                close_seconds.append(time.perf_counter() - started)

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
            with patch.object(native, '_GraphControl', capture_control), \
                    patch.object(native, '_new_http_client', measured_client), \
                    patch.object(native._HttpClient, 'close', measured_close):
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
        self.assertEqual(len(created), report['http_pool_count'])
        self.assertEqual(len(close_seconds), len(created))
        self.assertLess(len(created), len(sources), 'native calls did not reuse graph-loop clients')
        self.assertEqual(native._DAFT_CONTROLS, {})
        record_fixture('daft-http-activity', dict(report=report,
            prepared_input_rows=len(sources), prepared_ns=prepared_ns, consumed_ns=consumed_ns,
            source_preparation='complete before iteration', result_consumption='without deliberate waits',
            lifecycle=dict(client_creations=len(created), client_closes=len(close_seconds),
                client_create_seconds=create_seconds, client_close_seconds=close_seconds,
                timing_scope='individual actual client factory and awaited close calls; no cross-run subtraction'),
            observed_active_counts=sorted({event['active'] for event in events}),
            peak_active=activity['peak'], actual_posts=len(requests)))


if __name__ == '__main__':
    unittest.main()
