"""Localhost HTTP and deterministic native-call executors, without vendor libraries."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from urllib import error, request
from unittest import mock

from src.baselines.text.frameworks.native_calls import NativeCallOutcome
from src.execution_provider.adapters.full_response import FullModelResponse
from src.execution_provider.adapters.sema_native import SemaNativeService
from src.execution_provider.adapters.sema_service import SemaServiceError, SemaServiceLimits


class _Executor:
    backend = 'daft'

    def __init__(self, *, response=None, reverse=False, delay=0, gate=None):
        self.response = response or (lambda call: FullModelResponse(200,
            (('Content-Type', 'application/json'), ('X-Repeat', 'first'), ('X-Repeat', 'second')),
            call.payload))
        self.reverse, self.delay, self.gate = reverse, delay, gate
        self.started = threading.Event()
        self.batches, self.produced = [], []
        self.last_close_report = None

    def iter_batch(self, calls, *, cancelled, timeout_s):
        self.last_close_report = None
        self.batches.append(tuple(calls))
        self.started.set()
        deadline, count, clean = time.monotonic() + timeout_s, 0, False
        try:
            until = time.monotonic() + self.delay
            while ((self.gate is not None and not self.gate.is_set()) or time.monotonic() < until):
                if cancelled():
                    raise InterruptedError('fixture native graph cancelled')
                if time.monotonic() >= deadline:
                    raise TimeoutError('fixture native graph deadline')
                time.sleep(0.002)
            for call in reversed(calls) if self.reverse else calls:
                if cancelled():
                    raise InterruptedError('fixture native graph cancelled')
                response = self.response(call)
                outcome = (response if type(response) is NativeCallOutcome else
                           NativeCallOutcome(call.call_id, call.row_id, response))
                self.produced.append(outcome.call_id)
                count += 1
                yield outcome
            clean = True
        finally:
            self.last_close_report = dict(graph_consumer_stopped=True, clean=clean,
                prepared=len(calls), received=count, remote_http_stopped='unavailable')


class SemaNativeServiceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='sema-native-test-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.limits = SemaServiceLimits(16, 65536, 65536, 1)
        self.payload = b' { "model":"fixture", "messages":[{"role":"user","content":"equal"}] } '

    def service(self, executor=None, **options):
        return SemaNativeService(query_id='native-test', upstream_url='http://localhost/model',
            trace_path=self.root / 'requests.jsonl', limits=self.limits, backend='daft',
            executor=executor or _Executor(), **options)

    def post(self, endpoint, payload=None):
        try:
            with request.urlopen(request.Request(endpoint, data=payload or self.payload,
                    headers={'Content-Type': 'application/json'}), timeout=4) as response:
                return response.status, response.read(), tuple(response.headers.items())
        except error.HTTPError as response:
            return response.status, response.read(), tuple(response.headers.items())

    def wait(self, condition):
        deadline = time.monotonic() + 2
        while not condition() and time.monotonic() < deadline:
            time.sleep(0.002)
        self.assertTrue(condition(), 'fixture service did not reach the expected state')

    def test_sequential_equal_requests_do_not_wait_for_future_supply(self):
        executor = _Executor()
        with self.service(executor) as service:
            for _ in range(3):
                status, body, headers = self.post(service.endpoint_url)
                self.assertEqual((status, body), (200, self.payload))
                self.assertEqual([v for k, v in headers if k == 'X-Repeat'], ['first', 'second'])
            service.end_input()
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
        calls = [call for batch in executor.batches for call in batch]
        self.assertEqual(len(calls), 3)
        self.assertEqual([call.payload for call in calls], [self.payload] * 3)
        self.assertEqual([call.row_id for call in calls], list(range(3)))
        self.assertEqual(len({call.call_id for call in calls}), 3)
        self.assertTrue(all(len(batch) == 1 for batch in executor.batches))
        self.assertEqual(service.summary['uncertain_requests'], 0)
        self.assertEqual(service.summary['last_close_report']['remote_http_stopped'], 'unavailable')
        self.assertIsNone(service.summary['forwarded_posts'])
        self.assertFalse(service.summary['native_bridge_thread_alive'])
        self.assertTrue(service.summary['io_loop_closed'])
        rows = [json.loads(line) for line in service.trace_path.read_text().splitlines()]
        for row in rows[:3]:
            clocks = [row[key] for key in ('proxy_arrived_ns', 'body_read_ns',
                'native_intake_started_ns', 'service_accepted_ns', 'native_enqueued_ns',
                'native_snapshot_ns', 'native_graph_started_ns', 'native_outcome_received_ns',
                'response_written_ns')]
            self.assertEqual(clocks, sorted(clocks))
            self.assertIsNone(row['native_task_ready_ns'])
            self.assertIsNone(row['forward_started_ns'])
            self.assertIsNone(row['model_returned_ns'])

    def test_out_of_order_results_keep_request_occurrences(self):
        gate, executor = threading.Event(), _Executor(reverse=True)
        executor.gate = gate
        bodies = [json.dumps({'value': number}).encode() for number in range(5)]
        with self.service(executor) as service, ThreadPoolExecutor(max_workers=5) as pool:
            first = pool.submit(self.post, service.endpoint_url, bodies[0])
            self.assertTrue(executor.started.wait(1))
            rest = [pool.submit(self.post, service.endpoint_url, body) for body in bodies[1:]]
            self.wait(lambda: len(service._pending) == 4)
            gate.set()
            replies = [first.result()] + [future.result() for future in rest]
            self.assertEqual([reply[:2] for reply in replies], [(200, body) for body in bodies])
            service.end_input()
        self.assertEqual([len(batch) for batch in executor.batches], [1, 4])
        calls = [call.call_id for batch in executor.batches for call in batch]
        self.assertEqual(set(calls), set(executor.produced))
        self.assertEqual(executor.produced[1:], list(reversed(calls[1:])))

    def test_raw_error_and_duplicate_headers_stop_later_graphs(self):
        raw = b' {"error":"original", "detail":[1, 2]}\n'
        executor = _Executor(response=lambda call: FullModelResponse(429,
            (('X-Repeat', 'first'), ('X-Repeat', 'second')), raw))
        cancelled = []
        with self.service(executor) as service:
            service.bind_native_cancel(lambda: cancelled.append(True))
            status, body, headers = self.post(service.endpoint_url)
            self.assertEqual((status, body), (429, raw))
            self.assertEqual([v for k, v in headers if k == 'X-Repeat'], ['first', 'second'])
            first = service.first_error
            self.assertEqual((first.status, first.body), (429, raw))
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
            self.assertIs(service.first_error, first)
        self.assertEqual(len(executor.batches), 1)
        self.assertTrue(cancelled)
        self.assertEqual(service.summary['held_requests'], 0)

    def test_uncertain_transport_is_not_a_model_http_response_or_retry(self):
        executor = _Executor(response=lambda call: NativeCallOutcome(call.call_id,
            call.row_id, None, 'fixture peer disconnected'))
        with self.service(executor) as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
            self.assertEqual(service.first_error.phase, 'transport')
            self.assertEqual(self.post(service.endpoint_url)[0], 410)
        self.assertEqual(service.summary['completed_http_responses'], 0)
        self.assertEqual(service.summary['uncertain_request_sequences'], [0])
        self.assertEqual(len(executor.batches), 1)
        self.assertEqual(service.summary['held_requests'], 0)

    def test_cancel_running_snapshot_and_queued_supply(self):
        executor = _Executor(gate=threading.Event())
        with self.service(executor, held_request_capacity=2) as service, ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.post, service.endpoint_url)
            self.assertTrue(executor.started.wait(1))
            queued = pool.submit(self.post, service.endpoint_url)
            self.wait(lambda: len(service._pending) == 1)
            service.cancel()
            self.assertEqual(first.result()[0], 502)
            self.assertEqual(queued.result()[0], 502)
        self.assertEqual(len(executor.batches), 1)
        self.assertEqual(service.summary['uncertain_request_sequences'], [0])
        self.assertEqual(service.summary['held_requests'], 0)
        self.assertTrue(service.summary['native_driver_stopped'])

    def test_deadline_includes_response_slot_wait_without_reset(self):
        self.limits = SemaServiceLimits(16, 65536, 65536, 0.26)
        executor = _Executor(delay=0.16)
        with self.service(executor, held_request_capacity=1) as service, ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.post, service.endpoint_url)
            self.assertTrue(executor.started.wait(1))
            started = time.monotonic()
            second = pool.submit(self.post, service.endpoint_url)
            self.assertEqual(first.result()[0], 200)
            from http.client import RemoteDisconnected
            with self.assertRaises(RemoteDisconnected):
                second.result()
            self.assertEqual(service.first_error.status, 502)
            self.assertLess(time.monotonic() - started, 0.45)
        self.assertEqual(len(executor.batches), 2)
        self.assertEqual(service.summary['completed_http_responses'], 1)
        self.assertEqual(service.summary['held_requests'], 0)

    def test_held_response_is_not_released_until_http_write(self):
        from aiohttp import web
        entered, gate = threading.Event(), threading.Event()
        original = web.Response.write_eof

        async def delayed(response, *args, **kwargs):
            if response.status == 200:
                entered.set()
                while not gate.is_set():
                    await asyncio.sleep(0.002)
            return await original(response, *args, **kwargs)

        executor = _Executor()
        with mock.patch.object(web.Response, 'write_eof', delayed):
            with self.service(executor, held_request_capacity=1) as service, ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(self.post, service.endpoint_url)
                self.assertTrue(entered.wait(1))
                second = pool.submit(self.post, service.endpoint_url)
                self.wait(lambda: service.summary['received_requests'] == 2)
                self.assertEqual(service.summary['held_requests'], 1)
                self.assertEqual(len(executor.batches), 1)
                self.assertNotIn('service_accepted_ns', service._rows[1])
                gate.set()
                self.assertEqual(first.result()[0], 200)
                self.assertEqual(second.result()[0], 200)
                service.end_input()
        self.assertEqual(service.summary['peak_held_requests'], 1)
        self.assertEqual(service.summary['held_requests'], 0)

    def test_bad_occurrence_cannot_be_returned_to_another_request(self):
        executor = _Executor(response=lambda call: NativeCallOutcome('changed', call.row_id,
            FullModelResponse(200, (), b'wrong')))
        with self.service(executor) as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
            self.assertEqual(service.first_error.phase, 'native_outcome')
        self.assertEqual(service.summary['completed_http_responses'], 0)
        self.assertEqual(service.summary['uncertain_requests'], 1)

    def test_snapshot_prefix_fits_bytes_and_retains_every_suffix_call(self):
        gate, executor = threading.Event(), _Executor(response=lambda call: FullModelResponse(200, (), b''))
        executor.gate = gate
        with mock.patch('src.execution_provider.adapters.sema_native._SNAPSHOT_BYTES', 512):
            with self.service(executor, snapshot_rows=4) as service, ThreadPoolExecutor(max_workers=5) as pool:
                first = pool.submit(self.post, service.endpoint_url, b'{}')
                self.assertTrue(executor.started.wait(1))
                bodies = [b'x' * 210 + bytes([65 + number]) for number in range(4)]
                rest = [pool.submit(self.post, service.endpoint_url, body) for body in bodies]
                self.wait(lambda: len(service._pending) == 4)
                gate.set()
                self.assertEqual(first.result()[0], 200)
                self.assertEqual([future.result()[0] for future in rest], [200] * 4)
                service.end_input()
        self.assertEqual([len(batch) for batch in executor.batches], [1, 2, 2])
        self.assertEqual(len(set(executor.produced)), 5)
        self.assertEqual({call.payload for batch in executor.batches for call in batch}, {b'{}', *bodies})

    def test_first_http_error_survives_native_cancel_failure(self):
        raw = b'original failure'
        executor = _Executor(response=lambda call: FullModelResponse(503, (), raw))
        with self.service(executor) as service:
            def cancel_failure():
                raise OSError('fixture native cancel cleanup')
            service.bind_native_cancel(cancel_failure)
            self.assertEqual(self.post(service.endpoint_url)[:2], (503, raw))
        self.assertEqual((service.first_error.status, service.first_error.body), (503, raw))
        self.assertIn('OSError', service.summary['cleanup_errors'])
        self.assertFalse(service.summary['native_bridge_thread_alive'])

    def test_incomplete_graph_results_stop_and_report_uncertainty(self):
        class Incomplete(_Executor):
            def iter_batch(self, calls, **kwargs):
                self.batches.append(calls)
                self.last_close_report = dict(graph_consumer_stopped=True, clean=False,
                                             remote_http_stopped='unavailable')
                yield from ()
        with self.service(Incomplete()) as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
            self.assertEqual(service.first_error.phase, 'native_graph')
        self.assertEqual(service.summary['uncertain_requests'], 1)
        self.assertFalse(service.summary['last_close_report']['clean'])
        self.assertEqual(service.summary['intake_requests'], 0)
        self.assertEqual(service.summary['intake_bytes'], 0)

    def test_cleanup_error_after_final_response_and_end_input_is_reported(self):
        gate = threading.Event()
        class LateFailure(_Executor):
            def iter_batch(self, calls, **kwargs):
                self.last_close_report = None
                yield NativeCallOutcome(calls[0].call_id, calls[0].row_id,
                                        FullModelResponse(200, (), b'last complete response'))
                gate.wait(2)
                self.last_close_report = dict(graph_consumer_stopped=True, clean=False,
                                             remote_http_stopped='unavailable')
                raise OSError('fixture cleanup after complete response')
        service = self.service(LateFailure())
        service.__enter__()
        self.assertEqual(self.post(service.endpoint_url)[:2], (200, b'last complete response'))
        service.end_input()
        timer = threading.Timer(0.03, gate.set)
        timer.start()
        try:
            with self.assertRaises(SemaServiceError) as caught:
                service.__exit__(None, None, None)
        finally:
            gate.set()
            timer.join(2)
        self.assertEqual(caught.exception.phase, 'native_graph')
        self.assertIsInstance(caught.exception.__cause__, OSError)
        self.assertIn('OSError', service.summary['cleanup_errors'])
        self.assertTrue(service.summary['native_driver_stopped'])
        self.assertTrue(service.summary['io_loop_closed'])

    def test_normal_end_input_finishes_delayed_iterator_without_abort(self):
        observed = []
        class DelayedClose(_Executor):
            def iter_batch(self, calls, *, cancelled, **kwargs):
                yield NativeCallOutcome(calls[0].call_id, calls[0].row_id,
                                        FullModelResponse(200, (), b'complete'))
                deadline = time.monotonic() + 0.06
                while time.monotonic() < deadline:
                    if cancelled():
                        observed.append('aborted')
                        self.last_close_report = dict(graph_consumer_stopped=True, clean=False)
                        raise InterruptedError('fixture was aborted during normal iterator completion')
                    time.sleep(0.002)
                observed.append('closed')
                self.last_close_report = dict(graph_consumer_stopped=True, clean=True,
                                             remote_http_stopped='unavailable')
        with self.service(DelayedClose()) as service:
            self.assertEqual(self.post(service.endpoint_url)[:2], (200, b'complete'))
            service.end_input()
        self.assertEqual(observed, ['closed'])
        self.assertIsNone(service.first_error)
        self.assertTrue(service.summary['last_close_report']['clean'])
        self.assertFalse(service.summary['native_graph_abort_requested'])

    def test_normal_end_input_keeps_iterator_cleanup_deadline_failure(self):
        class DelayedTimeout(_Executor):
            def iter_batch(self, calls, *, cancelled, **kwargs):
                yield NativeCallOutcome(calls[0].call_id, calls[0].row_id,
                                        FullModelResponse(200, (), b'complete'))
                time.sleep(0.06)
                if cancelled():
                    raise InterruptedError('fixture unexpected normal-exit abort')
                self.last_close_report = dict(graph_consumer_stopped=True, clean=False)
                raise TimeoutError('fixture delayed iterator cleanup deadline')
        service = self.service(DelayedTimeout())
        service.__enter__()
        self.assertEqual(self.post(service.endpoint_url)[0], 200)
        service.end_input()
        with self.assertRaises(SemaServiceError) as caught:
            service.__exit__(None, None, None)
        self.assertIsInstance(caught.exception.__cause__, TimeoutError)
        self.assertIn('TimeoutError', service.summary['cleanup_errors'])
        self.assertFalse(service.summary['last_close_report']['clean'])

    def test_unclean_snapshot_without_explicit_abort_is_rejected(self):
        class Unclean(_Executor):
            def iter_batch(self, calls, **kwargs):
                yield NativeCallOutcome(calls[0].call_id, calls[0].row_id,
                                        FullModelResponse(200, (), b'complete'))
                self.last_close_report = dict(graph_consumer_stopped=True, clean=False)
        with self.service(Unclean()) as service:
            self.post(service.endpoint_url)
            self.wait(lambda: service.first_error is not None)
            self.assertEqual(service.first_error.phase, 'native_graph')
        self.assertTrue(any(item['stage'] == 'native_graph_close'
                            for item in service.summary['cleanup_failures']))

    def test_iterator_close_error_preserves_the_first_http_failure(self):
        class CloseFailureIterator:
            def __init__(self, call):
                self.call, self.returned = call, False
            def __iter__(self):
                return self
            def __next__(self):
                if self.returned:
                    raise StopIteration
                self.returned = True
                return NativeCallOutcome(self.call.call_id, self.call.row_id,
                                         FullModelResponse(422, (), b'original native error'))
            def close(self):
                raise OSError('fixture iterator close')
        class CloseFailure(_Executor):
            def iter_batch(self, calls, **kwargs):
                self.last_close_report = dict(graph_consumer_stopped=True, clean=False,
                                             remote_http_stopped='unavailable')
                return CloseFailureIterator(calls[0])
        with self.service(CloseFailure()) as service:
            self.assertEqual(self.post(service.endpoint_url)[:2], (422, b'original native error'))
        self.assertEqual(service.first_error.status, 422)
        self.assertTrue(any(item['stage'] == 'native_graph_close' for item in service.summary['cleanup_failures']))
        self.assertIn('OSError', '\n'.join(service.first_error.__notes__))

    def test_close_report_access_failure_does_not_strand_the_driver(self):
        class ReportFailure:
            backend = 'daft'
            @property
            def last_close_report(self):
                raise OSError('fixture close report read')
            def iter_batch(self, calls, **kwargs):
                yield from ()
        with self.service(ReportFailure()) as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
        self.assertTrue(service.summary['native_driver_stopped'])
        self.assertFalse(service.summary['native_bridge_thread_alive'])
        self.assertTrue(service.summary['io_loop_closed'])
        self.assertEqual(service.first_error.phase, 'native_graph')
        self.assertTrue(any(item['stage'] == 'native_graph_close'
                            for item in service.summary['cleanup_failures']))

    def test_native_limits_and_imlane_do_not_silently_fall_back(self):
        for options in ({'held_request_capacity': 0}, {'held_request_capacity': 129},
                        {'snapshot_rows': 0}, {'snapshot_rows': 129}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.service(**options)
        with self.assertRaisesRegex(ValueError, 'explicit native'):
            SemaNativeService(query_id='native-test', upstream_url='http://localhost/model',
                trace_path=self.root / 'unused', limits=self.limits, backend='imlane')


if __name__ == '__main__':
    unittest.main()
