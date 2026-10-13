"""Native graph conversion and compiled DuckDB SQL checks with zero model requests."""
from __future__ import annotations

from dataclasses import dataclass, replace
import gzip
import json
import os
import socket
import sys
import threading
import time
import types
import unittest
from unittest.mock import patch

from src.semantic_methods.duckdb_ai import DuckDBCall, DuckDBResponse, DuckDBSemLoomBridge
from src.semantic_methods.duckdb_native import DuckDBNativeBridge, DuckDBNativeExecutor
from tests.semantic_methods import test_duckdb_ai as existing


@dataclass(frozen=True)
class _CallValue:
    call_id: str
    row_id: int
    payload: bytes
    endpoint: str
    headers: tuple
    timeout_s: float
    connect_timeout_s: float
    max_response_bytes: int
    skip_auto_headers: tuple = ()


class _Graph:
    backend = 'daft'

    def __init__(self, responses, *, close_error=None):
        self.responses = responses
        self.close_error = close_error
        self.calls = []
        self.stream = None

    def iter_batch(self, calls, **options):
        self.calls.append((calls, options))
        self.stream = existing._ClosingResponses(self.responses, self.close_error)
        return self.stream


class DuckDBNativeConversionTests(unittest.TestCase):
    def setUp(self):
        self.calls = tuple(DuckDBCall(i, 'query', f'call-{i}', 'fixture', 'http://localhost/model',
                          b'{"messages": [{"content":"same"}],"seed":3}',
                          ('Content-Type: application/json', 'Authorization: Bearer EMPTY',
                           'User-Agent: native-fixture'), 9, 5, 2, 17) for i in range(3))
        # This isolated value substitute tests only conversion, never claims a native graph run.
        self.module = patch.dict(sys.modules, {
            'src.baselines.text.frameworks.native_calls': types.SimpleNamespace(NativeCall=_CallValue)})
        self.module.start()
        self.addCleanup(self.module.stop)

    @staticmethod
    def outcome(call, *, body=b'not-json\x00\xff', status=429, error=''):
        response = None if error else types.SimpleNamespace(body=body, status_code=status)
        return types.SimpleNamespace(call_id=call.call_id, row_id=call.row,
                                     response=response, transport_error=error)

    def test_complete_payload_headers_timeout_and_reverse_association_are_preserved(self):
        graph = _Graph([self.outcome(c) for c in reversed(self.calls)])
        result = list(DuckDBNativeExecutor(graph)(self.calls, lambda: False))
        self.assertEqual([r.call_id for r in result], ['call-0', 'call-1', 'call-2'])
        self.assertEqual([(r.body, r.http_status, r.elapsed_ms) for r in result],
                         [(b'not-json\x00\xff', 429, -1)] * 3)
        for sent, original in zip(graph.calls[0][0], self.calls):
            self.assertEqual((sent.call_id, sent.row_id, sent.payload, sent.endpoint,
                              sent.timeout_s, sent.connect_timeout_s),
                             (original.call_id, original.row, original.payload, original.endpoint, 5, 2))
            self.assertEqual(sent.headers, (('Content-Type', 'application/json'),
                                          ('Authorization', 'Bearer EMPTY'), ('User-Agent', 'native-fixture')))
            self.assertEqual(sent.skip_auto_headers, ('Accept-Encoding',))
        self.assertEqual(graph.stream.close_calls, 1)

    def test_transport_failure_is_a_single_native_call_result(self):
        outcomes = [self.outcome(self.calls[0]), self.outcome(self.calls[1], error='fixture socket failed'),
                    self.outcome(self.calls[2], status=200)]
        result = list(DuckDBNativeExecutor(_Graph(outcomes))(self.calls, lambda: False))
        self.assertEqual(result[1], DuckDBResponse('call-1', b'', 0, -1, 'fixture socket failed'))
        self.assertEqual(len(result), 3)

    def test_invalid_results_and_incomplete_batches_close_once(self):
        valid = self.outcome(self.calls[0])
        for outcomes in ([valid, valid], [replace(self.calls[0], row=8)], [],
                         [types.SimpleNamespace(call_id='unknown', row_id=0)],
                         [types.SimpleNamespace(call_id='call-0', row_id=0, response=None, transport_error='')]):
            graph = _Graph(outcomes)
            with self.subTest(count=len(outcomes)), self.assertRaises((ValueError, AttributeError)):
                list(DuckDBNativeExecutor(graph)(self.calls, lambda: False))
            self.assertEqual(graph.stream.close_calls, 1)

    def test_cancel_before_graph_and_after_first_response_closes_late_owner(self):
        graph = _Graph([self.outcome(c) for c in self.calls])
        adapter = DuckDBNativeExecutor(graph)
        with self.assertRaises(InterruptedError):
            list(adapter(self.calls, lambda: True))
        self.assertEqual(graph.calls, [])
        cancelled = [False]
        iterator = adapter(self.calls, lambda: cancelled[0])
        next(iterator)
        cancelled[0] = True
        with self.assertRaises(InterruptedError):
            next(iterator)
        self.assertEqual(graph.stream.close_calls, 1)

    def test_cancel_during_restored_prefix_stops_its_pending_suffix(self):
        graph = _Graph([self.outcome(c) for c in reversed(self.calls)])
        adapter = DuckDBNativeExecutor(graph)
        cancelled = [False]
        stream = adapter(self.calls, lambda: cancelled[0])
        self.assertEqual(next(stream).call_id, 'call-0')
        cancelled[0] = True
        with self.assertRaises(InterruptedError):
            next(stream)
        self.assertEqual(adapter.last_delivered_count, 1)
        self.assertEqual(graph.stream.close_calls, 1)

    def test_native_fatal_stop_closes_graph_without_consuming_suffix(self):
        graph = _Graph([self.outcome(c) for c in self.calls])
        adapter = DuckDBNativeExecutor(graph)
        bridge, _, status = existing.invoke(adapter, consume=lambda *_: 1)
        self.assertEqual(status, 3)
        self.assertEqual(graph.stream.close_calls, 1)
        self.assertEqual(next(graph.stream).call_id, 'call-1')
        bridge._release(1)

    def test_execution_first_error_survives_cleanup_and_next_batch_resets(self):
        graph = _Graph([types.SimpleNamespace(call_id='unknown', row_id=0)], close_error='fixture close failed')
        adapter = DuckDBNativeExecutor(graph)
        with self.assertRaisesRegex(ValueError, 'identity'):
            list(adapter(self.calls, lambda: False))
        self.assertIn('fixture close failed', adapter.last_cleanup_error)
        graph.responses = [self.outcome(c) for c in self.calls]
        graph.close_error = None
        self.assertEqual(len(list(adapter(self.calls, lambda: False))), 3)
        self.assertIsNone(adapter.last_cleanup_error)

    def test_native_parser_first_error_survives_graph_close_failure(self):
        graph = _Graph([self.outcome(c) for c in self.calls], close_error='fixture close failed')
        bridge, _, status = existing.invoke(DuckDBNativeExecutor(graph), consume=lambda *_: 1)
        self.assertEqual(status, 3)
        self.assertIsNone(bridge.last_error)
        self.assertIn('fixture close failed', bridge.last_cleanup_error)
        bridge._release(1)

    def test_close_failure_after_successful_results_fails_the_batch(self):
        graph = _Graph([self.outcome(c) for c in self.calls], close_error='fixture close failed')
        adapter = DuckDBNativeExecutor(graph)
        bridge, _, status = existing.invoke(adapter)
        self.assertEqual(status, 2)
        self.assertIn('fixture close failed', adapter.last_cleanup_error)
        self.assertIn('fixture close failed', bridge.last_error)
        self.assertEqual(graph.stream.close_calls, 1)
        bridge._release(1)

    def test_backend_and_wall_timeout_reject_invalid_values(self):
        graph = _Graph([])
        graph.backend = 'semloom'
        with self.assertRaisesRegex(ValueError, 'daft, ray, ray-data or imlane'):
            DuckDBNativeExecutor(graph)
        graph.backend = 'imlane'
        self.assertEqual(DuckDBNativeExecutor(graph).backend, 'imlane')
        graph.backend = 'daft'
        for timeout in (0, -1, 121, True, float('inf'), float('nan')):
            with self.subTest(timeout=timeout), self.assertRaisesRegex(ValueError, 'timeout'):
                DuckDBNativeExecutor(graph, timeout_s=timeout)


class _ExpandedFixture(existing._FixtureHandler):
    def do_POST(self):
        raw = self.rfile.read(int(self.headers['Content-Length']))
        value = json.loads(raw)
        prompt = value['messages'][-1]['content']
        with self.server.lock:
            self.server.records.append((raw, self.headers.get('Authorization')))
            self.server.total_requests += 1
            if not hasattr(self.server, 'header_records'):
                self.server.header_records = []
            self.server.header_records.append((self.headers.get('Accept-Encoding'), self.headers.get('User-Agent')))
            self.server.active += 1
            self.server.peak = max(self.server.peak, self.server.active)
        self.server.started.set()
        try:
            time.sleep(0.3 if prompt.startswith('slow') else 0.01 if prompt.endswith('0') else 0.001)
            status, body = 200, existing.response_body(prompt)
            encoding = None
            if prompt == 'http-error':
                status, body = 429, b'{"error":{"message":"fixture failure","code":"fixture-limit"},"extra":1}'
            elif prompt in ('slow-first-error', 'fast-second-error'):
                if prompt == 'slow-first-error':
                    if not self.server.second_error_sent.wait(15):
                        raise RuntimeError('dual-error fixture did not observe its second request')
                    time.sleep(0.05)
                status, body = 429, json.dumps({'error': {'message': prompt, 'code': 'fixture-order'}}).encode()
            elif prompt == 'http-not-json':
                status, body = 503, b'fixture unavailable'
            elif prompt == 'malformed':
                body = b'not-json'
            elif prompt == 'missing-choice':
                body = b'{"model":"fixture","choices":[]}'
            elif prompt == 'null-content':
                body = b'{"choices":[{"message":{"content":null},"finish_reason":"stop"}]}'
            elif prompt == 'not-object':
                body = b'["fixture"]'
            elif prompt == 'forced-gzip' or (prompt == 'gzip-if-accepted'
                                               and 'gzip' in self.headers.get('Accept-Encoding', '')):
                body = gzip.compress(body)
                encoding = 'gzip'
            elif prompt == 'redirect':
                status, body = 302, b'{"error":{"message":"fixture redirect"}}'
            elif prompt == 'transport-drop':
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            if encoding:
                self.send_header('Content-Encoding', encoding)
            if prompt == 'redirect':
                self.send_header('Location', f'http://127.0.0.1:{self.server.server_port}/v1/redirect-target')
            self.end_headers()
            try:
                self.wfile.write(body)
                if prompt == 'fast-second-error':
                    self.server.second_error_sent.set()
            except (BrokenPipeError, ConnectionResetError):
                pass
        finally:
            with self.server.lock:
                self.server.active -= 1


BACKEND = os.environ.get('DUCKDB_NATIVE_BACKEND')


@unittest.skipUnless(existing.EXTENSION and BACKEND in ('daft', 'ray', 'ray-data'),
                     'requires pinned compiled extension and an explicit native graph backend')
class DuckDBNativeGraphLibraryTests(unittest.TestCase):
    select = existing.DuckDBNativeLibraryTests.select

    def setUp(self):
        existing.DuckDBNativeLibraryTests.setUp(self)
        self.server.RequestHandlerClass = _ExpandedFixture
        self.server.total_requests = 0
        self.server.second_error_sent = threading.Event()

    def native(self):
        from src.baselines.text.frameworks.native_calls import NativeCallExecutor
        from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
        self.server.RequestHandlerClass = _ExpandedFixture
        self.graph = NativeCallExecutor(BACKEND, NativeGraphOptions(concurrency=4), max_batch_rows=2048)
        self.graph_observations = []
        self.adapter = DuckDBNativeExecutor(self.graph, timeout_s=60)
        self.bridge = DuckDBNativeBridge(existing.EXTENSION, self.adapter)
        self.bridge.enable(self.connection)
        self.assertEqual(self.connection.execute("SELECT current_setting('duckdb_ai_executor')").fetchone()[0],
                         BACKEND)

    def observe_graph(self, event):
        with self.server.lock:
            self.graph_observations.append(dict(event=event, http_requests=len(self.server.records),
                                               http_active=self.server.active, http_peak=self.server.peak,
                                               close_report=self.graph.last_close_report))

    def wait_fixture_idle(self):
        deadline = time.monotonic() + 5
        while self.server.active and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(self.server.active, 0)
        self.observe_graph('fixture-idle')

    def replace_graph_after_fixture_cleanup(self):
        # This proof is specific to the owned deterministic server. Production
        # code retains unknown remote work and never resets the executor flag.
        self.wait_fixture_idle()
        previous = self.graph.last_close_report
        self.bridge.close()
        self.graph.close()
        self.native()
        self.graph_observations.append(dict(event='fixture-owner-replaced-graph', previous=previous))

    def tearDown(self):
        if self.bridge:
            self.assertEqual(self.bridge.retained_batches, 0)
        existing.DuckDBNativeLibraryTests.tearDown(self)

    def test_native_defaults_community_control_and_original_complete_request_values(self):
        existing.DuckDBNativeLibraryTests.test_original_community_binary_and_patched_native_match(self)
        self.server.records.clear()
        self.server.RequestHandlerClass = _ExpandedFixture
        prompts = ['unicode 中文 "quoted"\nline', 'same', 'same', None]
        control = self.select(prompts)
        sent = sorted(self.server.records)
        self.server.records.clear()
        self.native()
        self.assertEqual(self.select(prompts), control)
        self.assertEqual(sorted(self.server.records), sent)
        self.assertEqual(self.bridge.batch_sizes, [3])

    def test_original_parser_http_json_truncation_and_usage_are_kept(self):
        self.server.RequestHandlerClass = _ExpandedFixture
        prompts = ['ok', 'http-error', 'http-not-json', 'truncated', 'malformed',
                   'missing-choice', 'null-content', 'not-object', '']
        control = self.select(prompts)
        sent = sorted(self.server.records)
        before = self.connection.execute('SELECT SUM(prompt_tokens),SUM(completion_tokens),SUM(total_tokens), '
                                         "COUNT(*) FILTER (WHERE status='error') FROM ai_usage()").fetchone()
        self.server.records.clear()
        self.native()
        selected = self.select(prompts)
        after = self.connection.execute('SELECT SUM(prompt_tokens),SUM(completion_tokens),SUM(total_tokens), '
                                        "COUNT(*) FILTER (WHERE status='error') FROM ai_usage()").fetchone()
        self.assertEqual(selected, control)
        self.assertEqual(sorted(self.server.records), sent)
        self.assertEqual(after, tuple(v * 2 for v in before))
        self.assertIsNone(self.bridge.last_error)
        for prompt in ('http-error', 'malformed', 'missing-choice', 'truncated'):
            self.connection.execute("SET duckdb_ai_executor='native'")
            try:
                self.select([prompt], try_complete=False)
            except Exception as error:
                native_error = str(error)
            else:
                self.fail('fixture expected original parser error')
            self.bridge.enable(self.connection)
            with self.assertRaises(Exception) as caught:
                self.select([prompt], try_complete=False)
            self.assertEqual(str(caught.exception), native_error)

    def test_transport_error_stays_per_row_for_try_complete(self):
        self.native()
        rows = self.select(['one', 'transport-drop', 'three'])
        self.assertEqual([rows[0][1]['response'], rows[2][1]['response']], ['out:one', 'out:three'])
        self.assertIsNone(rows[1][1]['response'])
        self.assertTrue(rows[1][1]['error'])
        self.assertEqual(len(self.server.records), 3)
        self.assertIsNone(self.bridge.last_error)
        self.assertFalse(self.graph.last_close_report['clean'])
        self.assertTrue(self.graph.last_close_report['unconfirmed_work'])
        before = self.server.total_requests
        with self.assertRaisesRegex(Exception, 'unconfirmed work'):
            self.select(['later'])
        self.assertEqual(self.server.total_requests, before)

    def test_fast_second_failure_keeps_first_input_error_and_try_usage(self):
        prompts = ['slow-first-error', 'fast-second-error']
        self.connection.execute('SET duckdb_ai_max_concurrent_requests=4')
        self.connection.execute('SET duckdb_ai_timeout_seconds=20')
        before = self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0]
        with self.assertRaisesRegex(Exception, 'slow-first-error') as control:
            self.select(prompts, try_complete=False)
        native_failed_usage = self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0] - before
        self.assertEqual(native_failed_usage, 2)
        self.server.second_error_sent.clear()
        before = self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0]
        control_try = self.select(prompts)
        native_try_usage = self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0] - before
        self.native()
        self.server.second_error_sent.clear()
        before = self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0]
        with self.assertRaises(type(control.exception)) as selected:
            self.select(prompts, try_complete=False)
        self.assertEqual(str(selected.exception), str(control.exception))
        self.assertEqual(self.adapter.last_delivered_count, 1)
        selected_failed_usage = self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0] - before
        self.graph_observations.append(dict(event='matched-parallel-fatal-usage',
                                           native_failed_usage=native_failed_usage,
                                           selected_failed_usage=selected_failed_usage,
                                           close_report=self.graph.last_close_report))
        self.server.second_error_sent.clear()
        before = self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0]
        self.assertEqual(self.select(prompts), control_try)
        selected_try_usage = self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0] - before
        self.assertEqual(selected_try_usage, native_try_usage)
        self.assertEqual(selected_try_usage, 2)

    def test_provider_failure_before_later_preparation_failure_keeps_first_input_error(self):
        native_errors = []
        for prompts in (['http-error', ''], ['ok', ''], ['', 'http-error']):
            before = self.server.total_requests
            with self.assertRaises(Exception) as error:
                self.select(prompts, try_complete=False)
            native_errors.append((type(error.exception), str(error.exception),
                                  self.server.total_requests - before))
        self.native()
        for prompts, (error_type, message, requests) in zip(
                (['http-error', ''], ['ok', ''], ['', 'http-error']), native_errors):
            before = self.server.total_requests
            with self.subTest(prompts=prompts), self.assertRaises(error_type) as error:
                self.select(prompts, try_complete=False)
            self.assertEqual(str(error.exception), message)
            self.assertEqual(self.server.total_requests - before, requests)

    def test_original_gzip_and_redirect_behavior_is_preserved(self):
        self.server.RequestHandlerClass = _ExpandedFixture
        prompts = ['gzip-if-accepted', 'redirect']
        control = self.select(prompts)
        native_headers = list(self.server.header_records)
        self.assertTrue(all(encoding is None for encoding, _ in native_headers))
        self.server.records.clear()
        self.server.header_records.clear()
        self.native()
        self.assertEqual(self.select(prompts), control)
        self.assertEqual(self.server.header_records, native_headers)
        self.assertEqual(len(self.server.records), len(prompts))
        self.assertEqual(control[0][1]['response'], 'out:gzip-if-accepted')
        self.assertIsNone(control[1][1]['response'])
        self.assertIn('fixture redirect', control[1][1]['error'])
        for try_complete in (True, False):
            self.connection.execute("SET duckdb_ai_executor='native'")
            try:
                self.select(['forced-gzip'], try_complete=try_complete)
            except Exception as error:
                native_error = str(error)
                native_type = type(error)
            else:
                self.fail('original parser expected a compressed-body error')
            self.bridge.enable(self.connection)
            with self.assertRaises(native_type) as caught:
                self.select(['forced-gzip'], try_complete=try_complete)
            self.assertEqual(str(caught.exception), native_error)

    def test_consecutive_sql_and_multiple_vectors_preserve_occurrence_identity(self):
        self.native()
        for count in (8, 8, 2051):
            before = len(self.server.records)
            prompts = ['same' if i % 13 == 0 else f'vector-{count}-{i}' for i in range(count)]
            rows = self.select(prompts)
            self.assertEqual([r[1]['response'] for r in rows], ['out:' + p for p in prompts])
            self.assertEqual(len(self.server.records) - before, count)
            self.assertEqual(self.bridge.retained_batches, 0)
        self.assertGreater(len(self.bridge.batch_sizes), 3)

    def test_executor_identity_and_unsupported_settings_fail_before_requests(self):
        self.native()
        self.connection.execute("SET duckdb_ai_executor='semloom'")
        with self.assertRaisesRegex(Exception, 'registered for another executor'):
            self.select(['one'])
        self.bridge.enable(self.connection)
        for key, value, reset in [('cache', 'true', 'false'), ('retry_count', '1', '0'),
                                  ('min_request_interval_ms', '1', '0'), ('token_limit_per_minute', '1', '0')]:
            with self.subTest(key=key):
                self.connection.execute(f'SET duckdb_ai_{key}={value}')
                with self.assertRaisesRegex(Exception, 'requires response cache'):
                    self.select(['one'])
                self.connection.execute(f'SET duckdb_ai_{key}={reset}')
        with self.assertRaisesRegex(Exception, 'one model'):
            self.connection.execute("SELECT ai_try_complete(prompt,model||'') "
                                    "FROM (VALUES ('a','fixture'),('b','other')) AS t(prompt,model)").fetchall()
        self.assertEqual(self.server.records, [])

    def test_fatal_parser_stops_result_consumer_and_following_sql_is_clean(self):
        self.native()
        with self.assertRaisesRegex(Exception, 'fixture failure'):
            self.select(['http-error'] + [f'slow-{i}' for i in range(32)], try_complete=False)
        self.assertEqual(self.bridge.retained_batches, 0)
        self.assertIsNone(self.bridge.last_error)
        # Native graphs may have submitted work before the first result is observable.
        # The assertion is on native parser consumption, never a SemLoom accepted prefix.
        consumed, errors = self.connection.execute(
            "SELECT COUNT(*),COUNT(*) FILTER (WHERE status='error') FROM ai_usage()").fetchone()
        self.assertEqual(errors, 1)
        self.assertEqual(consumed, self.adapter.last_delivered_count)
        self.assertLessEqual(consumed, 33)
        self.observe_graph('native-parser-stop')
        self.replace_graph_after_fixture_cleanup()
        self.assertEqual(self.select(['clean'])[0][1]['response'], 'out:clean')

    def test_real_graph_cleanup_failure_and_native_first_error_are_separate(self):
        self.native()
        original = self.graph.iter_batch
        closed = []

        class FaultAfterClose:
            def __init__(self, stream):
                self.stream = stream

            def __iter__(self):
                return self

            def __next__(self):
                return next(self.stream)

            def close(self):
                self.stream.close()
                closed.append(True)
                raise RuntimeError('fixture fault after real native graph close')

        def fault(*args, **kwargs):
            return FaultAfterClose(original(*args, **kwargs))

        for try_complete in (True, False):
            with self.subTest(try_complete=try_complete), patch.object(self.graph, 'iter_batch', fault):
                with self.assertRaisesRegex(Exception, 'batch executor failed'):
                    self.select(['one', 'two'], try_complete=try_complete)
                self.assertIn('after real native graph close', self.bridge.last_error)
                self.assertIn('after real native graph close', self.adapter.last_cleanup_error)
        self.wait_fixture_idle()
        with patch.object(self.graph, 'iter_batch', fault):
            with self.assertRaisesRegex(Exception, 'fixture failure'):
                self.select(['http-error'], try_complete=False)
        self.assertIsNone(self.bridge.last_error)
        self.assertIn('after real native graph close', self.bridge.last_cleanup_error)
        self.assertEqual(len(closed), 3)
        self.assertEqual(self.select(['after-fault'])[0][1]['response'], 'out:after-fault')
        self.assertIsNone(self.bridge.last_error)
        self.assertIsNone(self.bridge.last_cleanup_error)
        self.assertIsNone(self.adapter.last_cleanup_error)

    def test_query_cancel_late_responses_close_callback_and_next_query(self):
        self.native()
        triggered = []
        query_finished = threading.Event()
        def interrupt():
            while not self.server.started.wait(0.01):
                if query_finished.is_set():
                    return
            triggered.append(True)
            self.connection.interrupt()
        thread = threading.Thread(target=interrupt)
        thread.start()
        try:
            with self.assertRaises(Exception):
                self.select([f'slow-{i}' for i in range(40)])
        finally:
            query_finished.set()
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(triggered, [True])
        self.assertEqual(self.bridge.retained_batches, 0)
        self.observe_graph('query-cancel')
        self.replace_graph_after_fixture_cleanup()
        self.assertEqual(self.select(['after-cancel'])[0][1]['response'], 'out:after-cancel')


if __name__ == '__main__':
    unittest.main()
