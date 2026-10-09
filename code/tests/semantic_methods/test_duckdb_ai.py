"""DuckDB full-batch ABI behavior and compiled native-library checks; model calls are zero."""
from __future__ import annotations

import ctypes as ct
import json
import os
import threading
import time
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from src.semantic_methods.duckdb_ai import (
    DuckDBCall, DuckDBResponse, DuckDBSemLoomBridge, DuckDBNativeTaskExecutor,
    _Call, _Response, _Cancelled, _Consume,
)


def response_body(prompt):
    return json.dumps({'model': 'fixture', 'choices': [{'message': {'content': 'out:' + prompt},
                       'finish_reason': 'length' if prompt == 'truncated' else 'stop',
                       'logprobs': {'content': [{'token': 'out', 'logprob': -0.2}]}}],
                       'usage': {'prompt_tokens': 4, 'completion_tokens': 2, 'total_tokens': 6},
                       'provider_detail': {'kept': True}}, ensure_ascii=False).encode()


class _ClosingResponses:
    def __init__(self, responses, close_error='fixture iterator close failed'):
        self.responses = iter(responses)
        self.close_error = close_error
        self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.responses)

    def close(self):
        self.close_calls += 1
        if self.close_error:
            raise RuntimeError(self.close_error)


def invoke(execute, *, cancel=False, consume=lambda *_: 0, bridge=None, batch_id=1):
    if bridge is None:
        bridge = object.__new__(DuckDBSemLoomBridge)
        bridge.last_error = bridge.last_cleanup_error = None
        bridge._buffers, bridge.batch_sizes, bridge._lock = {}, [], threading.Lock()
    bridge.execute = execute
    owners = []
    calls = (_Call * 3)()
    for i in range(3):
        body = ct.create_string_buffer(f'{{"row":{i}}}'.encode())
        owners.append(body)
        calls[i] = _Call(i, b'query', f'call-{i}'.encode(), b'fixture', b'http://localhost/model',
                         ct.addressof(body), len(body.value), None, 0, 8, 5, 5, 1)
    outputs = (_Response * 3)()
    cancelled = _Cancelled(lambda _: int(cancel))
    status = bridge._dispatch(batch_id, calls, 3, outputs, cancelled, _Consume(consume), None)
    return bridge, outputs, status


class DuckDBAbiTests(unittest.TestCase):
    def test_out_of_order_binary_complete_response_is_reassociated_before_native_parse(self):
        def execute(calls, cancelled):
            self.assertEqual([c.payload for c in calls], [b'{"row":0}', b'{"row":1}', b'{"row":2}'])
            return [DuckDBResponse(c.call_id, b'\xff\x00' + c.payload, 429, 3) for c in reversed(calls)]
        bridge, output, status = invoke(execute)
        self.assertEqual(status, 0)
        self.assertEqual([(o.row, ct.string_at(o.body, o.body_size), o.http_status) for o in output],
                         [(i, b'\xff\x00' + f'{{"row":{i}}}'.encode(), 429) for i in range(3)])
        self.assertEqual(bridge.retained_batches, 1)
        bridge._release(1)
        self.assertEqual(bridge.retained_batches, 0)

    def test_duplicate_missing_unknown_and_oversize_results_fail_the_batch(self):
        results = [
            [DuckDBResponse('call-0', b'ok', 200, 0)] * 3,
            [DuckDBResponse('unknown', b'ok', 200, 0)],
            [],
            [DuckDBResponse('call-0', b'x' * (8 * 1024 * 1024 + 1), 200, 0)],
        ]
        for result in results:
            with self.subTest(result_count=len(result)):
                bridge, _, status = invoke(lambda *_: result)
                self.assertEqual(status, 2)
                self.assertIsNotNone(bridge.last_error)
                bridge._release(1)
                self.assertEqual(bridge.retained_batches, 0)

    def test_cancel_before_dispatch_does_not_execute(self):
        called = []
        bridge, _, status = invoke(lambda *_: called.append(True), cancel=True)
        self.assertEqual((status, called), (1, []))
        bridge._release(1)

    def test_unavailable_http_elapsed_time_is_explicit(self):
        bridge, output, status = invoke(lambda calls, _: [DuckDBResponse(c.call_id, b'ok', 200, -1) for c in calls])
        self.assertEqual(status, 0)
        self.assertEqual([r.elapsed_ms for r in output], [-1] * 3)
        bridge._release(1)

    def test_native_parse_stop_closes_producer_before_another_result(self):
        seen = []
        closed = []
        def execute(calls, _):
            try:
                for call in calls:
                    seen.append(call.call_id)
                    yield DuckDBResponse(call.call_id, b'ok', 200, 0)
            finally:
                closed.append(True)
        bridge, _, status = invoke(execute, consume=lambda *_: 1)
        self.assertEqual((status, seen, closed), (3, ['call-0'], [True]))
        bridge._release(1)

    def test_close_failure_after_all_consumed_responses_fails_the_batch(self):
        iterators, consumed = [], []
        def execute(calls, _):
            iterator = _ClosingResponses([DuckDBResponse(c.call_id, b'ok', 200, 0) for c in calls])
            iterators.append(iterator)
            return iterator
        bridge, _, status = invoke(execute, consume=lambda index, *_: consumed.append(index) or 0)
        self.assertEqual(consumed, [0, 1, 2])
        self.assertEqual(iterators[0].close_calls, 1)
        self.assertEqual(status, 2)
        self.assertIn('fixture iterator close failed', bridge.last_error)
        self.assertIn('fixture iterator close failed', bridge.last_cleanup_error)
        bridge._release(1)
        self.assertEqual(bridge.retained_batches, 0)

    def test_first_dispatch_or_consumer_failure_survives_iterator_close_failure(self):
        for bad_identity, parsed, expected_status, first_error in [
                (True, 0, 2, 'changes call identity'),
                (False, 2, 2, 'consumer rejected metadata'),
                (False, 1, 3, None)]:
            with self.subTest(bad_identity=bad_identity, consumer_status=parsed):
                iterators = []
                def execute(calls, _):
                    iterator = _ClosingResponses([DuckDBResponse(
                        'unknown' if bad_identity else c.call_id, b'ok', 200, 0) for c in calls])
                    iterators.append(iterator)
                    return iterator
                bridge, _, status = invoke(execute, consume=lambda *_: parsed)
                self.assertEqual(status, expected_status)
                if first_error is None:
                    self.assertIsNone(bridge.last_error)
                else:
                    self.assertIn(first_error, bridge.last_error)
                self.assertIn('fixture iterator close failed', bridge.last_cleanup_error)
                self.assertEqual(iterators[0].close_calls, 1)
                bridge._release(1)

    def test_next_batch_clears_dispatch_and_cleanup_diagnostics(self):
        bridge, _, status = invoke(lambda *_: _ClosingResponses([DuckDBResponse('unknown', b'ok', 200, 0)]))
        self.assertEqual(status, 2)
        self.assertIsNotNone(bridge.last_error)
        self.assertIsNotNone(bridge.last_cleanup_error)
        bridge._release(1)
        bridge, _, status = invoke(lambda calls, _: [DuckDBResponse(c.call_id, b'ok', 200, 0) for c in calls],
                                   bridge=bridge, batch_id=2)
        self.assertEqual(status, 0)
        self.assertIsNone(bridge.last_error)
        self.assertIsNone(bridge.last_cleanup_error)
        bridge._release(2)

    def test_nested_batch_cannot_replace_outer_status_or_diagnostics(self):
        for outer_bad_identity, nested_close_failure in ((False, True), (True, False)):
            with self.subTest(outer_bad_identity=outer_bad_identity, nested_close_failure=nested_close_failure):
                bridge = object.__new__(DuckDBSemLoomBridge)
                bridge.last_error = bridge.last_cleanup_error = None
                bridge._buffers, bridge.batch_sizes, bridge._lock = {}, [], threading.Lock()
                nested_statuses = []
                class NestedResponses(_ClosingResponses):
                    def close(self):
                        _, _, status = invoke(lambda calls, _: _ClosingResponses([
                            DuckDBResponse(c.call_id, b'ok', 200, 0) for c in calls],
                            close_error='nested close failed' if nested_close_failure else None),
                            bridge=bridge, batch_id=2)
                        nested_statuses.append(status)
                        bridge._release(2)
                        super().close()
                def execute(calls, _):
                    return NestedResponses([DuckDBResponse(
                        'unknown' if outer_bad_identity else c.call_id, b'ok', 200, 0) for c in calls],
                        close_error=None)
                bridge, _, status = invoke(execute, bridge=bridge)
                self.assertEqual(nested_statuses, [2 if nested_close_failure else 0])
                self.assertEqual(status, 2 if outer_bad_identity else 0)
                if outer_bad_identity:
                    self.assertIsNotNone(bridge.last_error)
                    self.assertIn('changes call identity', bridge.last_error)
                else:
                    self.assertIsNone(bridge.last_error)
                self.assertIsNone(bridge.last_cleanup_error)
                bridge._release(1)
                self.assertEqual(bridge.retained_batches, 0)


class _FixtureServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(('127.0.0.1', 0), _FixtureHandler)
        self.records, self.active, self.peak = [], 0, 0
        self.lock = threading.Lock()
        self.started = threading.Event()


class _FixtureHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers['Content-Length']))
        value = json.loads(raw)
        prompt = value['messages'][-1]['content']
        with self.server.lock:
            self.server.records.append((raw, self.headers.get('Authorization')))
            self.server.active += 1
            self.server.peak = max(self.server.peak, self.server.active)
        self.server.started.set()
        try:
            time.sleep(0.2 if prompt.startswith('slow') else (0.015 if prompt.endswith('0') else 0.002))
            status = 429 if prompt == 'http-error' else 200
            body = (b'{"error":{"message":"fixture failure","code":"fixture-limit"},"extra":1}'
                    if status == 429 else b'not-json' if prompt == 'malformed' else response_body(prompt))
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('x-fixture', 'preserved')
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
        finally:
            with self.server.lock:
                self.server.active -= 1


EXTENSION = os.environ.get('DUCKDB_SEMLOOM_EXTENSION')


@unittest.skipUnless(EXTENSION, 'requires the pinned compiled DuckDB extension; no native-library claim locally')
class DuckDBNativeLibraryTests(unittest.TestCase):
    def setUp(self):
        import duckdb
        import _duckdb
        # The small extension links to the inspected Python runtime instead of embedding another engine.
        self.runtime_library = ct.CDLL(_duckdb.__file__, mode=ct.RTLD_GLOBAL)
        self.server = _FixtureServer()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f'http://127.0.0.1:{self.server.server_port}/v1/chat/completions'
        self.connection = duckdb.connect(config={'allow_unsigned_extensions': 'true', 'threads': '1'})
        self.connection.execute('LOAD ' + "'" + str(Path(EXTENSION).resolve()).replace("'", "''") + "'")
        self.assertEqual(self.connection.execute('SELECT version()').fetchone()[0], 'v1.5.4')
        self.assertEqual(self.connection.execute("SELECT extension_version FROM duckdb_extensions() "
                                                "WHERE extension_name='ai' AND loaded").fetchone()[0],
                         '0.4.14-semloom1')
        self.assertEqual(self.connection.execute("SELECT current_setting('duckdb_ai_executor')").fetchone()[0],
                         'native')
        self.connection.execute("SET duckdb_ai_provider = 'openai_compatible'")
        self.connection.execute("SET duckdb_ai_model = 'fixture'")
        for key, value in [('cache', 'false'), ('prompt_cache', 'false'), ('retry_count', '0'),
                           ('retry_backoff_ms', '0'), ('min_request_interval_ms', '0'),
                           ('max_concurrent_requests', '1'), ('timeout_seconds', '5')]:
            self.connection.execute(f'SET duckdb_ai_{key} = {value}')
        self.connection.execute("CREATE SECRET fixture (TYPE duckdb_ai, AI_PROVIDER 'openai_compatible', "
                                f"BASE_URL 'http://127.0.0.1:{self.server.server_port}/v1', API_KEY 'EMPTY')")
        self.bridge = None
        self.execution = None

    def tearDown(self):
        if self.bridge:
            self.bridge.close()
        self.connection.close()
        if self.execution:
            deadline = time.monotonic() + 6
            while self.execution.engine.capacity.usage().active_requests and time.monotonic() < deadline:
                self.execution.engine.advance()
                time.sleep(0.01)
            self.assertTrue(self.execution.close())
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()

    def select(self, prompts, *, try_complete=True):
        self.connection.execute('CREATE OR REPLACE TABLE source(id BIGINT, prompt VARCHAR)')
        self.connection.executemany('INSERT INTO source VALUES (?, ?)', list(enumerate(prompts)))
        function = 'ai_try_complete' if try_complete else 'ai_complete'
        return self.connection.execute(f'SELECT id, {function}(prompt, max_tokens := 7, temperature := 0.2, '
                                       "system_prompt := 'system text') FROM source ORDER BY id").fetchall()

    def common(self):
        from src.execution_provider.adapters.native_tasks import build_native_execution
        from src.execution_provider.adapters.model_config import FixedModelConfig
        config = FixedModelConfig(self.endpoint, 'fixture', 5000, bearer_token='EMPTY')
        self.execution = build_native_execution(config, physical=None, max_tasks=4, max_active_requests=2)
        self.adapter = DuckDBNativeTaskExecutor(self.execution, config)
        self.bridge = DuckDBSemLoomBridge(EXTENSION, self.adapter)
        self.bridge.enable(self.connection)

    def test_compiled_batch_bypasses_native_one_worker_and_guard(self):
        from src.scheduling.core.session_contract import Usage
        self.common()
        rows = self.select([f'row-{i}' for i in range(17)] + ['same', 'same', None])
        self.assertEqual([r[1]['response'] if r[1] else None for r in rows],
                         ['out:row-' + str(i) for i in range(17)] + ['out:same', 'out:same', None])
        self.assertEqual(self.bridge.batch_sizes, [19])
        self.assertEqual(self.bridge.retained_batches, 0)
        self.assertEqual(len(self.server.records), 19)
        self.assertEqual(self.server.peak, 2)
        self.assertEqual(self.execution.engine.capacity.usage(), Usage())
        self.assertEqual(self.execution.engine.jobs.jobs, {})
        self.assertEqual(self.connection.execute('SELECT SUM(prompt_tokens),SUM(completion_tokens),SUM(total_tokens) '
                                                'FROM ai_usage()').fetchone(), (76, 38, 114))

    def test_patched_native_and_semloom_use_identical_complete_messages_and_return(self):
        prompts = ['unicode \u4e2d\u6587 "quoted"\nline', 'same', 'same', None]
        native = self.select(prompts)
        native_requests = sorted(self.server.records)
        self.server.records.clear()
        self.common()
        selected = self.select(prompts)
        self.assertEqual(selected, native)
        self.assertEqual(sorted(self.server.records), native_requests)
        self.assertTrue(all(json.loads(raw)['messages'][0] == {'role': 'system', 'content': 'system text'}
                            for raw, _ in self.server.records))

    def test_original_community_binary_and_patched_native_match(self):
        import duckdb
        original = duckdb.connect(config={'threads': '1'})
        original.execute('LOAD ai')
        self.assertEqual(original.execute("SELECT extension_version FROM duckdb_extensions() "
                                          "WHERE extension_name='ai' AND loaded").fetchone()[0], '0.4.14')
        original.execute("SET duckdb_ai_provider='openai_compatible'")
        original.execute("SET duckdb_ai_model='fixture'")
        for key, value in [('cache', 'false'), ('retry_count', '0'), ('max_concurrent_requests', '1'),
                           ('timeout_seconds', '5'), ('prompt_cache', 'false'), ('min_request_interval_ms', '0')]:
            original.execute(f'SET duckdb_ai_{key}={value}')
        original.execute("CREATE SECRET fixture (TYPE duckdb_ai, AI_PROVIDER 'openai_compatible', "
                         f"BASE_URL 'http://127.0.0.1:{self.server.server_port}/v1', API_KEY 'EMPTY')")
        current = self.connection
        try:
            self.connection = original
            result = self.select(['hello', 'same', 'same', None, 'truncated', 'http-error'])
            records = sorted(self.server.records)
        finally:
            self.connection = current
            original.close()
        self.server.records.clear()
        self.assertEqual(self.select(['hello', 'same', 'same', None, 'truncated', 'http-error']), result)
        self.assertEqual(sorted(self.server.records), records)

    def test_original_parser_and_on_error_keep_provider_errors_and_truncation(self):
        prompts = ['ok', 'http-error', 'truncated', 'malformed', '']
        native = self.select(prompts)
        self.common()
        selected = self.select(prompts)
        self.assertEqual(selected, native)
        self.assertEqual(selected[0][1]['response'], 'out:ok')
        for _, row in selected[1:]:
            self.assertIsNone(row['response'])
            self.assertTrue(row['error'])
        errors = self.connection.execute("SELECT COUNT(*) FROM ai_usage() WHERE status = 'error'").fetchone()[0]
        self.assertEqual(errors, 6)  # Native and selected: three dispatched errors each; empty prompt sends nothing.
        self.connection.execute("SET duckdb_ai_executor = 'semloom'")
        with self.assertRaisesRegex(Exception, 'max_tokens'):
            self.select(['truncated'], try_complete=False)

    def test_unsupported_cache_and_retry_fail_before_external_dispatch(self):
        self.common()
        for key, value in [('cache', 'true'), ('retry_count', '1')]:
            with self.subTest(key=key):
                self.connection.execute(f'SET duckdb_ai_{key} = {value}')
                with self.assertRaisesRegex(Exception, 'requires response cache'):
                    self.select(['ok'])
                self.connection.execute(f'SET duckdb_ai_{key} = ' + ('false' if key == 'cache' else '0'))
        self.assertEqual(self.server.records, [])

    def test_mixed_models_fail_before_the_batch_callback(self):
        self.common()
        with self.assertRaisesRegex(Exception, 'one model'):
            self.connection.execute("SELECT ai_try_complete(prompt, model || '') "
                                    "FROM (VALUES ('a','fixture'),('b','other')) AS source(prompt,model)").fetchall()
        self.assertEqual(self.server.records, [])
        self.assertEqual(self.bridge.batch_sizes, [])

    def test_query_cancel_reaches_same_core_without_retry_and_releases_callback(self):
        self.common()
        def interrupt():
            self.assertTrue(self.server.started.wait(2))
            self.connection.interrupt()
        thread = threading.Thread(target=interrupt)
        thread.start()
        with self.assertRaises(Exception):
            self.select(['slow-' + str(i) for i in range(30)])
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.bridge.retained_batches, 0)
        self.assertLessEqual(len(self.server.records), 2)
        self.assertLessEqual(self.server.peak, 2)

    def test_fatal_native_parse_stops_the_unaccepted_batch_suffix(self):
        self.common()
        with self.assertRaisesRegex(Exception, 'fixture failure'):
            self.select(['http-error'] + ['slow-' + str(i) for i in range(30)], try_complete=False)
        self.assertLessEqual(len(self.server.records), 4)
        self.assertEqual(self.bridge.retained_batches, 0)

    def test_backpressure_prepares_each_complete_task_once(self):
        from src.execution_provider.adapters import native_tasks
        self.common()
        count = 13
        with patch.object(native_tasks, 'prepare_native_task', wraps=native_tasks.prepare_native_task) as prepared:
            rows = self.select(['slow-' + str(i) for i in range(count)])
        self.assertEqual(len(rows), count)
        self.assertEqual(len(self.server.records), count)
        self.assertEqual(prepared.call_count, count)

    def test_all_native_responses_consumed_before_close_failure_still_fail_sql(self):
        fail_close, iterators = [True], []
        def execute(calls, _):
            iterator = _ClosingResponses([DuckDBResponse(c.call_id, response_body(
                json.loads(c.payload)['messages'][-1]['content']), 200, 0) for c in calls],
                close_error='fixture iterator close failed' if fail_close[0] else None)
            iterators.append(iterator)
            return iterator
        self.bridge = DuckDBSemLoomBridge(EXTENSION, execute)
        self.bridge.enable(self.connection)
        for try_complete in (False, True):
            with self.subTest(try_complete=try_complete):
                before = self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0]
                fail_close[0] = True
                with self.assertRaisesRegex(Exception, 'batch executor failed'):
                    self.select(['one', 'two'], try_complete=try_complete)
                self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0], before + 2)
                self.assertEqual(iterators[-1].close_calls, 1)
                self.assertIn('fixture iterator close failed', self.bridge.last_error)
                self.assertIn('fixture iterator close failed', self.bridge.last_cleanup_error)
                self.assertEqual(self.bridge.retained_batches, 0)
                fail_close[0] = False
                rows = self.select(['clean-one', 'clean-two'], try_complete=try_complete)
                self.assertEqual([r[1]['response'] if try_complete else r[1] for r in rows],
                                 ['out:clean-one', 'out:clean-two'])
                self.assertIsNone(self.bridge.last_error)
                self.assertIsNone(self.bridge.last_cleanup_error)
                self.assertEqual(self.bridge.retained_batches, 0)

    def test_native_parser_first_error_survives_iterator_close_failure(self):
        iterators = []
        def execute(calls, _):
            iterator = _ClosingResponses([DuckDBResponse(c.call_id,
                b'{"error":{"message":"fixture first parser failure","code":"fixture-first"}}', 429, 0)
                for c in calls])
            iterators.append(iterator)
            return iterator
        self.bridge = DuckDBSemLoomBridge(EXTENSION, execute)
        self.bridge.enable(self.connection)
        with self.assertRaisesRegex(Exception, 'fixture first parser failure'):
            self.select(['first', 'second'], try_complete=False)
        self.assertEqual(self.connection.execute('SELECT COUNT(*) FROM ai_usage()').fetchone()[0], 1)
        self.assertEqual(iterators[0].close_calls, 1)
        self.assertIsNone(self.bridge.last_error)
        self.assertIn('fixture iterator close failed', self.bridge.last_cleanup_error)
        self.assertEqual(self.bridge.retained_batches, 0)


if __name__ == '__main__':
    unittest.main()
