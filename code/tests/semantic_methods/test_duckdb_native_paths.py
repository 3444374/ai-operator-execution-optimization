"""DuckDB conversion and native consumer contracts; graph fixtures make no HTTP calls."""
from __future__ import annotations

from contextlib import contextmanager, redirect_stderr
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import native_calls as native
from src.execution_provider.adapters.full_response import FullModelResponse
from src.semantic_methods.duckdb_ai import DuckDBCall, DuckDBResponse
from src.semantic_methods import duckdb_native as adapter_module
from src.semantic_methods.duckdb_native import DuckDBNativeExecutor
from tests.semantic_methods.test_duckdb_ai import invoke


BACKENDS = ('daft', 'ray-data')
BODY = json.dumps({'choices': [{'message': {'content': '中文'}, 'finish_reason': 'length',
                    'logprobs': {'content': [{'token': '中文', 'logprob': -0.3}]}}],
                   'usage': {'prompt_tokens': 4, 'completion_tokens': 2, 'total_tokens': 6},
                   'provider_extra': {'retained': True}}, ensure_ascii=False).encode()


def calls(count=3):
    # Identical bytes and repeated source rows remain distinct call occurrences.
    return tuple(DuckDBCall(i // 2, 'query', f'call-{i}', 'fixture', 'http://localhost/fixture',
                 b'{ "messages" : ["same"] }',
                 ('Content-Type: application/json', 'X-Repeated: first', 'X-Repeated: second',
                  'X-Colon: a:b', 'Authorization: Bearer EMPTY'), 4, 5, 2, 1)
                 for i in range(count))


def outcome(call, *, body=BODY, status=429, transport_error='', execution_error=''):
    response = None if transport_error or execution_error else FullModelResponse(
        status, (('X-Repeated', 'one'), ('X-Repeated', 'two')), body)
    return native.NativeCallOutcome(call.call_id, call.row, response,
                                    transport_error, execution_error)


class _CloseFailure(RuntimeError):
    pass


class _BatchFixture:
    """Only a prepared batch seam fixture; never a vendor runtime substitute."""
    def __init__(self, responses, backend='daft', *, close_error=None, **limits):
        self.backend, self.responses, self.close_error = backend, responses, close_error
        self.started, self.close_calls = [], 0
        for name, value in limits.items():
            setattr(self, name, value)

    def iter_batch(self, prepared, **options):
        self.started.append((prepared, options))
        responses, fixture = iter(self.responses), self

        class Stream:
            def __iter__(self):
                return self

            def __next__(self):
                return next(responses)

            def close(self):
                fixture.close_calls += 1
                if fixture.close_error is not None:
                    raise fixture.close_error

        return Stream()


class DuckDBPreparedBatchLimitTests(unittest.TestCase):
    def test_selected_native_row_capacity_refuses_whole_vector_before_start(self):
        for backend in BACKENDS:
            graph = _BatchFixture([], backend, max_batch_rows=2)
            with self.subTest(backend=backend), self.assertRaisesRegex(ValueError, 'row'):
                list(DuckDBNativeExecutor(graph)(calls(), lambda: False))
            self.assertEqual(graph.started, [])

    def test_abi_row_capacity_refuses_whole_vector_before_start(self):
        graph = _BatchFixture([])
        with patch.object(adapter_module, 'MAX_BATCH_ROWS', 2), self.assertRaisesRegex(ValueError, 'row'):
            list(DuckDBNativeExecutor(graph)(calls(), lambda: False))
        self.assertEqual(graph.started, [])

    def test_selected_payload_capacity_and_abi_headers_refuse_before_start(self):
        selected = calls()
        payload_bytes = sum(len(call.payload) for call in selected)
        input_bytes = payload_bytes + sum(len(header.encode()) for call in selected for header in call.headers)
        graph = _BatchFixture([], max_batch_bytes=payload_bytes - 1)
        with self.assertRaisesRegex(ValueError, 'input'):
            list(DuckDBNativeExecutor(graph)(selected, lambda: False))
        self.assertEqual(graph.started, [])
        graph = _BatchFixture([])
        with patch.object(adapter_module, 'MAX_BATCH_BYTES', input_bytes - 1), self.assertRaisesRegex(ValueError, 'input'):
            list(DuckDBNativeExecutor(graph)(selected, lambda: False))
        self.assertEqual(graph.started, [])

    def test_last_invalid_header_does_not_start_a_partial_graph(self):
        selected = calls()
        selected = selected[:-1] + (replace(selected[-1], headers=('bad-header',)),)
        graph = _BatchFixture([])
        with self.assertRaisesRegex(ValueError, 'header'):
            list(DuckDBNativeExecutor(graph)(selected, lambda: False))
        self.assertEqual(graph.started, [])

    def test_out_of_order_results_are_bounded_before_the_first_row_arrives(self):
        selected = calls()
        limit = sum(len(call.payload) for call in selected)
        graph = _BatchFixture([outcome(call, body=b'x' * (limit // 2 + 1)) for call in reversed(selected)],
                              max_batch_bytes=limit)
        adapter = DuckDBNativeExecutor(graph)
        # The fixture deliberately bypasses NativeCallExecutor's own byte check.
        with self.assertRaisesRegex(ValueError, 'byte'):
            list(adapter(selected, lambda: False))
        self.assertEqual(adapter.last_delivered_count, 0)
        self.assertEqual(graph.close_calls, 1)

    def test_cumulative_results_stay_bounded_after_prior_rows_are_delivered(self):
        selected = calls()
        limit = sum(len(call.payload) + sum(len(header.encode()) for header in call.headers) for call in selected)
        graph = _BatchFixture([outcome(call, body=b'x' * (limit // 2 + 1)) for call in selected])
        adapter = DuckDBNativeExecutor(graph)
        stream = adapter(selected, lambda: False)
        with patch.object(adapter_module, 'MAX_BATCH_BYTES', limit):
            self.assertEqual(next(stream).call_id, 'call-0')
            with self.assertRaisesRegex(ValueError, 'byte'):
                next(stream)
        self.assertEqual(adapter.last_delivered_count, 1)
        self.assertEqual(graph.close_calls, 1)

    def test_response_type_size_status_and_transport_diagnostic_are_checked_before_pending(self):
        selected = calls()
        malformed = ((outcome(selected[-1], body=b'x' * 6), ValueError, 'byte'),
                     (outcome(selected[-1], body='not-bytes'), TypeError, 'body'),
                     (outcome(selected[-1], status=0), ValueError, 'status'),
                     (outcome(selected[-1], transport_error='x' * 4097), ValueError, 'diagnostics'))
        for result, error_type, message in malformed:
            graph = _BatchFixture([result])
            adapter = DuckDBNativeExecutor(graph)
            with self.subTest(message=message), patch.object(adapter_module, 'MAX_RESPONSE_BYTES', 5):
                with self.assertRaisesRegex(error_type, message):
                    list(adapter(selected, lambda: False))
            self.assertEqual(adapter.last_delivered_count, 0)
            self.assertEqual(graph.close_calls, 1)

    def test_execution_failure_keeps_type_and_redacted_cleanup_separately(self):
        selected = calls()
        graph = _BatchFixture([outcome(selected[0], execution_error=
            'OverflowError: fixture response limit api_key=fixture-private-value')],
            close_error=_CloseFailure('fixture close secret=fixture-close-value'))
        adapter = DuckDBNativeExecutor(graph)
        with self.assertRaisesRegex(OverflowError, 'fixture response limit') as caught:
            list(adapter(selected, lambda: False))
        self.assertNotIn('fixture-private-value', str(caught.exception))
        self.assertNotIn('fixture-private-value', caught.exception.native_execution_error)
        self.assertIn('fixture close', adapter.last_cleanup_error)
        self.assertNotIn('fixture-close-value', adapter.last_cleanup_error)
        self.assertEqual(graph.close_calls, 1)

    def test_execution_failure_cannot_coexist_with_an_http_response(self):
        selected = calls()
        malformed = replace(outcome(selected[0]), execution_error='ValueError: fixture failure')
        graph = _BatchFixture([malformed])
        with self.assertRaisesRegex(ValueError, 'one HTTP'):
            list(DuckDBNativeExecutor(graph)(selected, lambda: False))
        self.assertEqual(graph.close_calls, 1)

    def test_thrown_consumer_error_survives_close_failure_and_next_batch_resets(self):
        selected = calls()
        graph = _BatchFixture([outcome(call) for call in selected], close_error=_CloseFailure('fixture close'))
        adapter = DuckDBNativeExecutor(graph)
        stream = adapter(selected, lambda: False)
        next(stream)
        primary = ValueError('fixture consumer failed')
        with self.assertRaises(ValueError) as caught:
            stream.throw(primary)
        self.assertIs(caught.exception, primary)
        self.assertIn('fixture close', adapter.last_cleanup_error)
        graph.close_error = None
        self.assertEqual(len(list(adapter(selected, lambda: False))), 3)
        self.assertIsNone(adapter.last_cleanup_error)


@contextmanager
def native_consumer_fixture(backend, *, early=False, bad_identity=False, close_error=None):
    """Exercise the actual NativeCallExecutor owner with a deterministic row source."""
    prepared, closed = [], []

    class Graph:
        def __init__(self, selected, options, control):
            self.calls, self.control, self.index = selected, control, 0
            prepared.append(selected)

        def __iter__(self):
            return self

        def __next__(self):
            if early and self.index == 1:
                if not self.control.stopped.wait(1):
                    raise AssertionError('fixture consumer did not receive stop')
                raise StopIteration
            if self.index == len(self.calls):
                raise StopIteration
            call = self.calls[self.index if early else -self.index - 1]
            self.index += 1
            return native.NativeCallOutcome('unknown' if bad_identity else call.call_id, call.row_id,
                FullModelResponse(429, (('X-Repeated', 'one'), ('X-Repeated', 'two')), BODY))

        def close(self):
            closed.append(self.control.stopped.is_set())
            if close_error is not None:
                raise close_error

    executor = native.NativeCallExecutor(backend)
    with patch.object(native, '_daft_rows' if backend == 'daft' else '_ray_rows', Graph):
        yield executor, prepared, closed


class DuckDBNativeConsumerLifecycleTests(unittest.TestCase):
    def test_complete_calls_restore_order_duplicate_rows_and_exact_bytes_on_both_graph_seams(self):
        selected = calls()
        for backend in BACKENDS:
            with self.subTest(backend=backend), native_consumer_fixture(backend) as (executor, prepared, closed):
                adapter = DuckDBNativeExecutor(executor, timeout_s=2)
                returned = list(adapter(selected, lambda: False))
            self.assertEqual(returned, [DuckDBResponse(call.call_id, BODY, 429, -1) for call in selected])
            self.assertEqual(len(prepared), 1)
            self.assertEqual([call.row_id for call in prepared[0]], [0, 0, 1])
            self.assertEqual([call.payload for call in prepared[0]], [call.payload for call in selected])
            self.assertEqual(prepared[0][0].headers[1:4],
                             (('X-Repeated', 'first'), ('X-Repeated', 'second'), ('X-Colon', 'a:b')))
            self.assertEqual((prepared[0][0].timeout_s, prepared[0][0].connect_timeout_s), (5, 2))
            self.assertEqual(prepared[0][0].skip_auto_headers, ('Accept-Encoding',))
            self.assertFalse(prepared[0][0].follow_redirects)
            self.assertEqual(closed, [False])
            self.assertTrue(executor.last_close_report['clean'])

    def test_parser_stop_preserves_primary_status_and_secondary_native_cleanup(self):
        for backend in BACKENDS:
            closing = _CloseFailure('fixture native close')
            with self.subTest(backend=backend), native_consumer_fixture(backend, early=True, close_error=closing) as (executor, _, closed):
                adapter = DuckDBNativeExecutor(executor, timeout_s=2)
                bridge, _, status = invoke(adapter, consume=lambda *_: 1)
            self.assertEqual(status, 3)
            self.assertIsNone(bridge.last_error)
            self.assertIn('fixture native close', bridge.last_cleanup_error)
            self.assertIn('fixture native close', adapter.last_cleanup_error)
            self.assertEqual(adapter.last_delivered_count, 1)
            self.assertEqual(closed, [True])
            self.assertFalse(executor.last_close_report['clean'])
            self.assertTrue(executor.last_close_report['graph_consumer_stopped'])
            self.assertEqual(executor.last_close_report['remote_http_stopped'], 'unavailable')
            bridge._release(1)

    def test_native_primary_failure_keeps_its_separately_recorded_graph_close_error(self):
        selected = calls()
        for backend in BACKENDS:
            closing = _CloseFailure('fixture secondary close')
            with self.subTest(backend=backend), native_consumer_fixture(backend, early=True, bad_identity=True, close_error=closing) as (executor, _, closed):
                adapter = DuckDBNativeExecutor(executor, timeout_s=2)
                with self.assertRaisesRegex(ValueError, 'occurrence identity'):
                    list(adapter(selected, lambda: False))
            self.assertEqual(closed, [True])
            self.assertEqual(executor.last_cleanup_errors, (('graph_close', closing),))
            self.assertIn('fixture secondary close', adapter.last_cleanup_error)


@contextmanager
def ray_verifier_fixture(*, initialized=False, start_error=None, address_error=None, cleanup_failures=False):
    """Only verifier orchestration; SDK imports, SQL, HTTP and Ray startup are fixtures."""
    from integrations.duckdb_ai import verify_ray as verifier
    events = []

    def shutdown(component):
        events.append(component + '_shutdown')
        if cleanup_failures:
            raise _CloseFailure(f'fixture {component} shutdown api_key=fixture-private-{component}')

    def context():
        if address_error is not None:
            raise address_error
        return SimpleNamespace(gcs_address='127.0.0.1:12345')

    ray = SimpleNamespace(__version__='2.56.1', is_initialized=lambda: initialized,
                          get_runtime_context=context, shutdown=lambda: shutdown('ray'))
    cluster = SimpleNamespace(shutdown=lambda: shutdown('cluster'))
    modules = dict(ray=ray, daft=SimpleNamespace(__version__='0.7.21',
        set_runner_native=lambda **_: events.append('daft_runner')),
        duckdb=SimpleNamespace(__version__='1.5.4'))
    result = SimpleNamespace(testsRun=4, failures=[], errors=[], skipped=[], fixture_counts=[],
                             wasSuccessful=lambda: True)

    def start(*args):
        events.append('ray_start')
        if start_error is not None:
            raise start_error
        return cluster

    write = verifier.write_private_json
    def save(path, value):
        events.append('write_report')
        write(path, value)

    with tempfile.TemporaryDirectory(prefix='duckdb-ray-verifier-') as directory:
        root = Path(directory)
        extension, output = root / 'fixture.extension', root / 'report.json'
        extension.write_bytes(b'verifier unit fixture; no compiled extension')
        arguments = ['--ray-temp', '/tmp/dbr-short', '--output', str(output)]
        with (patch.dict(os.environ, {'DUCKDB_SEMLOOM_EXTENSION': str(extension), 'DUCKDB_FIXTURE_PORT': '0'}),
              patch.dict('sys.modules', {'daft': modules['daft']}),
              patch('src.infrastructure.daft_runtime._configured', None),
              patch.object(verifier.importlib, 'import_module', side_effect=lambda name: modules[name]) as imports,
              patch.object(verifier, 'start_ray', side_effect=start) as starter,
              patch.object(verifier.unittest.TextTestRunner, 'run', return_value=result),
              patch.object(verifier, 'write_private_json', side_effect=save),
              patch.object(verifier.os, 'sched_getaffinity', return_value={0, 1, 2, 3}, create=True),
              patch.object(verifier.os, 'sched_setaffinity', create=True)):
            yield verifier, arguments, output, events, imports, starter


class DuckDBRayVerifierTests(unittest.TestCase):
    def test_owned_cleanup_failures_are_independent_redacted_and_written_before_return(self):
        with ray_verifier_fixture(cleanup_failures=True) as (verifier, arguments, output, events, _, _):
            self.assertEqual(verifier.main(arguments), 1)
            report = json.loads(output.read_text())
        self.assertEqual(events[-3:], ['ray_shutdown', 'cluster_shutdown', 'write_report'])
        self.assertFalse(report['passed'])
        self.assertEqual(report['owned_ray_cleanup'], 'unconfirmed')
        self.assertEqual([item['component'] for item in report['cleanup_errors']], ['ray', 'cluster'])
        self.assertNotIn('fixture-private', json.dumps(report))

    def test_address_failure_closes_the_cluster_and_preserves_the_original_error(self):
        with ray_verifier_fixture(address_error=ValueError('fixture address api_key=fixture-private-address')) as (
                verifier, arguments, output, events, _, _):
            self.assertEqual(verifier.main(arguments), 1)
            report = json.loads(output.read_text())
        self.assertEqual(events[-3:], ['ray_shutdown', 'cluster_shutdown', 'write_report'])
        self.assertEqual((report['tests'], report['error_type'], report['owned_ray_cleanup']), (0, 'ValueError', 'completed'))
        self.assertIn('fixture address', report['error'])
        self.assertNotIn('fixture-private-address', report['error'])
        self.assertFalse(report['passed'])

    def test_start_failure_is_reported_and_owned_driver_cleanup_is_attempted(self):
        with ray_verifier_fixture(start_error=RuntimeError('fixture Ray start failed')) as (
                verifier, arguments, output, events, _, _):
            self.assertEqual(verifier.main(arguments), 1)
            report = json.loads(output.read_text())
        self.assertEqual(events[-2:], ['ray_shutdown', 'write_report'])
        self.assertFalse(report['owned_ray_started'])
        self.assertEqual(report['owned_ray_cleanup'], 'unconfirmed')
        self.assertIn('fixture Ray start failed', report['error'])

    def test_existing_ray_connection_is_left_with_its_caller(self):
        with ray_verifier_fixture(initialized=True) as (verifier, arguments, output, events, _, starter):
            self.assertEqual(verifier.main(arguments), 1)
            starter.assert_not_called()
            report = json.loads(output.read_text())
        self.assertEqual(events, ['write_report'])
        self.assertEqual(report['owned_ray_cleanup'], 'not_started')

    def test_fixture_port_overlap_fails_before_runtime_import_or_start(self):
        with ray_verifier_fixture() as (verifier, arguments, output, events, imports, starter):
            with patch.dict(os.environ, {'DUCKDB_FIXTURE_PORT': '43011'}), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as caught:
                    verifier.main(arguments + ['--ray-port-base', '43000'])
            self.assertEqual(caught.exception.code, 2)
            imports.assert_not_called()
            starter.assert_not_called()
            self.assertFalse(output.exists())
        self.assertEqual(events, [])

    def test_short_path_check_uses_the_same_absolute_path_passed_to_ray(self):
        resolve = Path.resolve
        def redirected(path, *args, **kwargs):
            return Path('/' + 'long-resolved-path' * 8) if path == Path('/tmp/dbr-short') else resolve(path, *args, **kwargs)
        with ray_verifier_fixture() as (verifier, arguments, output, events, _, starter):
            with patch.object(Path, 'resolve', redirected):
                self.assertEqual(verifier.main(arguments), 0)
            self.assertEqual(starter.call_args.args[1], Path('/tmp/dbr-short').absolute())
            report = json.loads(output.read_text())
        self.assertTrue(report['passed'])
        self.assertEqual(events[-3:], ['ray_shutdown', 'cluster_shutdown', 'write_report'])

    def test_connect_timeout_is_explicit_before_both_original_controls(self):
        from integrations.duckdb_ai.verify_ray import RayDuckDBChecks
        from tests.semantic_methods.test_duckdb_ai import DuckDBNativeLibraryTests
        import threading
        events = []
        case = RayDuckDBChecks('test_later_fast_failure_keeps_the_first_input_sql_error')
        def setup(value):
            value.connection = SimpleNamespace(execute=lambda sql: events.append(sql))
            value.server = SimpleNamespace(second_error_sent=threading.Event())
        with patch.object(DuckDBNativeLibraryTests, 'setUp', setup):
            case.setUp()
        self.assertEqual(events, ['SET duckdb_ai_connect_timeout_seconds=5'])
        def select(prompts, *, try_complete=True):
            events.append('original-or-selected-control')
            if not try_complete:
                raise ValueError('fixture first input error')
            return ['fixture response']
        case.select = select
        case.common = lambda: events.append('register-semloom')
        case.test_later_fast_failure_keeps_the_first_input_sql_error()
        self.assertLess(events.index('SET duckdb_ai_connect_timeout_seconds=20'),
                        events.index('original-or-selected-control'))


if __name__ == '__main__':
    unittest.main()
