"""DuckDB/IMLane contract fixtures and separate real-author SQL cases; models are zero."""
from __future__ import annotations

from dataclasses import asdict, replace
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

from integrations.duckdb_ai import verify_native
from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig
from src.baselines.text.frameworks.native_calls import NativeCall, NativeCallExecutor, NativeCallOutcome
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.full_response import FullModelResponse
from src.semantic_methods.duckdb_ai import DuckDBCall
from src.semantic_methods.duckdb_native import DuckDBNativeBridge, DuckDBNativeExecutor
from tests.semantic_methods import test_duckdb_ai as existing
from tests.semantic_methods.test_duckdb_native import _ExpandedFixture


class DuckDBIMLaneContractTests(unittest.TestCase):
    """Real public factory/adapter, synthetic delegate; no author process or HTTP."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='duckdb-imlane-contract-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        bridge = self.root / 'bridge.json'
        bridge.write_text(json.dumps(dict(author_commit='072f8257db80569e42c9f5727d839587df529652',
            bridge_binary=str(self.root / 'unused'), bridge_sha256='0' * 64, gateway_port=1,
            udf_path=str(self.root / 'unused.py'), udf_sha256='0' * 64, work_root=str(self.root),
            namespace='duckdb-contract', native_task_threads=4)))
        self.config = IMLaneCallConfig('cpython', str(bridge), 123, 456, str(self.root / 'build.json'))
        self.verifying = patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator',
            side_effect=lambda config, _: dict(runtime=config.runtime, evidence='synthetic contract only'))
        self.verify = self.verifying.start()
        self.addCleanup(self.verifying.stop)
        self.calls = tuple(DuckDBCall(5, 'query', 'occurrence-' + str(i), 'fixture',
            'http://localhost/unused', b' {"messages":[{"content":"same"}],"seed":3} ',
            ('Content-Type: application/json', 'X-Repeat: one', 'X-Repeat: two'), 9, 5, 2, 17)
            for i in range(2))

    def graph(self, config=None, **limits):
        graph = NativeCallExecutor('imlane', NativeGraphOptions(concurrency=4),
                                   imlane_config=config or self.config, **limits)
        self.addCleanup(graph.close)
        return graph

    def outcomes(self, calls):
        body = b'{"error":{"message":"kept"},"usage":{"total_tokens":6},"logprobs":[-0.2]}\x00'
        return [NativeCallOutcome(c.call_id, c.row_id,
                FullModelResponse(429, (('X-Repeat', 'one'), ('X-Repeat', 'two')), body))
                for c in reversed(calls)]

    def stream(self, graph, *, clean=True, error=None):
        seen = []

        def responses(calls, **options):
            seen.append((calls, options))
            try:
                yield from self.outcomes(calls)
                if error is not None:
                    raise error
            finally:
                graph._imlane.delegate.last_close_report = dict(
                    clean=clean, bridge_started=True, bridge_stopped=clean,
                    returncode=0 if clean else None, received=len(calls),
                    remote_http_stopped='unavailable')

        probe = patch.object(graph._imlane.delegate, 'iter_batch', responses)
        probe.start()
        self.addCleanup(probe.stop)
        return seen

    def test_two_author_modes_keep_full_calls_and_duplicate_rows_through_public_factory(self):
        for mode in ('cpython', 'ray'):
            with self.subTest(mode=mode):
                graph = self.graph(replace(self.config, runtime=mode))
                seen = self.stream(graph)
                results = list(DuckDBNativeExecutor(graph)(self.calls, lambda: False))
                sent = seen[0][0]
                self.assertEqual([r.call_id for r in results], [c.call_id for c in self.calls])
                self.assertEqual([(c.row_id, c.payload, c.headers, c.timeout_s, c.connect_timeout_s,
                                   c.skip_auto_headers) for c in sent],
                    [(5, self.calls[0].payload, (('Content-Type', 'application/json'),
                      ('X-Repeat', 'one'), ('X-Repeat', 'two')), 5, 2, ('Accept-Encoding',))] * 2)
                expected = self.outcomes(sent)[0].response
                self.assertEqual([(r.http_status, r.body) for r in results],
                                 [(expected.status_code, expected.body)] * 2)
                self.assertEqual(graph.last_close_report['runtime'], mode)
                self.assertIs(graph._imlane.limits, graph._imlane.delegate.limits)
                self.assertEqual(graph._imlane.limits.max_pending_calls, 128)
                self.assertTrue(graph.last_close_report['clean'])

    def test_configured_row_and_payload_limits_refuse_before_author_verification(self):
        for limits in (dict(max_batch_rows=1), dict(max_batch_bytes=1)):
            with self.subTest(limits=limits):
                graph = self.graph(**limits)
                with self.assertRaisesRegex(ValueError, 'capacity|allowance'):
                    list(DuckDBNativeExecutor(graph)(self.calls, lambda: False))
                self.assertIsNone(graph._imlane.delegate.last_close_report)
                self.assertFalse(graph._imlane._uncertain.exists())
        self.verify.assert_not_called()

    def test_private_configuration_carries_all_six_capacities_through_public_factory(self):
        config = replace(self.config, max_batch_rows=32, max_input_payload_bytes=8192,
                         max_input_wire_bytes=16384, max_item_payload_bytes=4096,
                         max_pending_calls=7, max_batch_response_bytes=1024)
        path = self.root / 'public-call.json'
        path.write_text(json.dumps(asdict(config)))
        loaded = verify_native.imlane_config('imlane-cpython', path)
        self.assertEqual(loaded, config)
        graph = self.graph(loaded)
        self.assertEqual(graph._imlane.limits, config.batch_limits)
        self.assertIs(graph._imlane.delegate.limits, graph._imlane.limits)
        self.assertEqual(graph._imlane.limits.cpp_arguments(), ['32', '8192', '16384', '4096', '7', '1024'])
        self.verify.assert_not_called()

    def test_cancel_before_graph_submission_does_not_create_author_work(self):
        graph = self.graph()
        with self.assertRaises(InterruptedError):
            list(DuckDBNativeExecutor(graph)(self.calls, lambda: True))
        self.verify.assert_not_called()
        self.assertIsNone(graph.last_close_report)
        self.assertFalse(graph._imlane._uncertain.exists())

    def test_unknown_stop_keeps_primary_error_and_blocks_a_new_public_factory_owner(self):
        graph = self.graph()
        primary = TimeoutError('synthetic author stop is unconfirmed')
        self.stream(graph, clean=False, error=primary)
        with self.assertRaises(TimeoutError) as caught:
            list(DuckDBNativeExecutor(graph)(self.calls, lambda: False))
        self.assertIs(caught.exception, primary)
        self.assertIsNone(graph.last_close_report['returncode'])
        self.assertTrue(graph.last_close_report['bridge_started'])
        self.assertTrue(graph.last_close_report['author_submission_possible'])
        self.assertFalse(graph.last_close_report['clean'])
        self.assertTrue(graph.last_close_report['external_coordinator_requires_cleanup'])
        self.assertEqual(graph.last_close_report['remote_http_stopped'], 'unavailable')
        self.assertTrue(graph._imlane._uncertain.is_file())
        another = self.graph()
        with self.assertRaisesRegex(RuntimeError, 'unsettled'):
            list(DuckDBNativeExecutor(another)(self.calls, lambda: False))


class DuckDBIMLaneVerifierTests(unittest.TestCase):
    def test_missing_exit_and_missing_submission_state_are_never_reported_reusable(self):
        state = verify_native.author_close_state([dict(bridge_started=True, clean=True,
                                                       bridge_stopped=True, returncode=None)])
        self.assertIs(state['author_cleanup_required'], True)
        self.assertIs(state['author_runtime_reusable'], False)
        self.assertIsNone(verify_native.author_close_state([dict(clean=True)])['author_runtime_reusable'])
        self.assertIsNone(verify_native.author_close_state([])['author_runtime_reusable'])
        state = verify_native.author_close_state([dict(bridge_started=True, clean=True,
                                                       bridge_stopped=True, returncode=0)])
        self.assertIs(state['author_runtime_reusable'], True)

    def test_configuration_requires_the_selected_author_mode(self):
        with tempfile.TemporaryDirectory(prefix='duckdb-imlane-config-') as directory:
            root = Path(directory)
            config = dict(runtime='cpython', bridge_config_path=str(root / 'bridge.json'),
                          coordinator_pid=1, coordinator_start_ticks=2,
                          author_build_identity=str(root / 'identity.json'))
            path = root / 'call.json'
            path.write_text(json.dumps(config))
            self.assertEqual(verify_native.imlane_config('imlane-cpython', path).runtime, 'cpython')
            with self.assertRaisesRegex(ValueError, 'mode differs'):
                verify_native.imlane_config('imlane-ray', path)

    def test_port_selection_uses_the_pinned_cluster_contract_and_owns_only_its_node(self):
        ray = types.SimpleNamespace(init=unittest.mock.Mock(), shutdown=unittest.mock.Mock())
        cluster = types.SimpleNamespace(add_node=unittest.mock.Mock(),
            shutdown=unittest.mock.Mock(), address='127.0.0.1:46320')
        constructor = unittest.mock.Mock(return_value=cluster)
        with patch.dict('sys.modules', {'ray.cluster_utils': types.SimpleNamespace(Cluster=constructor)}):
            self.assertIs(verify_native.start_ray(ray, Path('/tmp/dray'), 46320), cluster)
        options = cluster.add_node.call_args.kwargs
        self.assertEqual(options['gcs_server_port'], 46320)
        self.assertEqual((options['min_worker_port'], options['max_worker_port']), (46330, 46419))
        self.assertEqual(options['num_gpus'], 0)
        self.assertEqual(ray.init.call_args.kwargs['address'], cluster.address)
        self.assertNotIn('node_manager_port', ray.init.call_args.kwargs)
        cluster.shutdown.assert_not_called()
        for base in (True, 1023, 65437):
            with self.subTest(base=base), self.assertRaises(ValueError):
                verify_native.ray_node_ports(base)

    def test_ray_startup_first_error_survives_both_owned_cleanup_attempts(self):
        primary = RuntimeError('synthetic startup failure')
        ray = types.SimpleNamespace(init=unittest.mock.Mock(side_effect=primary),
                                    shutdown=unittest.mock.Mock(side_effect=OSError('synthetic disconnect failure')))
        cluster = types.SimpleNamespace(add_node=unittest.mock.Mock(),
            shutdown=unittest.mock.Mock(), address='127.0.0.1:46320')
        with patch.dict('sys.modules', {'ray.cluster_utils': types.SimpleNamespace(Cluster=lambda: cluster)}):
            with self.assertRaises(RuntimeError) as caught:
                verify_native.start_ray(ray, Path('/tmp/dray'), 46320)
        self.assertIs(caught.exception, primary)
        cluster.shutdown.assert_called_once_with()
        self.assertIn('owned Ray startup cleanup failed: OSError', primary.__notes__)

    def test_cli_imports_only_duckdb_for_author_modes_and_preserves_existing_reports(self):
        for backend in ('imlane-cpython', 'imlane-ray'):
            with self.subTest(backend=backend), tempfile.TemporaryDirectory(prefix='duckdb-imlane-main-') as directory:
                root = Path(directory)
                extension = root / 'ai.duckdb_extension'
                extension.write_bytes(b'synthetic extension identity; never loaded')
                output = root / 'result.json'
                suite = unittest.TestSuite([unittest.FunctionTestCase(lambda: None)])
                imports = unittest.mock.Mock(return_value=types.SimpleNamespace(__version__='1.5.4'))
                argv = ['--extension', str(extension), '--backend', backend,
                        '--imlane-config', str(root / 'config.json'), '--output', str(output)]
                config = IMLaneCallConfig(verify_native.IMLANE_MODES[backend], str(root / 'bridge.json'),
                                         1, 2, str(root / 'identity.json'))
                with patch.object(verify_native, 'imlane_config', return_value=config), \
                     patch.object(verify_native, 'author_build_versions', return_value=dict(evidence='synthetic only')), \
                     patch.object(verify_native, 'load_suite', return_value=suite), \
                     patch.dict(os.environ, {}), patch.object(sys, 'stderr', new=io.StringIO()), \
                     patch.object(verify_native.importlib, 'import_module', imports):
                    self.assertEqual(verify_native.main(argv), 0)
                    imports.assert_called_once_with('duckdb')
                    report = json.loads(output.read_text())
                    self.assertEqual(report['imlane_runtime'], verify_native.IMLANE_MODES[backend])
                    self.assertEqual(report['scheduler_owner'], verify_native.SCHEDULER_OWNERS[backend])
                    self.assertEqual(report['remote_http_stopped'], 'unavailable')
                    self.assertEqual(report['real_model_requests'], 0)
                    self.assertEqual(output.stat().st_mode & 0o777, 0o600)
                    with self.assertRaises(FileExistsError):
                        verify_native.main(argv)


class _IMLaneFixture(_ExpandedFixture):
    def send_header(self, key, value):
        super().send_header(key, value)
        if key == 'Content-Type':
            super().send_header('X-Repeat', 'one')
            super().send_header('X-Repeat', 'two')


@unittest.skipUnless(existing.EXTENSION, 'requires pinned extension and explicit ready IMLane configuration')
class _IMLaneSQLBase(unittest.TestCase):
    config = None
    select = existing.DuckDBNativeLibraryTests.select

    def setUp(self):
        if self.config is None:
            self.skipTest('select an author mode/config through verify_native.py')
        existing.DuckDBNativeLibraryTests.setUp(self)
        self.server.RequestHandlerClass = _IMLaneFixture
        self.server.total_requests = 0
        self.server.second_error_sent = threading.Event()
        self.graph = None
        self.graph_observations = []
        self.unsupported = []

    def native(self):
        self.graph = NativeCallExecutor('imlane', NativeGraphOptions(concurrency=8),
                                       max_batch_rows=2048, imlane_config=self.config)
        self.adapter = DuckDBNativeExecutor(self.graph, timeout_s=60)
        self.bridge = DuckDBNativeBridge(existing.EXTENSION, self.adapter)
        self.bridge.enable(self.connection)
        self.assertEqual(self.connection.execute("SELECT current_setting('duckdb_ai_executor')").fetchone()[0], 'imlane')

    def tearDown(self):
        try:
            if self.bridge:
                self.assertEqual(self.bridge.retained_batches, 0)
            existing.DuckDBNativeLibraryTests.tearDown(self)
        finally:
            if self.graph:
                self.graph.close()

    def assert_unknown_author_stop(self):
        close = self.graph.last_close_report
        self.assertTrue(close['bridge_started'])
        self.assertFalse(close['clean'])
        self.assertTrue(close['external_coordinator_requires_cleanup'])
        self.assertEqual(close['remote_http_stopped'], 'unavailable')
        self.assertTrue(self.graph._imlane._uncertain.is_file())
        self.graph_observations.append(dict(event='author-work-unconfirmed', close_report=dict(close)))
        with self.assertRaisesRegex(RuntimeError, 'unsettled'):
            self.graph._imlane.start()


class DuckDBIMLaneCompleteLibraryTests(_IMLaneSQLBase):
    def test_original_sql_parser_http_json_usage_and_null_duplicate_rows_match(self):
        prompts = ['unicode 中文 "quoted"\nline', 'same', 'same', None, 'ok', 'http-error',
                   'http-not-json', 'truncated', 'malformed', 'missing-choice', 'null-content',
                   'not-object', '', 'gzip-if-accepted', 'redirect']
        control = self.select(prompts)
        sent = sorted(self.server.records)
        before = self.connection.execute('SELECT SUM(prompt_tokens),SUM(completion_tokens),SUM(total_tokens), '
            "COUNT(*) FILTER (WHERE status='error') FROM ai_usage()").fetchone()
        self.server.records.clear()
        self.native()
        selected = self.select(prompts)
        self.assertEqual(selected, control)
        self.assertEqual(sorted(self.server.records), sent)
        after = self.connection.execute('SELECT SUM(prompt_tokens),SUM(completion_tokens),SUM(total_tokens), '
            "COUNT(*) FILTER (WHERE status='error') FROM ai_usage()").fetchone()
        self.assertEqual(after, tuple(value * 2 for value in before))
        self.assertTrue(self.graph.last_close_report['clean'])
        rows = self.select(['one', 'transport-drop', 'three'])
        self.assertEqual([rows[0][1]['response'], rows[2][1]['response']], ['out:one', 'out:three'])
        self.assertIsNone(rows[1][1]['response'])
        self.assertTrue(rows[1][1]['error'])
        self.assertTrue(self.graph.last_close_report['clean'])

    def test_complete_response_retains_duplicate_headers_status_body_usage_and_logprobs(self):
        self.native()
        calls = tuple(NativeCall('full-' + str(i), 5,
            json.dumps({'messages': [{'content': 'same'}]}).encode(), self.endpoint,
            (('Content-Type', 'application/json'), ('X-Repeat', 'one'), ('X-Repeat', 'two')),
            5, connect_timeout_s=2, skip_auto_headers=('Accept-Encoding',)) for i in range(2))
        outcomes = list(self.graph.iter_batch(calls, timeout_s=30))
        self.assertEqual({outcome.call_id for outcome in outcomes}, {'full-0', 'full-1'})
        for outcome in outcomes:
            self.assertEqual(outcome.row_id, 5)
            self.assertEqual(outcome.response.status_code, 200)
            self.assertEqual(outcome.response.body, existing.response_body('same'))
            self.assertEqual([value for key, value in outcome.response.headers if key.lower() == 'x-repeat'], ['one', 'two'])
            decoded = json.loads(outcome.response.body)
            self.assertEqual(decoded['usage']['total_tokens'], 6)
            self.assertEqual(decoded['choices'][0]['logprobs']['content'][0]['logprob'], -0.2)
        self.assertTrue(self.graph.last_close_report['clean'])
        self.assertEqual(self.graph.last_close_report['runtime'], self.config.runtime)

    def test_resident_sql_occurrences_and_declared_vector_capacity(self):
        self.native()
        for count in (8, 8):
            prompts = ['same'] * count
            self.assertEqual([row[1]['response'] for row in self.select(prompts)], ['out:same'] * count)
            self.assertTrue(self.graph.last_close_report['clean'])
        limits = self.graph._imlane.limits
        if limits.max_batch_rows >= 2048:
            prompts = ['same' if i % 13 == 0 else f'vector-{i}' for i in range(2051)]
            self.assertEqual([row[1]['response'] for row in self.select(prompts)], ['out:' + p for p in prompts])
            self.assertGreater(len(self.bridge.batch_sizes), 3)
            self.assertTrue(self.graph.last_close_report['clean'])
        else:
            before = self.server.total_requests
            with self.assertRaisesRegex(Exception, 'capacity|allowance'):
                self.select(['same'] * (limits.max_batch_rows + 1))
            self.assertEqual(self.server.total_requests, before)
            self.assertFalse(self.graph._imlane._uncertain.exists())
            self.unsupported.append(f'2048-row DuckDB vectors exceed configured row capacity {limits.max_batch_rows}')


class DuckDBIMLaneParserStopLibraryTests(_IMLaneSQLBase):
    def test_original_fatal_parser_error_keeps_unknown_author_work_and_refuses_reuse(self):
        with self.assertRaises(Exception) as original:
            self.select(['http-error'], try_complete=False)
        self.native()
        with self.assertRaises(type(original.exception)) as selected:
            self.select(['http-error'] + [f'slow-{i}' for i in range(8)], try_complete=False)
        self.assertEqual(str(selected.exception), str(original.exception))
        self.assert_unknown_author_stop()


class DuckDBIMLaneCancelLibraryTests(_IMLaneSQLBase):
    def test_sql_cancel_closes_callback_and_retains_unknown_author_ownership(self):
        self.native()
        query_finished = threading.Event()
        interrupted = []

        def interrupt():
            while not self.server.started.wait(0.01):
                if query_finished.is_set():
                    return
            interrupted.append(True)
            self.connection.interrupt()

        thread = threading.Thread(target=interrupt)
        thread.start()
        try:
            with self.assertRaises(Exception):
                self.select([f'slow-{i}' for i in range(8)])
        finally:
            query_finished.set()
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(interrupted, [True])
        self.assertEqual(self.bridge.retained_batches, 0)
        self.assert_unknown_author_stop()


def sql_suite(config, case):
    selected = {'complete': DuckDBIMLaneCompleteLibraryTests,
                'parser-stop': DuckDBIMLaneParserStopLibraryTests,
                'cancel': DuckDBIMLaneCancelLibraryTests}[case]
    selected.config = config
    return unittest.defaultTestLoader.loadTestsFromTestCase(selected)


if __name__ == '__main__':
    unittest.main()
