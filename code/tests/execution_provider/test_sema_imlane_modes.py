"""Both public IMLane modes on localhost protocol fixtures, without author/model execution."""
from contextlib import ExitStack
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import sysconfig
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig
from src.baselines.text.frameworks.imlane_limits import IMLaneBatchLimits
from src.baselines.text.frameworks.native_calls import NativeCallExecutor
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.sema_imlane import AUTHOR_COMMIT
from src.execution_provider.adapters.sema_service import SemaServiceLimits
from src.experiments.postgresql.sema_native_query import open_native_service
from tests.execution_provider import test_sema_imlane as fixture_tests


class SemaIMLaneModeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture_tests.SemaIMLaneBridgeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.options = NativeGraphOptions(concurrency=4)
        def facts(config, _bridge):
            return dict(runtime=config.runtime, actual_coordinator_executors=config.n_executors,
                        author_commit=AUTHOR_COMMIT, evidence='protocol fixture; no author runtime')
        verifying = mock.patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator', side_effect=facts)
        self.verify = verifying.start()
        self.addCleanup(verifying.stop)

    def config(self, mode, **fixture_options):
        bridge = self.fixture.executor(**fixture_options)
        return IMLaneCallConfig(mode, str(bridge.config_path), 123 if mode == 'cpython' else 124,
                                456, str(self.fixture.root / 'author-build.json'))

    def executor(self, config):
        value = NativeCallExecutor('imlane', self.options, imlane_config=config)
        self.addCleanup(value.close)
        return value

    @staticmethod
    def arm(mode):
        return 'sema-native-imlane' + ('-ray' if mode == 'ray' else '') + '-request-service'

    def response_body(self, query):
        value = dict(id='response-' + query, model='fixture', choices=[dict(index=0,
            message=dict(role='assistant', content='"ok"'), finish_reason='length',
            logprobs=dict(content=[dict(token='ok', logprob=-0.125, bytes=[111, 107],
                                      top_logprobs=[dict(token='other', logprob=-3.0)])]))],
            usage=dict(prompt_tokens=17, completion_tokens=3, total_tokens=20,
                       prompt_tokens_details=dict(cached_tokens=5)))
        return b' \n' + json.dumps(value, ensure_ascii=False).encode() + b'\n '

    def test_public_service_modes_keep_full_responses_and_query_ownership(self):
        saved = []
        for mode in ('cpython', 'ray'):
            config = self.config(mode)
            for index in range(2):
                query = mode + '-' + str(index)
                self.fixture.server.body = self.response_body(query)
                body = self.fixture.server.body
                with ExitStack() as stack:
                    service = open_native_service(stack, self.arm(mode), query_id=query,
                        model=SimpleNamespace(endpoint_url=self.fixture.call().endpoint),
                        limits=SemaServiceLimits(4, 65536, 65536, 5),
                        trace_path=self.fixture.root / (query + '.jsonl'), options=self.options,
                        held_tasks=4, imlane_call_configs={mode: config})
                    for _ in range(2):
                        status, response, headers = self.fixture.post(service.endpoint_url)
                        self.assertEqual((status, response), (200, body))
                        self.assertEqual([v for k, v in headers if k == 'X-Repeat'], ['one', 'two'])
                        self.assertEqual(json.loads(response)['choices'][0]['logprobs']['content'][0]['logprob'], -0.125)
                        saved.append((response, body))
                    service.end_input()
                summary = service.summary
                self.assertEqual(summary['imlane_runtime'], mode)
                self.assertTrue(summary['imlane_runtime_verified'])
                self.assertEqual(summary['actual_coordinator_executors'], 2)
                self.assertFalse(summary['native_settings_are_physical_executor_count'])
                self.assertEqual(summary['uncertain_requests'], 0)
                self.assertFalse(summary['native_bridge_thread_alive'])
                self.assertEqual(summary['held_requests'], 0)
                self.assertTrue(all(row['clean'] for row in summary['native_close_reports']))
        self.assertEqual(self.fixture.server.calls, [self.fixture.raw] * 8)
        self.assertTrue(all(response == body for response, body in saved))

    def test_mode_mismatch_rejects_before_runtime_or_http(self):
        config = self.config('ray')
        with ExitStack() as stack, self.assertRaisesRegex(ValueError, 'matching ready-runtime'):
            open_native_service(stack, self.arm('cpython'), query_id='mismatched',
                model=SimpleNamespace(endpoint_url=self.fixture.call().endpoint),
                limits=SemaServiceLimits(4, 65536, 65536, 5),
                trace_path=self.fixture.root / 'mismatch.jsonl', options=self.options,
                held_tasks=4, imlane_call_configs={'cpython': config})
        self.verify.assert_not_called()
        self.assertEqual(self.fixture.server.calls, [])

    def test_duplicate_source_rows_and_http_failures_keep_complete_occurrences_in_both_modes(self):
        self.fixture.server.status = 429
        self.fixture.server.body = b' {"error":{"code":"quota","message":"original"},"usage":{"total_tokens":23}}\n'
        for mode in ('cpython', 'ray'):
            executor = self.executor(self.config(mode))
            calls = [replace(self.fixture.call(i), call_id=mode + '-' + str(i), row_id=7) for i in range(2)]
            outcomes = list(executor.iter_batch(calls))
            self.assertEqual([o.call_id for o in outcomes], [mode + '-1', mode + '-0'])
            self.assertEqual([o.row_id for o in outcomes], [7, 7])
            for outcome in outcomes:
                self.assertEqual((outcome.response.status_code, outcome.response.body),
                                 (429, self.fixture.server.body))
                self.assertEqual([v for k, v in outcome.response.headers if k == 'X-Repeat'], ['one', 'two'])
                self.assertFalse(outcome.transport_error or outcome.execution_error)
            self.assertTrue(executor.last_close_report['clean'])
            self.assertEqual(executor.last_close_report['runtime'], mode)
        self.assertEqual(self.fixture.server.calls, [self.fixture.raw] * 4)

    def test_execution_failure_preserves_category_and_blocks_another_object_in_both_modes(self):
        mutation = 'wire["response"]=None;wire["execution_error"]="TypeError: original author failure"'
        for mode in ('cpython', 'ray'):
            config = self.config(mode, mutation=mutation)
            executor = self.executor(config)
            with self.assertRaises(TypeError) as caught:
                list(executor.iter_batch((self.fixture.call(),)))
            self.assertEqual(caught.exception.native_execution_error, 'TypeError: original author failure')
            self.assertTrue(executor.last_close_report['author_submission_possible'])
            self.assertTrue(executor.last_close_report['external_coordinator_requires_cleanup'])
            with self.assertRaisesRegex(RuntimeError, 'unsettled'):
                list(self.executor(config).iter_batch((self.fixture.call(1),)))
        self.assertEqual(self.fixture.server.calls, [self.fixture.raw] * 2)

    def test_partial_consumer_close_blocks_new_object_and_keeps_remote_unknown_in_both_modes(self):
        for mode in ('cpython', 'ray'):
            config = self.config(mode, pause=True)
            executor = self.executor(config)
            iterator = executor.iter_batch((self.fixture.call(0), self.fixture.call(1)))
            self.assertEqual(next(iterator).row_id, 1)
            iterator.close()
            report = executor.last_close_report
            self.assertFalse(report['clean'])
            self.assertTrue(report['bridge_started'])
            self.assertTrue(report['bridge_stopped'])
            self.assertEqual(report['remote_http_stopped'], 'unavailable')
            self.assertFalse(report['external_coordinator_stopped'])
            self.assertTrue(report['external_coordinator_requires_cleanup'])
            with self.assertRaisesRegex(RuntimeError, 'unsettled'):
                list(self.executor(config).iter_batch((self.fixture.call(),)))
        self.assertEqual(self.fixture.server.calls, [self.fixture.raw] * 2)

    def test_started_child_with_no_returncode_blocks_new_object_in_both_modes(self):
        for mode in ('cpython', 'ray'):
            config = self.config(mode)
            executor = self.executor(config)
            executor._imlane.delegate._started = True
            read, write = os.pipe()
            child = SimpleNamespace(stdout=os.fdopen(read, 'rb', buffering=0), returncode=None,
                                    poll=lambda: None)
            os.close(write)
            self.addCleanup(child.stdout.close)
            primary = ValueError('fixture failure after creation')
            with mock.patch('src.execution_provider.adapters.sema_imlane.subprocess.Popen', return_value=child), \
                 mock.patch('src.execution_provider.adapters.sema_imlane.os.set_blocking', side_effect=primary), \
                 mock.patch.object(executor._imlane.delegate, '_stop', side_effect=OSError('stop unknown')):
                with self.assertRaises(ValueError) as caught:
                    list(executor.iter_batch((self.fixture.call(),)))
                self.assertIs(caught.exception, primary)
                report = executor.last_close_report
                self.assertIsNone(report['returncode'])
                self.assertTrue(report['bridge_started'])
                self.assertFalse(report['bridge_stopped'])
                self.assertTrue(report['author_submission_possible'])
                self.assertTrue(report['external_coordinator_requires_cleanup'])
                with self.assertRaisesRegex(RuntimeError, 'unsettled'):
                    list(self.executor(config).iter_batch((self.fixture.call(),)))
            executor._imlane.delegate._process = None  # The synthetic child never had a PID.
        self.assertEqual(self.fixture.server.calls, [])

    def test_cumulative_response_limit_rejects_before_reporting_complete_batch(self):
        response_bytes = len(encode_full_response(FullModelResponse(200, (), self.fixture.server.body))) + 1024
        limits = IMLaneBatchLimits(max_pending_calls=1, max_batch_response_bytes=response_bytes)
        executor = self.fixture.executor(limits=limits)
        self.fixture.server.body = b'x' * (response_bytes - 400)
        with self.assertRaisesRegex(ValueError, 'byte allowance'):
            list(executor.iter_batch((self.fixture.call(0), self.fixture.call(1))))
        self.assertFalse(executor.last_close_report['clean'])
        self.assertTrue(executor.poisoned)
        self.assertLess(executor.last_close_report['received'], 2)
        self.assertEqual(executor.last_close_report['bridge_limits']['max_pending_calls'], 1)


class SemaIMLaneMethodVerifierTests(unittest.TestCase):
    def test_fixed_runtime_versions_and_uninitialized_ray_are_required(self):
        from integrations.sema.verify_imlane_methods import check_runtime_versions
        identity = dict(python_version=[3, 12], ray_initialized=False,
                        modules={'pyarrow': dict(version='16.1.0')})
        check_runtime_versions(identity, {'pyarrow': '16.1.0'})
        for changes in ({'python_version': [3, 11]}, {'ray_initialized': True},
                        {'modules': {'pyarrow': dict(version='24.0.0')}}):
            with self.assertRaises(ValueError):
                check_runtime_versions(dict(identity, **changes), {'pyarrow': '16.1.0'})

    def test_ray_temp_path_is_rejected_before_runtime_start(self):
        from integrations.sema.verify_imlane_methods import check_ray_temp_path
        facts = check_ray_temp_path('/tmp/im/author')
        self.assertLessEqual(facts['selected_path_bytes'], 50)
        with self.assertRaisesRegex(ValueError, '50 bytes'):
            check_ray_temp_path('/tmp/' + 'x' * 60 + '/im/author')

    def test_public_method_client_removes_author_site_from_driver_path(self):
        from integrations.sema.verify_imlane_methods import METHOD_CLIENT
        prefix = METHOD_CLIENT.split('from src.baselines.common.private_artifacts', 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            author_site = str(root / 'author/site-packages')
            driver_site = sysconfig.get_path('purelib')
            bridge = root / 'bridge.json'
            bridge.write_text(json.dumps(dict(env={'PYTHONPATH': author_site + os.pathsep + '/code'})))
            config = root / 'config.json'
            capacities = dict(max_batch_rows=16, max_input_payload_bytes=1024,
                max_input_wire_bytes=4096, max_item_payload_bytes=256,
                max_pending_calls=2, max_batch_response_bytes=2048)
            config.write_text(json.dumps(dict(fixture_endpoint='http://localhost/fixture',
                runtime='ray', coordinator_pid=123, coordinator_start_ticks=456,
                bridge_config_path=str(bridge), **capacities,
                author_build_identity=str(root / 'author/artifacts/build-patched/build.json'))))
            with mock.patch.object(sys, 'argv', ['client', '/code', str(config), 'source', 'tokenizer', 'result']), \
                 mock.patch.object(sys, 'path', [author_site, driver_site, '/code']), \
                 mock.patch.dict(os.environ, {'PYTHONPATH': author_site + os.pathsep + driver_site,
                    'LD_LIBRARY_PATH': str(root / 'author/lib') + os.pathsep + '/usr/lib',
                    'PATH': str(root / 'author/venv/bin') + os.pathsep + '/usr/bin'}):
                namespace = {}
                exec(compile(prefix, 'method-client-prefix', 'exec'), namespace)
                self.assertNotIn(author_site, sys.path)
                self.assertNotIn(author_site, os.environ['PYTHONPATH'].split(os.pathsep))
                self.assertIn(driver_site, sys.path)
                self.assertEqual(os.environ['LD_LIBRARY_PATH'], '/usr/lib')
                self.assertNotIn(str(root / 'author/venv/bin'), os.environ['PATH'].split(os.pathsep))
                self.assertEqual(os.environ['PATH'].split(os.pathsep)[0], str(Path(sys.executable).parent))
                self.assertEqual(json.loads(bridge.read_text())['env']['PYTHONPATH'],
                                 author_site + os.pathsep + '/code')
                selected = IMLaneCallConfig(**namespace['value'])
                self.assertEqual(vars(selected.batch_limits), capacities)

    def test_preflight_failure_is_retained_before_any_fixture_or_author(self):
        from integrations.sema import verify_imlane_methods as verifier
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'failed'
            argv = ['--output', str(output)]
            for name in ('author-root','bridge-binary','lotus-python','lotus-native-source',
                         'tokenizer','duckdb-extension','sema-binary'):
                argv += ['--' + name, str(root / name)]
            with mock.patch.object(verifier, 'method_runtime_preflight', side_effect=ValueError('fixed runtime differs')), \
                 mock.patch.object(verifier.fixture_module, 'HTTPFixture') as fixture, \
                 mock.patch.object(verifier.fixture_module, 'check_mode') as author, \
                 mock.patch.dict(os.environ, {}):
                with self.assertRaisesRegex(ValueError, 'fixed runtime differs'):
                    verifier.main(argv)
                fixture.assert_not_called()
                author.assert_not_called()
            failure = json.loads((output / 'public-method-error.json').read_text())
            self.assertEqual(failure['type'], 'ValueError')
            self.assertEqual(failure['models'], 0)
            self.assertEqual(failure['gpu_resources'], 0)
        compile(verifier.METHOD_CLIENT, 'imlane-public-method-client', 'exec')
        compile(verifier._RUNTIME_PROBE, 'imlane-runtime-probe', 'exec')


if __name__ == '__main__':
    unittest.main()
