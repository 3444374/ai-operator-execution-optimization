"""Both public IMLane modes on localhost protocol fixtures, without author/model execution."""
from contextlib import ExitStack, contextmanager
from dataclasses import replace
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import sysconfig
import tempfile
import threading
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock

from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig
from src.baselines.text.frameworks.imlane_limits import IMLaneBatchLimits
from src.baselines.text.frameworks.native_calls import (
    NativeCall, NativeCallExecutor, NativeCallOutcome, _call_wire, _outcome_wire,
)
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
    def test_driver_symlink_keeps_its_selected_virtual_environment(self):
        from integrations.sema import verify_imlane_methods as verifier
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            driver = root/'driver/bin/python'
            driver.parent.mkdir(parents=True)
            driver.symlink_to(sys.executable)
            args = verifier.parse_arguments(['--methods','sema','--author-root',str(root/'author'),
                '--output',str(root/'output'),'--bridge-binary',str(root/'bridge'),
                '--driver-python',str(driver),'--sema-binary',str(root/'Sema')])
            self.assertEqual(args.lotus_python, driver.absolute())
            self.assertNotEqual(args.lotus_python, driver.resolve())

    def test_ray_address_is_published_only_after_complete_resource_rpc(self):
        from integrations.sema import verify_imlane_calls as helper
        captured = []
        def capture(command, _env, _log, **_options):
            captured.append(command)
            raise RuntimeError('capture only; no owner process started')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(output=root, author_root=root/'author',
                ray_address='127.0.0.1:46520', coordinator_ports={'ray':46651})
            owner_helper = SimpleNamespace(spawn=capture, resources=lambda _prefix:{})
            affinity = mock.patch.object(helper.os, 'sched_getaffinity', return_value=set(range(8,16)), create=True)
            info = json.dumps(dict(site=str(root/'site'),lib=str(root/'lib'),base=str(root/'base')))
            with affinity, mock.patch.object(helper.subprocess, 'check_output', side_effect=[info,str(root/'libstdc++.so.6')]), \
                 mock.patch.dict(sys.modules, {'psutil':ModuleType('psutil')}):
                report = helper.check_mode(args, 'ray', owner_helper, SimpleNamespace(receipts=[]))
            self.assertFalse(report['passed'])
            self.assertEqual(len(captured), 1)
            command = captured[0]
            address = Path(command[4])
            started, release = threading.Event(), threading.Event()
            ray = ModuleType('ray')
            ray.init = mock.Mock(return_value=SimpleNamespace(address_info=dict(gcs_address='127.0.0.1:46520')))
            def resources():
                started.set()
                if not release.wait(3):
                    raise TimeoutError('fixture resource RPC did not release')
                return {'CPU':2,'GPU':0}
            ray.cluster_resources, ray.shutdown = resources, mock.Mock()
            failures = []
            def execute_owner():
                try:
                    exec(compile(command[2], 'actual-ray-owner-ready-code', 'exec'), {})
                except BaseException as error:
                    failures.append(error)
            with mock.patch.dict(sys.modules, {'ray':ray}), \
                 mock.patch.object(sys, 'argv', [command[0],command[3],command[4]]), \
                 mock.patch.object(sys, 'stdin', io.StringIO('\n')):
                thread = threading.Thread(target=execute_owner)
                thread.start()
                try:
                    self.assertTrue(started.wait(3))
                    self.assertFalse(address.exists(), 'final address became visible while resource RPC was blocked')
                finally:
                    release.set()
                    thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(json.loads(address.read_text()),
                             dict(address='127.0.0.1:46520',resources={'CPU':2,'GPU':0}))
            self.assertEqual(list(address.parent.glob(address.name+'*')), [address])
            ray.shutdown.assert_called_once()

    def test_worker_trace_checks_actual_author_libraries_and_two_workers_and_actors(self):
        from integrations.sema import verify_imlane_methods as verifier
        modules = {'pyarrow': dict(version='16.1.0', source_sha256='a'*64),
                   'aiohttp': dict(version='3.14.5', source_sha256='b'*64),
                   'src.baselines.text.frameworks.native_calls': dict(source_sha256=
                       verifier.fixture_module.digest(verifier.CODE/'src/baselines/text/frameworks/native_calls.py'))}
        preflight = dict(author=dict(modules={name:dict(sha256=modules[name]['source_sha256'])
                                             for name in ('pyarrow','aiohttp')}))
        rows = [dict(kernel_sha256=verifier.fixture_module.digest(verifier.CODE/'integrations/sema/imlane_complete_call.py'),
            runtime=dict(python_version=[3,12,1],python_prefix='/author',python_executable='/author/bin/python',
                         modules=copy.deepcopy(modules)), status=200,worker_pid=100+i,ray_actor_id='actor-'+str(i))
            for i in range(2)]
        report = dict(runtime='ray', kernel_traces=rows)
        self.assertTrue(verifier.check_author_runtime_traces(report, preflight)['passed'])
        for field, bad in (('kernel_sha256','old'), ('worker_pid',100), ('ray_actor_id','actor-0')):
            changed = copy.deepcopy(report)
            changed['kernel_traces'][1][field] = bad
            with self.assertRaises(ValueError):
                verifier.check_author_runtime_traces(changed, preflight)
        for name, field, bad in (('pyarrow','version','24.0.0'), ('aiohttp','version','3.14.3'),
                                ('pyarrow','source_sha256','old'),
                                ('src.baselines.text.frameworks.native_calls','source_sha256','old')):
            changed = copy.deepcopy(report)
            changed['kernel_traces'][1]['runtime']['modules'][name][field] = bad
            with self.assertRaises(ValueError):
                verifier.check_author_runtime_traces(changed, preflight)
        self.assertTrue(verifier.check_author_runtime_traces(dict(runtime='cpython',kernel_traces=rows),preflight)['passed'])

    def test_kernel_trace_observes_loaded_modules_without_changing_complete_response(self):
        from integrations.sema import verify_imlane_methods as verifier
        def table(columns):
            return {key:[SimpleNamespace(as_py=lambda value=value:value) for value in values]
                    for key, values in columns.items()}
        call = NativeCall('trace-fixture',0,b'{}','http://localhost/fixture',(),1)
        response = NativeCallOutcome(call.call_id,call.row_id,FullModelResponse(200,(),b'unchanged'))
        result = table(dict(native_outcome=[_outcome_wire(response)]))
        original = ModuleType('integrations.sema.imlane_http_udf')
        original.complete_http = mock.Mock(return_value=result)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            loaded = {}
            for name, version in (('pyarrow','16.1.0'),('aiohttp','3.14.5')):
                source = root/(name+'.py');source.write_text('fixture loaded module\n')
                loaded[name] = ModuleType(name)
                loaded[name].__file__, loaded[name].__version__ = str(source), version
            path = verifier.CODE/'integrations/sema/imlane_complete_call.py'
            spec = importlib.util.spec_from_file_location('imlane_observation_fixture',path)
            module = importlib.util.module_from_spec(spec)
            with mock.patch.dict(sys.modules, dict(loaded, **{original.__name__:original, 'ray':None})), \
                 mock.patch.dict(os.environ, {'IMLANE_HTTP_TRACE_DIR':str(root/'traces')}):
                spec.loader.exec_module(module)
                observed = module.complete_http(table(dict(native_call=[_call_wire(call)])))
            self.assertIs(observed, result)
            original.complete_http.assert_called_once()
            row = json.loads(next((root/'traces').glob('*.jsonl')).read_text())
            runtime = row['runtime']
            self.assertEqual(runtime['python_version'], list(sys.version_info[:3]))
            self.assertEqual(runtime['python_prefix'], sys.prefix)
            self.assertEqual(runtime['python_executable'], sys.executable)
            for name, version in (('pyarrow','16.1.0'),('aiohttp','3.14.5')):
                self.assertEqual(runtime['modules'][name]['version'], version)
                self.assertEqual(runtime['modules'][name]['source_sha256'],
                                 verifier.fixture_module.digest(root/(name+'.py')))
            self.assertEqual(runtime['modules']['src.baselines.text.frameworks.native_calls']['source_sha256'],
                            verifier.fixture_module.digest(verifier.CODE/'src/baselines/text/frameworks/native_calls.py'))

    def test_sema_only_needs_no_other_supplier_assets_and_keeps_owned_ports(self):
        from integrations.sema import verify_imlane_methods as verifier
        argv = ['--author-root','/tmp/fixed-author','--bridge-binary','/tmp/bridge',
                '--output','/tmp/rs7/new-run','--driver-python','/tmp/driver/bin/python',
                '--methods','sema','--sema-binary','/tmp/fixed-sema','--fixture-port','46510',
                '--model-fixture-port','46513','--service-port','46511',
                '--observation-port','46512','--coordinator-port-start','46650',
                '--ray-address','127.0.0.1:46520','--ray-temp-root','/tmp/rs7',
                '--expected-cpus','8-15']
        args = verifier.parse_arguments(argv)
        self.assertEqual(args.methods, ('sema',))
        self.assertIsNone(args.duckdb_extension)
        self.assertIsNone(args.lotus_native_source)
        self.assertIsNone(args.tokenizer)
        self.assertEqual(args.coordinator_ports, {'cpython':46650, 'ray':46651})
        self.assertEqual(args.expected_cpus, tuple(range(8, 16)))
        self.assertEqual(args.ray_address, '127.0.0.1:46520')
        self.assertEqual(verifier.check_ray_temp_path(args.author_root, args.ray_temp_root)['parent'], str(args.ray_temp_root))
        program = verifier.method_client_program(args)
        constants = {}
        exec(program.split('import json,os,pathlib,sys,sysconfig,time', 1)[0], constants)
        self.assertEqual(constants['METHODS'], ('sema',))
        self.assertIsNone(constants['DUCKDB_EXTENSION'])
        self.assertEqual(constants['MODEL_FIXTURE_PORT'], 46513)
        self.assertEqual(constants['SERVICE_PORT'], 46511)
        self.assertEqual(constants['OBSERVATION_PORT'], 46512)
        compile(program, 'sema-only-method-client', 'exec')
        for extra in (['--methods','sema,sema'], ['--fixture-port','46511'],
                      ['--ray-address','auto'], ['--expected-cpus','15-8']):
            with mock.patch('sys.stderr'), self.assertRaises(SystemExit):
                verifier.parse_arguments(argv + extra)

    def test_fixture_listener_wrapper_keeps_original_handler(self):
        from integrations.sema.verify_imlane_methods import listen_server_factory
        original = mock.Mock(return_value='same-server')
        handler = object()
        self.assertIs(listen_server_factory(original, 0), original)
        self.assertEqual(listen_server_factory(original, 46513)(('127.0.0.1', 0), handler), 'same-server')
        original.assert_called_once_with(('127.0.0.1', 46513), handler)

    def test_sema_only_client_uses_public_sema_queries_without_other_assets(self):
        from integrations.sema import verify_imlane_methods as verifier
        from src.experiments.postgresql import supplier_adapter_query as supplier
        from src.experiments.postgresql import persistent_native_adapter_query as persistent
        from tests.experiments import test_native_adapter_query as fixture
        active, calls = [], []
        @contextmanager
        def model_fixture(**_options):
            posts = []
            active.append(posts)
            try:
                yield 'http://127.0.0.1:46513/v1/chat/completions', posts
            finally:
                active.pop()
        def query(arm, **options):
            rows = list(options['load_source']())
            calls.append((arm, len(rows), options))
            active[-1].extend(rows)
            service = dict(uncertain_requests=0, native_bridge_thread_alive=False, held_requests=0,
                native_driver_stopped=True, imlane_runtime='cpython', imlane_runtime_verified=True,
                native_close_reports=[dict(clean=True)])
            return dict(status='passed', actual_posts=len(rows), execution=dict(recorded_rows=len(rows)),
                identity=dict(imlane_runtime='cpython'), runtime=dict(owner='IMLane author Coordinator'),
                request_service=service)
        class Group:
            def __init__(self, arms, **options):
                self.arm, self.options = arms[0], options
            def __enter__(self): return self
            def __exit__(self, *_args): pass
            def run(self, arm, **options): return query(arm, **dict(self.options, **options))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bridge = root / 'bridge.json'
            bridge.write_text(json.dumps(dict(env={})))
            config = root / 'config.json'
            config.write_text(json.dumps(dict(fixture_endpoint='http://127.0.0.1:46510/complete',
                runtime='cpython', coordinator_pid=123, coordinator_start_ticks=456,
                bridge_config_path=str(bridge), author_build_identity=str(root / 'author/artifacts/build-patched/build.json'))))
            args = SimpleNamespace(duckdb_extension=None, sema_binary=root/'sema', methods=('sema',),
                                   model_fixture_port=46513, service_port=46511, observation_port=46512)
            with mock.patch.object(sys, 'argv', ['client', str(verifier.CODE), str(config), 'None', 'None', str(root/'result.json')]), \
                 mock.patch.object(sys, 'path', list(sys.path)), mock.patch.dict(os.environ, {}), \
                 mock.patch.object(supplier, 'run_supplier_query', side_effect=query), \
                 mock.patch.object(persistent, 'PersistentAdapterGroup', Group), \
                 mock.patch.object(fixture, 'fixture_server', model_fixture), \
                 mock.patch.object(fixture, 'ThreadingHTTPServer'), \
                 mock.patch.object(supplier, 'ObservationGateway'), \
                 mock.patch.object(persistent, 'ObservationGateway'):
                exec(compile(verifier.method_client_program(args), 'sema-only-public-routing', 'exec'), {})
            self.assertEqual([(arm, count) for arm, count, _ in calls],
                [('sema-native-imlane-request-service',4), ('sema-native-imlane-request-service',2),
                 ('sema-native-imlane-request-service',2)])
            self.assertTrue(all(options['tokenizer_path'] is None for _, _, options in calls))
            self.assertIsNone(calls[0][2]['duckdb_library'])
            self.assertTrue(all(options['sema_service_config'].listen_port == 46511 for _, _, options in calls))
            self.assertEqual(json.loads((root/'result.json').read_text())['fixture_posts'], 8)

    def test_bridge_build_requires_matching_fixed_source_binary_and_success(self):
        from integrations.sema import verify_imlane_methods as verifier
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / 'bridge'
            binary.write_bytes(b'protocol identity fixture; no author binary')
            evidence = dict(upstream_commit=AUTHOR_COMMIT, returncode=0,
                binary_sha256=verifier.fixture_module.digest(binary),
                source_sha256=verifier.fixture_module.digest(verifier.CODE / 'integrations/sema/imlane_http.cpp'))
            build = binary.with_suffix('.build.json')
            build.write_text(json.dumps(evidence))
            self.assertEqual(verifier.check_bridge_identity(binary)['binary_sha256'], evidence['binary_sha256'])
            for change in (dict(returncode=1), dict(binary_sha256='0'*64),
                           dict(source_sha256='0'*64), dict(upstream_commit='0'*40)):
                build.write_text(json.dumps(dict(evidence, **change)))
                with self.assertRaisesRegex(ValueError, 'six-capacity ABI'):
                    verifier.check_bridge_identity(binary)

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
