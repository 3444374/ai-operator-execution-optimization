"""Sema source/sink capacities and unresolved-executor ownership, without vendor runtimes."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
import json
import os
from pathlib import Path
import socket
from types import SimpleNamespace
import tempfile
import threading
import time
import unittest
from urllib import error, request
from urllib.parse import urlsplit
from unittest import mock

from integrations.sema import verify_native
from src.baselines.text.frameworks.native_calls import NativeCallExecutor, NativeCallOutcome, _request
from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.sema_native import SemaNativeLimits, SemaNativeService
from src.execution_provider.adapters.sema_service import SemaServiceLimits
from tests.execution_provider.test_sema_native_service import _Executor


class SemaNativeLimitsTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='sema-capacity-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.limits = SemaServiceLimits(16, 65536, 65536, 1)
        self.trace_count = 0

    def service(self, executor=None, **options):
        self.trace_count += 1
        backend = options.pop('backend', 'daft')
        return SemaNativeService(query_id='capacity', upstream_url='http://localhost/model',
            trace_path=self.root / f'requests-{self.trace_count}.jsonl', limits=self.limits,
            backend=backend, executor=executor or _Executor(), **options)

    @staticmethod
    def post(url, payload=b'{}'):
        try:
            response = request.urlopen(request.Request(url, data=payload), timeout=4)
        except error.HTTPError as response:
            return response.code, response.read(), tuple(response.headers.items())
        with response:
            return response.status, response.read(), tuple(response.headers.items())

    def wait(self, predicate):
        deadline = time.monotonic() + 2
        while not predicate() and time.monotonic() < deadline:
            time.sleep(0.002)
        self.assertTrue(predicate(), 'bounded service did not reach the fixture state')

    def test_typed_capacities_reject_nonintegers_and_conflicting_rows(self):
        defaults = SemaNativeLimits()
        for field in vars(defaults):
            for invalid in (0, -1, True, 1.5, '4'):
                with self.subTest(field=field, invalid=invalid), self.assertRaises(ValueError):
                    replace(defaults, **{field: invalid})
        for options in ({'native_limits': {}}, {'native_limits': defaults, 'snapshot_rows': 2},
                        {'native_limits': defaults, 'held_request_capacity': 1},
                        {'native_limits': SemaNativeLimits(snapshot_rows=1), 'snapshot_rows': True},
                        {'snapshot_rows': 2, 'held_request_capacity': 1}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.service(**options)
        # The old held-capacity-only API keeps its effective finite snapshot size.
        self.assertEqual(self.service(held_request_capacity=1).snapshot_rows, 1)

    def test_vendor_factory_receives_the_declared_source_and_result_envelope(self):
        capacities = SemaNativeLimits(3, 4096, 2048, 8192)
        for backend in ('daft', 'ray-data'):
            executor = SimpleNamespace(backend=backend, iter_batch=lambda *_args, **_kwargs: iter(()))
            with self.subTest(backend=backend), mock.patch(
                    'src.execution_provider.adapters.sema_native.NativeCallExecutor', return_value=executor) as factory:
                service = SemaNativeService(query_id='factory', upstream_url='http://localhost/model',
                    limits=self.limits, trace_path=self.root / 'unused', backend=backend,
                    native_limits=capacities, held_request_capacity=3)
                factory.assert_called_once_with(backend, None, max_batch_rows=3, max_batch_bytes=8192)
                self.assertEqual(service.summary['native_limits'], vars(capacities))
                self.assertEqual(service.summary['cumulative_response_scope'], 'per_snapshot')

    def test_single_input_larger_than_snapshot_is_rejected_before_graph_start(self):
        executor = _Executor()
        with self.service(executor, native_limits=SemaNativeLimits(snapshot_payload_bytes=2)) as service:
            self.assertEqual(self.post(service.endpoint_url, b'123')[0], 502)
            self.assertEqual(service.first_error.phase, 'transport')
        self.assertEqual(executor.batches, [])
        self.assertEqual(service.summary['uncertain_requests'], 0)
        self.assertEqual(service.summary['intake_bytes'], 0)

    def test_complete_response_above_single_limit_keeps_remote_completion_known(self):
        full = FullModelResponse(200, (('X-Repeat', 'one'), ('X-Repeat', 'two')), b'finished')
        self.limits = replace(self.limits, response_bytes=len(encode_full_response(full)) - 1)
        with self.service(_Executor(response=lambda _call: full)) as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
            self.assertEqual(service.first_error.phase, 'native_outcome')
        self.assertEqual(service.summary['completed_http_responses'], 1)
        self.assertEqual(service.summary['uncertain_requests'], 0)
        self.assertEqual(service.summary['pending_response_bytes'], 0)

    def test_pending_response_bytes_stay_owned_through_http_write(self):
        from aiohttp import web
        response = FullModelResponse(200, (), b'complete reply')
        size = len(encode_full_response(response))
        entered, release = threading.Event(), threading.Event()
        original = web.Response.write_eof

        async def delayed(value, *args, **kwargs):
            if value.status == 200:
                entered.set()
                while not release.is_set():
                    await asyncio.sleep(0.002)
            return await original(value, *args, **kwargs)

        executor = _Executor(response=lambda _call: response)
        capacities = SemaNativeLimits(snapshot_rows=1, pending_response_bytes=size,
                                      cumulative_response_bytes=size)
        with mock.patch.object(web.Response, 'write_eof', delayed):
            with self.service(executor, native_limits=capacities, held_request_capacity=2) as service, \
                    ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(self.post, service.endpoint_url)
                try:
                    self.assertTrue(entered.wait(1))
                    self.assertEqual(service.summary['pending_response_bytes'], size)
                    second = pool.submit(self.post, service.endpoint_url)
                    self.assertEqual(second.result()[0], 502)
                    self.assertEqual(service.first_error.phase, 'native_outcome')
                    self.assertEqual(service.summary['pending_response_bytes'], size)
                finally:
                    release.set()
                self.assertEqual(first.result()[:2], (200, response.body))
        self.assertEqual(service.summary['peak_pending_response_bytes'], size)
        self.assertEqual(service.summary['pending_response_bytes'], 0)
        self.assertEqual(service.summary['uncertain_requests'], 0)

    def test_http_write_releases_future_response_before_graph_eof(self):
        closed = threading.Event()

        class WaitAfterReply(_Executor):
            def iter_batch(self, calls, **_kwargs):
                yield NativeCallOutcome(calls[0].call_id, calls[0].row_id, FullModelResponse(200, (), b'done'))
                closed.wait(2)
                self.last_close_report = dict(clean=True, graph_consumer_stopped=True,
                                              remote_http_stopped='unavailable')

        with self.service(WaitAfterReply()) as service:
            try:
                self.assertEqual(self.post(service.endpoint_url)[:2], (200, b'done'))
                self.wait(lambda: service.summary['pending_response_bytes'] == 0)
                self.assertEqual(len(service._active), 1)
                self.assertIsNone(next(iter(service._active.values())).future)
            finally:
                closed.set()
            service.end_input()

    def test_cumulative_response_capacity_counts_already_written_results_per_snapshot(self):
        full = FullModelResponse(200, (), b'complete')
        size = len(encode_full_response(full))
        release, executor = threading.Event(), _Executor(response=lambda _call: full)
        executor.gate = release
        capacities = SemaNativeLimits(snapshot_rows=2, cumulative_response_bytes=2 * size - 1)
        with self.service(executor, native_limits=capacities, held_request_capacity=3) as service, \
                ThreadPoolExecutor(max_workers=3) as pool:
            first = pool.submit(self.post, service.endpoint_url)
            self.assertTrue(executor.started.wait(1))
            rest = [pool.submit(self.post, service.endpoint_url) for _ in range(2)]
            self.wait(lambda: len(service._pending) == 2)
            release.set()
            self.assertEqual(first.result()[0], 200)
            self.assertCountEqual([future.result()[0] for future in rest], [200, 502])
        self.assertEqual([len(batch) for batch in executor.batches], [1, 2])
        self.assertEqual(service.summary['completed_http_responses'], 3)
        self.assertEqual(service.summary['uncertain_requests'], 0)
        self.assertEqual(service.summary['peak_snapshot_response_bytes'], size)
        self.assertEqual(service.summary['pending_response_bytes'], 0)

    def test_fake_clean_report_cannot_enable_reuse_after_missing_results(self):
        class FakeClean(_Executor):
            def iter_batch(self, calls, **_kwargs):
                self.batches.append(calls)
                self.last_close_report = dict(clean=True, graph_consumer_stopped=True,
                                              remote_http_stopped='unavailable')
                yield from ()

        executor = FakeClean()
        with self.service(executor) as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
        report = service.summary['last_close_report']
        self.assertTrue(report['reported_clean'])
        self.assertFalse(report['clean'])
        self.assertFalse(report['service_snapshot_complete'])
        self.assertFalse(service.summary['native_executor_reusable'])
        reused = self.service(executor)
        with self.assertRaisesRegex(RuntimeError, 'startup failed') as caught:
            reused.__enter__()
        self.assertIn('unsettled', str(caught.exception.__cause__))
        self.assertEqual(len(executor.batches), 1)

    def test_transport_unknown_poison_survives_service_shutdown(self):
        executor = _Executor(response=lambda call: NativeCallOutcome(call.call_id, call.row_id,
                                                                      None, 'fixture disconnect'))
        with self.service(executor) as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
        self.assertEqual(service.summary['uncertain_request_sequences'], [0])
        self.assertFalse(service.summary['last_close_report']['clean'])
        self.assertFalse(service.summary['native_executor_reusable'])
        with self.assertRaisesRegex(RuntimeError, 'startup failed'):
            self.service(executor).__enter__()
        self.assertEqual(len(executor.batches), 1)

    def test_explicit_imlane_prestart_rejection_is_unsent_and_query_failed_without_poison(self):
        class PrestartRejected(_Executor):
            backend = 'imlane'

            def iter_batch(self, calls, **options):
                if not self.batches:
                    self.batches.append(calls)
                    self.last_close_report = dict(graph_consumer_stopped=True, clean=False,
                        bridge_started=False, author_submission_possible=False,
                        author_dispatch_started=False, external_coordinator_requires_cleanup=False,
                        prepared=len(calls), received=0, remote_http_stopped='unavailable')
                    raise ValueError('fixture rejected before author bridge creation')
                yield from super().iter_batch(calls, **options)

        executor = PrestartRejected()
        with self.service(executor, backend='imlane') as rejected:
            self.assertEqual(self.post(rejected.endpoint_url)[0], 502)
        report = rejected.summary['last_close_report']
        self.assertFalse(report['clean'])
        self.assertTrue(report['service_query_failed'])
        self.assertTrue(report['service_submission_not_started'])
        self.assertEqual(rejected.summary['uncertain_requests'], 0)
        self.assertEqual(rejected.summary['not_submitted_request_sequences'], [0])
        self.assertEqual(rejected._rows[0]['native_submission_state'], 'not_submitted')
        self.assertIsInstance(rejected.first_error.__cause__, ValueError)
        self.assertEqual(rejected.summary['intake_bytes'], 0)
        self.assertTrue(rejected.summary['native_executor_reusable'])
        with self.service(executor, backend='imlane') as retry:
            self.assertEqual(self.post(retry.endpoint_url)[0], 200)
            retry.end_input()
        self.assertEqual(len(executor.batches), 2)

    def test_missing_or_nonboolean_imlane_prestart_proof_keeps_unknown_and_blocks_reuse(self):
        proof = dict(graph_consumer_stopped=True, clean=False, bridge_started=False,
                     author_submission_possible=False, prepared=1, received=0)
        reports = []
        for field in ('graph_consumer_stopped', 'bridge_started', 'author_submission_possible'):
            reports.append({key: value for key, value in proof.items() if key != field})
            invalid_values = (None, 0, 1, False, 'true') if field == 'graph_consumer_stopped' else (None, 0, True, 'false')
            reports.extend({**proof, field: value} for value in invalid_values)
        reports.extend({**proof, **contradiction} for contradiction in (
            {'graph_started': True}, {'author_dispatch_started': True},
            {'external_coordinator_requires_cleanup': True}, {'received': 1},
            {'received': False}, {'prepared': 0}))
        for report in reports:
            with self.subTest(report=report):
                class Unproven(_Executor):
                    backend = 'imlane'

                    def iter_batch(self, calls, **_options):
                        self.batches.append(calls)
                        self.last_close_report = dict(report)
                        raise ValueError('fixture submission status not proven')
                        yield from ()

                executor = Unproven()
                with self.service(executor, backend='imlane') as service:
                    self.assertEqual(self.post(service.endpoint_url)[0], 502)
                self.assertEqual(service.summary['uncertain_request_sequences'], [0])
                self.assertEqual(service.summary['not_submitted_requests'], 0)
                self.assertFalse(service.summary['last_close_report']['clean'])
                self.assertFalse(service.summary['native_executor_reusable'])
                with self.assertRaisesRegex(RuntimeError, 'startup failed'):
                    self.service(executor, backend='imlane').__enter__()
                self.assertEqual(len(executor.batches), 1)

    def test_imlane_transport_outcome_contradicts_unsubmitted_report(self):
        class Contradictory(_Executor):
            backend = 'imlane'

            def iter_batch(self, calls, **_options):
                self.batches.append(calls)
                yield NativeCallOutcome(calls[0].call_id, calls[0].row_id, None, 'fixture peer disconnect')
                self.last_close_report = dict(graph_consumer_stopped=True, clean=False,
                    bridge_started=False, author_submission_possible=False, prepared=len(calls), received=0)

        with self.service(Contradictory(), backend='imlane') as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
        self.assertEqual(service.summary['uncertain_request_sequences'], [0])
        self.assertFalse(service.summary['last_close_report']['service_submission_not_started'])
        self.assertFalse(service.summary['native_executor_reusable'])

    def test_local_cleanup_error_keeps_poison_even_when_author_submission_never_started(self):
        class CloseFailureIterator:
            def __iter__(self):
                return self

            def __next__(self):
                raise ValueError('fixture rejected before submission')

            def close(self):
                raise OSError('fixture local resource close failed')

        class UnsentCloseFailure(_Executor):
            backend = 'imlane'

            def iter_batch(self, calls, **_options):
                self.last_close_report = dict(graph_consumer_stopped=True, clean=False,
                    bridge_started=False, author_submission_possible=False, prepared=len(calls), received=0)
                return CloseFailureIterator()

        with self.service(UnsentCloseFailure(), backend='imlane') as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
        self.assertEqual(service.summary['uncertain_requests'], 0)
        self.assertEqual(service.summary['not_submitted_request_sequences'], [0])
        self.assertTrue(service.summary['last_close_report']['service_query_failed'])
        self.assertFalse(service.summary['last_close_report']['clean'])
        self.assertFalse(service.summary['native_executor_reusable'])

    def test_daft_no_outcomes_has_no_supported_generic_prestart_proof(self):
        class UnprovedGraph(_Executor):
            def iter_batch(self, calls, **_options):
                self.last_close_report = dict(graph_consumer_stopped=True, clean=False,
                    graph_started=False, bridge_started=False, author_submission_possible=False,
                    prepared=len(calls), received=0)
                raise ValueError('fixture generic graph has no checked prestart contract')
                yield from ()

        with self.service(UnprovedGraph()) as service:
            self.assertEqual(self.post(service.endpoint_url)[0], 502)
        self.assertEqual(service.summary['uncertain_request_sequences'], [0])
        self.assertEqual(service.summary['not_submitted_requests'], 0)
        self.assertFalse(service.summary['native_executor_reusable'])

    def test_active_executor_cannot_be_borrowed_by_another_service(self):
        executor = _Executor()
        with self.service(executor) as first:
            with self.assertRaisesRegex(RuntimeError, 'startup failed') as caught:
                self.service(executor).__enter__()
            self.assertIn('already owned', str(caught.exception.__cause__))
            self.assertEqual(self.post(first.endpoint_url)[0], 200)
            first.end_input()
        self.assertEqual(len(executor.batches), 1)
        self.assertTrue(first.summary['native_executor_reusable'])

    def test_normal_complete_graph_allows_a_new_service_to_reuse_executor(self):
        executor = _Executor()
        for _ in range(2):
            with self.service(executor) as service:
                self.assertEqual(self.post(service.endpoint_url)[0], 200)
                service.end_input()
            self.assertTrue(service.summary['native_executor_reusable'])
        self.assertEqual(len(executor.batches), 2)

    def test_full_chat_envelope_usage_logprobs_and_headers_are_unchanged(self):
        envelope = dict(id='response-occurrence', object='chat.completion', model='fixture',
            choices=[dict(index=0, message=dict(role='assistant', content='result'),
                          finish_reason='stop', logprobs=dict(content=[dict(token='result', logprob=-0.25)]))],
            usage=dict(prompt_tokens=11, completion_tokens=1, total_tokens=12))
        raw = json.dumps(envelope, indent=2).encode() + b'\n'
        full = FullModelResponse(201, (('Content-Type', 'application/json'),
            ('X-Repeat', 'one'), ('X-Repeat', 'two')), raw)
        with self.service(_Executor(response=lambda _call: full)) as service:
            status, body, headers = self.post(service.endpoint_url)
            self.assertEqual((status, body, json.loads(body)), (201, raw, envelope))
            self.assertEqual([v for k, v in headers if k == 'X-Repeat'], ['one', 'two'])
            service.end_input()
        self.assertEqual(service.summary['uncertain_requests'], 0)

    def test_verifier_saves_cli_capacities_without_installing_missing_runtime(self):
        root = self.root / 'verifier'
        with mock.patch.dict(os.environ, {}), mock.patch.object(verify_native.os, 'sched_setaffinity', create=True), \
                mock.patch.object(verify_native, 'library_identity', side_effect=ModuleNotFoundError('daft')), \
                mock.patch('builtins.print'):
            self.assertEqual(verify_native.main(['--backend', 'daft', '--artifact-root', str(root),
                '--snapshot-rows', '3', '--snapshot-payload-bytes', '4096',
                '--pending-response-bytes', '2048', '--cumulative-response-bytes', '8192']), 1)
        summary = json.loads((root / 'summary.json').read_text())
        self.assertEqual(summary['native_limits'], vars(SemaNativeLimits(3, 4096, 2048, 8192)))
        self.assertEqual(summary['tests_run'], 0)
        self.assertEqual(summary['real_model_requests'], 0)

    def test_verifier_rejects_nonempty_ray_temp_before_runtime_start(self):
        with tempfile.TemporaryDirectory(prefix='sr-', dir='/tmp') as directory:
            root = Path(directory)
            ray_temp = root / 'ray'
            ray_temp.mkdir()
            (ray_temp / 'other-owner').write_text('existing resource')
            ray = SimpleNamespace(is_initialized=lambda: False, init=mock.Mock(), shutdown=mock.Mock())
            with mock.patch.dict(os.environ, {}), mock.patch.dict('sys.modules', {'ray': ray}), \
                    mock.patch.object(verify_native.os, 'sched_setaffinity', create=True), \
                    mock.patch.object(verify_native, 'library_identity', return_value={}), \
                    mock.patch('builtins.print'):
                self.assertEqual(verify_native.main(['--backend', 'ray-data', '--artifact-root',
                    str(root / 'artifacts'), '--ray-temp', str(ray_temp)]), 1)
            summary = json.loads((root / 'artifacts/summary.json').read_text())
            self.assertIn('empty and caller-owned', summary['startup_error'])
            ray.init.assert_not_called()
            ray.shutdown.assert_not_called()

    def test_verifier_configures_daft_through_the_process_runtime_owner(self):
        root = self.root / 'daft-owner'
        result = SimpleNamespace(testsRun=0, failures=[], errors=[], skipped=[], wasSuccessful=lambda: False)
        with mock.patch.dict(os.environ, {}), mock.patch.object(verify_native.os, 'sched_setaffinity', create=True), \
                mock.patch.object(verify_native, 'library_identity', return_value={}), \
                mock.patch.object(verify_native, 'configure_daft_runner') as configure, \
                mock.patch.object(verify_native.unittest.TextTestRunner, 'run', return_value=result), \
                mock.patch('builtins.print'):
            self.assertEqual(verify_native.main(['--backend', 'daft', '--artifact-root', str(root)]), 1)
        configure.assert_called_once_with('native', num_threads=8)
        self.assertFalse(json.loads((root / 'summary.json').read_text())['passed'])

    def test_verifier_connects_only_to_explicit_private_ray_address_and_records_ports(self):
        root = self.root / 'ray-connection'
        ray = SimpleNamespace(is_initialized=lambda: False, init=mock.Mock(), shutdown=mock.Mock())
        result = SimpleNamespace(testsRun=0, failures=[], errors=[], skipped=[], wasSuccessful=lambda: False)
        with mock.patch.dict(os.environ, {}), mock.patch.dict('sys.modules', {'ray': ray}), \
                mock.patch.object(verify_native.os, 'sched_setaffinity', create=True), \
                mock.patch.object(verify_native, 'library_identity', return_value={}), \
                mock.patch.object(verify_native.unittest.TextTestRunner, 'run', return_value=result), \
                mock.patch('builtins.print'):
            self.assertEqual(verify_native.main(['--backend', 'ray-data', '--artifact-root', str(root),
                '--ray-address', '127.0.0.1:46520', '--fixture-port', '46510', '--service-port', '46511']), 1)
        self.assertEqual(ray.init.call_args.kwargs, dict(address='127.0.0.1:46520',
            namespace=f'sema-native-{os.getpid()}'))
        ray.shutdown.assert_called_once_with()
        summary = json.loads((root / 'summary.json').read_text())
        self.assertEqual((summary['fixture_host'], summary['fixture_port'], summary['service_port']),
                         ('127.0.0.1', 46510, 46511))
        self.assertEqual(summary['ray_mode'], 'caller_started_cluster_connection')
        self.assertEqual(summary['ray_shutdown_scope'], 'verifier connection only; cluster remains caller-owned')
        self.assertFalse(summary['passed'])

    def test_verifier_rejects_auto_nonlocal_or_conflicting_runtime_and_port_settings(self):
        settings = [['--ray-address', value] for value in
            ('auto', 'local', 'ray://127.0.0.1:46520', '192.0.2.1:46520', 'localhost:0', 'localhost:65536')]
        settings.extend((['--ray-address', '127.0.0.1:46520', '--ray-temp', '/tmp/unused-ray'],
                         ['--fixture-port', '65536'], ['--service-port', '-1'],
                         ['--fixture-port', '46510', '--service-port', '46510']))
        for extra in settings:
            with self.subTest(extra=extra), mock.patch('sys.stderr'), self.assertRaises(SystemExit) as caught:
                verify_native.main(['--backend', 'ray-data', '--artifact-root', str(self.root / 'unused')] + extra)
            self.assertEqual(caught.exception.code, 2)
        self.assertFalse((self.root / 'unused').exists())

    def test_public_sema_factory_transmits_explicit_listener_port(self):
        from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
        from src.experiments.postgresql.sema_native_query import SemaServiceConfig, open_native_service
        configuration = SemaServiceConfig(listen_port=46511)
        with mock.patch('src.execution_provider.adapters.sema_native.SemaNativeService') as factory, ExitStack() as stack:
            open_native_service(stack, 'sema-native-daft-request-service', query_id='port-factory',
                model=SimpleNamespace(endpoint_url='http://localhost/model'),
                limits=configuration.service_limits(16, 1), trace_path=self.root / 'unused',
                options=NativeGraphOptions(), held_tasks=8, service_config=configuration)
            self.assertEqual(factory.call_args.kwargs['listen_port'], 46511)
            self.assertEqual(factory.call_args.kwargs['native_limits'].snapshot_rows, 8)

    def test_explicit_service_listener_port_is_bound_and_recorded(self):
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1', 0))
            port = reserved.getsockname()[1]
        checks = verify_native.NativeSemaChecks('test_service_sequential_supply_does_not_wait_for_query_end')
        checks.service_port, checks.observations = port, []
        with self.service(listen_port=port) as service:
            self.assertEqual(urlsplit(service.endpoint_url).port, port)
            checks.service_running(service)
            self.assertEqual(self.post(service.endpoint_url)[0], 200)
            service.end_input()
        self.assertEqual(checks.observations[0]['service_listener_port'], port)
        self.assertEqual(checks.observations[0]['configured_service_port'], port)

    def test_verifier_asserts_transport_unknown_and_full_429_reuse_on_public_executor(self):
        checks = verify_native.NativeSemaChecks('test_graph_transport_timeout_is_a_value_without_retry')
        checks.observations = []
        current = verify_native.call('http://localhost/model')

        def failed_rows(calls, _options, _control):
            for call in calls:
                yield NativeCallOutcome(call.call_id, call.row_id, None, 'fixture TimeoutError')

        with mock.patch('src.baselines.text.frameworks.native_calls._daft_rows', failed_rows):
            failed = NativeCallExecutor('daft')
            self.assertEqual(len(tuple(failed.iter_batch((current,)))), 1)
            checks.settled(failed, unconfirmed=True)
            checks.refuses_reuse(failed, current.endpoint, [])

        full = FullModelResponse(429, (('X-Repeated', 'one'), ('X-Repeated', 'two')), b'original failure')

        def response_rows(calls, _options, _control):
            for call in calls:
                yield NativeCallOutcome(call.call_id, call.row_id, full)

        with mock.patch('src.baselines.text.frameworks.native_calls._daft_rows', response_rows):
            reusable = NativeCallExecutor('daft')
            for _ in range(2):
                outcomes = tuple(reusable.iter_batch((current,)))
                verify_native.check_outcomes(outcomes, {(current.call_id, current.row_id)}, 429, full.body)
                checks.settled(reusable)


class SemaStructuredFixtureTests(unittest.IsolatedAsyncioTestCase):
    async def test_structured_verifier_fixture_uses_real_http_kernel_without_model(self):
        with verify_native.structured_response_server() as (url, calls, raw, envelope):
            outcome = await _request(verify_native.call(url))
            response = outcome.response
            self.assertEqual((response.status_code, response.body, response.http_version), (200, raw, 'HTTP/1.1'))
            self.assertEqual(json.loads(response.body), envelope)
            self.assertEqual(tuple((key, value) for key, value in response.headers
                if key.lower() not in ('server', 'date')),
                (('Content-Type', 'application/json'), ('X-Repeated', 'one'), ('X-Repeated', 'two'),
                 ('Content-Length', str(len(raw)))))
            self.assertEqual(calls, [verify_native.call(url).payload])

    async def test_all_real_verifier_fixtures_reuse_the_selected_loopback_port_sequentially(self):
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1', 0))
            port = reserved.getsockname()[1]
        with verify_native.complete_server(fixture_port=port) as (url, calls, raw):
            self.assertEqual(urlsplit(url).port, port)
            self.assertEqual((await _request(verify_native.call(url))).response.body, raw)
            self.assertEqual(len(calls), 1)
        with verify_native.structured_response_server(fixture_port=port) as (url, calls, raw, _envelope):
            self.assertEqual(urlsplit(url).port, port)
            self.assertEqual((await _request(verify_native.call(url))).response.body, raw)
            self.assertEqual(len(calls), 1)
        with verify_native.staggered_server(fixture_port=port) as (url, calls, completed):
            self.assertEqual(urlsplit(url).port, port)
            call = verify_native.call(url)
            call = replace(call, headers=call.headers + (('X-Order', '1'),))
            self.assertEqual((await _request(call)).response.body, call.payload + b':1')
            self.assertEqual((len(calls), completed), (1, [1]))


if __name__ == '__main__':
    unittest.main()
