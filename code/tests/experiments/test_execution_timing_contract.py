"""Optional diagnostic protocol and adapter dispatch; zero real models or GPUs."""
import asyncio
from contextlib import contextmanager, ExitStack
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.experiments import execution_timing as timing
from src.experiments.buffered_events import BufferedEvents, EventRecordingError


class MemoryWriter:
    def __init__(self, path):
        self.events = []

    def record(self, event):
        self.events.append(event)

    def close(self):
        pass


class ExecutionTimingContractTests(unittest.TestCase):
    @contextmanager
    def enabled(self, root):
        with patch.dict(os.environ, {'SEMLOOM_DETAILED_TIMING_DIR': str(root),
                                    'SEMLOOM_TIMING_RUN_ID': 'fixture-run'}), \
                patch.object(timing, '_shared', None):
            try:
                yield
            finally:
                timing.close_timing()

    @staticmethod
    def read_events(root):
        return [json.loads(line) for path in sorted(Path(root).glob('timing-*.jsonl'))
                for line in path.read_text().splitlines()]

    def test_default_off_keeps_return_identity_without_new_clock_or_writer(self):
        result = {'full_query_seconds': .5, 'execution': {'t_submit_ns': 10}}

        @timing.timed_call('fixture.query', query=True, finalize=True)
        def query(arm, *, unit_id):
            return result

        with patch.dict(os.environ, {'SEMLOOM_DETAILED_TIMING_DIR': ''}), \
                patch.object(timing, 'Timing', side_effect=AssertionError('disabled writer')), \
                patch.object(timing.time, 'monotonic_ns', side_effect=AssertionError('disabled clock')):
            self.assertIsNone(timing.get_timing())
            self.assertIs(query('fixture-arm', unit_id='query-a'), result)
            with timing.timing_span('fixture.manual') as span:
                self.assertIsNone(span)

    def test_enabled_query_reuses_base_timestamps_and_never_rewrites_totals(self):
        result = dict(full_query_seconds=3., query_preparation_started_ns=10,
            execution=dict(t_backend_ready_ns=20, t_submit_ns=30, t_query_terminal_ns=40,
                           t_stream_cleanup_ns=50, query_status='completed'))
        before = copy.deepcopy(result)

        @timing.timed_call('fixture.query', query=True)
        def query(arm, *, unit_id):
            return result

        with tempfile.TemporaryDirectory() as root:
            with self.enabled(root):
                self.assertIs(query('fixture-arm', unit_id='query-a'), result)
            events = self.read_events(root)
        self.assertEqual(result, before)
        pairs = {}
        for event in events:
            if event['operation'].startswith('query.') and event['event'].startswith('span_'):
                pairs.setdefault(event['operation'], []).append(event['monotonic_ns'])
        self.assertEqual(pairs, {'query.total_to_terminal': [10, 40],
            'query.preparation': [10, 20], 'query.submitted_to_terminal': [30, 40],
            'query.stream_cleanup': [40, 50]})
        self.assertTrue(all(event['query_id'] == 'query-a' and event['arm'] == 'fixture-arm'
                            for event in events))

    def test_missing_backend_boundaries_stay_absent(self):
        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root, writer_factory=MemoryWriter)
            result = {'full_query_seconds': 1., 'execution': {'query_status': 'failed'}}
            timing.record_query_timing(trace, result)
            self.assertEqual(trace.writer.events, [])
            trace.close()

    def test_query_request_actor_identity_and_clock_domain_are_retained(self):
        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root, run_id='known-run', writer_factory=MemoryWriter)
            with trace.bind(query_id='query-a', arm='fixture-arm'):
                trace.record_interval('request.complete', 100, 200, request_id='request-7',
                                      actor_id='actor-2', stage_id='model')
            trace.mark('after-query', monotonic_ns=250)
            first, last, after = trace.writer.events
            self.assertEqual((first['query_id'], first['request_id'], first['actor_id']),
                             ('query-a', 'request-7', 'actor-2'))
            self.assertEqual((first['run_id'], first['pid'], first['clock_domain']),
                             ('known-run', os.getpid(), trace.clock_domain))
            self.assertEqual(first['span_id'], last['span_id'])
            self.assertNotIn('query_id', after)
            trace.close()

    def test_unverified_process_clocks_are_not_given_a_shared_domain(self):
        with tempfile.TemporaryDirectory() as root, \
                patch('src.execution_provider.adapters.ray_map_transport._clock_domain', return_value=None):
            left = timing.Timing(root, writer_factory=MemoryWriter)
            right = timing.Timing(root, writer_factory=MemoryWriter)
            self.assertFalse(left.clock_verified)
            self.assertFalse(right.clock_verified)
            self.assertNotEqual(left.clock_domain, right.clock_domain)
            left.close()
            right.close()

    def test_nested_parallel_capable_spans_keep_parentage_without_a_summed_total(self):
        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root, writer_factory=MemoryWriter)
            with patch.object(timing.time, 'monotonic_ns', side_effect=[10, 20, 30, 40]):
                with trace.span('outer'):
                    with trace.span('inner'):
                        pass
            start, inner_start, inner_end, end = trace.writer.events
            self.assertEqual(inner_start['parent_span_id'], start['span_id'])
            self.assertEqual(inner_end['span_id'], inner_start['span_id'])
            self.assertEqual(end['span_id'], start['span_id'])
            self.assertEqual([event['monotonic_ns'] for event in trace.writer.events], [10, 20, 30, 40])
            self.assertFalse(any('total_seconds' in event for event in trace.writer.events))
            trace.close()

    def test_errors_and_cancellation_preserve_the_original_exception_object(self):
        for primary, status in ((ValueError('original execution'), 'error'),
                                (asyncio.CancelledError('original cancellation'), 'cancelled')):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as root:
                trace = timing.Timing(root, writer_factory=MemoryWriter)
                with self.assertRaises(type(primary)) as caught:
                    with trace.span('fixture.failure'):
                        raise primary
                self.assertIs(caught.exception, primary)
                event = trace.writer.events[-1]
                self.assertEqual((event['status'], event['error_type']), (status, type(primary).__name__))
                trace.close()

    def test_observer_failure_at_completion_does_not_replace_execution_error(self):
        class FailingWriter(MemoryWriter):
            def record(self, event):
                if event['event'] == 'span_ended':
                    raise OSError('secondary observer failure')
                super().record(event)

        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root, writer_factory=FailingWriter)
            primary = RuntimeError('original execution')
            with self.assertRaises(RuntimeError) as caught:
                with trace.span('fixture.failure'):
                    raise primary
            self.assertIs(caught.exception, primary)
            self.assertTrue(any('OSError' in note for note in primary.__notes__))
            self.assertEqual(len(trace.writer.events), 1)
            trace.close()

    def test_observer_start_failure_still_executes_body_and_keeps_its_error(self):
        class FailingWriter(MemoryWriter):
            def record(self, event):
                raise OSError('writer failed before execution')

        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root, writer_factory=FailingWriter)
            entered = []
            primary = ValueError('original execution')
            with self.assertRaises(ValueError) as caught:
                with trace.span('fixture.failure'):
                    entered.append(True)
                    raise primary
            self.assertIs(caught.exception, primary)
            self.assertEqual(entered, [True])
            self.assertTrue(any('OSError' in note for note in primary.__notes__))
            trace.close()

    def test_setup_failure_still_runs_cleanup_and_keeps_active_execution_error(self):
        primary = ValueError('original execution')
        cleaned = []
        with patch.object(timing, 'get_timing', side_effect=OSError('observer setup failed')):
            with self.assertRaises(ValueError) as caught:
                try:
                    raise primary
                finally:
                    with timing.timing_span('fixture.cleanup'):
                        cleaned.append(True)
        self.assertIs(caught.exception, primary)
        self.assertEqual(cleaned, [True])
        self.assertTrue(any('OSError' in note for note in primary.__notes__))

    def test_close_failure_is_recorded_once_and_does_not_retry_at_exit(self):
        class CloseFailure(MemoryWriter):
            closes = 0

            def close(self):
                self.closes += 1
                raise OSError('writer close failed')

        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root, writer_factory=CloseFailure)
            with patch.object(timing, '_shared', trace):
                with self.assertRaises(OSError):
                    timing.close_timing()
                self.assertTrue(trace.closed)
                timing.close_timing()
            self.assertEqual(trace.writer.closes, 1)
            self.assertEqual(list(Path(root).glob('*.closed.json')), [])

    def test_real_close_marker_matches_the_written_trace_counts(self):
        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root)
            trace.record_interval('completed', 1, 2)
            marker = trace.path.with_suffix('.closed.json')
            self.assertFalse(marker.exists())
            trace.close()
            saved = json.loads(marker.read_text())
            writer = saved['writer']
            self.assertEqual((writer['accepted_events'], writer['written_events'], writer['pending_bytes']), (2, 2, 0))
            self.assertTrue(writer['closed'])
            self.assertEqual(saved['process_instance'], trace.process_instance)
            self.assertEqual(len(self.read_events(root)), writer['written_events'])

    def test_nested_and_owner_calls_leave_finalization_to_the_outer_owner(self):
        @timing.timed_call('inner', finalize=True)
        def inner():
            return timing.get_timing()

        @timing.timed_call('outer', finalize=True)
        def outer():
            trace = inner()
            self.assertFalse(trace.closed)
            return trace

        @timing.timed_call('owned-query', finalize=True)
        def owned(*, owner):
            return timing.get_timing()

        with tempfile.TemporaryDirectory() as root:
            with self.enabled(root):
                trace = outer()
                self.assertTrue(trace.closed)
            with self.enabled(root):
                trace = owned(owner=object())
                self.assertFalse(trace.closed)
            self.assertTrue(trace.closed)

    def test_live_writer_cannot_be_reassigned_to_a_different_directory(self):
        with tempfile.TemporaryDirectory() as root, self.enabled(Path(root)/'first'):
            original = timing.get_timing()
            with patch.dict(os.environ, {'SEMLOOM_DETAILED_TIMING_DIR': str(Path(root)/'second')}):
                with self.assertRaisesRegex(ValueError, 'before changing'):
                    timing.get_timing()
                self.assertFalse((Path(root)/'second').exists())
                timing.close_timing()
                replacement = timing.get_timing()
                self.assertNotEqual(replacement.process_instance, original.process_instance)
                self.assertTrue(original.closed)

    def test_owner_enter_closes_registered_resources_after_observer_end_failure(self):
        from src.experiments.postgresql import persistent_native_adapter_query as persistent

        class ArmEndFailure(MemoryWriter):
            def record(self, event):
                if event['operation'] == 'owner.arm_prepare' and event['event'] == 'span_ended':
                    raise OSError('observer failed after owner was created')
                super().record(event)

        transcript = []
        owner = SimpleNamespace(close=lambda: transcript.append('owner-close'))

        def runtime(stack, *args):
            stack.callback(lambda: transcript.append('runtime-close'))
            return None, {'owner': 'fixture runtime'}

        def create(group, arm):
            transcript.append('owner-created')
            return owner

        with tempfile.TemporaryDirectory() as root, self.enabled(Path(root)/'timing'):
            trace = timing.Timing(Path(root)/'timing', writer_factory=ArmEndFailure)
            group = persistent.PersistentAdapterGroup(('fixed-map-native-daft',),
                plan=None, model=None, ledger=None, root=Path(root)/'group')
            with patch.object(timing, '_shared', trace), patch.object(persistent, '_runtime', runtime), \
                    patch.object(persistent, '_ArmOwner', side_effect=create):
                with self.assertRaisesRegex(OSError, 'after owner was created'):
                    group.__enter__()
                self.assertEqual(transcript, ['owner-created', 'owner-close', 'runtime-close'])
                trace.close()

    def test_owner_exit_passed_cancelled_error_takes_priority_over_observer_failure(self):
        from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup

        class OwnerCloseFailure(MemoryWriter):
            def record(self, event):
                if event['operation'] == 'owner.close':
                    raise OSError('observer failed during owner close')
                super().record(event)

        with tempfile.TemporaryDirectory() as root, self.enabled(Path(root)/'timing'):
            trace = timing.Timing(Path(root)/'timing', writer_factory=OwnerCloseFailure)
            group = PersistentAdapterGroup(('fixed-map-native-daft',),
                plan=None, model=None, ledger=None, root=Path(root)/'group')
            group.root.mkdir()
            closed = []
            group.stack.callback(lambda: closed.append('resource-close'))
            primary = asyncio.CancelledError('passed context cancellation')
            self.assertIsNone(sys.exception())
            with patch.object(timing, '_shared', trace):
                self.assertIsNone(group.__exit__(type(primary), primary, None))
            self.assertEqual(closed, ['resource-close'])
            self.assertTrue(any('OSError' in note for note in primary.__notes__))
            self.assertTrue(trace.closed)

    def test_finalizing_subprocess_close_failure_is_nonzero_and_keeps_original_error(self):
        program = textwrap.dedent('''
            import os
            from pathlib import Path
            from unittest.mock import patch
            from src.experiments import execution_timing as timing
            class FailedClose:
                def __init__(self, path): pass
                def record(self, event): pass
                def close(self): raise OSError('fixture writer close failed')
            original = timing.Timing
            class FailedTiming(original):
                def __init__(self, directory, **options):
                    super().__init__(directory, writer_factory=FailedClose, **options)
            @timing.timed_call('fixture.cli', finalize=True)
            def main():
                Path(os.environ['SEMLOOM_DETAILED_TIMING_DIR'], 'body-executed').write_text('yes')
                if os.environ.get('FIXTURE_PRIMARY') == '1':
                    raise ValueError('fixture original execution')
            with patch.object(timing, 'Timing', FailedTiming):
                main()
        ''')
        for primary in (False, True):
            with self.subTest(primary=primary), tempfile.TemporaryDirectory() as root:
                env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2]),
                    SEMLOOM_DETAILED_TIMING_DIR=root, FIXTURE_PRIMARY='1' if primary else '0')
                completed = subprocess.run([sys.executable, '-B', '-c', program], env=env,
                    text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
                self.assertNotEqual(completed.returncode, 0)
                self.assertTrue((Path(root)/'body-executed').exists())
                self.assertEqual(list(Path(root).glob('*.closed.json')), [])
                self.assertIn('fixture original execution' if primary else 'fixture writer close failed', completed.stderr)
                if primary:
                    self.assertIn('Detailed timing close also failed: OSError', completed.stderr)

    def test_exit_zero_without_a_close_marker_does_not_certify_trace_completion(self):
        program = textwrap.dedent('''
            import os
            from src.experiments.execution_timing import Timing
            trace = Timing(os.environ['SEMLOOM_DETAILED_TIMING_DIR'])
            trace.record_interval('fixture', 1, 2)
            trace.writer.close()
            os._exit(0)
        ''')
        with tempfile.TemporaryDirectory() as root:
            env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2]),
                       SEMLOOM_DETAILED_TIMING_DIR=root)
            completed = subprocess.run([sys.executable, '-B', '-c', program], env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
            self.assertEqual(completed.returncode, 0)
            self.assertEqual(len(self.read_events(root)), 2)
            self.assertEqual(list(Path(root).glob('*.closed.json')), [])

    def test_explicit_status_and_owner_identity_do_not_collide_with_span_fields(self):
        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root, writer_factory=MemoryWriter)
            with trace.span('owner.cleanup', status='unknown', owner_id='owner-7'):
                pass
            self.assertEqual(trace.writer.events[-1]['status'], 'unknown')
            self.assertEqual(trace.writer.events[-1]['owner_id'], 'owner-7')
            trace.record_interval('still-running', 10, None, status='unknown')
            self.assertEqual(trace.writer.events[-1]['event'], 'span_started')
            trace.close()

    def test_close_retains_unfinished_span_without_fabricating_an_endpoint(self):
        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root)
            span = trace.span('fixture.unfinished')
            span.__enter__()
            trace.close()
            events = self.read_events(root)
            self.assertEqual([event['event'] for event in events], ['span_started'])
            primary = RuntimeError('consumer stopped before endpoint')
            self.assertFalse(span.__exit__(type(primary), primary, None))
            self.assertEqual(self.read_events(root), events)

    def test_real_writer_is_bounded_and_refuses_partial_oversized_events(self):
        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root, writer_factory=lambda path: BufferedEvents(path, max_pending_bytes=32))
            with self.assertRaises(EventRecordingError):
                trace.mark('fixture.too-large', monotonic_ns=1)
            snapshot = trace.writer.snapshot()
            self.assertEqual((snapshot['accepted_events'], snapshot['pending_bytes']), (0, 0))
            with self.assertRaises(EventRecordingError):
                trace.close()
            self.assertEqual(self.read_events(root), [])

    def test_callbacks_do_not_write_or_fsync_on_the_execution_thread(self):
        caller = threading.get_ident()
        real_write, real_fsync = os.write, os.fsync

        def write(*args):
            self.assertNotEqual(threading.get_ident(), caller)
            return real_write(*args)

        def fsync(*args):
            self.assertNotEqual(threading.get_ident(), caller)
            return real_fsync(*args)

        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root)
            with patch('os.write', side_effect=write), patch('os.fsync', side_effect=fsync):
                trace.record_interval('fixture.completed', 1, 2)
                trace.writer.close()
            # One explicit final close may synchronously publish its completion marker.
            trace.close()
            self.assertEqual(len(self.read_events(root)), 2)

    def test_diagnostic_records_exclude_request_and_response_content(self):
        with tempfile.TemporaryDirectory() as root:
            trace = timing.Timing(root, writer_factory=MemoryWriter)
            trace.mark('fixture.safe', monotonic_ns=1, request_id='request-1',
                       payload='private-input', body='private-response', api_key='private-secret')
            saved = json.dumps(trace.writer.events)
            self.assertNotIn('private-', saved)
            self.assertIn('request-1', saved)
            trace.close()


class SupplierTimingDispatchTests(unittest.TestCase):
    """Real supplier dispatch/Core/bridge protocol; vendor libraries and SDK are substitutes.

    The IMLane child is the existing complete-response protocol fixture. It does
    not run the author's Coordinator or establish GPU/native scheduler behavior.
    """
    ARMS = ('lotus-adapted-native', 'lotus-method-native-daft',
            'lotus-method-native-ray-data', 'lotus-method-semloom-local-diagnostic',
            'lotus-method-native-imlane', 'lotus-method-native-imlane-ray')
    read_events = staticmethod(ExecutionTimingContractTests.read_events)

    @contextmanager
    def method_fixture(self, *, imlane_mode=None):
        from src.baselines.text.frameworks import native_calls
        from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig
        from src.semantic_methods.lotus.sdk import PreparedLotusCall
        from tests.execution_provider.test_sema_imlane import SemaIMLaneBridgeTests

        state = dict(lm=None, native_pool_calls=0, dispatched=[], methods=0)
        configs = {}

        def prepared(lm, messages, kwargs):
            model = lm.config
            return PreparedLotusCall(json.dumps(dict(model=model.model_id, messages=messages,
                max_tokens=kwargs['max_tokens'], temperature=0), separators=(',', ':')).encode(),
                model.endpoint_url.removesuffix('/chat/completions'), model.bearer_token, model.timeout_ms/1000)

        def response(full):
            value = json.loads(full.body)
            return SimpleNamespace(model_dump=lambda: value)

        class LM:
            def __init__(self, config):
                from dataclasses import replace
                self.config = replace(config, bearer_token=config.bearer_token or 'local-fixture')
                self.model = 'openai/' + config.model_id

            def _process_uncached_messages(self, data, kwargs, show, description):
                state['native_pool_calls'] += 1
                values = []
                for index, (messages, _) in enumerate(data):
                    call = prepared(self, messages, kwargs)
                    result = asyncio.run(native_calls._request(native_calls.NativeCall(
                        'original-' + str(index), index, call.payload,
                        call.api_base + '/chat/completions', (('Content-Type', 'application/json'),),
                        call.timeout_s)))
                    values.append(response(result.response))
                return values

        class Frame:
            def __init__(self, values):
                self.values = values

            def sem_map(self, instruction, *, suffix, return_raw_outputs):
                state['methods'] += 1
                lm = state['lm']
                data = [([dict(role='user', content=value)], None) for value in self.values['text']]
                result = lm._process_uncached_messages(data,
                    dict(max_tokens=16, num_retries=0, max_retries=0), False, 'method fixture')
                return dict(row_id=self.values['row_id'],
                            _answer=[value.model_dump()['choices'][0]['message']['content'] for value in result])

        @contextmanager
        def settings(**values):
            previous, state['lm'] = state['lm'], values['lm']
            try:
                yield
            finally:
                state['lm'] = previous

        def graph_rows(backend):
            def rows(calls, options, control):
                state['dispatched'].append(backend)
                for call in reversed(calls):
                    yield asyncio.run(native_calls._request(call))
            return rows

        lotus, pandas, openai, litellm = (ModuleType(name) for name in ('lotus', 'pandas', 'openai', 'litellm'))
        lotus.settings = SimpleNamespace(context=settings)
        pandas.DataFrame = Frame
        openai.OpenAIError = type('OpenAIError', (Exception,), {})
        litellm.cache = None
        with ExitStack() as stack:
            if imlane_mode:
                protocol = SemaIMLaneBridgeTests()
                protocol.setUp()
                stack.callback(protocol.doCleanups)
                original = protocol.executor()
                configs[imlane_mode] = IMLaneCallConfig(imlane_mode, str(original.config_path),
                    123, 456, str(protocol.root/'author-build.json'))
                stack.enter_context(patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator',
                    side_effect=lambda config, bridge: dict(runtime=config.runtime,
                        actual_coordinator_executors=config.n_executors,
                        evidence='synthetic complete-call protocol; author process not run')))
            stack.enter_context(patch.dict('sys.modules', dict(lotus=lotus, pandas=pandas,
                                                              openai=openai, litellm=litellm)))
            stack.enter_context(patch('src.experiments.postgresql.supplier_adapter_query.prepare_lotus_lm',
                                      side_effect=lambda plan, model, *args, **kwargs: LM(model)))
            stack.enter_context(patch('src.experiments.postgresql.supplier_adapter_query._runtime',
                                      return_value=(None, dict(owner='fixture runtime substitute'))))
            stack.enter_context(patch('src.semantic_methods.lotus.batch.validate_source', return_value=None))
            for module in ('sdk', 'batch', 'native'):
                stack.enter_context(patch('src.semantic_methods.lotus.' + module + '.prepare_call', prepared))
            for module in ('batch', 'native'):
                stack.enter_context(patch('src.semantic_methods.lotus.' + module + '.parse_lotus_response',
                    side_effect=lambda lm, messages, kwargs, call, full, *args, **options: response(full)))
            stack.enter_context(patch.object(native_calls, '_daft_rows', graph_rows('daft')))
            stack.enter_context(patch.object(native_calls, '_ray_rows', graph_rows('ray-data')))
            yield state, configs

    def test_six_supplier_paths_reach_method_and_executor_with_equal_results_on_and_off(self):
        from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
        from src.execution_provider.adapters.model_config import FixedModelConfig
        from src.execution_provider.semantic_map import SemanticMapPlan
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        from src.experiments.postgresql.supplier_adapter_query import run_supplier_query
        from tests.experiments.test_native_adapter_query import fixture_server

        rows = [dict(row_id='a', text='duplicate input'), dict(row_id='b', text='duplicate input')]
        for arm in self.ARMS:
            mode = ('ray' if arm.endswith('imlane-ray') else 'cpython') if 'imlane' in arm else None
            with self.subTest(arm=arm), tempfile.TemporaryDirectory() as root, \
                    fixture_server(delay=0) as (endpoint, requests), self.method_fixture(imlane_mode=mode) as (state, configs):
                root = Path(root)
                results, bodies = [], []
                for enabled in (False, True):
                    query_root = root/('on' if enabled else 'off')
                    trace_root = root/'timing'
                    ledger = CellBudgetLedger.create(root/(('on' if enabled else 'off')+'.sqlite'),
                        AttemptBudget('fixture', 2), deadline_utc=time.time()+30)
                    env = {'SEMLOOM_DETAILED_TIMING_DIR': str(trace_root) if enabled else '',
                           'SEMLOOM_TIMING_RUN_ID': 'supplier-fixture'}
                    with patch.dict(os.environ, env), patch.object(timing, '_shared', None):
                        try:
                            result = run_supplier_query(arm, load_source=lambda: iter(rows),
                                plan=SemanticMapPlan('Return ok.', 'fixture', 16),
                                model=FixedModelConfig(endpoint, 'fixture', 3000), ledger=ledger,
                                unit_id='query-a', root=query_root, options=NativeGraphOptions(concurrency=4),
                                query_timeout_s=10, allowed_outputs=('ok',),
                                reference_outputs={'a': 'ok', 'b': 'ok'}, imlane_call_configs=configs)
                        finally:
                            timing.close_timing()
                    self.assertEqual((result['status'], result['rows'], result['actual_posts']), ('passed', 2, 2))
                    self.assertEqual(result['execution']['recorded_rows'], 2)
                    self.assertAlmostEqual(result['full_query_seconds'],
                        (result['execution']['t_query_terminal_ns']-result['query_preparation_started_ns'])/1e9)
                    if not enabled:
                        self.assertFalse(trace_root.exists())
                    result_rows = [json.loads(line) for line in (query_root/'q0/results.jsonl').read_text().splitlines()]
                    results.append([value['row'] for value in result_rows])
                    bodies.append(requests[-2:])
                self.assertEqual(results[0], results[1])
                self.assertEqual(bodies[0], bodies[1])
                self.assertEqual(len(requests), 4)
                self.assertEqual(state['methods'], 2)
                if arm == 'lotus-adapted-native':
                    self.assertEqual(state['native_pool_calls'], 2)
                elif 'daft' in arm:
                    self.assertEqual(state['dispatched'], ['daft', 'daft'])
                elif 'ray-data' in arm:
                    self.assertEqual(state['dispatched'], ['ray-data', 'ray-data'])
                if mode:
                    self.assertEqual(result['identity']['imlane_runtime'], mode)
                events = self.read_events(trace_root)
                self.assertTrue(any(event.get('query_id') == 'query-a' and event.get('arm') == arm
                                    for event in events), 'public query timing hook was not reached')
                operations = {event['operation'] for event in events}
                self.assertTrue({'query.invocation', 'query.source_load', 'query.total_to_terminal',
                                 'query.consumer_terminal'} <= operations)
                markers = []
                for trace_path in trace_root.glob('timing-*.jsonl'):
                    marker_path = trace_path.with_suffix('.closed.json')
                    self.assertTrue(marker_path.exists(), 'a process trace was not finalized')
                    marker = json.loads(marker_path.read_text())
                    writer = marker['writer']
                    self.assertEqual(writer['accepted_events'], writer['written_events'])
                    self.assertEqual(writer['pending_bytes'], 0)
                    self.assertEqual(len(trace_path.read_text().splitlines()), writer['written_events'])
                    markers.append(marker)
                evidence = os.environ.get('SEMLOOM_TIMING_CONTRACT_EVIDENCE')
                if evidence:
                    from src.baselines.common.private_artifacts import write_private_json
                    from src.baselines.common.redact import redact_json_values
                    output = Path(evidence)
                    output.mkdir(parents=True, exist_ok=True)
                    summaries = {label: json.loads((root/label/'summary.json').read_text()) for label in ('off', 'on')}
                    write_private_json(output/(arm+'.json'), redact_json_values(dict(arm=arm,
                        real_model_requests=0, gpu_resources=0, fixture_posts=4,
                        vendor_library_and_sdk_substitutes=True, author_imlane_process_run=False,
                        imlane_runtime=mode, method_calls=state['methods'], dispatched_graphs=state['dispatched'],
                        results_equal=results[0] == results[1],
                        request_digests=[[hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()
                            for value in group] for group in bodies],
                        summaries=summaries, detailed_events=events, close_markers=markers)))


if __name__ == '__main__':
    unittest.main()
