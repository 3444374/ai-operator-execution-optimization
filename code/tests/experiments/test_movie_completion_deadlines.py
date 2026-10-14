"""Aggregate completion opt-in; simulated long work, no models or vendor runtime.

Only the two bridge protocol tests use localhost, with one request each. Their
subprocess is a synthetic protocol executable, not the author IMLane runtime.
"""
from dataclasses import replace
from contextlib import ExitStack, nullcontext
import math
from pathlib import Path
import pickle
import sqlite3
import tempfile
import time
import threading
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock

from src.baselines.text.frameworks.native_calls import NativeCall, NativeCallExecutor, NativeCallOutcome
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.full_response import FullModelResponse
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger, COMPLETION_SCHEMA, SCHEMA
from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup
from src.experiments.postgresql.query_deadline import validate_owner_timeouts
from src.experiments.postgresql.sembench_queries import MovieDataset
from src.semantic_methods.lotus.batch import LotusBatchExecutor
from src.semantic_methods.lotus.limits import LotusCallLimits


class CompletionPolicyTests(unittest.TestCase):
    def test_generic_supplier_cannot_drop_deadline_before_source_or_output(self):
        from src.experiments.postgresql.supplier_adapter_query import run_supplier_query
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)/'unstarted-output'
            source, ledger = mock.Mock(), mock.Mock()
            with self.assertRaisesRegex(ValueError, 'explicit original Movie'):
                run_supplier_query('lotus-method-native-daft', load_source=source,
                    plan=SemanticMapPlan('Map', 'fixture', 8),
                    model=FixedModelConfig('http://localhost/v1/chat/completions', 'fixture', 30000),
                    ledger=ledger, unit_id='fixture', root=output,
                    options=NativeGraphOptions(), query_timeout_s=None)
            source.assert_not_called()
            ledger.reserve_unit.assert_not_called()
            self.assertFalse(output.exists())

    def test_explicit_pair_only_and_finite_defaults_keep_cleanup_reserve(self):
        validate_owner_timeouts(120, 1800)
        validate_owner_timeouts(None, None, completion_allowed=True)
        for query, owner in ((None, 1800), (120, None), (None, None),
                             (True, 1800), (120, True), (0, 1800), (121, 1800),
                             (120, 240), (120, 1801), (math.nan, 1800), (120, math.inf)):
            with self.subTest(query=query, owner=owner), self.assertRaises(ValueError):
                validate_owner_timeouts(query, owner)

    def test_original_movie_owner_accepts_pair_before_any_startup(self):
        dataset = object.__new__(MovieDataset)
        for arm in ('lotus-adapted-native', 'lotus-method-native-daft',
                    'lotus-method-native-ray-data', 'lotus-method-semloom'):
            with self.subTest(arm=arm), mock.patch(
                    'src.experiments.postgresql.persistent_native_adapter_query._runtime') as runtime:
                group = PersistentAdapterGroup((arm,),
                    plan=SemanticMapPlan('Filter', 'fixture', 8),
                    model=FixedModelConfig('http://localhost/v1/chat/completions', 'fixture', 30000),
                    ledger=None, root=Path('/tmp/movie-completion-fixture'),
                    options=NativeGraphOptions(concurrency=8, num_threads=8),
                    query_timeout_s=None, owner_timeout_s=None, sembench_dataset=dataset)
                self.assertIsNone(group.query_timeout_s)
                runtime.assert_not_called()
        with self.assertRaises(ValueError):
            PersistentAdapterGroup(('lotus-adapted-native',),
                plan=SemanticMapPlan('Filter', 'fixture', 8),
                model=FixedModelConfig('http://localhost/v1/chat/completions', 'fixture', 30000),
                ledger=None, root=Path('/tmp/movie-completion-fixture'),
                query_timeout_s=None, owner_timeout_s=None)

    def test_reusable_imlane_owner_rejects_pair_before_startup(self):
        from tests.experiments.test_imlane_query_entry import configs
        values = {mode: replace(value, reuse_bridge=True) for mode, value in configs().items()}
        with self.assertRaisesRegex(ValueError, 'reusable IMLane'):
            PersistentAdapterGroup(('lotus-method-native-imlane',),
                plan=SemanticMapPlan('Filter', 'fixture', 8),
                model=FixedModelConfig('http://localhost/v1/chat/completions', 'fixture', 30000),
                ledger=None, root=Path('/tmp/movie-completion-fixture'),
                query_timeout_s=None, owner_timeout_s=None,
                sembench_dataset=object.__new__(MovieDataset), imlane_call_configs=values)

    def test_cli_creates_nullable_ledger_without_an_owner_duration_multiply(self):
        from src.experiments.postgresql import sembench_query_cli as cli
        config = dict(arms=['lotus-adapted-native'], queries=[2], warmups=0, repeats=1,
                      max_attempts=2, query_timeout_s=None, owner_timeout_s=None,
                      native_options={})
        value = dict(status='passed', actual_posts=0, full_query_seconds=0,
                     execution=dict(ready_query_seconds=0), quality={})
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / 'new-output'
            ready = (object.__new__(MovieDataset),
                     FixedModelConfig('http://localhost/v1/chat/completions', 'fixture', 30000),
                     NativeGraphOptions(), None, None, {}, output, None, {})
            with mock.patch.object(cli, 'preflight', return_value=ready), \
                 mock.patch.object(cli, 'MovieQuery'), \
                 mock.patch.object(cli, 'PersistentAdapterGroup') as factory:
                factory.return_value.__enter__.return_value.run.return_value = value
                result = cli.run(config)
            self.assertEqual(result['status'], 'passed')
            self.assertEqual(result['budget']['schema'], COMPLETION_SCHEMA)
            self.assertIsNone(result['budget']['deadline_utc'])
            self.assertIsNone(factory.call_args.kwargs['query_timeout_s'])
            self.assertIsNone(factory.call_args.kwargs['owner_timeout_s'])


class CompletionBudgetTests(unittest.TestCase):
    def test_nullable_claim_has_no_wall_expiry_but_keeps_quota_pid_and_single_claim(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'budget.sqlite'
            budget = AttemptBudget('completion.fixture', 3)
            ledger = CellBudgetLedger.create(path, budget, deadline_utc=None)
            self.assertEqual(ledger.snapshot()['schema'], COMPLETION_SCHEMA)
            ledger.reserve_unit('unit', 2)
            unit = ledger.claim_unit('unit')
            with mock.patch('time.monotonic', return_value=time.monotonic()+1e9), \
                 mock.patch('time.time', return_value=time.time()+1e9):
                self.assertEqual(unit.reserve('a'*64), 1)
                self.assertEqual(ledger.reserve_unit('last', 1).first_attempt, 3)
            with mock.patch('os.getpid', return_value=-1), self.assertRaises(BudgetError):
                unit.reserve('a'*64)
            with self.assertRaises(TypeError):
                pickle.dumps(unit)
            self.assertEqual(unit.reserve('a'*64), 2)
            with self.assertRaises(BudgetExhausted):
                unit.reserve('a'*64)
            with self.assertRaises(BudgetExhausted):
                ledger.reserve_unit('extra', 1)
            with self.assertRaises(BudgetError):
                CellBudgetLedger(path, budget).claim_unit('unit')

    def test_nullable_shared_claim_keeps_durable_close_and_spend(self):
        with tempfile.TemporaryDirectory() as temporary:
            budget = AttemptBudget('completion.shared', 2)
            ledger = CellBudgetLedger.create(Path(temporary)/'budget', budget, deadline_utc=None)
            ledger.reserve_unit('unit', 2)
            shared = ledger.claim_shared_unit('unit')
            self.assertEqual(shared.reserve('b'*64), 1)
            ledger.close_shared_unit('unit')
            with self.assertRaises(BudgetExhausted):
                shared.reserve('b'*64)
            self.assertEqual(shared.attempts, 1)

    def test_finite_v1_schema_constraint_reopen_and_expiry_are_retained(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'budget'
            budget = AttemptBudget('finite.fixture', 2)
            ledger = CellBudgetLedger.create(path, budget, deadline_utc=time.time()+10)
            self.assertEqual(ledger.snapshot()['schema'], SCHEMA)
            with sqlite3.connect(path) as connection:
                self.assertEqual(next(row[3] for row in connection.execute('PRAGMA table_info(budget)')
                                      if row[1] == 'deadline'), 1)
            reopened = CellBudgetLedger(path, budget)
            with mock.patch('time.time', return_value=time.time()+20), self.assertRaises(BudgetExhausted):
                reopened.reserve_unit('expired', 1)

    def test_v1_cannot_silently_accept_a_null_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'budget'
            budget = AttemptBudget('tamper.fixture', 2)
            CellBudgetLedger.create(path, budget, deadline_utc=None)
            with sqlite3.connect(path) as connection:
                connection.execute('UPDATE budget SET schema=?', (SCHEMA,))
            with self.assertRaises(BudgetError):
                CellBudgetLedger(path, budget)


class VendorCompletionTests(unittest.TestCase):
    def test_simulated_long_vendor_batch_keeps_finite_request_and_cleanup(self):
        for backend, target in (('daft', '_daft_rows'), ('ray-data', '_ray_rows')):
            state = dict(now=0.)
            def rows(calls, options, control):
                state['now'] = 125.
                yield NativeCallOutcome(calls[0].call_id, calls[0].row_id,
                                        FullModelResponse(200, (), b'{}'))
            clock = SimpleNamespace(monotonic=lambda: state['now'])
            call = NativeCall('call', 0, b'{}', 'http://localhost/fixture', (), 30)
            with self.subTest(backend=backend), \
                 mock.patch('src.baselines.text.frameworks.native_calls.'+target, rows), \
                 mock.patch('src.baselines.text.frameworks.native_calls.time', clock):
                executor = NativeCallExecutor(backend)
                self.assertEqual(list(executor.iter_batch([call], timeout_s=None))[0].call_id, 'call')
                self.assertTrue(executor.last_close_report['clean'])
                self.assertEqual(call.timeout_s, 30)
                state['now'] = 0.
                with self.assertRaises(TimeoutError):
                    list(executor.iter_batch([call]))

    def test_none_does_not_remove_cancel_or_enable_the_ray_core_reference(self):
        call = NativeCall('call', 0, b'{}', 'http://localhost/fixture', (), 30)
        with self.assertRaises(InterruptedError):
            list(NativeCallExecutor('daft').iter_batch([call], timeout_s=None, cancelled=lambda: True))
        with self.assertRaises(ValueError):
            list(NativeCallExecutor('ray').iter_batch([call], timeout_s=None))
        with self.assertRaises(ValueError):
            replace(call, timeout_s=None)


class LotusCompletionTests(unittest.TestCase):
    def test_public_movie_supplier_entry_propagates_completion_to_semloom_batch(self):
        from src.experiments.postgresql.supplier_adapter_query import _lotus_rows
        from tests.execution_provider.test_native_tasks import fixture
        lotus, pandas = ModuleType('lotus'), ModuleType('pandas')
        lotus.settings = SimpleNamespace(context=lambda **options: nullcontext())
        result = SimpleNamespace(model_rows=(), query=SimpleNamespace(query_id=2))
        execution, _, _ = fixture()
        for timeout in (None, 120):
            with self.subTest(timeout=timeout), ExitStack() as stack, \
                 mock.patch.dict('sys.modules', {'lotus': lotus, 'pandas': pandas}), \
                 mock.patch('src.semantic_methods.lotus.batch.LotusBatchExecutor', wraps=LotusBatchExecutor) as constructor, \
                 mock.patch('src.semantic_methods.lotus.batch.lotus_executor', return_value=nullcontext()), \
                 mock.patch('src.experiments.postgresql.supplier_adapter_query._LotusBatchObservation') as observer:
                context = _lotus_rows(stack, 'lotus-method-semloom', (),
                    SemanticMapPlan('Filter', 'fixture', 8),
                    FixedModelConfig('http://localhost/v1/chat/completions', 'fixture', 30000),
                    execution, None, NativeGraphOptions(), 'fixture', None, threading.Event(), None,
                    lm=SimpleNamespace(), sembench_result=result, query_timeout_s=timeout)
                _, metadata = stack.enter_context(context)
                self.assertEqual(constructor.call_args.kwargs['timeout_s'], timeout)
                self.assertEqual(observer.call_args.args[3].timeout_s, timeout)
                self.assertEqual(metadata['executor'], 'semloom')

    def test_native_completion_covers_sdk_after_120_and_still_declares_request_deadline(self):
        from tests.semantic_methods import test_lotus_call_limits as fixture
        case = fixture.LotusCallLimitsTests()
        graph = fixture.CaptureGraph()
        captured = []
        original = graph.iter_batch
        def iterate(calls, **options):
            captured.append(options['timeout_s'])
            yield from original(calls, **options)
        graph.iter_batch = iterate
        selected = case.selected(graph, call_limits=LotusCallLimits(1., batch_timeout_s=None))
        state = dict(now=0.)
        with mock.patch('src.semantic_methods.lotus.native.time', SimpleNamespace(monotonic=lambda: state['now'])):
            self.assertEqual(len(case.invoke(selected, after_parse=lambda: state.update(now=125.))), 1)
        self.assertEqual(captured, [None])
        self.assertEqual(graph.calls[0].timeout_s, 1.)
        self.assertTrue(graph.closed)
        with self.assertRaises(ValueError):
            LotusCallLimits(None, batch_timeout_s=None)
        self.assertEqual(LotusCallLimits(1.).batch_timeout_s, 120)

    def test_semloom_completion_keeps_cancel_and_core_cleanup(self):
        from tests.semantic_methods import test_lotus_control as fixture
        case = fixture.LotusControlTests()
        with case.batch() as (original, execution, state):
            selected = LotusBatchExecutor(execution, original.model_config, query_id='query', operator_id='map',
                timeout_s=None, cancelled=lambda: state['cancelled'],
                on_prepared=lambda *args: state.update(now=125.))
            self.assertEqual(len(case.run_batch(selected)), 2)
            self.assertEqual(execution.engine.jobs.jobs, {})
            state['cancelled'] = True
            with self.assertRaisesRegex(RuntimeError, 'cancelled'):
                case.run_batch(selected)
            self.assertEqual(execution.engine.jobs.jobs, {})


class IMLaneCompletionProtocolTests(unittest.TestCase):
    def setUp(self):
        from tests.execution_provider.test_sema_imlane import SemaIMLaneBridgeTests
        self.fixture = SemaIMLaneBridgeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def public_executor(self, *, late_failure=False):
        from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig, IMLaneCallExecutor
        original = self.fixture.executor(late_failure=late_failure)
        config = IMLaneCallConfig('cpython', str(original.config_path), 123, 456,
                                 str(self.fixture.root/'author-build.json'))
        executor = IMLaneCallExecutor(config, NativeGraphOptions(concurrency=4))
        self.addCleanup(executor.close)
        return executor

    def test_per_batch_none_reaches_protocol_and_retains_complete_response(self):
        executor = self.public_executor()
        with mock.patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator',
                        return_value=dict(runtime='cpython', actual_coordinator_executors=2)):
            outcomes = list(executor.iter_batch([self.fixture.call(0)], timeout_s=None))
        self.assertEqual(outcomes[0].response.body, self.fixture.server.body)
        self.assertEqual(len(self.fixture.server.calls), 1)
        self.assertTrue(executor.last_close_report['clean'])

    def test_none_does_not_hide_late_nonzero_bridge_exit(self):
        executor = self.public_executor(late_failure=True)
        with mock.patch('src.baselines.text.frameworks.imlane_calls.verify_coordinator',
                        return_value=dict(runtime='cpython', actual_coordinator_executors=2)):
            iterator = executor.iter_batch([self.fixture.call(0)], timeout_s=None)
            self.assertEqual(next(iterator).response.status_code, 200)
            with self.assertRaises(RuntimeError):
                next(iterator)
        self.assertEqual(len(self.fixture.server.calls), 1)
        self.assertEqual(executor.last_close_report['returncode'], 7)
        self.assertFalse(executor.last_close_report['clean'])


if __name__ == '__main__':
    unittest.main()
