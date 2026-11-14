"""Resident configuration, failure ownership and observed clock boundaries."""
from copy import deepcopy
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.experiments import sembench_text_query as entry
from src.experiments import sembench_text_resident as resident
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.infrastructure.private_artifacts import file_sha256
from src.observability.native_query_timing import (
    observe_native_requests, native_request_boundaries, resident_launch_timing,
)


class ResidentBindingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root/'tokenizer.json').write_text('CPU asset')
        self.query = SimpleNamespace(files={'input.csv':{'sha256':'input'}},
            operation=SimpleNamespace(records=[SimpleNamespace(row_id='a',source_position=0,
                source_index=7,values={'text':'first'})]),describe=lambda:{'expected_model_calls':1})
        self.config = dict(backend='lotus-native',query_id='test',budget=dict(path=str(self.root/'budget.sqlite'),
            id='resident-test',limit=2,unit_ids=['cold','warm']),
            resident=dict(repetitions=2,timeout_s=600,cleanup_s=30,python_hash_seed=3,
                binding=dict(source=dict(commit='selected-current-source',code_tree='selected-code-tree'),
                    input_files={'input.csv':'input'},occurrences_sha256=resident.occurrence_digest(self.query),
                    model_files={'tokenizer.json':file_sha256(self.root/'tokenizer.json')},
                    tokenizer_files={'tokenizer.json':file_sha256(self.root/'tokenizer.json')})))
        self.engine = SimpleNamespace(model_path=str(self.root),tokenizer_path=str(self.root),max_model_calls=2)

    def verify(self):
        def git(argv,**kwargs):
            if argv[-1] == 'HEAD':return 'selected-current-source\n'
            if argv[-1] == 'HEAD:code':return 'selected-code-tree\n'
            return ''
        with patch.object(resident.subprocess,'check_output',side_effect=git), \
                patch.dict(resident.os.environ,{'PYTHONHASHSEED':'3'}):
            return resident.verify_binding(self.config,self.query,self.engine)

    def test_current_source_and_file_bindings_require_no_historical_commit_or_helper(self):
        resident.validate_configuration(self.config)
        result = self.verify()
        self.assertEqual(result['source']['commit'],'selected-current-source')
        self.assertEqual(result['logical_requests'],2)
        self.assertEqual(result['phase_labels'],['cold','warm'])
        (self.root/'tokenizer.json').write_text('changed')
        with self.assertRaisesRegex(ValueError,'asset hash mismatch'):
            self.verify()

    def test_source_occurrence_and_total_budget_changes_stop_before_model(self):
        self.query.operation.records[0].source_index = 8
        with self.assertRaisesRegex(ValueError,'occurrences changed'):
            self.verify()
        self.query.operation.records[0].source_index = 7
        self.engine.max_model_calls = 1
        with self.assertRaisesRegex(ValueError,'cover the declared queries'):
            self.verify()

    def test_finite_headroom_and_unequal_units_do_not_change_source_request_count(self):
        self.config['budget']['limit']=self.engine.max_model_calls=1
        with self.assertRaisesRegex(ValueError,'cover the declared queries'):
            self.verify()
        self.config['budget']['limit']=7
        self.engine.max_model_calls=4
        self.assertEqual(self.verify()['logical_requests'],2)
        ledger=CellBudgetLedger.create(self.root/'budget.sqlite',AttemptBudget('resident-test',7),deadline_utc=None)
        ledger.reserve_unit('cold',4);ledger.reserve_unit('warm',2)
        with patch.object(resident,'run_sequence',return_value={}) as sequence, \
                patch.object(entry,'query_from_config',return_value=self.query) as prepare:
            resident.run(self.config,'checkout','data',None,None,None,self.root/'run',checked_query=self.query)
            prepare.assert_not_called()
            resident.run(self.config,'checkout','data',None,None,None,self.root/'fallback')
            prepare.assert_called_once_with(self.config,'checkout','data',None)
        self.assertEqual([q['unit_id'] for q in sequence.call_args.args[0]],['cold','warm'])
        self.assertTrue(all(row['claimed']==0 for row in ledger.snapshot()['units']))

    def test_insufficient_later_unit_stops_before_model_even_with_large_total_limit(self):
        self.config['budget']['limit']=self.engine.max_model_calls=7
        self.query.describe=lambda:dict(expected_model_calls=2)
        ledger=CellBudgetLedger.create(self.root/'budget.sqlite',AttemptBudget('resident-test',7),deadline_utc=None)
        ledger.reserve_unit('cold',4);ledger.reserve_unit('warm',1)
        with patch.object(resident,'ResidentDriver') as owner,self.assertRaisesRegex(ValueError,'reserve all fresh'):
            resident.run(self.config,'checkout','data',None,None,None,self.root/'run',checked_query=self.query)
        owner.assert_not_called()

    def test_fresh_execution_identity_preserves_input_binding_but_source_changes_do_not(self):
        self.query.operation.records[0].row_id = 'warm-row-0'
        self.verify()
        for name, value in (('source_position', 1), ('source_index', 8), ('values', {'text':'changed'})):
            row = self.query.operation.records[0]
            previous = getattr(row, name)
            setattr(row, name, value)
            with self.subTest(field=name), self.assertRaisesRegex(ValueError,'occurrences changed'):
                self.verify()
            setattr(row, name, previous)

    def test_residency_requires_native_backend_finite_window_and_distinct_units(self):
        for change in ('backend','timeout','units'):
            config = deepcopy(self.config)
            if change == 'backend':config['backend']='daft'
            if change == 'timeout':config['resident']['timeout_s']=float('inf')
            if change == 'units':config['budget']['unit_ids']=['same','same']
            with self.subTest(change=change),self.assertRaises(ValueError):
                resident.validate_configuration(config)

    def test_explicit_initialization_uses_existing_owner_start_without_query_calls(self):
        from src.baselines.text.frameworks import lotus_native_local
        from src.serving.backends.vllm_protocol import GenerationSpec

        self.config['resident']['model_initialization']='before_queries'
        self.config['profile']=dict(native_lm_max_batch_size=3)
        self.query.operation.generation=GenerationSpec(response_format={'type':'json_object'})
        owner=SimpleNamespace(start=Mock(return_value={'engine_uid':'fixture-ready'}),
                              close=Mock(return_value={'status':'closed'}))
        resources=object()
        driver=resident.ResidentDriver(self.config,'source','data',self.engine,GenerationSpec(),resources,None)
        with patch.object(entry,'query_from_config',return_value=self.query), \
                patch.object(lotus_native_local,'LocalNativeModel',return_value=owner) as factory, \
                patch.object(resident.time,'monotonic_ns',side_effect=[10,20]):
            report=driver.start()
        factory.assert_called_once_with(self.engine,self.query.operation.generation,resources,3)
        owner.start.assert_called_once_with()
        self.assertTrue(report['model_ready'])
        self.assertEqual((report['model_start_ns'],report['model_ready_observed_ns']),(10,20))
        self.assertEqual(report['model_query_calls'],0)
        self.assertEqual(report['extra_warmup'],0)
        driver.close()
        owner.close.assert_called_once_with(timeout=30)

    def test_lazy_initialization_stays_default_and_invalid_profiles_are_rejected(self):
        from src.baselines.text.frameworks import lotus_native_local

        driver=resident.ResidentDriver(self.config,None,None,None,None,None,None)
        with patch.object(lotus_native_local,'LocalNativeModel') as factory:
            self.assertFalse(driver.start()['model_ready'])
        factory.assert_not_called()
        for value in ('unknown',None,True):
            self.config['resident']['model_initialization']=value
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'model_initialization'):
                resident.validate_configuration(self.config)

    def test_ready_profile_uses_explicit_first_and_repeat_labels(self):
        self.config['resident']['model_initialization']='before_queries'
        self.assertEqual(self.verify()['phase_labels'],['ready_first','ready_repeat'])
        ledger = CellBudgetLedger.create(self.root/'budget.sqlite',AttemptBudget('resident-test',2),deadline_utc=None)
        for unit in ('cold','warm'):ledger.reserve_unit(unit,1)
        with patch.object(resident,'run_sequence',return_value={}) as sequence:
            resident.run(self.config,'checkout','data',None,None,None,self.root/'run',checked_query=self.query)
        self.assertEqual([q['temperature'] for q in sequence.call_args.args[0]],['ready_first','ready_repeat'])

    def test_missing_or_spent_query_unit_prevents_owner_creation(self):
        ledger = CellBudgetLedger.create(self.root/'budget.sqlite',AttemptBudget('resident-test',2),deadline_utc=None)
        ledger.reserve_unit('cold',1)
        with patch.object(resident,'ResidentDriver') as owner,self.assertRaisesRegex(ValueError,'reserve all fresh'):
            resident.run(self.config,'checkout','data',None,None,None,self.root/'run',checked_query=self.query)
        owner.assert_not_called()
        ledger.reserve_unit('warm',1)
        ledger.claim_shared_unit('cold')
        with patch.object(resident,'ResidentDriver') as owner,self.assertRaisesRegex(ValueError,'reserve all fresh'):
            resident.run(self.config,'checkout','data',None,None,None,self.root/'run',checked_query=self.query)
        owner.assert_not_called()

    def test_existing_sequence_closes_resident_once_and_skips_warm_after_failure(self):
        ledger = CellBudgetLedger.create(self.root/'budget.sqlite',AttemptBudget('resident-test',2),deadline_utc=None)
        for unit in ('cold','warm'):ledger.reserve_unit(unit,1)
        driver = SimpleNamespace(start=lambda:dict(model_ready=False),closes=0)
        failure = RuntimeError('native query failed')
        failure.query_report = dict(status='failed',unknown_calls=1)
        def query(*args):raise failure
        def close():
            driver.closes += 1
            return dict(status='closed',cleanup_errors=[])
        driver.query,driver.close = query,close
        with patch.object(resident,'ResidentDriver',return_value=driver), \
                self.assertRaisesRegex(RuntimeError,'native query failed') as caught:
            resident.run(self.config,'checkout','data',None,None,None,self.root/'run',checked_query=self.query)
        self.assertIs(caught.exception,failure)
        self.assertEqual(driver.closes,1)
        self.assertEqual(failure.session_report['skipped_queries'],['test-1'])
        self.assertEqual(failure.session_report['queries'][0]['backend_report']['unknown_calls'],1)


class ResidentReportTests(unittest.TestCase):
    def run_controller(self, root, write_report):
        config = dict(lifecycle='resident', resident=dict(timeout_s=600, cleanup_s=30))
        supervision = dict(started_monotonic=2, timeout_s=600, cleanup_reserved_s=30)

        def supervise(command, output, **options):
            self.assertIn('--execute-model', command)
            self.assertIn('--worker', command)
            self.assertEqual(command[command.index('--data-dir')+1], str(Path('data').resolve()))
            self.assertEqual(options, dict(timeout_s=600, grace_s=30))
            (output/'session').mkdir()
            write_report(output/'session/session.json')
            return supervision

        from src.experiments import lotus_records_supervisor
        with patch.object(entry, 'configuration', return_value=(config, None, None, None)), \
                patch.object(entry, 'query_from_config'), patch.object(entry, 'check', return_value={}), \
                patch.object(resident, 'verify_binding', return_value={}), \
                patch.object(lotus_records_supervisor, 'supervise', side_effect=supervise):
            return entry.main(['run', '--config', 'config.json', '--data-dir', 'data',
                '--sembench-checkout', 'checkout', '--output-dir', str(root), '--execute-model'],
                controller_entry_ns=1_000_000_000)

    def test_controller_accepts_large_completed_session_without_changing_clocks(self):
        session = dict(queries=[dict(query_result_ready_ns=5_000_000_000, full_query_seconds=1.5),
                               dict(query_result_ready_ns=7_000_000_000, full_query_seconds=.4)],
                       retained_responses='x' * (9 * 1024**2))
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)/'run'
            self.assertEqual(self.run_controller(root, lambda path: path.write_text(json.dumps(session))), 0)
            self.assertGreater((root/'session/session.json').stat().st_size, 8 * 1024**2)
            timing = json.loads((root/'resident-timing.json').read_text())
            self.assertEqual(timing, resident_launch_timing(
                dict(started_monotonic=2, timeout_s=600, cleanup_reserved_s=30), session, 1_000_000_000))

    def test_controller_still_rejects_symlink_and_nonfinite_reports(self):
        with tempfile.TemporaryDirectory() as raw:
            target = Path(raw)/'target.json'
            target.write_text('{}')
            for name, write_report, error in (
                    ('symlink', lambda path: path.symlink_to(target), 'bounded regular JSON'),
                    ('nonfinite', lambda path: path.write_text('{"queries": NaN}'), 'nonfinite JSON')):
                root = Path(raw)/name
                with self.subTest(name=name), self.assertRaisesRegex(ValueError, error):
                    self.run_controller(root, write_report)
                self.assertFalse((root/'resident-timing.json').exists())

    def test_configuration_keeps_small_file_limit(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw)/'config.json'
            path.write_text(json.dumps(dict(model_name='x' * (9 * 1024**2))))
            with self.assertRaisesRegex(ValueError, 'bounded regular JSON'):
                entry.configuration(path)


class ResidentClockTests(unittest.TestCase):
    def stage_clock(self, *, post_error=None, statistics_error=None, original_join=False, allowance=1):
        import pandas as pd
        from src.baselines.text.frameworks import lotus_native_stage as stage
        from src.baselines.text.frameworks import lotus_native_join as join
        from src.baselines.text.sembench_prepared import result_value
        from src.infrastructure.private_artifacts import content_digest
        from src.semantic_methods.lotus import record_method
        from src.serving.backends.vllm_protocol import GenerationSpec

        now, sink, events = [1_000_000_000], [False], []
        unit = SimpleNamespace(attempts=0, remaining=allowance)
        native_frame = (pd.DataFrame({'id:left': [1], 'id:right': [2]}) if original_join else
                        pd.DataFrame(dict(_map=['POSITIVE'])))
        preserved = result_value(native_frame.copy(deep=True))
        result = pd.DataFrame(dict(count=[1]))
        calls = [dict(call_id='native', engine_request_id='request', text='POSITIVE',
                      messages_sha256=content_digest([]))]
        case = self

        class Binding:
            released = False
            def report(current):
                case.assertFalse(current.released, 'statistics read a binding released by owner close')
                case.assertTrue(sink[0] or post_error is not None)
                events.append('receipts')
                now[0] += 13_000_000_000
                return dict(calls=dict(counts=dict(succeeded=1, missing=0), records=calls))
        binding = Binding()
        class Ready(dict):
            def __deepcopy__(current, memo):
                case.assertTrue(sink[0] or post_error is not None)
                events.append('ready-copy')
                now[0] += 7_000_000_000
                return dict(current)
        ready = Ready(engine_uid='CPU-fixture')
        def snapshot():
            events.append('snapshot')
            return dict(active_query=False, tainted=False, transport=None, ready=dict(startup_seconds=0))
        def close(**kwargs):
            events.append('close')
            binding.released = True
            return dict(status='closed')
        owner = SimpleNamespace(ready_report=ready, transport=None, snapshot=snapshot, close=close)
        operation = SimpleNamespace(describe=lambda: dict(kind='join' if original_join else 'map'),
            frame=native_frame, stage=object(), records=())
        state = {}

        if original_join:
            from src.baselines.text.sembench_ecomm import bind_original_query
            from src.baselines.text.sembench_text import _operators
            checkout = os.environ.get('SEMLOOM_NATIVE_TEST_CHECKOUT')
            if checkout is None:
                self.skipTest('a pinned SemBench checkout is required for original E-Commerce postprocessing')
            source = pd.DataFrame(dict(id=[1], price=[100], productDisplayName=['shirt'], productDescriptors=['brand']))
            original = bind_original_query(checkout, '7', lambda *args, **kwargs: source, data_dir='/tmp')

        def native(*args, **kwargs):
            if not original_join:
                self.assertTrue(kwargs['defer_accounting'])
            self.assertIs(kwargs['shared_owner'], owner)
            unit.attempts, unit.remaining = 1, allowance-1
            now[0] += 3_000_000_000
            frame = kwargs['invoke']()
            value = (dict(status='native_returned', expected_model_calls=1,
                frame=frame, _native_binding=binding, _native_join_inputs=[]) if original_join else
                dict(schema='semloom.lotus_native_stage.v1', status='native_returned', stage_calls=[{}],
                    stage_call_ids=dict(native='finite'), frame=frame,
                    _native_binding=binding, _native_lm=object(), _native_ready=ready))
            state['operator'] = value
            return value
        def execute(handler):
            if original_join:
                def dispatch(kind, frame, args, kwargs, invoke):
                    self.assertEqual(kind, 'join')
                    return handler(operation, lambda: native_frame)['frame']
                with _operators(dispatch):
                    complete = original()
                value = state['operator']
            else:
                value = handler(operation, lambda: native_frame)
                complete = result
            now[0] += 2_000_000_000
            if post_error is not None:
                post_error.operator_report = value
                raise post_error
            return complete, value
        def prepare(*args):
            now[0] += 17_000_000_000
            return query
        def bind():
            now[0] += 7_000_000_000
            return execute
        def audit():
            events.append('source-audit')
            now[0] += 11_000_000_000
        query = SimpleNamespace(query_id='q', operation=operation, describe=lambda: dict(expected_model_calls=1),
            bind_execution=bind, verify_source=audit, evaluate=lambda frame: dict(count=1))
        method = SimpleNamespace(prepare_rows=lambda *args, **kwargs: [dict(call_id='finite', messages_json='[]')])
        ledger = SimpleNamespace(claim_shared_unit=lambda key: unit, close_shared_unit=lambda key: None)
        driver = resident.ResidentDriver(dict(resident=dict(cleanup_s=1), profile=dict(native_lm_max_batch_size=2)),
            'unused', 'unused', None, GenerationSpec(), None, ledger)
        driver.owner = owner
        def emit(event):
            self.assertEqual(events, [])
            sink[0] = True
            events.append('full-sink')
            return now[0]
        def lm_report(lm):
            self.assertTrue(sink[0] or post_error is not None)
            events.append('lm')
            now[0] += 19_000_000_000
            if statistics_error is not None:
                raise statistics_error
            return dict(role='CPU statistics fixture')
        def join_audit(query, operator, result, relation):
            import numpy as np
            self.assertEqual(result.tolist(), ['1-2'])
            self.assertIn('id', native_frame.columns)
            self.assertEqual(result_value(relation), preserved)
            self.assertTrue(np.shares_memory(relation['id:left'].to_numpy(), native_frame['id:left'].to_numpy()))
        try:
            with patch.object(entry, 'query_from_config', side_effect=prepare), \
                    patch.object(entry, 'complete_calls', return_value=calls), \
                    patch.object(resident, 'verify_inputs'), \
                    patch.object(resident, 'native_request_boundaries', return_value={}), \
                    patch.object(resident.time, 'monotonic_ns', side_effect=lambda: now[0]), \
                    patch.object(stage, 'execute_native', side_effect=native), \
                    patch.object(join, 'execute_native_join', side_effect=native), \
                    patch.object(join, 'audit', side_effect=join_audit), \
                    patch.object(stage, 'lm_report', side_effect=lm_report), \
                    patch.object(record_method, 'LotusRecordMethod', return_value=method), \
                    patch('src.baselines.text.sembench_prepared.finish', return_value=(
                        result, dict(operator_relation=preserved))):
                report = driver.query(dict(query_id='q', temperature='warm', unit_id='q'), emit)
        except BaseException as error:
            state['error'] = error
            report = error.query_report
        state.update(report=report, events=events, driver=driver, binding=binding, unit=unit)
        return state

    def test_headroom_leaves_unused_allowance_without_extra_native_work_or_clock_changes(self):
        state=self.stage_clock(allowance=4)
        self.assertNotIn('error',state)
        self.assertEqual(state['unit'].attempts,1)
        self.assertEqual(state['unit'].remaining,3)
        self.assertEqual(state['report']['source_to_sink_seconds'],5)
        self.assertEqual(state['report']['result']['data'],[[1]])
        state['driver'].close()

    def test_resident_stage_statistics_follow_full_sink_and_preserve_relation_snapshot(self):
        state = self.stage_clock()
        report = state['report']
        self.assertNotIn('error', state)
        self.assertEqual(report['source_to_sink_seconds'], 5)
        self.assertEqual(report['result']['data'], [[1]])
        self.assertEqual(report['operator']['accounting_phase'], 'after_full_sink')
        self.assertEqual(state['events'], ['full-sink', 'receipts', 'lm', 'ready-copy', 'source-audit', 'snapshot'])
        self.assertGreaterEqual(report['audit_finished_ns']-report['sink_ready_ns'], 50_000_000_000)
        self.assertIn('shallow intermediate relation schema snapshots remain inside', report['timing_scope'])
        self.assertFalse(any(key.startswith('_native_') for key in report['operator']))
        state['driver'].close()
        self.assertEqual(state['events'][-1], 'close')
        self.assertTrue(state['binding'].released)
        json.dumps(report, allow_nan=False)

    def test_original_ecomm_join_column_addition_keeps_a_shallow_schema_snapshot(self):
        state = self.stage_clock(original_join=True)
        self.assertNotIn('error', state)
        report = state['report']
        self.assertEqual(report['source_to_sink_seconds'], 5)
        self.assertEqual(report['result']['data'], [['1-2']])
        self.assertEqual(report['operator_relation']['columns'], ['id:left', 'id:right'])
        self.assertEqual(state['events'], ['full-sink', 'receipts', 'source-audit', 'snapshot'])
        state['driver'].close()
        json.dumps(report, allow_nan=False)

    def test_original_postprocess_failure_accounts_before_close_without_replacing_error(self):
        failure = ValueError('original postprocess failed')
        state = self.stage_clock(post_error=failure)
        self.assertIs(state['error'], failure)
        self.assertEqual(state['report']['status'], 'failed')
        self.assertIsNone(state['report'].get('sink_ready_ns'))
        self.assertEqual(state['report']['operator']['calls']['counts']['succeeded'], 1)
        self.assertEqual(state['report']['operator']['accounting_phase'], 'failure')
        self.assertEqual(state['events'], ['receipts', 'lm', 'ready-copy'])
        self.assertFalse(any(key.startswith('_native_') for key in state['report']['operator']))
        state['driver'].close()
        json.dumps(state['report'], allow_nan=False)

    def test_post_sink_statistics_failure_keeps_the_actual_complete_result(self):
        failure = RuntimeError('statistics getter failed')
        state = self.stage_clock(statistics_error=failure)
        self.assertIs(state['error'], failure)
        self.assertEqual(state['report']['status'], 'failed')
        self.assertEqual(state['report']['source_to_sink_seconds'], 5)
        self.assertEqual(state['report']['result']['data'], [[1]])
        self.assertEqual(state['report']['operator']['calls']['counts']['succeeded'], 1)
        self.assertFalse(any(key.startswith('_native_') for key in state['report']['operator']))
        state['driver'].close()
        self.assertEqual(state['events'], ['full-sink', 'receipts', 'lm', 'close'])

    def test_source_and_budget_preflight_precede_the_fresh_query_clock(self):
        from src.serving.backends.vllm_protocol import GenerationSpec
        now = [1_000_000_000]
        def prepare(*args):
            now[0] += 17_000_000_000
            return query
        def execute(handler):
            now[0] += 3_000_000_000
            return 0, None
        def bind():
            now[0] += 7_000_000_000
            return execute
        def audit():
            now[0] += 11_000_000_000
        query = SimpleNamespace(operation=None, describe=lambda: {'expected_model_calls': 0},
            execute=execute, bind_execution=bind, verify_source=audit,
            evaluate=lambda result: {'count': result})
        ledger = SimpleNamespace(claim_shared_unit=lambda unit: SimpleNamespace(attempts=0, remaining=1),
                                 close_shared_unit=lambda unit: None)
        driver = resident.ResidentDriver(dict(resident={}), 'unused', 'unused', None, GenerationSpec(), None, ledger)
        with patch.object(entry, 'query_from_config', side_effect=prepare), \
                patch.object(resident, 'verify_inputs'), \
                patch.object(resident.time, 'monotonic_ns', side_effect=lambda: now[0]):
            report = driver.query(dict(query_id='empty', temperature='cold', unit_id='unit'), lambda event: now[0])
        self.assertEqual(report['preflight_finished_ns'] - report['worker_entry_ns'], 24_000_000_000)
        self.assertEqual(report['source_to_sink_seconds'], 3)
        self.assertGreaterEqual(report['audit_finished_ns']-report['sink_ready_ns'],11_000_000_000)
        self.assertEqual(report['result'], 0)
        self.assertEqual(report['query_clock_version'], 'sembench.source_to_sink.v3')
        self.assertFalse(report['model_ready_before_source'])
        self.assertIsNone(report['model_ready_before_source_observed_ns'])

    def test_IPC_observer_restores_hooks_and_rejects_unconfirmed_or_duplicate_terminals(self):
        transport = SimpleNamespace(submit=lambda row,**kw:SimpleNamespace(status='queued',request_id='a'),
            poll=lambda:[SimpleNamespace(request_id='a',engine_terminal_confirmed=True)])
        original = transport.submit,transport.poll
        events = dict(submit=[],terminal=[])
        with self.assertRaisesRegex(RuntimeError,'accessor failed'):
            with observe_native_requests(transport,events):
                transport.submit({});transport.poll()
                raise RuntimeError('accessor failed')
        self.assertEqual((transport.submit,transport.poll),original)
        receipt = native_request_boundaries(events,[{'engine_request_id':'a'}])
        self.assertIsNone(receipt['first_engine_accept_ns'])
        self.assertIsNone(receipt['kernel_seconds'])
        events['terminal'].append(events['terminal'][0])
        with self.assertRaises(ValueError):
            native_request_boundaries(events,[{'engine_request_id':'a'}])

    def test_controller_runner_source_and_reserved_cleanup_are_distinct(self):
        supervision = dict(started_monotonic=2,timeout_s=600,cleanup_reserved_s=30)
        session = dict(queries=[dict(query_result_ready_ns=5_000_000_000,full_query_seconds=1.5),
                               dict(query_result_ready_ns=7_000_000_000,full_query_seconds=.4)])
        receipt = resident_launch_timing(supervision,session,1_000_000_000)
        self.assertEqual(receipt['controller_entry_to_sink_seconds'],4)
        self.assertEqual(receipt['runner_launch_to_sink_seconds'],3)
        self.assertEqual(receipt['query_full_seconds'],[1.5,.4])
        self.assertEqual(receipt['supervisor_cleanup_reserved_s'],30)
        self.assertIsNone(receipt['outer_host_deadline_seconds'])
        self.assertIsNone(resident_launch_timing(supervision,session,None)['controller_entry_to_sink_seconds'])


if __name__ == '__main__':
    unittest.main()
