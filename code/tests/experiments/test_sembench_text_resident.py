"""Resident configuration, failure ownership and observed clock boundaries."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

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
        (self.root/'tokenizer.json').write_text('changed')
        with self.assertRaisesRegex(ValueError,'asset hash mismatch'):
            self.verify()

    def test_source_occurrence_and_total_budget_changes_stop_before_model(self):
        self.query.operation.records[0].source_index = 8
        with self.assertRaisesRegex(ValueError,'occurrences changed'):
            self.verify()
        self.query.operation.records[0].source_index = 7
        self.engine.max_model_calls = 3
        with self.assertRaisesRegex(ValueError,'exactly the declared queries'):
            self.verify()

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

    def test_missing_or_spent_query_unit_prevents_owner_creation(self):
        ledger = CellBudgetLedger.create(self.root/'budget.sqlite',AttemptBudget('resident-test',2),deadline_utc=None)
        ledger.reserve_unit('cold',1)
        with patch.object(resident,'ResidentDriver') as owner,self.assertRaisesRegex(ValueError,'reserve all fresh'):
            resident.run(self.config,'checkout','data',None,None,None,self.root/'run')
        owner.assert_not_called()
        ledger.reserve_unit('warm',1)
        ledger.claim_shared_unit('cold')
        with patch.object(resident,'ResidentDriver') as owner,self.assertRaisesRegex(ValueError,'reserve all fresh'):
            resident.run(self.config,'checkout','data',None,None,None,self.root/'run')
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
            resident.run(self.config,'checkout','data',None,None,None,self.root/'run')
        self.assertIs(caught.exception,failure)
        self.assertEqual(driver.closes,1)
        self.assertEqual(failure.session_report['skipped_queries'],['test-1'])
        self.assertEqual(failure.session_report['queries'][0]['backend_report']['unknown_calls'],1)


class ResidentClockTests(unittest.TestCase):
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
