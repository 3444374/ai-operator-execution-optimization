"""Git entry routing and receipts over original async IPC, CPU inference only."""
from contextlib import redirect_stdout
from copy import deepcopy
from dataclasses import asdict
import io
import json
from pathlib import Path
import time
import unittest
from unittest.mock import MagicMock, Mock, patch

from src.experiments import record_chain_validation as checks
from src.experiments import record_chain_validation_run as entry
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from tests.experiments import test_record_chain_validation as fixtures
from tests.execution_provider.chain_tokenizer_fixture import tokenizer
from tests.execution_provider.test_lotus_chain_token_budget import Process
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime


class ProgressionEntryTests(unittest.TestCase):
    setUp = fixtures.ConfigTests.setUp
    save = fixtures.ConfigTests.save

    def configure(self, C=2, group=2):
        self.config.update(suite='progression', progression_batch_rows=group)
        self.config['resources']['max_active_batches'] = C
        self.config['ray_roots'] = {name:str(self.root / ('ray-' + name)) for name in entry.PROGRESSION_CASES}
        self.save()
        return entry.configuration(self.path)

    def test_explicit_pair_and_C_are_visible_through_the_existing_CLI(self):
        config, _, resources, _ = self.configure()
        self.assertEqual(resources.max_active_batches, 2)
        self.assertEqual([(c['progression'], c['progression_batch_rows']) for c in config['cases']],
                         [('record', None), ('batch_stage', 2)])
        output = io.StringIO()
        with patch.object(entry, 'checkout_identity', return_value={'commit':'a'*40}), redirect_stdout(output):
            self.assertEqual(entry.main(['check', '--config', str(self.path)]), 0)
        report = json.loads(output.getvalue())
        self.assertEqual((report['suite'], report['max_requests'], report['models'], report['claims']),
                         ('progression', 96, 0, 0))
        self.assertEqual(report['progression_cases'], config['cases'])

    def test_bad_group_capacity_suite_and_token_qualification_stop_before_loading(self):
        for C, group in [(2, 3), (5, 2), (2, True), (2, 0), (2, None)]:
            with self.subTest(C=C, group=group), self.assertRaises(ValueError):
                self.configure(C, group)
        self.configure()
        with patch.object(entry, 'objects') as load, self.assertRaisesRegex(ValueError, 'count-only'):
            entry.qualify(self.path, self.root / 'bad-ready.json')
        load.assert_not_called()
        with self.assertRaisesRegex(ValueError, 'not part'):
            entry.qualified(self.path, self.root / 'missing-ready.json', case_id='fifo-count')
        self.config['suite'] = 'options'; self.config['resources']['max_active_batches'] = 4; self.save()
        with self.assertRaisesRegex(ValueError, 'requires the progression suite'):
            entry.configuration(self.path)
        self.config['suite'] = 'typo'; self.save()
        with self.assertRaisesRegex(ValueError, 'unknown validation suite'):
            entry.configuration(self.path)

    def test_policy_change_invalidates_the_ready_config_binding(self):
        config, _, resources, _ = self.configure()
        source = {'commit':'a'*40, 'tree':'b'*40}
        ready = dict(status='passed_CPU_only_host_lease_pending', source=source,
            token_budget_required=False, resources=asdict(resources),
            config_sha256=entry.file_identity(self.path)['sha256'])
        path = self.root / 'ready.json'; path.write_text(json.dumps(ready))
        with patch.object(entry, 'checkout_identity', return_value=source):
            entry.qualified(self.path, path, case_id='batch-stage-count')
            self.config['progression_batch_rows'] = 1; self.save()
            with self.assertRaisesRegex(ValueError, 'changed'):
                entry.qualified(self.path, path, case_id='batch-stage-count')

    def test_case_reuses_supervisor_and_selected_public_worker_command(self):
        config, engine, _, _ = self.configure()
        config.update(execution_enabled=True, external_owner={'fixture':True})
        with patch.object(entry, 'qualified', return_value=(config, engine, None, None, None)) as qualified, \
                patch('integrations.imlane_records.chain_runtime.check_external_lease'), \
                patch('src.experiments.lotus_records_supervisor.supervise', return_value={}) as supervise:
            entry.supervised_case(self.path, self.root / 'ready.json', 'batch-stage-count')
        self.assertEqual(qualified.call_args.kwargs, {'case_id':'batch-stage-count'})
        command = supervise.call_args.args[0]
        self.assertEqual(command[2], str(entry.ENTRY))
        self.assertEqual(command[-3:], ['--case', 'batch-stage-count', '--execute-model'])

    def test_worker_CLI_executes_real_helper_both_policies_and_comparison(self):
        self.configure()
        self.config.update(execution_enabled=True, external_owner={'fixture':True}, physical_gpu_UUID='GPU-fixture')
        self.save()
        tok = tokenizer(); tok.save_pretrained(self.config['engine']['tokenizer_path'])
        ready = self.root / 'ready.json'
        source = {'commit':'a'*40, 'tree':'b'*40}
        sampler = MagicMock(); sampler.summary.return_value = {}
        telemetry = Mock(); telemetry.start.return_value = telemetry; telemetry.close.return_value = {}
        with ThreadedRuntime([]) as runtime, patch.object(AsyncCoreModelActor, '_process_type', return_value=Process), \
                patch.object(entry, 'checkout_identity', return_value=source), \
                patch.object(entry.metadata, 'version', side_effect=lambda name:'2.56.1' if name=='ray' else 'CPU-fixture'), \
                patch('integrations.imlane_records.chain_runtime.check_external_lease'), \
                patch('src.infrastructure.request_budget.ClaimedUnit.bind_execution'), \
                patch('src.experiments.process_sampling.ProcessSampler', return_value=sampler), \
                patch('src.observability.records_telemetry.RecordsTelemetry', return_value=telemetry), \
                redirect_stdout(io.StringIO()):
            state = [False]
            runtime.ray.is_initialized = lambda: state[0]
            runtime.ray.init = Mock(side_effect=lambda **kw: state.__setitem__(0, True))
            runtime.ray.shutdown = Mock(side_effect=lambda: state.__setitem__(0, False))
            entry.main(['qualify', '--config', str(self.path), '--count-only', '--output', str(ready)])
            root = Path(self.config['artifact_root']); root.mkdir()
            ledger = CellBudgetLedger.create(root / 'budget.sqlite', AttemptBudget(self.config['run_id'], 96),
                                            deadline_utc=time.time()+60)
            for name in entry.PROGRESSION_CASES: ledger.reserve_unit(name, 48)
            for name, policy, group in [('record-count', 'record', None), ('batch-stage-count', 'batch_stage', 2)]:
                entry.main(['worker', '--config', str(self.path), '--ready', str(ready),
                            '--case', name, '--execute-model'])
                report = entry.read(root / 'cases' / name / 'case.json')
                self.assertEqual((report['status'], report['actual_calls']), ('completed', 48))
                self.assertEqual((report['progression']['policy'], report['progression']['batch_rows']), (policy, group))
                self.assertEqual(report['progression'], report['close']['progression'])
                self.assertEqual(report['driver_progression'], {'policy':policy, 'batch_rows':group})
                self.assertEqual(report['capacity']['held_task_limit'], 2)
                self.assertEqual(report['capacity']['retained_rows'], 2)
                self.assertLessEqual(report['close']['peak_core_usage']['active_requests'], 2)
                self.assertEqual(report['replay']['recorded_answers_replayed'], 48)
                self.assertTrue(report['replay']['native_prompt_ids_checked'])
            output = self.root / 'comparison.json'
            entry.main(['compare', '--config', str(self.path), '--output', str(output)])
            self.assertTrue(entry.read(output)['outputs_strictly_comparable'])
            self.assertEqual([u['claimed'] for u in ledger.snapshot()['units']], [1, 1])
            self.assertEqual(runtime.ray.init.call_count, 2)
            self.assertEqual(runtime.ray.shutdown.call_count, 2)

    def test_comparator_rejects_stale_policy_and_capacity_receipts(self):
        config, engine, resources, generation = self.configure()
        fixture = fixtures.CapturedUidTests(); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        digest = entry.file_identity(self.path)['sha256']
        reports = fixture.reports[:2]
        for case, report in zip(config['cases'], reports):
            progression = dict(policy=case['progression'], batch_rows=case['progression_batch_rows'])
            report.update(case_id=case['case_id'], suite='progression', config_sha256=digest,
                input_sha256=config['input']['sha256'], execution='record_local_sections',
                engine=json.loads(json.dumps(asdict(engine))), generation=asdict(generation), resources=asdict(resources),
                progression=progression, driver_progression=deepcopy(progression),
                capacity=dict(active_request_limit=2, held_task_limit=2,
                    retained_rows=2, retained_bytes=200, row_reservation_bytes=100),
                close=dict(execution='record_local_sections', progression=progression, status='closed', query_completed=True,
                    cross_query_model_reuse=False, model_batch_rows=1, core_usage={'active_work':0},
                    peak_core_usage={'active_requests':2, 'held_tasks':2}))
        def compare():
            root = Path(config['artifact_root'])
            for case, report in zip(config['cases'], reports):
                path = root / 'cases' / case['case_id']; path.mkdir(parents=True, exist_ok=True)
                (path / 'case.json').write_text(json.dumps(report))
            return checks.compare(root, config, config_sha256=digest)
        self.assertTrue(compare()['outputs_strictly_comparable'])
        original = deepcopy(reports[1])
        mutations = [
            ('progression', {'policy':'record', 'batch_rows':None}),
            ('progression', {'policy':'batch_stage', 'batch_rows':2.0}),
            ('driver_progression', {'policy':'record', 'batch_rows':None}),
            ('capacity', None),
            ('resources', {**original['resources'], 'max_active_batches':4}),
            ('config_sha256', '0'*64), ('input_sha256', '0'*64),
            ('capacity', {**original['capacity'], 'held_task_limit':1}),
            ('capacity', {**original['capacity'], 'retained_bytes':100}),
            ('capacity', {**original['capacity'], 'row_reservation_bytes':0}),
            ('close', {**original['close'], 'progression':{'policy':'record', 'batch_rows':None}}),
            ('close', {**original['close'], 'peak_core_usage':{'active_requests':3, 'held_tasks':2}}),
            ('close', {**original['close'], 'query_completed':False}),
        ]
        for field, invalid in mutations:
            with self.subTest(field=field):
                reports[1] = {**deepcopy(original), field:invalid}
                with self.assertRaises(ValueError): compare()
        reports[1] = deepcopy(original)
        reports[1]['progression']['batch_rows'] = 1
        reports[1]['close']['progression']['batch_rows'] = 1
        with self.assertRaises(ValueError): compare()
