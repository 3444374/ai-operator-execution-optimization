import argparse
import ast
import contextlib
import copy
import datetime
import json
import hashlib
import io
import os
from pathlib import Path
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import Mock, patch

"""CPU fixtures for the maintained entry; no model or external process launched."""
from functools import partial
from src.experiments import movie_records_matrix as maintained
from src.experiments import movie_matrix_support as support


def declared_budget(calls=1, queries=1):
    return dict(max_new_calls=calls,query_count=queries,max_total_wall_s=10800,
                launch_deadline_s=9600,cleanup_reserve_s=1200,automatic_retries=0,model_repair_reserve=0)


def functions(*names):
    # Reuse the same fault cases against actual maintained methods, not source copies.
    ns = dict(Path=Path, datetime=datetime, json=json, os=os, sys=sys, time=time,
              print=Mock(), COMMIT='fixture-commit', LOCK='fixture-lock',
              BUDGET_ID='fixture-only-no-ledger')
    def invoke(name, *args, **kwargs):
        driver = object.__new__(maintained.MovieMatrix)
        driver.root = ns.get('ROOT', Path('.'))
        source_root = Path(maintained.__file__).resolve().parents[3]
        driver.entrypoint = source_root / 'code/scripts/experiments/run_movie_records_matrix.py'
        driver.budget_id = ns['BUDGET_ID']
        driver.budget_path = driver.root / 'budget.sqlite'
        driver.packet_digest = 'packet'
        driver.packet = types.SimpleNamespace(HERE=ns.get('HERE'), SOURCE=ns.get('SOURCE',source_root),
            COMMIT=ns['COMMIT'], LOCK=ns['LOCK'],
            **{key: ns.get(key) for key in ('read','read_report','save','sha','verify','runtime_binding')})
        driver.host = ns.get('host')
        driver.evidence = types.SimpleNamespace(durable_terminals=ns.get('durable_terminals'))
        driver.audit = types.SimpleNamespace(normalize_ids=lambda *_: ([1,2],[]))
        driver.finalizers = ns.get('original')
        if name != 'bound' and 'bound' in ns: driver.bound = ns['bound']
        if name != 'run_group' and 'run_group' in ns: driver.run_group = ns['run_group']
        driver.validate_context = ns.get('validate_launch_context',ns.get('bootstrap_context',Mock()))
        def fixture_context(cfg, packet_sha, index, group, lease, output, deadline, gpu):
            # Group failure tests stub only configuration; real schema coverage is below.
            return dict(ipc_prefix='/s000000000',ipc_receipts=str(output/'ipc'))
        driver.build_context = ns.get('build_launch_context',fixture_context)
        dependencies = {key: ns[key] for key in ('CellBudgetLedger','AttemptBudget','MovieRecordSource',
            'check_plan','validate_result','CallLedger','TelemetryEvents','QueryClock','run_movie_session',
            'subprocess','time') if key in ns}
        with patch.multiple(maintained,**dependencies),patch.object(maintained,'print',ns['print'],create=True),patch.object(support,'print',ns['print'],create=True):
            return getattr(driver,name)(*args,**kwargs)
    for name in names: ns[name] = partial(invoke,name)
    return ns


def imported_modules(**symbols):
    modules = {}
    for qualified, exports in symbols.items():
        parts = qualified.split('.')
        for end in range(1, len(parts) + 1):
            name = '.'.join(parts[:end])
            if name not in modules:
                modules[name] = types.ModuleType(name)
                modules[name].__path__ = []
        modules[qualified].__dict__.update(exports)
    return modules

class WorkerFixture:

    def __init__(self, root, *, clock_error=None, business_error=None, required_error=None):
        self.output = root / 'output'
        self.output.mkdir()
        self.ns = ns = functions('worker')
        here = root / 'packet'
        declaration = dict(query_id='q0', manifest='fixture-manifest')
        plan = dict(backend='fixture', lifecycle='isolated', queries=[declaration])
        group = dict(plan='session.json', max_calls=1)
        context = dict(owner_context={}, packet_sha256='packet', plan_sha256='plan', assigned_gpu_uuid='fixture-device', worker_deadline_monotonic=float('inf'))
        raw = dict(calls=dict(records=[dict(status='succeeded', submission_confirmed=True, messages_sha256='messages', row_id='r0', call_id='c0')]))
        self.query = dict(query_id='q0', task='movie-q3', backend_report=raw)
        report = dict(status='completed', owners=[dict(status='closed')], queries=[self.query])
        values = {here / 'session.json': plan, root / 'context.json': context, here / 'input-bindings.json': {'movie-q3': {'token_reference': 'reference'}}, 'reference': {'rows': {'r0': dict(messages_sha256='messages', expected_prompt_token_ids=[1, 2])}}}
        ns.update(HERE=here, ROOT=root, bound=Mock(return_value=({}, dict(budget=declared_budget()), [group])), read=lambda path: copy.deepcopy(values[path]), read_report=Mock(return_value=self.query), check_plan=Mock(return_value=dict(queries=[declaration])), bootstrap_context=Mock(), host=types.SimpleNamespace(assigned_gpu=Mock(return_value={'uuid': 'fixture-device'}), helper=types.SimpleNamespace(error_record=lambda error: dict(type=type(error).__name__))), CellBudgetLedger=Mock(return_value=types.SimpleNamespace(claim_unit=Mock(return_value='fixture-claim'))), AttemptBudget=Mock(), sha=Mock(return_value='plan'), MovieRecordSource=types.SimpleNamespace(from_manifest=Mock(return_value=types.SimpleNamespace(expected=Mock(return_value=['r0'])))), validate_result=Mock(), durable_terminals=lambda *_a, **_kw: contextlib.nullcontext(), CallLedger=object, TelemetryEvents=lambda *_a: contextlib.nullcontext(Mock()), QueryClock=lambda **_kw: types.SimpleNamespace(events=[], record=Mock()))

        def save(path, value):
            if path.name == 'clock.json' and clock_error is not None:
                raise clock_error
            if path.name == required_error:
                raise OSError('required report unavailable')
            path.write_text(json.dumps(value))

        def session(_plan, _context, _output, *, observer, checkpoint):
            if business_error is not None:
                raise business_error
            checkpoint(dict(query_index=0))
            return report
        ns.update(save=save, run_movie_session=Mock(side_effect=session))
        self.modules = imported_modules(**{'integrations.imlane_records.chain_runtime': {'check_external_lease': Mock()}, 'token_audit': {'normalize_ids': Mock(return_value=([1, 2], []))}})
        self.args = ('packet', 0, root / 'context.json', self.output)

    def run(self):
        with patch.dict(sys.modules, self.modules):
            return self.ns['worker'](*self.args)

class WorkerTests(unittest.TestCase):

    def test_optional_clock_failure_keeps_success(self):
        with tempfile.TemporaryDirectory() as folder:
            f = WorkerFixture(Path(folder), clock_error=OSError('optional clock unavailable'))
            f.run()
            self.assertEqual(json.loads((f.output / 'functional-validation.json').read_text())['actual_calls'], 1)
            f.ns['validate_result'].assert_called_once()
            f.ns['check_plan'].assert_called_once()
            f.ns['bound'].assert_called_once_with('packet', check_schedule=False)
            f.ns['print'].assert_called_once()
            self.assertIs(f.ns['print'].call_args.kwargs['file'], sys.stderr)

    def test_optional_clock_failure_keeps_original_exception_identity(self):
        original = RuntimeError('original business failure')
        with tempfile.TemporaryDirectory() as folder:
            f = WorkerFixture(Path(folder), clock_error=OSError('clock failed'), business_error=original)
            with self.assertRaises(RuntimeError) as observed:
                f.run()
            self.assertIs(observed.exception, original)
            self.assertEqual(json.loads((f.output / 'query.json').read_text())['status'], 'failed')

    def test_required_result_write_failure_stays_fatal(self):
        with tempfile.TemporaryDirectory() as folder:
            f = WorkerFixture(Path(folder), required_error='functional-validation.json')
            with self.assertRaisesRegex(OSError, 'required report'):
                f.run()

    def test_clock_interrupt_is_not_downgraded(self):
        with tempfile.TemporaryDirectory() as folder:
            f = WorkerFixture(Path(folder), clock_error=KeyboardInterrupt())
            with self.assertRaises(KeyboardInterrupt):
                f.run()

class GroupFixture:

    def __init__(self, root, *, optional_error=None, business_error=None, physical_error=None, confirmed=True):
        self.ns = ns = functions('run_group')
        (root / 'units').mkdir()
        here = root / 'packet'
        group = dict(max_calls=1, backend='fixture', profile='p1', repetition=0, group='fixture', plan='session.json')
        cfg = dict(driver_python='fixture-python', engine={'model_python': 'fixture-model-python'}, rpc_root_prefix=str(root / 'rpc-'), ray_root_prefix=str(root / 'ray-'), environment_file='fixture-env', lease_scope=str(root / 'scope'))
        query = dict(query_id='q0', task='movie-q3', phase='measured', full_query_seconds=1, post_result_seconds=0.1, backend_report=dict(calls={'counts': {}}, result={}))
        values = {'session.json': {'backend': 'fixture', 'lifecycle':'isolated', 'resources':dict(startup_timeout_s=300,query_timeout_s=480), 'queries': [{'query_id': 'q0'}]}, 'matrix.json':dict(budget=declared_budget()), 'comparison.json': {'shared_resources': {}}, 'functional-validation.json': {'status': 'passed'}, 'token-audit.json': {'status': 'passed'}, 'query.json': {'application_e2e_s': 2, 'queries': [query]}}
        self.supervise = Mock(side_effect=business_error, return_value={'status': 'passed'})
        self.cleanup = Mock(side_effect=physical_error, return_value=dict(original_supervisor_passed=confirmed, independent_physical_exit=confirmed))
        self.performance = types.SimpleNamespace(close=Mock(side_effect=optional_error))
        ns.update(ROOT=root, HERE=here, SOURCE=root / 'source', read=lambda path: copy.deepcopy(values[path.name]), read_report=lambda path: copy.deepcopy(values[path.name]), bootstrap_context=Mock(), runtime_binding=Mock(return_value={}), sha=Mock(return_value='sha'), save=lambda path, value: path.write_text(json.dumps(value)), host=types.SimpleNamespace(idle=Mock(), helper=types.SimpleNamespace(check_lease=Mock()), assigned_gpu=Mock(return_value={'uuid': 'fixture-device'}), FullMonitor=Mock()), original=types.SimpleNamespace(optional_performance=Mock(return_value=self.performance), unit_final=self.cleanup), subprocess=types.SimpleNamespace(run=Mock(return_value=types.SimpleNamespace(returncode=0, stdout='', stderr=''))))
        self.modules = imported_modules(**{'src.experiments.lotus_records_supervisor': {'supervise': self.supervise}})
        self.args = (cfg, 'packet', 0, group, types.SimpleNamespace(path=root / 'lease'), time.monotonic() + 10000, Mock())

    def run(self):
        with patch.dict(sys.modules, self.modules):
            return self.ns['run_group'](*self.args)

class GroupTests(unittest.TestCase):

    def test_optional_sampler_close_cannot_skip_physical_cleanup(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}):
            f = GroupFixture(Path(folder), optional_error=OSError('optional sampler close'))
            self.assertEqual(f.run()['status'], 'completed')
            f.cleanup.assert_called_once()
            self.assertNotIn('SEMLOOM_RUN_IPC_PREFIX', os.environ)
            f.ns['subprocess'].run.assert_called_once()

    def test_optional_sampler_close_does_not_mask_supervisor_error(self):
        original = RuntimeError('original supervisor failure')
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}):
            f = GroupFixture(Path(folder), optional_error=OSError('sampler'), business_error=original)
            with self.assertRaises(RuntimeError) as observed:
                f.run()
            self.assertIs(observed.exception, original)
            f.cleanup.assert_called_once()
            f.ns['subprocess'].run.assert_not_called()

    def test_unconfirmed_exit_remains_fatal(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}):
            f = GroupFixture(Path(folder), confirmed=False)
            with self.assertRaises(AssertionError):
                f.run()
            f.supervise.assert_called_once()
            f.ns['subprocess'].run.assert_not_called()

    def test_independent_cleanup_failure_remains_fatal(self):
        error = RuntimeError('resource ownership unresolved')
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}):
            f = GroupFixture(Path(folder), physical_error=error)
            with self.assertRaises(RuntimeError) as observed:
                f.run()
            self.assertIs(observed.exception, error)
            self.assertNotIn('SEMLOOM_RUN_IPC_CONTEXT', os.environ)

    def test_sampler_interrupt_propagates_after_mandatory_cleanup(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}):
            f = GroupFixture(Path(folder), optional_error=KeyboardInterrupt())
            with self.assertRaises(KeyboardInterrupt):
                f.run()
            f.cleanup.assert_called_once()
            self.assertNotIn('SEMLOOM_RUN_IPC_CONTEXT', os.environ)

class ControllerFixture:

    def __init__(self, root, *, required_error=None, group_error=None, overrun=False):
        self.ns = ns = functions('controller')
        self.clock = [0.0]
        self.lease = types.SimpleNamespace(release=Mock())
        self.ledger = types.SimpleNamespace(reserve_unit=Mock(), snapshot=Mock(return_value=dict(allocated_requests=1)))
        group = dict(plan='session.json',max_calls=1)
        self.host = types.SimpleNamespace(monitoring_flags=Mock(return_value={}), idle=Mock(return_value={}), acquire_owner_lease=Mock(return_value=self.lease), helper=types.SimpleNamespace(error_record=lambda e: dict(type=type(e).__name__), check_lease=Mock()))

        def save(path, value):
            if path.name == required_error:
                raise OSError('required evidence unavailable')
            path.write_text(json.dumps(value))

        def run(*_args):
            if group_error is not None:
                raise group_error
            if overrun:
                self.clock[0] = 10801.0
            return dict(status='completed', actual_calls=1)
        ns.update(ROOT=root, HERE=root / 'packet', bound=Mock(return_value=({'lease_scope': str(root / 'lease-scope')}, dict(budget=declared_budget()), [group])), host=self.host, CellBudgetLedger=types.SimpleNamespace(create=Mock(return_value=self.ledger)), AttemptBudget=Mock(), check_plan=Mock(return_value=dict(expected_calls=1,queries=[dict(query_id='q0',task='movie-q3',expected_calls=1)])), read=Mock(return_value=dict(queries=[dict(query_id='q0', task='movie-q3')])), save=save, run_group=Mock(side_effect=run), time=types.SimpleNamespace(monotonic=lambda: self.clock[0], time=lambda: 0.0))

    def run(self, *, progress_error=None):
        original_open = Path.open

        def open_path(path, *args, **kwargs):
            if path.name == 'progress.jsonl' and progress_error is not None:
                raise progress_error
            return original_open(path, *args, **kwargs)
        with patch.object(Path, 'open', open_path):
            return self.ns['controller']('packet')

class ControllerTests(unittest.TestCase):

    def test_optional_progress_failure_keeps_success_and_required_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            f = ControllerFixture(Path(folder))
            report = f.run(progress_error=OSError('progress unavailable'))
            self.assertEqual(report['status'], 'completed')
            self.assertTrue((Path(folder) / 'campaign.json').is_file())
            f.lease.release.assert_called_once()
            f.ns['run_group'].assert_called_once()

    def test_real_gpu_conflict_remains_fatal_before_ledger_or_work(self):
        with tempfile.TemporaryDirectory() as folder:
            f = ControllerFixture(Path(folder))
            f.host.idle.side_effect = RuntimeError('foreign GPU work')
            with self.assertRaisesRegex(RuntimeError, 'foreign GPU'):
                f.run()
            f.ns['CellBudgetLedger'].create.assert_not_called()
            f.ns['run_group'].assert_not_called()

    def test_unknown_group_is_not_retried_or_released_as_clean(self):
        original = RuntimeError('submitted outcome unknown')
        with tempfile.TemporaryDirectory() as folder:
            f = ControllerFixture(Path(folder), group_error=original)
            with self.assertRaises(RuntimeError) as observed:
                f.run()
            self.assertIs(observed.exception, original)
            f.ns['run_group'].assert_called_once()
            f.lease.release.assert_not_called()
            self.assertFalse(json.loads((Path(folder) / 'campaign.json').read_text())['physical_exit_confirmed'])

    def test_required_before_release_evidence_failure_keeps_lease(self):
        with tempfile.TemporaryDirectory() as folder:
            f = ControllerFixture(Path(folder), required_error='before-release.json')
            with self.assertRaisesRegex(OSError, 'required evidence'):
                f.run()
            f.lease.release.assert_not_called()

    def test_required_campaign_evidence_failure_stays_fatal(self):
        with tempfile.TemporaryDirectory() as folder:
            f = ControllerFixture(Path(folder), required_error='campaign.json')
            with self.assertRaisesRegex(OSError, 'required evidence'):
                f.run()

    def test_total_wall_overrun_remains_fatal(self):
        with tempfile.TemporaryDirectory() as folder:
            f = ControllerFixture(Path(folder), overrun=True)
            with self.assertRaises(TimeoutError):
                f.run()
            report = json.loads((Path(folder) / 'campaign.json').read_text())
            self.assertEqual(report['status'], 'failed')
            self.assertIn('total_wall', [e['phase'] for e in report['finalization_errors']])

class BoundTests(unittest.TestCase):

    def fixture(self):
        ns = functions('bound')
        here = Path('fixture-packet')
        matrix = dict(execution_enabled=True, source_binding=dict(final_source='fixture-commit', final_lock='fixture-lock'), budget=declared_budget(44280,87))
        groups = [dict(plan='session.json', max_calls=44280)]
        values = {'matrix.json': matrix, 'owner-groups.json': groups, 'session.json': {'queries': [dict(query_id='q' + str(i)) for i in range(87)]}, 'input-bindings.json': {'movie-q3': dict(manifest='manifest', manifest_sha256='sha', token_reference='tokens', token_reference_sha256='sha', messages='messages', messages_sha256='sha', calls=1)}}
        ns.update(HERE=here, verify=Mock(return_value=dict(budget_limit=44280,wall_s=10800)), host=types.SimpleNamespace(protected=Mock(), verify_assets=Mock()), read=Mock(side_effect=lambda path: copy.deepcopy(values[path.name])), sha=Mock(return_value='sha'), MovieRecordSource=types.SimpleNamespace(from_manifest=Mock(return_value=types.SimpleNamespace(task=types.SimpleNamespace(name='movie-q3'), expected=Mock(return_value=['r0'])))))
        return (ns, values)

    def test_controller_still_checks_full_matrix_and_input_hashes(self):
        ns, values = self.fixture()
        ns['bound']('packet')
        self.assertEqual(ns['read'].call_count, 4)
        ns['host'].verify_assets.assert_called_once()
        values['input-bindings.json']['movie-q3']['messages_sha256'] = 'changed'
        with self.assertRaises(AssertionError):
            ns['bound']('packet')

    def test_duplicate_global_ids_remain_rejected_by_default(self):
        ns, values = self.fixture()
        values['session.json']['queries'][-1]['query_id'] = 'q0'
        with self.assertRaises(AssertionError):
            ns['bound']('packet')

    def test_worker_skips_schedule_scan_but_keeps_packet_assets_inputs_and_ledger_protection(self):
        ns, _ = self.fixture()
        ns['bound']('packet', check_schedule=False)
        self.assertEqual(ns['read'].call_count, 3)
        ns['verify'].assert_called_once_with('packet')
        ns['host'].verify_assets.assert_called_once()
        ns['host'].protected.assert_called_once()
        ns['MovieRecordSource'].from_manifest.assert_called_once()

    def test_worker_does_not_skip_source_binding_failure(self):
        ns, values = self.fixture()
        values['matrix.json']['source_binding']['final_lock'] = 'changed'
        with self.assertRaises(AssertionError):
            ns['bound']('packet', check_schedule=False)

    def test_old_qualified_source_cannot_cover_an_external_new_driver(self):
        ns, _ = self.fixture()
        ns['SOURCE'] = Path('/fixture/different-old-source')
        with self.assertRaisesRegex(ValueError,'bound source'):
            ns['bound']('packet',check_schedule=False)
        ns['verify'].assert_not_called()

    def test_offline_checks_bind_source_and_inputs_without_enabling_model_execution(self):
        ns, values = self.fixture()
        values['matrix.json']['execution_enabled'] = False
        ns['bound']('packet',host_checks=False)
        ns['host'].protected.assert_not_called()
        ns['host'].verify_assets.assert_not_called()
        ns['MovieRecordSource'].from_manifest.assert_called_once()
        with self.assertRaises(AssertionError):ns['bound']('packet')


class MaintainedIntegrationTests(unittest.TestCase):
    def test_current_packet_relocates_without_historical_code_imports(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(sys.modules), patch.object(sys, 'path', list(sys.path)):
            packet = Path(folder) / 'relocated-current-packet'
            packet.mkdir()
            sources = {name: 'MARK = "current-sealed-packet"\n' for name in (
                'q3_packet', 'q3_host', 'host_helpers', 'execution_policy',
                'active_failure_evidence', 'token_audit')}
            sources['q3_host'] += 'import host_helpers, execution_policy\n'
            for name, body in sources.items():
                sys.modules.pop(name, None)
                (packet / (name + '.py')).write_text(body)
            files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in packet.iterdir()}
            (packet / 'packet.json').write_text(json.dumps(dict(files=files)))
            digest = support.sha(packet / 'packet.json')
            runtime = maintained.load_runtime(packet, digest)
            self.assertEqual(runtime.host.MARK, 'current-sealed-packet')
            for name in sources:
                self.assertEqual(Path(sys.modules[name].__file__).parent, packet.resolve())

    def test_changed_packet_fails_before_any_runtime_adapter_import(self):
        with tempfile.TemporaryDirectory() as folder:
            packet = Path(folder)
            (packet / 'packet.json').write_text('{}')
            with patch.object(maintained.importlib, 'import_module') as imported:
                with self.assertRaises(ValueError):
                    maintained.load_runtime(packet, 'wrong-sha')
                imported.assert_not_called()

    def test_unsealed_helper_fails_before_any_runtime_adapter_import(self):
        with tempfile.TemporaryDirectory() as folder:
            packet = Path(folder)
            files = {}
            for name in ('q3_packet', 'q3_host', 'host_helpers', 'execution_policy', 'active_failure_evidence', 'token_audit'):
                path = packet / (name + '.py')
                path.write_text('MARK = 1\n')
                files[path.name] = support.sha(path)
            (packet / 'packet.json').write_text(json.dumps(dict(files=files)))
            digest = support.sha(packet / 'packet.json')
            (packet / 'host_helpers.py').write_text('MARK = 2\n')
            with patch.object(maintained.importlib, 'import_module') as imported:
                with self.assertRaises(ValueError):
                    maintained.load_runtime(packet, digest)
                imported.assert_not_called()

    def test_public_run_without_execution_flag_only_checks(self):
        driver = Mock()
        driver.check.return_value = dict(status='checked_not_executed', actual_calls=0, claims=0)
        argv = ['run', '--packet', '/fixture/current-packet', '--packet-sha', 'sha', '--output-dir', '/fixture/output']
        with patch.object(maintained, 'load_runtime'), patch.object(maintained, 'MovieMatrix', return_value=driver), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(maintained.main(argv), 0)
        driver.check.assert_called_once_with('sha',offline=False)
        driver.controller.assert_not_called()

    def test_offline_cannot_be_used_to_execute_or_bypass_runtime_gates(self):
        with patch.object(maintained,'load_runtime') as runtime,contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                maintained.main(['run','--offline','--execute-model','--packet','/fixture/packet',
                    '--packet-sha','sha','--output-dir','/fixture/out'])
        runtime.assert_not_called()

    def test_worker_missing_arguments_stop_before_packet_loading(self):
        with patch.object(maintained, 'load_runtime') as runtime, contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                maintained.main(['worker', '--packet', '/fixture/packet', '--packet-sha', 'sha', '--output-dir', '/fixture/out'])
        runtime.assert_not_called()

    def test_subprocess_commands_use_the_maintained_entry(self):
        driver = object.__new__(maintained.MovieMatrix)
        driver.entrypoint = Path('/fixture/source/code/scripts/experiments/run_movie_records_matrix.py')
        driver.root, driver.packet = Path('/fixture/output'), types.SimpleNamespace(HERE=Path('/fixture/current-packet'))
        driver.packet_digest = 'sha'
        cfg = dict(driver_python='driver-python', engine=dict(model_python='model-python'))
        worker = driver.worker_argv(cfg, 'sha', 2, Path('/fixture/context'), Path('/fixture/worker'))
        audit = driver.audit_argv(cfg, Path('/fixture/report'), Path('/fixture/audit'))
        self.assertEqual(worker[:4], ['driver-python', '-B', str(driver.entrypoint), 'worker'])
        self.assertEqual(audit[:4], ['model-python', '-B', str(driver.entrypoint), 'audit'])
        self.assertNotIn('matrix_driver.py', ' '.join(worker + audit))

    def test_r3_continuation_preserves_snapshot_skips_and_never_reserves(self):
        with tempfile.TemporaryDirectory() as folder:
            fixture = ControllerFixture(Path(folder))
            packet = Path(folder) / 'packet'
            packet.mkdir()
            (packet / 'continuation.json').write_text('{}')
            snapshot = dict(allocated_requests=44280, deadline_utc=10800)
            fixture.ledger.snapshot.return_value = snapshot
            policy = dict(budget_path=str(Path(folder) / 'budget.sqlite'), budget_id='fixture-only-no-ledger',
                skip_owner_groups=[0], remaining_queries=83, max_new_calls=41920,
                original_budget_snapshot=copy.deepcopy(snapshot), original_controller_deadline_utc=10800)
            cfg = dict(lease_scope=str(Path(folder) / 'lease-scope'))
            fixture.ns['bound'].return_value = (cfg, dict(budget=declared_budget(44280,87)), [dict(plan='g0.json'), dict(plan='g1.json')])
            fixture.ns['read'] = lambda path: policy if path.name == 'continuation.json' else {}
            ledger_type = Mock(return_value=fixture.ledger)
            fixture.ns['CellBudgetLedger'] = ledger_type
            result = fixture.run()
            ledger_type.create.assert_not_called()
            fixture.ledger.reserve_unit.assert_not_called()
            fixture.ns['run_group'].assert_called_once()
            self.assertEqual(fixture.ns['run_group'].call_args.args[2], 1)
            self.assertEqual(result['status'], 'completed_remaining83_original_matrix_incomplete')
            self.assertEqual(result['original_logical_unknown'], 120)
            self.assertEqual(result['budget'], policy['original_budget_snapshot'])

    def test_consecutive_resident_queries_require_the_same_engine_identity(self):
        for same_engine in (True,False):
            with self.subTest(same_engine=same_engine), tempfile.TemporaryDirectory() as folder:
                fixture = WorkerFixture(Path(folder))
                declarations = [dict(query_id='q'+str(i),manifest='fixture-manifest') for i in range(2)]
                plan = dict(backend='fixture',lifecycle='resident',queries=declarations)
                fixture.ns['bound'].return_value = ({},dict(budget=declared_budget(2,2)),[dict(plan='session.json',max_calls=2)])
                fixture.ns['check_plan'].return_value = dict(queries=declarations)
                original_read = fixture.ns['read']
                def read(path):
                    if getattr(path,'name',None)=='session.json':return plan
                    if getattr(path,'name',None)=='owner-ready-0.json':return dict(ready={})
                    return original_read(path)
                fixture.ns['read'] = read
                queries = []
                for i in range(2):
                    query = copy.deepcopy(fixture.query)
                    query['query_id'] = 'q'+str(i)
                    query['backend_report']['calls']['records'][0].update(call_id='c'+str(i),engine_uid='u0' if same_engine else 'u'+str(i))
                    queries.append(query)
                fixture.ns['read_report'].side_effect = lambda path:queries[int(path.stem.split('-')[-1])]
                def session(_plan,_context,_output,*,observer,checkpoint):
                    for i in range(2):checkpoint(dict(query_index=i))
                    return dict(status='completed',owners=[dict(status='closed')],queries=queries)
                fixture.ns['run_movie_session'].side_effect = session
                if same_engine:
                    fixture.run()
                    self.assertEqual(json.loads((fixture.output/'functional-validation.json').read_text())['actual_calls'],2)
                else:
                    with self.assertRaises(AssertionError):fixture.run()


class SharedLaunchTests(unittest.TestCase):
    def setUp(self):
        from src.infrastructure.runner_lease import HOST_SCOPE_DIR_NAME, LEASE_NAME
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.output = self.root / 'output'
        (self.output / 'ipc').mkdir(parents=True, mode=0o700)
        self.lease = self.root / 'artifact' / HOST_SCOPE_DIR_NAME / LEASE_NAME
        self.lease.parent.mkdir(parents=True)
        self.lease.write_text('CPU fixture only; no real host lease acquired')
        self.here = self.root / 'packet'
        self.here.mkdir()
        (self.here / 'session.json').write_text(json.dumps(dict(backend='imlane')))
        self.cfg = dict(budget_id='fixture', lease_scope=str(self.lease.parent),budget_limit=1,gpu_idle_max_mib=1,ipc_prefix_base='/s21f20000')
        self.binding = dict(driver_python='fixture-driver', model_python='fixture-model-python',
            model_path='/fixture/model', cuda_visible_devices='fixture-device', imlane_artifact_root='/fixture/artifacts',
            imlane_author_root='/fixture/author', imlane_method_python='/fixture/method-python',
            imlane_bridge_binary='/fixture/bridge', runtime_preflight_report='/fixture/preflight')
        self.packet = types.SimpleNamespace(HERE=self.here,SOURCE=self.root/'source',sha=support.sha,
            read=lambda p:dict(shared_resources=dict(cpu_affinity_slots=list(range(8)),max_sampled_owned_rss_bytes=21474836480,max_owned_shm_bytes=536870912)),
            runtime_binding=lambda _cfg: dict(self.binding))
        self.group = dict(plan='session.json')
        self.context = support.build_launch_context(self.cfg, 'packet', 1, self.group,
            types.SimpleNamespace(path=self.lease), self.output, time.monotonic()+600,
            dict(uuid='fixture-device'), packet=self.packet,
            budget_path=self.root/'never-opened-budget.sqlite', budget_id='fixture')

    def test_complete_configuration_and_optional_imlane_fields(self):
        for backend in ('lotus-native', 'semloom-core', 'ray-data', 'daft', 'imlane'):
            support.validate_launch_context(self.context, self.output, backend)
        context = copy.deepcopy(self.context)
        for name in ('imlane_artifact_root','imlane_author_root','imlane_method_python','imlane_bridge_binary','runtime_preflight_report'):
            context['runtime_binding'].pop(name)
        support.validate_launch_context(context, self.output, 'lotus-native')
        with self.assertRaises(ValueError):
            support.validate_launch_context(context, self.output, 'imlane')

    def test_every_required_field_stops_actual_worker_before_claim(self):
        required = [(key,) for key in ('packet_sha256','plan_sha256','assigned_gpu_uuid','worker_deadline_monotonic',
            'ipc_prefix','ipc_receipts','budget','owner_context','runtime_binding','sembench_checkout')]
        required += [('budget', key) for key in ('path','id','limit')]
        required += [('owner_context', key) for key in ('lease_path','lease_sha256','artifact_root','gpu_idle_max_mib','owner_resource_limits')]
        required += [('runtime_binding', key) for key in self.binding]
        required += [('owner_context','owner_resource_limits',key) for key in ('cpu_cores','rss_bytes','shm_bytes')]
        driver = object.__new__(maintained.MovieMatrix)
        driver.bound = Mock(return_value=(self.cfg, {}, [self.group]))
        driver.packet = types.SimpleNamespace(HERE=self.here, read=lambda path: json.loads(path.read_text()))
        driver.validate_context = support.validate_launch_context
        target = self.root/'worker-context.json'
        with patch.object(maintained,'CellBudgetLedger',side_effect=AssertionError('budget opened before validation')) as ledger, patch.object(maintained,'check_plan',return_value={}):
            for keys in required:
                with self.subTest(field='.'.join(keys)):
                    context = copy.deepcopy(self.context)
                    parent = context
                    for key in keys[:-1]: parent = parent[key]
                    parent.pop(keys[-1]);target.write_text(json.dumps(context))
                    with self.assertRaises(ValueError):
                        driver.worker('packet',0,target,self.output)
            ledger.assert_not_called()

    def test_actual_group_uses_shared_constructor_and_validator(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}):
            fixture = GroupFixture(Path(folder))
            cfg, packet_sha, index, group, lease, deadline, emit = fixture.args
            cfg.update(lease_scope=str(self.lease.parent),budget_limit=1,gpu_idle_max_mib=1,ipc_prefix_base='/s21f20000')
            lease.path = self.lease
            packet = types.SimpleNamespace(HERE=fixture.ns['HERE'],SOURCE=self.packet.SOURCE,read=self.packet.read,sha=fixture.ns['sha'], runtime_binding=lambda _: dict(self.binding))
            fixture.ns['build_launch_context'] = partial(support.build_launch_context,packet=packet,
                budget_path=self.root/'never-opened-budget.sqlite',budget_id='fixture')
            fixture.ns['validate_launch_context'] = support.validate_launch_context
            result = fixture.run()
            context = json.loads((Path(folder)/'units/g000/context.json').read_text())
            self.assertEqual(result['status'],'completed')
            self.assertEqual(context['owner_context']['lease_path'],str(self.lease))
            self.assertEqual(context['ipc_receipts'],str(Path(folder)/'units/g000/ipc'))
            self.assertEqual(context['runtime_binding']['model_path'],self.binding['model_path'])


class SharedFinalizerTests(unittest.TestCase):
    def run_finalizer(self, folder, *, supervised_ok=True, live=False, require_supervisor=True):
        packet = types.SimpleNamespace(read_report=Mock(return_value={}),save=Mock())
        monitor = types.SimpleNamespace(finish=Mock(return_value=dict(passed=True)),owned={(123,456.)})
        host = types.SimpleNamespace(api=Mock(),identities=lambda _: set(),cleanup_ipc=Mock(),idle=Mock(return_value={}),
            helper=types.SimpleNamespace(live_owned=Mock(return_value=live),cleanup_owned=Mock(return_value={})))
        supervised = dict(status='passed' if supervised_ok else 'failed',observed_owned_exit_confirmed=supervised_ok)
        return support.unit_final({},Path(folder),supervised,monitor,require_supervisor=require_supervisor,host=host,packet=packet)

    def test_normal_physical_exit_remains_distinct_from_supervisor_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            self.assertTrue(self.run_finalizer(folder)['independent_physical_exit'])
            report = self.run_finalizer(folder,supervised_ok=False,require_supervisor=False)
            self.assertTrue(report['independent_physical_exit'])
            self.assertFalse(report['original_supervisor_passed'])

    def test_failed_supervisor_still_blocks_qualification(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(RuntimeError):self.run_finalizer(folder,supervised_ok=False)

    def test_unconfirmed_live_owner_still_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(RuntimeError):self.run_finalizer(folder,live=True)
