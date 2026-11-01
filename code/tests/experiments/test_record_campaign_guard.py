"""Whole-campaign limits and existing supervisor cleanup; no models or GPU."""
from copy import deepcopy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from src.experiments.attempt_ledger import BudgetError
from src.experiments import lotus_records_supervisor as supervisor


TOOL = Path(__file__).resolve().parents[2] / 'scripts/profiling/record_observation/campaign_guard.py'
SPEC = importlib.util.spec_from_file_location('record_campaign_guard_tests_target', TOOL)
guard_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = guard_module
SPEC.loader.exec_module(guard_module)


class Clock:
    def __init__(self):
        self.now = 100.
        self.epoch = time.time()

    def monotonic(self):
        return self.now

    def wall(self):
        return self.epoch + self.now - 100.


class CampaignFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='record-campaign-test-')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / 'campaign'
        self.artifacts = self.base / 'artifacts'
        self.artifacts.mkdir()
        self.clock = Clock()
        self.clock_identity = 'CPU-test-monotonic-clock'

    def manifest(self, **changes):
        config = self.base / 'base-config.json'
        config.write_text('{}\n')
        cells = []
        for name, calls in [('first', 3), ('second', 4)]:
            cell = self.artifacts / name
            cell.mkdir(exist_ok=True)
            cells.append(dict(id=name, calls=calls, stage='qualification',
                base_config=str(config), base_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
                prepared_config=str(cell / 'PREPARED-CONFIG.json')))
        manifest = dict(schema='semloom.record_campaign.v1', campaign_id='cpu-campaign',
            source=dict(head='1'*40, tree='2'*40), wall_seconds=180., max_bytes=100_000,
            scan_interval_s=2., cleanup_reserve_s=20., collection_reserve_s=10.,
            artifact_roots=[str(self.artifacts)], cells=cells,
            call_limit=7, historical_charge=12860)
        return dict(manifest, **changes)

    def create(self, **changes):
        return guard_module.Guard.create(self.root, self.manifest(**changes),
            clock=self.clock.monotonic, epoch=self.clock.wall,
            clock_identity=self.clock_identity)

    def reopen(self, **changes):
        options = dict(clock=self.clock.monotonic, epoch=self.clock.wall,
                       clock_identity=self.clock_identity)
        return guard_module.Guard(self.root, **dict(options, **changes))

    def record(self, call_id='q:0', **changes):
        return dict(dict(call_id=call_id, query_id='q', status='succeeded',
            submission_confirmed=True, engine_uid='cpu-engine',
            engine_request_id='cpu-engine:'+call_id), **changes)

    def query_report(self, root, records, *, index=0, compatibility=False):
        session = root / 'session'
        session.mkdir(parents=True, exist_ok=True)
        key = 'backend_close' if compatibility else 'calls'
        value = records if compatibility else dict(records=records)
        (session / ('query-'+str(index)+'.json')).write_text(json.dumps(
            dict(backend_report={key: dict(calls=value) if compatibility else value})))


class CampaignStorageTests(CampaignFixture):
    def test_nested_roots_and_internal_symlinks_count_each_regular_file_once(self):
        child = self.artifacts / 'child'
        child.mkdir()
        (self.artifacts / 'one').write_bytes(b'123')
        (child / 'two').write_bytes(b'12345')
        (self.artifacts / 'same-file-link').symlink_to(child / 'two')
        (child / 'parent-link').symlink_to(self.artifacts, target_is_directory=True)
        report = guard_module.scan_bytes([self.artifacts, child, self.artifacts])
        link_bytes = sum(path.lstat().st_size for path in (
            self.artifacts / 'same-file-link', child / 'parent-link'))
        self.assertEqual((report['bytes'], report['files'], report['vanished']), (8+link_bytes, 2, 0))

    def test_external_symlink_fails_instead_of_counting_or_silently_skipping_it(self):
        external = self.base / 'outside'
        external.write_bytes(b'outside')
        (self.artifacts / 'external').symlink_to(external)
        with self.assertRaises((OSError, ValueError, guard_module.GuardStop)):
            guard_module.scan_bytes([self.artifacts])

    def test_missing_root_and_permission_failure_are_not_reported_as_zero(self):
        with self.assertRaises((OSError, ValueError, guard_module.GuardStop)):
            guard_module.scan_bytes([self.base / 'absent'])
        with patch.object(guard_module.os, 'scandir', side_effect=PermissionError('CPU fixture unreadable')):
            with self.assertRaises((OSError, ValueError, guard_module.GuardStop)):
                guard_module.scan_bytes([self.artifacts])

    def test_file_vanishing_after_directory_enumeration_does_not_hide_survivors(self):
        disappearing = self.artifacts / 'temporary'
        disappearing.write_bytes(b'temporary data')
        (self.artifacts / 'stable').write_bytes(b'12345')
        original = os.scandir

        class Enumerated:
            def __init__(self, path):
                with original(path) as entries:
                    self.entries = list(entries)
                if Path(path).resolve() == disappearing.parent.resolve() and disappearing.exists():
                    disappearing.unlink()

            def __enter__(self): return self
            def __exit__(self, *args): return False
            def __iter__(self): return iter(self.entries)
            def close(self): pass

        with patch.object(guard_module.os, 'scandir', Enumerated):
            report = guard_module.scan_bytes([self.artifacts])
        self.assertEqual((report['bytes'], report['files']), (5, 1))
        self.assertEqual(report['vanished'], 1)

    def test_space_is_aggregated_across_cells_and_equal_to_limit_stops(self):
        guard = self.create(max_bytes=20)
        (self.artifacts / 'first' / 'output').write_bytes(b'a'*9)
        (self.artifacts / 'second' / 'output').write_bytes(b'b'*10)
        guard.check(force=True)
        with (self.artifacts / 'second' / 'output').open('ab') as stream:
            stream.write(b'b')
        with self.assertRaises(guard_module.GuardStop) as stopped:
            guard.check(force=True)
        self.assertEqual(stopped.exception.reason, 'artifact_limit')

    def test_deleted_temporary_files_are_not_kept_in_current_storage_usage(self):
        guard = self.create(max_bytes=20)
        temporary = self.artifacts / 'first' / 'temporary'
        temporary.write_bytes(b'a'*18)
        guard.check(force=True)
        temporary.unlink()
        (self.artifacts / 'second' / 'output').write_bytes(b'b'*19)
        guard.check(force=True)
        scan = guard_module.scan_bytes([self.artifacts])
        self.assertEqual((scan['bytes'], scan['files']), (19, 1))


    def test_scan_failure_latches_the_campaign_stop_before_another_cell_can_claim(self):
        guard = self.create()
        moved = self.base / 'temporarily-moved-artifacts'
        self.artifacts.rename(moved)
        with self.assertRaises(guard_module.GuardStop) as stopped:
            guard.check(force=True)
        self.assertEqual(stopped.exception.reason, 'artifact_scan_error')
        moved.rename(self.artifacts)
        with self.assertRaises(guard_module.GuardStop) as repeated:
            self.reopen().claim('first')
        self.assertEqual(repeated.exception.reason, 'campaign_already_stopped')
        self.assertFalse(any(unit['claimed'] for unit in guard.ledger.snapshot()['units']))


class CampaignDeadlineAndClaimTests(CampaignFixture):
    def test_cleanup_and_collection_reserves_stop_work_at_exact_global_deadline(self):
        guard = self.create()
        self.clock.now = 249.999  # start100 + wall180 - cleanup20 - collection10.
        guard.check(force=True)
        self.clock.now = 250.
        with self.assertRaises(guard_module.GuardStop) as stopped:
            guard.check()
        self.assertEqual(stopped.exception.reason, 'time_limit')

    def test_reopen_does_not_restart_the_wall_clock_or_duplicate_cell_claim(self):
        guard = self.create()
        claim = guard.claim('first')
        self.assertIsNotNone(claim)
        self.clock.now = 245.
        reopened = self.reopen()
        with self.assertRaises((BudgetError, guard_module.GuardStop)):
            reopened.claim('first')
        reopened.check(force=True)
        self.clock.now = 250.
        with self.assertRaises(guard_module.GuardStop):
            reopened.check(force=True)

    def test_global_reservations_are_not_model_submissions_or_refunded_after_no_work(self):
        guard = self.create()
        snapshot = guard.ledger.snapshot()
        self.assertEqual((snapshot['limit'], snapshot['allocated_requests']), (7, 7))
        self.assertFalse(any(unit['claimed'] for unit in snapshot['units']))
        guard.claim('first')
        accounted = guard_module.account_calls(self.artifacts / 'first', cap=3, claimed=True)
        self.assertEqual(accounted['confirmed_submitted'], 0)
        self.assertEqual(accounted['missing_claimed'], 3)
        self.assertEqual(accounted['conservative_charge'], 3)
        self.assertIsNone(accounted['actual_submitted_exact'])
        with self.assertRaises((BudgetError, guard_module.GuardStop)):
            self.reopen().claim('first')
        self.assertIsNotNone(self.reopen().claim('second'))
        with self.assertRaises((BudgetError, guard_module.GuardStop, KeyError, ValueError)):
            guard.claim('not-a-declared-cell')

    def test_recreate_and_changed_monotonic_clock_identity_cannot_reset_campaign(self):
        guard = self.create()
        guard.claim('first')
        with self.assertRaises((FileExistsError, BudgetError, guard_module.GuardStop, ValueError)):
            self.create()
        with self.assertRaises((BudgetError, guard_module.GuardStop, ValueError)):
            self.reopen(clock_identity='different-CPU-boot')


class CampaignAccountingTests(CampaignFixture):
    def test_consistent_cross_file_call_ids_deduplicate_without_inflating_submissions(self):
        root = self.artifacts / 'deduplicate'
        records = [self.record('q:0'), self.record('q:1', status='failed')]
        self.query_report(root, records)
        (root / 'WORKER-RESULT.json').write_text(json.dumps(dict(calls=deepcopy(records))))
        report = guard_module.account_calls(root, cap=2, claimed=2)
        self.assertEqual((report['confirmed_submitted'], report['succeeded'], report['failed']), (2, 1, 1))
        self.assertEqual(report['unknown_records'], 0)
        self.assertEqual(report['missing_claimed'], 0)
        self.assertEqual(report['conservative_charge'], 2)
        self.assertEqual(report['actual_submitted_exact'], 2)

    def test_compatibility_backend_close_calls_are_counted(self):
        root = self.artifacts / 'compatibility'
        self.query_report(root, [self.record()], compatibility=True)
        report = guard_module.account_calls(root, cap=1, claimed=True)
        self.assertEqual(report['actual_submitted_exact'], 1)
        self.assertEqual(report['succeeded'], 1)

    def test_unknown_and_missing_claimed_records_do_not_manufacture_an_exact_total(self):
        root = self.artifacts / 'unknown'
        records = [self.record('q:0'), self.record('q:1', status='failed'),
                   self.record('q:2', status='unknown', submission_confirmed=False)]
        self.query_report(root, records)
        report = guard_module.account_calls(root, cap=4, claimed=True)
        self.assertEqual((report['confirmed_submitted'], report['succeeded'], report['failed'],
                          report['unknown_records'], report['missing_claimed']), (2, 1, 1, 1, 1))
        self.assertEqual(report['unknown_responsibilities'], 2)
        self.assertEqual(report['conservative_charge'], 4)
        self.assertIsNone(report['actual_submitted_exact'])

    def test_unclaimed_absent_cell_is_exact_zero_while_claimed_absence_stays_unknown(self):
        absent = self.artifacts / 'never-started'
        report = guard_module.account_calls(absent, cap=3, claimed=False)
        self.assertEqual((report['actual_submitted_exact'], report['conservative_charge']), (0, 0))
        report = guard_module.account_calls(absent, cap=3, claimed=True)
        self.assertEqual((report['missing_claimed'], report['conservative_charge']), (3, 3))
        self.assertIsNone(report['actual_submitted_exact'])

    def test_duplicate_in_one_record_list_and_cross_file_identity_conflicts_fail(self):
        duplicate = self.artifacts / 'duplicate'
        self.query_report(duplicate, [self.record(), self.record()])
        with self.assertRaises(BudgetError):
            guard_module.account_calls(duplicate, cap=2, claimed=True)
        for field, value in [('query_id', 'other-query'), ('status', 'failed'),
                             ('submission_confirmed', False)]:
            with self.subTest(conflict=field):
                root = self.artifacts / field
                self.query_report(root, [self.record()])
                (root / 'WORKER-RESULT.json').write_text(json.dumps(
                    dict(calls=[self.record(**{field: value})])))
                with self.assertRaises(BudgetError):
                    guard_module.account_calls(root, cap=1, claimed=True)

    def test_record_count_above_cap_fails_instead_of_truncating_the_ledger(self):
        root = self.artifacts / 'too-many'
        self.query_report(root, [self.record('q:0'), self.record('q:1')])
        with self.assertRaises(BudgetError):
            guard_module.account_calls(root, cap=1, claimed=True)


class CampaignSupervisionTests(CampaignFixture):
    def test_monitor_guard_failure_cleans_owned_child_and_preserves_foreign_process(self):
        guard = self.create()
        foreign = subprocess.Popen([sys.executable, '-c',
            'import threading; threading.Event().wait()'], start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        def reap_foreign():
            if foreign.poll() is None:
                foreign.kill()
            foreign.wait(timeout=2)
        self.addCleanup(reap_foreign)
        original = supervisor.supervise
        called = []

        def monitor(identities):
            called.append(tuple(identities))
            self.clock.now = 250.

        try:
            with self.assertRaises(guard_module.GuardStop) as stopped:
                with guard.supervision():
                    supervisor.supervise([sys.executable, '-c',
                        'import threading; threading.Event().wait()'], self.base / 'supervision',
                        timeout_s=4, grace_s=1, monitor=monitor)
            self.assertIs(supervisor.supervise, original)
            self.assertTrue(called)
            report = stopped.exception.report
            self.assertTrue(report['observed_owned_exit_confirmed'])
            self.assertEqual(report['remaining_owned_pids'], [])
            self.assertNotEqual(report['owned_pid'], foreign.pid)
            self.assertIsNone(foreign.poll(), 'an existing unrelated process must remain alive')
            self.assertFalse(any(event['pid'] == foreign.pid for event in report['signal_events']))
        finally:
            if foreign.poll() is None:
                foreign.kill()
            foreign.wait(timeout=2)

    def test_supervision_restores_the_original_function_after_an_unrelated_body_exception(self):
        guard = self.create()
        original = supervisor.supervise
        with self.assertRaisesRegex(ValueError, 'CPU-only caller error'):
            with guard.supervision():
                self.assertIsNot(supervisor.supervise, original)
                raise ValueError('CPU-only caller error')
        self.assertIs(supervisor.supervise, original)


class CampaignEntryTests(CampaignFixture):
    """Guard orchestration with explicit CPU replacements for model boundaries."""

    def entry(self, label):
        self.root = self.base / label
        manifest = self.manifest()
        cell_root = self.artifacts / label
        cell_root.mkdir()
        manifest['cells'][0]['prepared_config'] = str(cell_root / 'PREPARED-CONFIG.json')
        guard = guard_module.Guard.create(self.root, manifest,
            clock=self.clock.monotonic, epoch=self.clock.wall, clock_identity=self.clock_identity)
        cell = guard.manifest['cells'][0]
        config = dict(artifact_root=str(cell_root), expected_source=guard.manifest['source'],
            tool_source=guard.manifest['source'], workload=dict(expected_calls=cell['calls']),
            base_config=dict(sha256=cell['base_sha256']), limits=dict(controller_timeout_s=3))
        return guard, cell_root, config

    def cpu_fake_ray_ready(self, guard):
        # A synthetic guard fixture, never evidence that actual Ray ran here.
        log = guard.root / ('CPU-ray-'+guard.manifest['cells'][0]['id']) / 'worker.log'
        log.parent.mkdir()
        log.write_text('CPU FAKE qualification log; no actual Ray execution\n'
                       'Ran 5 tests in 0.001s\n\nOK\n')
        marker = dict(status='passed', source=guard.manifest['source'], tests=5, skips=0,
            log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),
            evidence_scope='explicit CPU fake for guard orchestration, not real Ray qualification')
        (guard.root / 'REAL-RAY-READY.json').write_text(json.dumps(marker))

    def test_run_cell_cpu_boundary_claims_once_restores_environment_and_publishes_complete_counts(self):
        import workflow
        import control_resource_C2 as controller
        guard, cell_root, config = self.entry('normal-entry')
        original = supervisor.supervise
        calls = []

        def cpu_controller(root):
            # This executes a real CPU subprocess, not the formal model controller.
            self.assertEqual(root, cell_root)
            calls.append(root)
            os.environ['CPU_CAMPAIGN_BOUNDARY'] = 'changed inside CPU fixture'
            os.environ.pop('CPU_CAMPAIGN_KEEP', None)
            result = supervisor.supervise([sys.executable, '-c', 'print("CPU boundary only")'],
                root / 'cpu-supervision', timeout_s=3, grace_s=.5)
            self.assertTrue(result['observed_owned_exit_confirmed'])
            records = [self.record('q:'+str(i)) for i in range(3)]
            (root / 'WORKER-RESULT.json').write_text(json.dumps(dict(calls=records)))
            (root / 'RESULT.json').write_text(json.dumps(dict(status='completed',
                physical_cleanup_confirmed=True, lease_released=True)))

        with patch.dict(os.environ, CPU_CAMPAIGN_KEEP='original'):
            before = dict(os.environ)
            with patch.object(workflow, 'check', return_value=config), \
                    patch.object(controller, 'profiled', return_value=False), \
                    patch.object(controller, 'worker_environment', return_value={}), \
                    patch.object(controller, 'main', side_effect=cpu_controller) as execute:
                with self.assertRaises(BudgetError):
                    guard_module.run_cell(guard, 'first')
                execute.assert_not_called()
                self.assertFalse(any(u['claimed'] for u in guard.ledger.snapshot()['units']))
                self.cpu_fake_ray_ready(guard)
                guard_module.run_cell(guard, 'first')
            self.assertEqual(dict(os.environ), before)
        self.assertIs(supervisor.supervise, original)
        self.assertEqual(calls, [cell_root])
        end = json.loads((guard.root / 'first-GUARD-END.json').read_text())
        self.assertEqual(end['status'], 'passed')
        self.assertEqual((end['counts']['actual_submitted_exact'], end['counts']['succeeded'],
                          end['counts']['conservative_charge']), (3, 3, 3))
        self.assertEqual(end['counts']['unknown_responsibilities'], 0)
        self.assertEqual(end['conservative_historical_plus_charge'], 12863)
        snapshot = guard.ledger.snapshot()
        self.assertEqual([u['claimed'] for u in snapshot['units']], [1, 0])
        with self.assertRaises(BudgetError):
            self.reopen().claim('first')

    def test_malformed_worker_or_result_keeps_unknown_claimed_responsibility_and_blocks_next_cell(self):
        import workflow
        import control_resource_C2 as controller
        for name in ('WORKER-RESULT.json', 'RESULT.json'):
            with self.subTest(malformed=name):
                guard, cell_root, config = self.entry('bad-'+name.split('.')[0])
                self.cpu_fake_ray_ready(guard)

                def malformed_controller(root):
                    os.environ['CPU_CAMPAIGN_BOUNDARY'] = 'must be restored after error'
                    (root / name).write_text('{not valid JSON')

                before = dict(os.environ)
                with patch.object(workflow, 'check', return_value=config), \
                        patch.object(controller, 'profiled', return_value=False), \
                        patch.object(controller, 'worker_environment', return_value={}), \
                        patch.object(controller, 'main', side_effect=malformed_controller):
                    with self.assertRaises(guard_module.GuardStop):
                        guard_module.run_cell(guard, 'first')
                self.assertEqual(dict(os.environ), before)
                end = json.loads((guard.root / 'first-GUARD-END.json').read_text())
                self.assertEqual(end['status'], 'failed')
                self.assertEqual(end['counts']['conservative_charge'], 3)
                self.assertIsNone(end['counts']['actual_submitted_exact'])
                self.assertNotEqual(end['counts']['unknown_responsibilities'], 0)
                self.assertEqual([u['claimed'] for u in guard.ledger.snapshot()['units']], [1, 0])
                with self.assertRaises(guard_module.GuardStop):
                    self.reopen().claim('second')
                with self.assertRaises(BudgetError):
                    guard_module._check_gate(guard, guard.manifest['cells'][1])

    def test_stage_gate_requires_previous_success_frozen_source_and_unchanged_receipt_hash(self):
        manifest = self.manifest()
        manifest['cells'][1]['stage'] = 'measured'
        guard = guard_module.Guard.create(self.root, manifest,
            clock=self.clock.monotonic, epoch=self.clock.wall, clock_identity=self.clock_identity)
        next_cell = guard.manifest['cells'][1]
        with self.assertRaises(BudgetError):
            guard_module._check_gate(guard, next_cell)
        previous = guard.root / 'first-GUARD-END.json'
        previous.write_text(json.dumps(dict(status='passed', source=manifest['source'], checked='initial')))
        with self.assertRaises((OSError, BudgetError)):
            guard_module._check_gate(guard, next_cell)
        gate = dict(status='passed', source=dict(head='f'*40, tree='e'*40),
            cells=dict(first=hashlib.sha256(previous.read_bytes()).hexdigest()))
        gate_path = guard.root / 'GATE-qualification.json'
        gate_path.write_text(json.dumps(gate))
        with self.assertRaises(BudgetError):
            guard_module._check_gate(guard, next_cell)
        gate['source'] = manifest['source']
        gate_path.write_text(json.dumps(gate))
        guard_module._check_gate(guard, next_cell)
        previous.write_text(json.dumps(dict(status='passed', source=manifest['source'], checked='changed')))
        with self.assertRaises(BudgetError):
            guard_module._check_gate(guard, next_cell)
        gate['cells']['first'] = hashlib.sha256(previous.read_bytes()).hexdigest()
        gate_path.write_text(json.dumps(gate))
        guard_module._check_gate(guard, next_cell)
        self.assertFalse(any(unit['claimed'] for unit in guard.ledger.snapshot()['units']))

    def test_run_cpu_ray_requires_all_five_without_skips_and_restores_environment_without_claims(self):
        import workflow
        cases = [('four', 'Ran 4 tests in 0.001s\n\nOK\n', True, False),
                 ('skipped', 'Ran 5 tests in 0.001s\n\nOK (skipped=1)\n', True, False),
                 ('truncated', 'Ran 5 tests in 0.001s\n\nOK\n', False, False),
                 ('complete', 'Ran 5 tests in 0.001s\n\nOK\n', True, True)]
        original = supervisor.supervise
        for label, text, complete, accepted in cases:
            with self.subTest(qualification=label):
                guard, _, _ = self.entry('ray-'+label)
                base = dict(movie_plan=dict(engine=dict(driver_python=sys.executable)),
                    limits=dict(worker_timeout_s=3, cleanup_grace_s=.5))
                observed = []

                def cpu_supervision(command, output, **options):
                    # Explicit log fixture: this does not claim real Ray qualification.
                    observed.append(command)
                    self.assertEqual(os.environ['CUDA_VISIBLE_DEVICES'], '')
                    self.assertEqual(os.environ['SEMLOOM_REAL_RAY_CPU'], '1')
                    self.assertNotIn('RAY_ADDRESS', os.environ)
                    self.assertFalse(any(k.startswith('SEM_RECORD_') for k in os.environ))
                    self.assertNotIn('SEMLOOM_MFU_TRACE_DIR', os.environ)
                    output.mkdir()
                    (output / 'worker.log').write_text(text)
                    return dict(status='passed', output_capture=dict(complete=complete))

                with patch.dict(os.environ, RAY_ADDRESS='CPU-sentinel', SEM_RECORD_TEST='CPU-only',
                                SEMLOOM_MFU_TRACE_DIR='CPU-trace-sentinel'):
                    before = dict(os.environ)
                    with patch.object(workflow, 'read_private', return_value=base), \
                            patch.object(workflow, 'validate_base', return_value=None), \
                            patch.object(supervisor, 'supervise', cpu_supervision):
                        if accepted:
                            guard_module.run_cpu(guard, 'ray')
                        else:
                            with self.assertRaises(guard_module.GuardStop):
                                guard_module.run_cpu(guard, 'ray')
                        self.assertIs(supervisor.supervise, cpu_supervision)
                    self.assertEqual(dict(os.environ), before)
                self.assertIs(supervisor.supervise, original)
                marker = guard.root / 'REAL-RAY-READY.json'
                self.assertEqual(marker.exists(), accepted)
                if accepted:
                    ready = json.loads(marker.read_text())
                    log = guard.root / 'CPU-ray-first' / 'worker.log'
                    self.assertEqual((ready['status'], ready['tests'], ready['skips']), ('passed', 5, 0))
                    self.assertEqual(ready['source'], guard.manifest['source'])
                    self.assertEqual(ready['log_sha256'], hashlib.sha256(log.read_bytes()).hexdigest())
                    guard_module._check_ray(guard)
                self.assertEqual(len(observed), 1)
                self.assertIn('tests.execution_provider.test_core_feedback_ray_future', observed[0])
                snapshot = guard.ledger.snapshot()
                self.assertEqual(snapshot['allocated_requests'], 7)
                self.assertFalse(any(unit['claimed'] for unit in snapshot['units']))


if __name__ == '__main__':
    unittest.main()
