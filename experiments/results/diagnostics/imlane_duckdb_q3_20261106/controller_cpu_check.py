"""Exercise actual candidate helper ASTs; never execute controller top level."""

import ast
from contextlib import redirect_stdout
import hashlib
import importlib.abc
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import traceback
import unittest

BLOCKED_MODULES = ('vllm', 'torch', 'ray', 'daft', 'lotus', 'transformers', 'openai',
                   'src.core', 'src.serving', 'src.execution_provider')


class NoModelImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == name or fullname.startswith(name + '.') for name in BLOCKED_MODULES):
            raise AssertionError('CPU controller harness forbids import: ' + fullname)
        return None


sys.meta_path.insert(0, NoModelImports())


def no_network(event, args):
    if event in ('socket.connect', 'socket.bind', 'os.system'):
        raise AssertionError('CPU controller harness forbids operation: ' + event)
    if event == 'subprocess.Popen':
        command = args[1]
        if not (type(command) is list and len(command) == 4 and command[:3] == [sys.executable, '-B', '-c']
                and command[3].startswith('import ast\n') and '<candidate-tail>' in command[3]):
            raise AssertionError('CPU controller harness permits only its isolated AST-tail child')


sys.addaudithook(no_network)

CODE = Path(os.environ['SEMLOOM_CODE_ROOT']).resolve()
sys.path.insert(0, str(CODE))
from src.infrastructure.private_artifacts import publish_private_json, write_private_json
from src.infrastructure.runner_lease import RunnerOwner, acquire_runner_lease

FIXTURES = Path(os.environ['IMLANE_CONTROLLER_FIXTURE_DIR']).resolve()
CANDIDATE = FIXTURES / 'controller.candidate.py'
ORIGINAL = FIXTURES / 'controller.original.py'
TREE = ast.parse(CANDIDATE.read_text(), filename=str(CANDIDATE))
HELPERS = {'write', '_retain', '_terminal_budget', 'stage', 'finalize_run'}
HELPER_AST = ast.Module(body=[node for node in TREE.body if isinstance(node, ast.FunctionDef)
                             and node.name in HELPERS], type_ignores=[])
assert {node.name for node in HELPER_AST.body} == HELPERS
HELPER_CODE = compile(HELPER_AST, str(CANDIDATE), 'exec')


class Fixture:
    def __init__(self, directory):
        self.root = Path(directory)
        self.events = []
        self.failures = {}
        self.stage_error = None
        self.monitor_finish_calls = 0
        self.stage_live = []
        self.lease = acquire_runner_lease(self.root / 'lease', config_fingerprint='cpu-fixture',
            repository_commit='cpu-only', owner=RunnerOwner('fixture-host', 7, 'fixture-start', 'fixture-owner'))
        self.original_release = self.lease.release
        self.release_calls = 0
        def release():
            self.release_calls += 1
            self.events.append('release')
            if 'release' in self.failures:
                raise self.failures['release']
            return self.original_release()
        self.lease.release = release
        self.report = dict(status='failed', stages=[],
            model_budget=dict(logical_limit=120, claimed=120, engine_generate_invocations=None,
                              physical_model_attempts=None))
        self.namespace = dict(ROOT=self.root, TEMP=self.root, h=self, api=self,
            report=self.report, lease=self.lease, base={'shared_resources': {}},
            json=json, os=os, time=time, Path=Path, last_scan=[0.0],
            redact_text=lambda value: value, redact_json_values=lambda value: value,
            supervised_run=self.supervise, ResourceMonitor=lambda config: self)
        exec(HELPER_CODE, self.namespace)

    def private_json(self, destination, value):
        self.events.append('write:' + destination.name)
        if destination.name in self.failures:
            raise self.failures[destination.name]
        write_private_json(destination, value)

    def report_identities(self, supervisor):
        return set(supervisor.get('identities', []))

    def live_owned(self, api, identities):
        return self.stage_live

    def cleanup_owned(self, api, identities, seconds):
        self.events.append('cleanup')
        if 'cleanup' in self.failures:
            raise self.failures['cleanup']
        self.stage_live = []
        return {'recovered': True}

    def snapshot(self, api, base, label, identities):
        self.events.append('snapshot')
        if 'snapshot' in self.failures:
            raise self.failures['snapshot']
        return {'idle': True}

    def assert_idle(self, after, base):
        if not after['idle']:
            raise RuntimeError('fixture host not idle')

    def check_lease(self, lease):
        self.events.append('check_lease')
        if 'check_lease' in self.failures:
            raise self.failures['check_lease']
        if json.loads(lease.path.read_text())['owner_token'] != lease.owner.owner_token:
            raise RuntimeError('fixture lease ownership changed')

    def supervise(self, command, output, *extra, **kwargs):
        kwargs['monitor']([])
        if self.stage_error is not None:
            raise self.stage_error
        return {'identities': []}

    def __call__(self, identities):
        self.events.append('sample')

    def finish(self):
        self.events.append('finish')
        self.monitor_finish_calls += 1
        if 'finish' in self.failures:
            raise self.failures['finish']
        return {'passed': True}

    def receipt(self, name, count, *, terminal=True):
        directory = self.root / 'cell/model-calls'
        directory.mkdir(parents=True, exist_ok=True)
        write_private_json(directory / (name + '.started.json'), {'phase': 'started'})
        if terminal:
            write_private_json(directory / (name + '.terminal.json'),
                {'status': 'succeeded' if count is not None else 'unknown',
                 'engine_generate_invocations': count, 'calls': []})

    def finalize(self, primary=None):
        with redirect_stdout(io.StringIO()):
            return self.namespace['finalize_run'](primary)


class ControllerCPUTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='imlane-finalization-cpu-')
        self.addCleanup(self.temporary.cleanup)
        self.fixture = Fixture(self.temporary.name)

    def test_fresh_terminal_ledger_preserves_reservation_bytes(self):
        fixture = self.fixture
        fixture.receipt('one', 120)
        fixture.report['status'] = 'completed'
        initial = fixture.root / 'LEDGER.json'
        fixture.private_json(initial, fixture.report['model_budget'])
        before = initial.read_bytes()
        self.assertIsNone(fixture.finalize())
        self.assertEqual(initial.read_bytes(), before)
        terminal = json.loads((fixture.root / 'LEDGER-TERMINAL.json').read_text())
        self.assertEqual(terminal['engine_generate_invocations'], 120)
        self.assertEqual(terminal['claimed'], 120)
        self.assertTrue(terminal['accounting_complete'])
        self.assertFalse(fixture.lease.path.exists())
        self.assertEqual(json.loads((fixture.root / 'FINAL.json').read_text())['status'], 'completed')

    def test_both_public_writers_refuse_replacement(self):
        destination = self.fixture.root / 'immutable.json'
        write_private_json(destination, {'claimed': 120})
        before = destination.read_bytes()
        for writer in (write_private_json, publish_private_json):
            with self.subTest(writer=writer.__name__), self.assertRaises(FileExistsError):
                writer(destination, {'claimed': 0})
            self.assertEqual(destination.read_bytes(), before)

    def test_none_count_remains_unknown_with_known_subtotal(self):
        self.fixture.receipt('known', 3)
        self.fixture.receipt('unknown', None)
        budget, errors = self.fixture.namespace['_terminal_budget']()
        self.assertEqual(errors, [])
        self.assertIsNone(budget['engine_generate_invocations'])
        self.assertEqual(budget['known_engine_generate_invocations'], 3)
        self.assertFalse(budget['accounting_complete'])
        self.assertIsNone(budget['physical_model_attempts'])

    def test_missing_terminal_does_not_underreport_total(self):
        self.fixture.receipt('known', 3)
        self.fixture.receipt('missing', None, terminal=False)
        budget, errors = self.fixture.namespace['_terminal_budget']()
        self.assertEqual(errors, [])
        self.assertIsNone(budget['engine_generate_invocations'])
        self.assertEqual(budget['known_engine_generate_invocations'], 3)
        self.assertEqual(budget['missing_terminal_batches'], ['missing'])

    def test_no_receipts_alone_does_not_prove_zero_dispatch(self):
        budget, errors = self.fixture.namespace['_terminal_budget']()
        self.assertEqual(errors, [])
        self.assertIsNone(budget['engine_generate_invocations'])
        self.assertEqual(budget['known_engine_generate_invocations'], 0)
        self.assertFalse(budget['accounting_complete'])

    def test_faults_cannot_skip_release_or_replace_primary(self):
        for step in ('LEDGER-TERMINAL.json', 'snapshot', 'FINAL.json'):
            with self.subTest(step=step), tempfile.TemporaryDirectory(prefix='imlane-final-fault-') as directory:
                fixture = Fixture(directory)
                primary = RuntimeError('original resource guard failure')
                fixture.failures[step] = OSError('injected ' + step)
                retained = fixture.finalize(primary)
                self.assertIs(retained, primary)
                self.assertEqual(fixture.release_calls, 1)
                self.assertFalse(fixture.lease.path.exists())
                self.assertIn('write:FINAL.json', fixture.events)
                self.assertEqual(fixture.report['status'], 'failed')
                self.assertTrue(any(step in note for note in primary.__notes__))
                if step != 'snapshot':
                    self.assertIn('snapshot', fixture.events)

    def test_release_failure_preserves_primary_and_reports_not_released(self):
        fixture = self.fixture
        primary = RuntimeError('original query failure')
        fixture.failures['release'] = OSError('injected release failure')
        self.assertIs(fixture.finalize(primary), primary)
        self.assertEqual(fixture.release_calls, 1)
        self.assertTrue(fixture.lease.path.exists())
        self.assertFalse(fixture.report['lease_released'])
        self.assertIn('write:FINAL.json', fixture.events)
        self.assertTrue(any('release runner lease' in note for note in primary.__notes__))
        fixture.original_release()

    def test_failed_separate_lease_check_still_attempts_token_checked_release(self):
        fixture = self.fixture
        primary = RuntimeError('original query failure')
        fixture.failures['check_lease'] = OSError('injected observation failure')
        self.assertIs(fixture.finalize(primary), primary)
        self.assertEqual(fixture.release_calls, 1)
        self.assertFalse(fixture.lease.path.exists())
        self.assertTrue(any('check runner lease identity' in note for note in primary.__notes__))

    def test_changed_lease_owner_is_never_removed(self):
        fixture = self.fixture
        record = json.loads(fixture.lease.path.read_text())
        record['owner_token'] = 'another-fixture-owner'
        fixture.lease.path.write_text(json.dumps(record))
        before = fixture.lease.path.read_bytes()
        primary = RuntimeError('original query failure')
        self.assertIs(fixture.finalize(primary), primary)
        self.assertEqual(fixture.release_calls, 1)
        self.assertEqual(fixture.lease.path.read_bytes(), before)
        self.assertFalse(fixture.report['lease_released'])
        self.assertEqual({item['step'] for item in fixture.report['finalization_errors']},
                         {'check runner lease identity', 'release runner lease'})

    def test_bad_receipt_cannot_skip_cleanup_or_claim_complete_count(self):
        fixture = self.fixture
        fixture.receipt('bad', 1)
        (fixture.root / 'cell/model-calls/bad.terminal.json').write_text('{invalid')
        primary = RuntimeError('original query failure')
        self.assertIs(fixture.finalize(primary), primary)
        self.assertFalse(fixture.lease.path.exists())
        self.assertIsNone(fixture.report['model_budget']['engine_generate_invocations'])
        self.assertEqual(len(fixture.report['model_budget']['receipt_errors']), 1)
        self.assertTrue((fixture.root / 'LEDGER-TERMINAL.json').is_file())

    def test_new_finalization_error_becomes_primary_and_failed_status(self):
        fixture = self.fixture
        error = OSError('injected first terminal ledger failure')
        fixture.failures['LEDGER-TERMINAL.json'] = error
        self.assertIs(fixture.finalize(), error)
        self.assertEqual(fixture.report['status'], 'failed')
        self.assertFalse(fixture.lease.path.exists())
        self.assertTrue((fixture.root / 'FINAL.json').is_file())

    def test_stage_preserves_guard_error_through_monitor_and_summary_errors(self):
        fixture = self.fixture
        primary = TimeoutError('original resource guard interruption')
        fixture.stage_error = primary
        fixture.failures['finish'] = OSError('injected finish failure')
        fixture.failures['query-summary.json'] = OSError('injected stage summary failure')
        with self.assertRaises(TimeoutError) as raised:
            fixture.namespace['stage']('query', [], model=True)
        self.assertIs(raised.exception, primary)
        self.assertEqual(fixture.monitor_finish_calls, 1)
        self.assertTrue(any('finish stage resource monitor' in note for note in primary.__notes__))
        self.assertTrue(any('write stage summary' in note for note in primary.__notes__))
        self.assertEqual(fixture.report['stages'][0]['error']['type'], 'TimeoutError')

    def test_stage_cleanup_error_still_finishes_monitor_once(self):
        fixture = self.fixture
        primary = RuntimeError('original stage failure')
        fixture.stage_error = primary
        fixture.stage_live = ['fixture-owned-process']
        fixture.failures['cleanup'] = OSError('injected process cleanup failure')
        with self.assertRaises(RuntimeError) as raised:
            fixture.namespace['stage']('preflight', [])
        self.assertIs(raised.exception, primary)
        self.assertEqual(fixture.monitor_finish_calls, 1)
        self.assertIn('write:preflight-summary.json', fixture.events)

    def test_successful_supervisor_does_not_hide_monitor_failure(self):
        fixture = self.fixture
        failure = OSError('injected first monitor failure')
        fixture.failures['finish'] = failure
        with self.assertRaises(OSError) as raised:
            fixture.namespace['stage']('preflight', [])
        self.assertIs(raised.exception, failure)
        self.assertEqual(fixture.monitor_finish_calls, 1)

    def test_candidate_tail_rethrows_primary_with_nonzero_cpu_process(self):
        tail = TREE.body[-1]
        self.assertIsInstance(tail, ast.If)
        self.assertIsInstance(tail.body[0], ast.Raise)
        child = (
            'import ast\n'
            f'tree=ast.parse(open({str(CANDIDATE)!r}).read())\n'
            "primary=RuntimeError('cpu-only primary rethrow fixture')\n"
            "exec(compile(ast.Module(body=[tree.body[-1]],type_ignores=[]),'<candidate-tail>','exec'))\n"
        )
        result = subprocess.run([sys.executable, '-B', '-c', child], capture_output=True,
                                text=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertIn('cpu-only primary rethrow fixture', result.stderr)

    def test_main_failure_print_cannot_replace_primary_or_skip_release(self):
        fixture = self.fixture
        primary = RuntimeError('actual primary resource failure')
        fixture.namespace.update(main_error=primary, traceback=traceback,
            print=lambda *a, **k: (_ for _ in ()).throw(BrokenPipeError('diagnostic pipe closed')))
        main = next(node for node in TREE.body if isinstance(node, ast.Try))
        probe = ast.Try(body=[ast.Raise(exc=ast.Name(id='main_error', ctx=ast.Load()))],
            handlers=main.handlers, orelse=[], finalbody=main.finalbody)
        program = ast.fix_missing_locations(ast.Module(body=[probe, TREE.body[-1]], type_ignores=[]))
        with self.assertRaises(RuntimeError) as raised:
            exec(compile(program, '<candidate-main-failure>', 'exec'), fixture.namespace)
        self.assertIs(raised.exception, primary)
        self.assertEqual(fixture.release_calls, 1)
        self.assertIn('print primary failure', primary.__notes__[0])
        self.assertFalse(fixture.lease.path.exists())

    def test_whole_run_SHM_boundary_uses_available_filesystem_space(self):
        from types import SimpleNamespace
        nodes = [node for node in TREE.body if isinstance(node, ast.Assign)
            and (any(isinstance(target, ast.Name) and target.id == 'shm_capacity' for target in node.targets)
                or ast.unparse(node.targets[0]) == "base['shared_resources']['max_owned_shm_bytes']")]
        self.assertEqual(len(nodes), 2)
        for free_blocks in (1024, 15728634):
            reads = []
            fake_os = SimpleNamespace(statvfs=lambda path: reads.append(path)
                or SimpleNamespace(f_bavail=free_blocks, f_frsize=4096))
            namespace = dict(os=fake_os, base=dict(shared_resources=dict(max_owned_shm_bytes=536969216)))
            exec(compile(ast.Module(body=nodes, type_ignores=[]), '<candidate-physical-SHM>', 'exec'), namespace)
            self.assertEqual(reads, ['/dev/shm'])
            self.assertEqual(namespace['base']['shared_resources']['max_owned_shm_bytes'], free_blocks * 4096)

    def test_model_and_author_workload_unchanged_except_declared_whole_stage_SHM(self):
        original = ast.parse(ORIGINAL.read_text())
        def workload(tree):
            initial = []
            main = next(node for node in tree.body if isinstance(node, ast.Try))
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.Try)):
                    continue
                if isinstance(node, ast.If) and isinstance(node.body[0], ast.Raise):
                    continue
                if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
                        and target.id in {'primary', 'shm_capacity'} for target in node.targets):
                    continue
                if (isinstance(node, ast.Assign)
                        and ast.unparse(node.targets[0]) == "base['shared_resources']['max_owned_shm_bytes']"):
                    continue
                initial.append(node)
            return [ast.dump(node, include_attributes=False) for node in initial + main.body]
        self.assertEqual(workload(original), workload(TREE))
        budget = next(node.value for node in TREE.body if isinstance(node, ast.Assign)
            and ast.unparse(node.targets[0]) == "base['shared_resources']['max_owned_shm_bytes']")
        expected = ast.parse("shm_capacity.f_bavail * shm_capacity.f_frsize", mode='eval').body
        self.assertEqual(ast.dump(budget), ast.dump(expected))
        self.assertEqual(hashlib.sha256(ORIGINAL.read_bytes()).hexdigest(),
            'f0f4db2997d1b5865aef30fdb70384fcb3406ad39823071485ad3e6a5db07297')


if __name__ == '__main__':
    unittest.main(verbosity=2)
