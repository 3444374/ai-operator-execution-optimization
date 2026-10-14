"""No-network regressions for optional SQLite ledger diagnostic boundaries."""
import asyncio
import ast
import builtins
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

try:
    from . import test_send_counts as fixture
except ImportError:
    import test_send_counts as fixture


class Recorder:
    def __init__(self, root, *, fail=False):
        self.root, self.fail, self.rows, self.marks = root, fail, [], []

    def record_interval(self, operation, started, ended, **fields):
        # A second connection must acquire the write lock before observation.
        with sqlite3.connect(self.root/'sends.sqlite', timeout=.01) as db:
            db.execute('BEGIN IMMEDIATE')
        if self.fail:
            raise OSError('fixture observer failure')
        self.rows.append(dict(operation=operation, started=started, ended=ended, **fields))

    def mark(self, operation, **fields):
        self.marks.append(dict(operation=operation, **fields))


class SendLedgerTiming(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='semloom-ledger-timing-')
        self.root = Path(self.directory.name)
        self.ctx = fixture.load(self.root, limit=10000, post_limit=30000)
        self.ctx['initialize']()
        fixture.control(self.root, 'software')

    def tearDown(self):
        self.directory.cleanup()

    def enable(self, *, fail=False):
        recorder = Recorder(self.root, fail=fail)
        self.ctx['_get_timing'] = lambda: recorder
        return recorder

    def test_disabled_does_not_import_timing_or_read_extra_clocks(self):
        calls = []
        def clock():
            calls.append(True)
            return time.monotonic_ns()
        self.ctx['time'] = SimpleNamespace(monotonic_ns=clock, time=time.time)
        imported = []
        original = builtins.__import__
        def observed_import(name, *args, **kwargs):
            imported.append(name)
            return original(name, *args, **kwargs)
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('SEMLOOM_DETAILED_TIMING_DIR', None)
            with patch('builtins.__import__', observed_import):
                self.ctx['initialize']()
                seq = self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
                self.ctx['finish'](seq, SimpleNamespace(status=200))
                self.assertEqual(self.ctx['counts']()['total'], 1)
        # Original software reserve has deadline + at_ns; finish has returned_ns.
        self.assertEqual(len(calls), 3)
        self.assertFalse(any('execution_timing' in name for name in imported))

    def test_enabled_samples_are_published_after_commit_with_send_identity(self):
        recorder = self.enable()
        self.ctx['initialize']()
        seq = self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
        self.ctx['finish'](seq, SimpleNamespace(status=200))
        self.assertEqual(self.ctx['counts'](), dict(total=1, gateway=1, router=0, model=0, fixture=0))
        indexed = {row['operation']: row for row in recorder.rows}
        for name in ('initialize.identity_check', 'initialize.identity_publish',
                     'initialize.history_group_by_and_fold', 'initialize.initialize_counts',
                     'reserve.begin_immediate_call', 'reserve.control_select', 'reserve.counter_snapshot',
                     'reserve.insert_sql', 'reserve.verify_increment', 'reserve.transaction_body',
                     'reserve.context_exit_commit', 'finish.update_sql', 'finish.context_exit_commit',
                     'counts.group_by_fetch'):
            self.assertIn('send_ledger.'+name, indexed)
        for row in recorder.rows:
            self.assertLessEqual(row['started'], row['ended'])
            self.assertEqual(row['status'], 'completed')
        reserve = indexed['send_ledger.reserve.context_exit_commit']
        self.assertEqual((reserve['phase'], reserve['slot'], reserve['query_id'], reserve['request_id'], reserve['kind']),
                         ('software', 'slot', 'unit', seq, 'gateway'))
        self.assertLessEqual(indexed['send_ledger.reserve.begin_immediate_call']['ended'],
                             indexed['send_ledger.reserve.transaction_body']['started'])
        self.assertLessEqual(indexed['send_ledger.reserve.transaction_body']['ended'], reserve['started'])

    def test_zero_and_2520_retained_rows_keep_identical_counts_with_switch(self):
        for history_rows in (0, 2520):
            for enabled in (False, True):
                with self.subTest(history_rows=history_rows, enabled=enabled), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    ctx = fixture.load(root, limit=10000, post_limit=30000)
                    ctx['initialize']()
                    with sqlite3.connect(root/'sends.sqlite') as db:
                        db.executemany("INSERT INTO sends(phase,kind)VALUES('software','fixture')", [()] * history_rows)
                    recorder = Recorder(root)
                    ctx['_get_timing'] = (lambda: recorder) if enabled else (lambda: None)
                    ctx['initialize']()
                    fixture.control(root, 'software')
                    seq = ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
                    ctx['finish'](seq, SimpleNamespace(status=204))
                    self.assertEqual(seq, history_rows+1)
                    self.assertEqual(ctx['counts'](), dict(total=history_rows+1, gateway=1, router=0, model=0, fixture=history_rows))
                    with sqlite3.connect(root/'sends.sqlite') as db:
                        self.assertEqual(db.execute('SELECT status FROM sends WHERE sequence=?', (seq,)).fetchone(), (204,))
                    self.assertEqual(bool(recorder.rows), enabled)

    def test_cancelled_transaction_retains_cancellation_when_observer_fails(self):
        self.enable(fail=True)
        original = asyncio.CancelledError('original fixture cancellation')
        def cancelled(*args, **kwargs):
            raise original
        with patch.object(fixture.counts, 'snapshot', cancelled):
            self.ctx['_hooks']['send_counts_snapshot'] = (fixture.counts, 'snapshot', cancelled)
            with self.assertRaises(asyncio.CancelledError) as captured:
                self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
        self.assertIs(captured.exception, original)
        self.assertTrue(any('Send ledger timing also failed: OSError' in note for note in original.__notes__))
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            self.assertEqual(db.execute('SELECT count(*) FROM sends').fetchone(), (0,))

    def test_finish_observer_failures_do_not_hide_transport_error_or_skip_update(self):
        seq = self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
        original = OSError('fixture transport error')
        self.enable(fail=True)
        self.ctx['finish'](seq, error=original)
        self.assertTrue(any('timing also failed' in note for note in original.__notes__))
        def unavailable():
            raise OSError('fixture writer unavailable')
        self.ctx['_get_timing'] = unavailable
        self.ctx['finish'](seq, error=original)
        self.assertEqual(len(original.__notes__), 2)
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            self.assertEqual(db.execute('SELECT error_type FROM sends WHERE sequence=?', (seq,)).fetchone(), ('OSError',))

    def test_post_commit_observer_failure_retains_budget_without_returning_permission(self):
        self.enable(fail=True)
        with self.assertRaisesRegex(OSError, 'fixture observer failure'):
            self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
        # The call returned no sequence, so the caller cannot send; a completed
        # reservation remains charged instead of being refunded by observation.
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            self.assertEqual(db.execute('SELECT count(*) FROM sends').fetchone(), (1,))
            self.assertEqual(db.execute("SELECT total,logical FROM send_totals WHERE scope='software'").fetchone(), (1, 1))

    def test_install_first_ready_is_emitted_once_after_successful_install(self):
        recorder = self.enable()
        functions = [node for node in ast.parse(fixture.HELPER.read_text()).body
                     if isinstance(node, ast.FunctionDef) and node.name=='install']
        exec(compile(ast.Module(body=functions, type_ignores=[]), 'actual-install-timing', 'exec'), self.ctx)
        self.ctx['_installed'] = False
        def install():
            self.ctx['_installed'] = True
        self.ctx['_install'] = install
        self.ctx['install']()
        self.ctx['install']()
        self.assertEqual([row['operation'] for row in recorder.marks], ['send_ledger.install.first_ready'])
        self.assertEqual([row['operation'] for row in recorder.rows], ['send_ledger.install', 'send_ledger.install'])

    def test_rollback_is_visible_without_changing_original_budget_error(self):
        recorder = self.enable()
        fixture.control(self.root, 'software', deadline=time.monotonic_ns()-1)
        with self.assertRaisesRegex(TimeoutError, 'cleanup reserve'):
            self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
        rollback = next(row for row in recorder.rows if row['operation']=='send_ledger.reserve.context_exit_rollback')
        self.assertEqual(rollback['error_type'], 'TimeoutError')

    def test_commit_failure_never_returns_permission_and_labels_context_rollback(self):
        recorder = self.enable()
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            db.execute('CREATE TABLE commit_parent(id INTEGER PRIMARY KEY)')
            db.execute('CREATE TABLE commit_child(id INTEGER REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED)')
            db.execute('CREATE TRIGGER test_commit_failure AFTER INSERT ON sends BEGIN INSERT INTO commit_child VALUES(9); END')
        def connect(*args, **kwargs):
            db = sqlite3.connect(*args, **kwargs)
            db.execute('PRAGMA foreign_keys=ON')
            return db
        self.ctx['sqlite3'] = SimpleNamespace(connect=connect)
        with self.assertRaises(sqlite3.IntegrityError):
            self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
        commit = next(row for row in recorder.rows if row['operation']=='send_ledger.reserve.context_exit_commit')
        self.assertEqual(commit['error_type'], 'IntegrityError')
        self.assertTrue(commit['context_exit_includes_failed_commit_rollback'])
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            self.assertEqual(db.execute('SELECT count(*) FROM sends').fetchone(), (0,))
            self.assertEqual(db.execute("SELECT total FROM send_totals WHERE scope='software'").fetchone(), (0,))

    def test_begin_immediate_call_failure_is_not_reported_as_pure_lock_wait(self):
        recorder = self.enable()
        def connect(*args, **kwargs):
            kwargs['timeout'] = .02
            return sqlite3.connect(*args, **kwargs)
        self.ctx['sqlite3'] = SimpleNamespace(connect=connect)
        holder = sqlite3.connect(self.root/'sends.sqlite')
        try:
            holder.execute('BEGIN IMMEDIATE')
            # The recorder checks lock release; release this independent holder
            # only when the original connection has finished its failed BEGIN.
            original_emit = self.ctx['_emit_timing']
            def emit(*args, **kwargs):
                holder.rollback()
                return original_emit(*args, **kwargs)
            self.ctx['_emit_timing'] = emit
            with self.assertRaises(sqlite3.OperationalError):
                self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
        finally:
            holder.close()
        begin = next(row for row in recorder.rows if row['operation']=='send_ledger.reserve.begin_immediate_call')
        self.assertEqual(begin['error_type'], 'OperationalError')
        self.assertFalse(any('lock_wait' in row['operation'] for row in recorder.rows))

    def test_enabled_private_harness_requires_runtime_and_copy_fingerprints(self):
        validation = self.root/'validation'
        validation.mkdir()
        code_root = fixture.COUNTS.parents[2]
        sources = {'pre_send_ledger.py': fixture.HELPER, 'send_counts.py': fixture.COUNTS,
                   'execution_timing.py': code_root/'src/experiments/execution_timing.py',
                   'buffered_events.py': code_root/'src/experiments/buffered_events.py',
                   'ray_map_transport.py': code_root/'src/execution_provider/adapters/ray_map_transport.py'}
        for name, source in sources.items():
            (validation/name).write_bytes(source.read_bytes())
        files = {name: hashlib.sha256((validation/name).read_bytes()).hexdigest() for name in sources}
        identity = self.root/'harness-identity.json'
        identity.write_text(json.dumps({'files': files}))
        (self.root/'state.json').write_text(json.dumps(self.ctx['STATE']))
        environment = dict(os.environ, MODELRUN_ROOT=str(self.root),
                           SEMLOOM_DETAILED_TIMING_DIR=str(self.root/'timings'),
                           PYTHONPATH=os.pathsep.join((str(validation), str(code_root))), PYTHONDONTWRITEBYTECODE='1')
        code = 'import pre_send_ledger as ledger; ledger.harness_identity(); ledger.initialize(); from src.experiments.execution_timing import close_timing; close_timing()'
        def run():
            return subprocess.run([os.sys.executable, '-B', '-c', code], env=environment, capture_output=True, text=True, timeout=10)
        success = run()
        self.assertEqual(success.returncode, 0, success.stderr)
        self.assertTrue(list((self.root/'timings').glob('timing-*.jsonl')))
        (validation/'execution_timing.py').write_text('# mismatching copied fixture\n')
        changed_copy = run()
        self.assertNotEqual(changed_copy.returncode, 0)
        self.assertIn('source differs from fixed identity', changed_copy.stderr)
        files['execution_timing.py'] = hashlib.sha256((validation/'execution_timing.py').read_bytes()).hexdigest()
        identity.write_text(json.dumps({'files': files}))
        changed_runtime = run()
        self.assertNotEqual(changed_runtime.returncode, 0)
        self.assertIn('timing helper differs from fixed identity', changed_runtime.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
