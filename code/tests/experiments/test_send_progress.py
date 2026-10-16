"""Read-only progress preserves raw aggregates without scanning retained sends."""
from contextlib import closing
import sqlite3
import threading
import unittest

try:
    from . import test_send_counts as fixture
except ImportError:
    import test_send_counts as fixture


class SendProgress(unittest.TestCase):
    setUp = fixture.SendCounters.setUp
    tearDown = fixture.SendCounters.tearDown

    def progress(self):
        with closing(sqlite3.connect((self.root/'sends.sqlite').as_uri()+'?mode=ro', uri=True)) as db, db:
            db.execute('BEGIN')
            return fixture.counts.progress_snapshot(db)

    def raw(self, db):
        return db.execute('SELECT count(*),max(at_ns),max(returned_ns) FROM sends').fetchone()

    def test_empty_and_all_phases_kinds_failures_and_unfinished_match_raw(self):
        self.assertEqual(self.progress(), (0, None, None))
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            for index, (phase, kind) in enumerate((p, k) for p in fixture.counts.PHASES for k in fixture.counts.KINDS):
                db.execute('INSERT INTO sends(phase,kind,at_ns,returned_ns,status,error_type) VALUES(?,?,?,?,?,?)',
                           (phase, kind, 100-index, None if index%3==0 else index*10, 503 if index%2 else None,
                            'OSError' if index%4==0 else ''))
            expected = self.raw(db)
        self.assertEqual(self.progress(), expected)
        self.assertEqual(expected, (16, 100, 140))
        self.assertEqual(self.ctx['counts']()['total'], 16)
        for phase in fixture.counts.PHASES:
            self.assertEqual(self.ctx['counts'](phase)['total'], 4)

    def test_failed_finish_retains_count_and_repeated_finish_tracks_actual_max(self):
        first = self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
        self.ctx['reserve']('router', 'http://localhost/fixture', b'{}')
        self.ctx['finish'](first, error=OSError('fixture'))
        self.assertEqual(self.progress()[0], 2)
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            db.execute('UPDATE sends SET returned_ns=900 WHERE sequence=?', (first,))
            db.execute('UPDATE sends SET returned_ns=10 WHERE sequence=?', (first,))
            expected = self.raw(db)
        self.assertEqual(self.progress(), expected)
        self.assertEqual(expected[2], 10)

    def test_progress_requires_explicit_consistent_transaction(self):
        with closing(sqlite3.connect(self.root/'sends.sqlite')) as db:
            with self.assertRaises(RuntimeError):
                fixture.counts.progress_snapshot(db)

    def test_fresh_preparation_commits_before_first_supervisor_poll(self):
        # Existing initialize exposes sends before its counter transaction.
        # The launcher must finish preparation before it starts supervision.
        root = self.root/'fresh'
        root.mkdir()
        ctx = fixture.load(root)
        initialize_counts = fixture.counts.initialize_counts
        during = []
        def observe_unpublished(db, **kwargs):
            with closing(sqlite3.connect(root/'sends.sqlite')) as reader, reader:
                self.assertEqual(self.raw(reader), (0, None, None))
                reader.execute('BEGIN')
                with self.assertRaises(sqlite3.OperationalError):
                    fixture.counts.progress_snapshot(reader)
                during.append(True)
            return initialize_counts(db, **kwargs)
        original = ctx['_send_counts']
        from types import SimpleNamespace
        ctx['_send_counts'] = SimpleNamespace(**vars(original))
        ctx['_send_counts'].initialize_counts = observe_unpublished
        ctx['initialize']()
        self.assertEqual(during, [True])
        with closing(sqlite3.connect(root/'sends.sqlite')) as reader, reader:
            reader.execute('BEGIN')
            self.assertEqual(fixture.counts.progress_snapshot(reader), (0, None, None))

    def test_reader_sees_one_snapshot_while_writer_waits_to_commit(self):
        inserted, committed = threading.Event(), threading.Event()
        errors = []
        def write():
            try:
                with closing(sqlite3.connect(self.root/'sends.sqlite', timeout=2)) as writer, writer:
                    writer.execute("INSERT INTO sends(phase,kind,at_ns) VALUES('single','gateway',123)")
                    inserted.set()
                committed.set()
            except BaseException as error:
                errors.append(error)
        with closing(sqlite3.connect(self.root/'sends.sqlite')) as db:
            self.assertEqual(db.execute('PRAGMA journal_mode').fetchone(), ('delete',))
            db.execute('BEGIN')
            self.assertEqual(fixture.counts.progress_snapshot(db), (0, None, None))
            worker = threading.Thread(target=write)
            worker.start()
            try:
                self.assertTrue(inserted.wait(1))
                self.assertFalse(committed.wait(.03))
                self.assertEqual(fixture.counts.progress_snapshot(db), (0, None, None))
            finally:
                db.commit()
                worker.join(3)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(committed.is_set())
            db.execute('BEGIN')
            self.assertEqual(fixture.counts.progress_snapshot(db), (1, 123, None))

    def test_uncommitted_send_is_not_visible_and_rollback_does_not_charge(self):
        with closing(sqlite3.connect(self.root/'sends.sqlite')) as writer:
            writer.execute('BEGIN IMMEDIATE')
            writer.execute("INSERT INTO sends(phase,kind,at_ns) VALUES('single','gateway',10)")
            self.assertEqual(self.progress(), (0, None, None))
            writer.rollback()
        self.assertEqual(self.progress(), (0, None, None))

    def test_missing_or_inconsistent_counter_refuses_instead_of_omitting_rows(self):
        self.ctx['reserve']('gateway', 'http://localhost/fixture', b'{}')
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            db.execute("UPDATE send_totals SET total=2 WHERE scope IN ('single','__model__')")
        with self.assertRaises(RuntimeError):
            self.progress()
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            db.execute("DELETE FROM send_totals WHERE scope='quality'")
        with self.assertRaises(RuntimeError):
            self.progress()

    def test_unknown_send_is_rejected_and_unaccounted_unknown_history_refuses(self):
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("INSERT INTO sends(phase,kind) VALUES('single','unknown')")
            db.execute('DROP TRIGGER send_totals_insert')
            db.execute("INSERT INTO sends(phase,kind) VALUES('single','unknown')")
        with self.assertRaises(RuntimeError):
            self.progress()
        with self.assertRaises(RuntimeError):
            self.ctx['initialize']()

    def test_missing_or_replaced_index_refuses_without_scan_fallback(self):
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            db.execute('DROP INDEX send_progress_at_ns')
        with self.assertRaises(sqlite3.OperationalError):
            self.progress()
        with sqlite3.connect(self.root/'sends.sqlite') as db:
            db.execute('CREATE INDEX send_progress_at_ns ON sends(kind)')
        with self.assertRaises(RuntimeError):
            self.ctx['initialize']()

    def test_poll_is_read_only_and_timestamp_plans_use_index_searches(self):
        statements = []
        with closing(sqlite3.connect((self.root/'sends.sqlite').as_uri()+'?mode=ro', uri=True)) as db, db:
            db.set_trace_callback(statements.append)
            db.execute('BEGIN')
            fixture.counts.progress_snapshot(db)
            for column, index in [('at_ns', 'send_progress_at_ns'), ('returned_ns', 'send_progress_returned_ns')]:
                plan = list(db.execute(f'EXPLAIN QUERY PLAN SELECT max({column}) FROM sends INDEXED BY {index}'))
                self.assertTrue(any('SEARCH sends USING COVERING INDEX '+index in row[-1] for row in plan), plan)
        sql = '\n'.join(statements).lower()
        self.assertNotIn('count(', sql)
        self.assertNotIn('group by', sql)
        self.assertNotIn('begin immediate', sql)
        self.assertNotIn('update ', sql)


if __name__ == '__main__':
    unittest.main(verbosity=2)
