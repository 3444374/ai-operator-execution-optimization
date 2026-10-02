"""Native workers share one finite quota, including crashes and failed POSTs."""
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from src.experiments.attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger


def spend(shared):
    values = []
    for _ in range(20):
        try:
            values.append(shared.reserve('a'*64))
        except BudgetExhausted:
            break
    return values


def spend_batches(shared):
    values = []
    for _ in range(20):
        try:
            values.extend(shared.reserve_many(['a'*64]*4))
        except BudgetExhausted:
            try:
                values.append(shared.reserve('b'*64))
            except BudgetExhausted:
                break
    return values


class SharedRequestBudgetTests(unittest.TestCase):
    def test_cancelled_unit_cannot_send_queued_worker_requests(self):
        with tempfile.TemporaryDirectory() as name:
            ledger=CellBudgetLedger.create(Path(name)/'budget.sqlite',AttemptBudget('native',4),deadline_utc=time.time()+60)
            ledger.reserve_unit('cell',4)
            shared=ledger.claim_shared_unit('cell')
            shared.reserve('a'*64)
            ledger.close_shared_unit('cell')
            with self.assertRaises(BudgetExhausted):shared.reserve('b'*64)
            self.assertEqual(shared.attempts,1)
            self.assertEqual(ledger.snapshot()['allocated_requests'],4)

    def test_processes_cannot_overspend_or_reclaim(self):
        with tempfile.TemporaryDirectory() as name:
            ledger = CellBudgetLedger.create(Path(name)/'budget.sqlite', AttemptBudget('native', 37),
                                              deadline_utc=time.time()+60)
            ledger.reserve_unit('cell', 37)
            shared = ledger.claim_shared_unit('cell')
            with ProcessPoolExecutor(3) as pool:
                values = sum(list(pool.map(spend, [shared]*3)), [])
            self.assertEqual(sorted(values), list(range(1, 38)))
            self.assertEqual(shared.attempts, 37)
            for claim in (ledger.claim_unit, ledger.claim_shared_unit):
                with self.assertRaises(BudgetError):
                    claim('cell')
            with self.assertRaises(BudgetExhausted):
                shared.reserve('a'*64)

    def test_existing_process_claim_is_not_shareable(self):
        with tempfile.TemporaryDirectory() as name:
            ledger = CellBudgetLedger.create(Path(name)/'budget.sqlite', AttemptBudget('native', 4),
                                              deadline_utc=time.time()+60)
            ledger.reserve_unit('cell', 4)
            claimed = ledger.claim_unit('cell')
            with self.assertRaises(BudgetError):
                ledger.claim_shared_unit('cell')
            self.assertEqual(claimed.reserve('b'*64), 1)

    def test_deadline_no_refund_and_missing_file_fail_closed(self):
        with tempfile.TemporaryDirectory() as name:
            ledger = CellBudgetLedger.create(Path(name)/'budget.sqlite', AttemptBudget('native', 4),
                                              deadline_utc=time.time()+60)
            ledger.reserve_unit('cell', 4)
            shared = ledger.claim_shared_unit('cell')
            self.assertEqual(shared.reserve('c'*64), 1)
            with ledger._transaction() as connection:
                connection.execute('UPDATE budget SET deadline=?', (time.time()-1,))
            with self.assertRaises(BudgetExhausted):
                shared.reserve('c'*64)
            self.assertEqual(shared.attempts, 1)
            self.assertEqual(ledger.snapshot()['allocated_requests'], 4)
            ledger.path.unlink()
            with self.assertRaises(BudgetError):
                shared.reserve('c'*64)


class BatchRequestBudgetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'budget.sqlite'
        self.budget = AttemptBudget('batch.fixture', 37)
        self.ledger = CellBudgetLedger.create(self.path, self.budget, deadline_utc=time.time()+60)
        self.ledger.reserve_unit('earlier', 2)
        self.ledger.reserve_unit('cell', 35)
        self.shared = self.ledger.claim_shared_unit('cell')

    def history(self):
        with sqlite3.connect(self.path) as connection:
            return connection.execute(
                'SELECT sequence,request_sha256 FROM shared_requests WHERE unit_id=? ORDER BY sequence',
                ('cell',)).fetchall()

    def test_order_duplicate_bodies_and_single_tail_survive_reopen(self):
        digests = ['a'*64, 'b'*64, 'a'*64]
        self.assertEqual(self.shared.reserve_many(digests), (3, 4, 5))
        self.assertEqual(self.shared.reserve('c'*64), 6)
        self.assertEqual(self.history(), list(enumerate(digests+['c'*64], 1)))
        reopened = CellBudgetLedger(self.path, self.budget)
        self.assertEqual(reopened.snapshot()['allocated_requests'], 37)
        with self.assertRaises(BudgetError):
            reopened.claim_shared_unit('cell')

    def test_bad_or_unbounded_input_never_partially_charges(self):
        for value in ([], (), 'a'*64, iter(['a'*64]), ['a'*64, 'bad'], ['a'*64, None]):
            with self.subTest(value=type(value).__name__), self.assertRaises(BudgetError):
                self.shared.reserve_many(value)
            self.assertEqual(self.history(), [])

    def test_insufficient_quota_rejects_whole_batch_and_tail_can_spend(self):
        self.shared.reserve_many(['a'*64]*33)
        with self.assertRaises(BudgetExhausted):
            self.shared.reserve_many(['b'*64]*3)
        self.assertEqual(self.shared.attempts, 33)
        self.assertEqual(self.shared.reserve_many(['c'*64]*2), (36, 37))
        with self.assertRaises(BudgetExhausted):
            self.shared.reserve('d'*64)

    def test_close_and_deadline_stop_new_batches_without_refund(self):
        self.shared.reserve_many(['a'*64]*2)
        self.ledger.close_shared_unit('cell')
        with self.assertRaises(BudgetExhausted):
            self.shared.reserve_many(['b'*64]*2)
        with self.ledger._transaction() as connection:
            connection.execute('UPDATE budget SET deadline=?', (time.time()-1,))
        with self.assertRaises(BudgetExhausted):
            self.shared.reserve_many(['c'*64]*2)
        self.assertEqual(self.shared.attempts, 2)
        self.assertEqual(self.ledger.snapshot()['allocated_requests'], 37)

    def test_insert_failure_rolls_back_entire_batch(self):
        with self.ledger._transaction() as connection:
            connection.execute('''CREATE TRIGGER reject_second BEFORE INSERT ON shared_requests
                WHEN NEW.sequence=2 BEGIN SELECT RAISE(ABORT, 'injected failure'); END''')
        with self.assertRaises(BudgetError):
            self.shared.reserve_many(['a'*64]*3)
        self.assertEqual(self.history(), [])

    def test_commit_failure_returns_no_permission_and_preserves_actual_commit(self):
        real_connect = sqlite3.connect
        for commit_first in (False, True):
            class FailingCommit(sqlite3.Connection):
                def commit(connection):
                    if commit_first:
                        super(FailingCommit, connection).commit()
                    raise sqlite3.OperationalError('injected commit error')

            def connect(*args, **kwargs):
                return real_connect(*args, factory=FailingCommit, **kwargs)

            with self.subTest(commit_first=commit_first):
                with patch('src.experiments.cell_budget.sqlite3.connect', side_effect=connect):
                    with self.assertRaises(BudgetError):
                        self.shared.reserve_many(['a'*64]*3)
                self.assertEqual(len(self.history()), 3 if commit_first else 0)
        self.assertEqual(self.shared.reserve('b'*64), 6)

    def test_parallel_single_and_batch_calls_have_unique_numbers_and_finite_total(self):
        with ProcessPoolExecutor(3) as pool:
            values = sum(list(pool.map(spend_batches, [self.shared]*3)), [])
        self.assertEqual(sorted(values), list(range(3, 38)))
        self.assertEqual(self.shared.attempts, 35)
        self.assertEqual(len(self.history()), 35)

    def test_crash_after_commit_does_not_reissue_attempts(self):
        script = '''
import os, sys
from pathlib import Path
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.shared_request_budget import SharedClaimedUnit
unit = SharedClaimedUnit(Path(sys.argv[1]), AttemptBudget('batch.fixture', 37), 'cell')
unit.reserve_many(['a'*64]*4)
os._exit(7)
'''
        result = subprocess.run([sys.executable, '-c', script, str(self.path)], timeout=10)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(self.shared.attempts, 4)
        self.assertEqual(self.shared.reserve_many(['b'*64]*2), (7, 8))
        self.assertEqual(self.ledger.snapshot()['allocated_requests'], 37)
