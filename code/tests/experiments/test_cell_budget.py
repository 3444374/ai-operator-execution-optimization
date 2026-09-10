"""Large no-network budgets remain durable across claims, crashes, and restart."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import pickle
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from src.experiments.attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger


class CellBudgetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'budget.sqlite'
        self.budget = AttemptBudget('fixture.capacity', 100_000)
        self.ledger = CellBudgetLedger.create(self.path, self.budget, deadline_utc=time.time() + 3600)

    def test_one_hundred_thousand_sends_have_no_history_io(self):
        self.ledger.reserve_unit('large', 100_000)
        unit = self.ledger.claim_unit('large')

        async def consume():
            with patch.object(sqlite3, 'connect', side_effect=AssertionError('send-time SQLite I/O')):
                with patch('os.fsync', side_effect=AssertionError('send-time fsync')):
                    for i in range(100_000):
                        self.assertEqual(unit.reserve('a' * 64), i + 1)
                        if i % 1000 == 0:
                            await asyncio.sleep(0)
        asyncio.run(consume())
        self.assertEqual(unit.remaining, 0)
        with self.assertRaises(BudgetExhausted):
            unit.reserve('a' * 64)
        self.assertEqual(CellBudgetLedger(self.path, self.budget).snapshot()['allocated_requests'], 100_000)

    def test_one_claim_wins_and_restart_cannot_claim_again(self):
        self.ledger.reserve_unit('shared', 10)
        def claim(_):
            try:
                return CellBudgetLedger(self.path, self.budget).claim_unit('shared')
            except BudgetError:
                return None
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(claim, range(16)))
        self.assertEqual(sum(value is not None for value in results), 1)
        with self.assertRaises(BudgetError):
            CellBudgetLedger(self.path, self.budget).claim_unit('shared')

    def test_crash_before_first_send_does_not_refund_or_restore_claim(self):
        self.ledger.reserve_unit('crash', 10)
        script = '''
import os, sys
from pathlib import Path
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
CellBudgetLedger(Path(sys.argv[1]), AttemptBudget('fixture.capacity', 100000)).claim_unit('crash')
os._exit(7)
'''
        result = subprocess.run([sys.executable, '-c', script, str(self.path)], timeout=10)
        self.assertEqual(result.returncode, 7)
        reopened = CellBudgetLedger(self.path, self.budget)
        self.assertEqual(reopened.snapshot()['allocated_requests'], 10)
        with self.assertRaises(BudgetError):
            reopened.claim_unit('crash')

    def test_unit_cannot_be_copied_or_used_in_another_process(self):
        self.ledger.reserve_unit('owner', 3)
        unit = self.ledger.claim_unit('owner')
        with self.assertRaises(TypeError):
            pickle.dumps(unit)
        with patch('os.getpid', return_value=-1), self.assertRaises(BudgetError):
            unit.reserve('a' * 64)
        self.assertEqual(unit.reserve('a' * 64), 1)

    def test_full_reservation_counts_even_when_only_partially_consumed(self):
        self.ledger.reserve_unit('first', 3)
        first = self.ledger.claim_unit('first')
        first.reserve('a' * 64)
        reservation = self.ledger.reserve_unit('rest', 99_997)
        self.assertEqual(reservation.first_attempt, 4)
        with self.assertRaises(BudgetExhausted):
            self.ledger.reserve_unit('extra', 1)
        with self.assertRaises(BudgetError):
            self.ledger.reserve_unit('first', 3)

    def test_concurrent_requests_share_one_unit_limit(self):
        self.ledger.reserve_unit('concurrent', 100)
        unit = self.ledger.claim_unit('concurrent')
        def use(_):
            try:
                return unit.reserve('b' * 64)
            except BudgetExhausted:
                return None
        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(use, range(120)))
        self.assertEqual(sorted(x for x in outcomes if x is not None), list(range(1, 101)))

    def test_missing_corrupt_alias_wrong_budget_and_overwrite_rejected(self):
        with self.assertRaises(FileExistsError):
            CellBudgetLedger.create(self.path, self.budget, deadline_utc=time.time() + 3600)
        with self.assertRaises(BudgetError):
            CellBudgetLedger(self.path, AttemptBudget('wrong.identity', 100_000))
        alias = self.path.with_name('alias')
        alias.symlink_to(self.path)
        with self.assertRaises(BudgetError):
            CellBudgetLedger(alias, self.budget)
        missing = self.path.with_name('missing')
        with self.assertRaises(BudgetError):
            CellBudgetLedger(missing, self.budget)
        self.assertFalse(missing.exists())
        self.path.write_bytes(b'corrupt database')
        with self.assertRaises(BudgetError):
            CellBudgetLedger(self.path, self.budget)

    def test_invalid_digest_or_expiry_never_authorizes_a_send(self):
        self.ledger.reserve_unit('expires', 3)
        unit = self.ledger.claim_unit('expires')
        with self.assertRaises(BudgetError):
            unit.reserve('bad digest')
        self.assertEqual(unit.attempts, 0)
        with patch('time.monotonic', return_value=time.monotonic() + 7200):
            with self.assertRaises(BudgetExhausted):
                unit.reserve('a' * 64)
        with patch('time.time', return_value=time.time() + 7200):
            with self.assertRaises(BudgetExhausted):
                self.ledger.reserve_unit('too-late', 1)


if __name__ == '__main__':
    unittest.main()
