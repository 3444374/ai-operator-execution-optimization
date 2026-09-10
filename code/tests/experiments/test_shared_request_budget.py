"""Native workers share one finite quota, including crashes and failed POSTs."""
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import tempfile
import time
import unittest

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
