"""Portable completion-budget behavior retained from the retired SQL runner tests."""
from pathlib import Path
import pickle
import sqlite3
import tempfile
import time
import unittest
from unittest import mock
from src.experiments.attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger, COMPLETION_SCHEMA, SCHEMA

class CompletionBudgetTests(unittest.TestCase):
    def test_nullable_claim_has_no_wall_expiry_but_keeps_quota_pid_and_single_claim(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "budget.sqlite"
            budget = AttemptBudget("completion.fixture", 3)
            ledger = CellBudgetLedger.create(path, budget, deadline_utc=None)
            self.assertEqual(ledger.snapshot()["schema"], COMPLETION_SCHEMA)
            ledger.reserve_unit("unit", 2)
            unit = ledger.claim_unit("unit")
            with (
                mock.patch("time.monotonic", return_value=time.monotonic() + 1e9),
                mock.patch("time.time", return_value=time.time() + 1e9),
            ):
                self.assertEqual(unit.reserve("a" * 64), 1)
                self.assertEqual(ledger.reserve_unit("last", 1).first_attempt, 3)
            with mock.patch("os.getpid", return_value=-1), self.assertRaises(BudgetError):
                unit.reserve("a" * 64)
            with self.assertRaises(TypeError):
                pickle.dumps(unit)
            self.assertEqual(unit.reserve("a" * 64), 2)
            with self.assertRaises(BudgetExhausted):
                unit.reserve("a" * 64)
            with self.assertRaises(BudgetExhausted):
                ledger.reserve_unit("extra", 1)
            with self.assertRaises(BudgetError):
                CellBudgetLedger(path, budget).claim_unit("unit")

    def test_nullable_shared_claim_keeps_durable_close_and_spend(self):
        with tempfile.TemporaryDirectory() as temporary:
            budget = AttemptBudget("completion.shared", 2)
            ledger = CellBudgetLedger.create(Path(temporary) / "budget", budget, deadline_utc=None)
            ledger.reserve_unit("unit", 2)
            shared = ledger.claim_shared_unit("unit")
            self.assertEqual(shared.reserve("b" * 64), 1)
            ledger.close_shared_unit("unit")
            with self.assertRaises(BudgetExhausted):
                shared.reserve("b" * 64)
            self.assertEqual(shared.attempts, 1)

    def test_finite_v1_schema_constraint_reopen_and_expiry_are_retained(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "budget"
            budget = AttemptBudget("finite.fixture", 2)
            ledger = CellBudgetLedger.create(path, budget, deadline_utc=time.time() + 10)
            self.assertEqual(ledger.snapshot()["schema"], SCHEMA)
            with sqlite3.connect(path) as connection:
                self.assertEqual(
                    next(
                        row[3]
                        for row in connection.execute("PRAGMA table_info(budget)")
                        if row[1] == "deadline"
                    ),
                    1,
                )
            reopened = CellBudgetLedger(path, budget)
            with (
                mock.patch("time.time", return_value=time.time() + 20),
                self.assertRaises(BudgetExhausted),
            ):
                reopened.reserve_unit("expired", 1)

    def test_v1_cannot_silently_accept_a_null_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "budget"
            budget = AttemptBudget("tamper.fixture", 2)
            CellBudgetLedger.create(path, budget, deadline_utc=None)
            with sqlite3.connect(path) as connection:
                connection.execute("UPDATE budget SET schema=?", (SCHEMA,))
            with self.assertRaises(BudgetError):
                CellBudgetLedger(path, budget)

