"""The smoke request limit is durable and shared across runner instances."""
import tempfile
import unittest
from pathlib import Path

from src.experiments.choice_attempt_ledger import AttemptLedger, BudgetExhausted


class AttemptLedgerTests(unittest.TestCase):
    def test_reopen_does_not_restore_spent_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'attempts.jsonl'
            ledger = AttemptLedger.create(path)
            self.assertEqual(ledger.reserve('a' * 64), 1)
            reopened = AttemptLedger(path)
            for expected in range(2, 101):
                self.assertEqual(reopened.reserve('b' * 64), expected)
            with self.assertRaises(BudgetExhausted):
                ledger.reserve('c' * 64)
            self.assertEqual(AttemptLedger(path).attempts, 100)


if __name__ == '__main__':
    unittest.main()
