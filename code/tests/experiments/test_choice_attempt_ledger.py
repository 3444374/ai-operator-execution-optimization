"""The smoke request limit is durable and shared across runner instances."""
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from src.experiments.attempt_ledger import AttemptLedger, BudgetError, BudgetExhausted
from src.experiments.attempt_ledger import AttemptBudget, AttemptLedger as ConfiguredLedger


BUDGET = AttemptBudget("fixture.request-budget", 100)


class AttemptLedgerTests(unittest.TestCase):
    def test_operator_budgets_share_storage_without_sharing_authorization(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'attempts.jsonl'
            budget = AttemptBudget('fixture.map.v1', 2)
            ledger = ConfiguredLedger.create(path, budget)
            ledger.reserve('a' * 64)
            before = path.read_bytes()
            for wrong in (AttemptBudget('fixture.filter.v1', 2), AttemptBudget('fixture.map.v1', 3)):
                with self.subTest(budget=wrong), self.assertRaises(BudgetError):
                    ConfiguredLedger(path, wrong)
                self.assertEqual(path.read_bytes(), before)
            reopened = ConfiguredLedger(path, budget)
            self.assertEqual(reopened.reserve('b' * 64), 2)
            with self.assertRaises(BudgetExhausted):
                ledger.reserve('c' * 64)

    def test_invalid_budget_configuration_is_rejected(self):
        for identity, limit in (('', 1), ('a\nb', 1), ('valid', True), ('valid', 0)):
            with self.subTest(identity=identity, limit=limit), self.assertRaises(ValueError):
                AttemptBudget(identity, limit)

    def test_reopen_does_not_restore_spent_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'attempts.jsonl'
            ledger = AttemptLedger.create(path, BUDGET)
            self.assertEqual(ledger.reserve('a' * 64), 1)
            reopened = AttemptLedger(path, BUDGET)
            for expected in range(2, 101):
                self.assertEqual(reopened.reserve('b' * 64), expected)
            with self.assertRaises(BudgetExhausted):
                ledger.reserve('c' * 64)
            self.assertEqual(AttemptLedger(path, BUDGET).attempts, 100)

    def test_concurrent_runners_cannot_overspend(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'attempts.jsonl'
            AttemptLedger.create(path, BUDGET)

            def reserve(_):
                try:
                    return AttemptLedger(path, BUDGET).reserve('d' * 64)
                except BudgetExhausted:
                    return None

            with ThreadPoolExecutor(max_workers=8) as pool:
                outcomes = list(pool.map(reserve, range(120)))
            self.assertEqual(sorted(n for n in outcomes if n is not None), list(range(1, 101)))
            self.assertEqual(outcomes.count(None), 20)

    def test_corrupt_or_missing_history_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'attempts.jsonl'
            with self.assertRaises(BudgetError):
                AttemptLedger(path, BUDGET)
            ledger = AttemptLedger.create(path, BUDGET)
            original = path.read_text()
            for invalid in (
                '', original.rstrip('\n'),
                original.replace('100', '101'),
                original.replace('"schema_version":1', '"schema_version":true'),
                original.replace('"limit":100', '"limit":100,"limit":100'),
                original + '{"attempt":2,"request_sha256":"' + 'a' * 64 + '"}\n',
                original + '{"attempt":1,"request_sha256":"short"}\n',
                original + '{"attempt":',
                'x' * 65537 + '\n',
            ):
                with self.subTest(value=invalid[:80]):
                    path.write_text(invalid)
                    with self.assertRaises(BudgetError):
                        ledger.reserve('e' * 64)
                    self.assertEqual(path.read_text(), invalid)

    def test_existing_history_is_never_reinitialized(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'attempts.jsonl'
            AttemptLedger.create(path, BUDGET).reserve('f' * 64)
            before = path.read_bytes()
            with self.assertRaises(FileExistsError):
                AttemptLedger.create(path, BUDGET)
            self.assertEqual(path.read_bytes(), before)
            alias = path.with_name('alias.jsonl')
            alias.symlink_to(path)
            with self.assertRaises(BudgetError):
                AttemptLedger(alias, BUDGET)

    def test_failed_persistence_or_http_does_not_refund_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'attempts.jsonl'
            ledger = AttemptLedger.create(path, BUDGET)
            with patch('os.fsync', side_effect=OSError('injected storage failure')):
                with self.assertRaises(BudgetError):
                    ledger.reserve('1' * 64)
            self.assertEqual(AttemptLedger(path, BUDGET).attempts, 1)
            self.assertEqual(ledger.reserve('2' * 64), 2)
            # No completion or refund is necessary to retain an uncertain attempt.
            self.assertEqual(AttemptLedger(path, BUDGET).attempts, 2)

    def test_invalid_digest_consumes_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = AttemptLedger.create(Path(directory) / 'attempts.jsonl', BUDGET)
            for value in ('payload', '', 'A' * 64, None):
                with self.assertRaises(BudgetError):
                    ledger.reserve(value)
            self.assertEqual(ledger.attempts, 0)


if __name__ == '__main__':
    unittest.main()
