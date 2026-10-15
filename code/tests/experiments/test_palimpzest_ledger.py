"""Generator failure/default/usage accounting without any model or HTTP calls."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import unittest

from src.experiments.palimpzest.ledger import PalimpzestLedger


def response(*, usage=True):
    return SimpleNamespace(model='fixture', choices=[SimpleNamespace(message=SimpleNamespace(content='False'))],
        usage=SimpleNamespace(model_dump=lambda: dict(prompt_tokens=2, completion_tokens=1)) if usage else None)


class PZLedgerTests(unittest.TestCase):
    def start(self, ledger, key, index):
        ledger.started(key, dict(model='fixture', messages=[dict(role='user', content='sensitive fixture')]),
                       row_id=('reviews-'+str(index),))

    def test_completion_error_and_parser_default_are_invalid_even_if_native_returns_false(self):
        ledger = PalimpzestLedger(max_calls=3)
        self.start(ledger, 'failed', 0)
        ledger.finished('failed', error=RuntimeError('completion fixture failed'))
        ledger.generator_finished('failed', ({'passed_operator': False}, None, None, []))
        self.start(ledger, 'default', 1)
        ledger.finished('default', response=response())
        ledger.parsed('default', error=ValueError('unparseable fixture'), parser_default=True)
        ledger.generator_finished('default', ({'passed_operator': False}, None, None, []))
        self.start(ledger, 'negative', 2)
        ledger.finished('negative', response=response())
        ledger.parsed('negative', {'passed_operator': False})
        ledger.generator_finished('negative', ({'passed_operator': False}, None, None, []))
        self.assertEqual(ledger.predictions({'reviews-0': 'r0', 'reviews-1': 'r1', 'reviews-2': 'r2'}),
                         [('r0', None), ('r1', None), ('r2', 'NEGATIVE')])
        summary = ledger.snapshot()
        self.assertEqual(summary['completion_failed'], 1)
        self.assertEqual(summary['parser_default_retained'], 1)
        self.assertEqual(summary['known_prompt_tokens'], 4)
        self.assertIsNone(summary['total_prompt_tokens'])
        self.assertIsNone(summary['monetary_cost_usd'])
        self.assertNotIn('sensitive fixture', str(ledger.calls()))

    def test_missing_or_partial_usage_is_unavailable_not_zero_or_free(self):
        for usage in (None, {}, {'prompt_tokens': 2}, {'prompt_tokens': True, 'completion_tokens': 1}):
            ledger = PalimpzestLedger(max_calls=1)
            self.start(ledger, 'one', 0)
            ledger.finished('one', response=dict(usage=usage))
            value = ledger.snapshot()
            self.assertEqual(value['usage_status'], 'unavailable')
            self.assertEqual(value['usage_unavailable_calls'], 1)
            self.assertIsNone(value['total_prompt_tokens'])
            self.assertIsNone(value['total_output_tokens'])
            self.assertIsNone(value['monetary_cost_usd'])

    def test_parallel_generators_are_independent_and_outstanding_work_is_retained(self):
        ledger = PalimpzestLedger(max_calls=21)
        def one(index):
            key = str(index)
            self.start(ledger, key, index)
            ledger.finished(key, response=response())
            ledger.parsed(key, {'passed_operator': False})
            ledger.generator_finished(key, ({'passed_operator': False}, None, None, []), row_id=('reviews-'+key,))
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(one, range(20)))
        self.start(ledger, 'pending', 20)
        value = ledger.snapshot()
        self.assertEqual((value['completion_submitted'], value['completion_completed'],
                          value['completion_outstanding']), (21, 20, 1))
        self.assertEqual(value['known_prompt_tokens'], 40)
        with self.assertRaisesRegex(ValueError, 'excessive'):
            self.start(ledger, 'overflow', 21)

    def test_duplicate_occurrences_and_changed_source_association_are_rejected(self):
        ledger = PalimpzestLedger(max_calls=2)
        for key in ('one', 'two'):
            self.start(ledger, key, 0)
        with self.assertRaisesRegex(ValueError, 'more than one'):
            ledger.predictions({'reviews-0': 'independent-occurrence'})
        with self.assertRaisesRegex(ValueError, 'association changed'):
            ledger.generator_finished('one', row_id=('reviews-1',))
