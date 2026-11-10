import threading
import unittest
from unittest.mock import patch

from src.modalities.image.preparation_ahead import ImagePreparationAhead, PreparedImage, preparation_value


class PreparationAheadTests(unittest.TestCase):
    def test_capacity_is_held_until_work_is_consumed_or_discarded(self):
        started, release = threading.Event(), threading.Event()
        def work(raw):
            started.set(); release.wait(2)
            return PreparedImage((1, 1), raw, 10, 5)
        with patch('src.modalities.image.preparation_ahead.prepare_rgb', work):
            owner = ImagePreparationAhead(max_pending=2, workers=1)
            try:
                self.assertTrue(owner.submit('used', b'abc')); self.assertTrue(started.wait(1))
                self.assertTrue(owner.submit('dropped', b'def'))
                self.assertFalse(owner.submit('later', b'xyz'))
                owner.discard('dropped')
                self.assertEqual(owner.snapshot()['cancelled_before_start'], 1)
                release.set()
                self.assertEqual(owner.take('used').pixels, b'abc')
            finally:
                release.set(); owner.close()
            self.assertEqual(owner.snapshot()['pending'], 0)
            self.assertEqual(owner.snapshot()['consumed_cpu_ns'], 5)

    def test_unneeded_preparation_error_is_recorded_but_consumed_error_is_returned(self):
        def failed(raw): raise ValueError('decode')
        with patch('src.modalities.image.preparation_ahead.prepare_rgb', failed):
            owner = ImagePreparationAhead(max_pending=1, workers=1)
            try:
                owner.submit('used', b'x')
                with self.assertRaisesRegex(ValueError, 'decode'):
                    owner.take('used')
                owner.submit('not-used', b'x')
                self.assertIsInstance(owner.pending['not-used'].exception(timeout=1), ValueError)
            finally:
                owner.close()
            self.assertEqual(owner.snapshot()['pending'], 0)
            self.assertEqual(owner.snapshot()['discarded'], 1)
            self.assertEqual(owner.snapshot()['discard_errors'], ['ValueError'])

    def test_cost_rule_counts_unconsumed_work(self):
        self.assertFalse(preparation_value(.2, 100, 100, 0)['prepare'])
        self.assertTrue(preparation_value(.9, 100, 50, 0)['prepare'])
        self.assertFalse(preparation_value(.9, 100, 50, 50)['prepare'])


if __name__ == '__main__':
    unittest.main()
