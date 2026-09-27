"""Finite text and encoded-image windows retain binary identity through Daft."""

import unittest
from src.data.materializers.payloads import PayloadBatchLimits, iter_payload_batches


class PayloadBatchTests(unittest.TestCase):
    def test_text_image_duplicate_values_and_tail_batch_keep_row_identity(self):
        rows = ((1, 0, '中文'.encode()), (1, 1, b'\x89PNG\x00\xff'),
                (1, 2, b''), (1, 3, b'same'), (1, 4, b'same'))
        batches = list(iter_payload_batches(rows, PayloadBatchLimits(5, 1024), batch_rows=2))
        self.assertEqual([b.num_rows for b in batches], [2, 2, 1])
        actual = [tuple(row[name] for name in ('session_id', 'sequence', 'payload'))
                  for batch in batches for row in batch.to_pylist()]
        self.assertEqual(actual, list(rows))

    def test_source_iterator_is_rejected_without_pulling(self):
        consumed = []
        def source():
            consumed.append(True)
            yield (1, 0, b'data')
        with self.assertRaises(ValueError):
            next(iter_payload_batches(source(), PayloadBatchLimits(1, 100), batch_rows=1))
        self.assertEqual(consumed, [])

    def test_row_bytes_and_duplicate_identity_are_checked_before_daft(self):
        for rows, limits in (
            (((1, 0, b'a'), (1, 1, b'b')), PayloadBatchLimits(1, 100)),
            (((1, 0, b'ab'),), PayloadBatchLimits(1, 25)),
            (((1, 0, b'a'), (1, 0, b'b')), PayloadBatchLimits(2, 100)),
        ):
            with self.subTest(rows=rows, limits=limits), self.assertRaises(ValueError):
                next(iter_payload_batches(rows, limits, batch_rows=1))

    def test_consumer_can_stop_at_the_first_partition(self):
        stream = iter_payload_batches(tuple((1, i, b'data') for i in range(10)),
                                      PayloadBatchLimits(10, 1024), batch_rows=2)
        self.assertEqual(next(stream).num_rows, 2)
        stream.close()
        self.assertEqual(list(stream), [])
