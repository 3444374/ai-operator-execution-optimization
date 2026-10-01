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

    def test_arrow_keeps_binary_identity_tail_and_independent_buffers(self):
        import pyarrow as pa
        from unittest.mock import patch
        rows = ((7, 0, '中文'.encode()), (7, 1, b'\x89PNG\x00\xff'),
                (8, 0, b''), (8, 1, b'same'), (8, 2, b'same'))
        with patch.dict('sys.modules', {'daft': None}):
            batches = list(iter_payload_batches(rows, PayloadBatchLimits(5, 1024),
                                                batch_rows=2, backend='arrow'))
        self.assertEqual([b.num_rows for b in batches], [2, 2, 1])
        self.assertEqual([tuple(row[name] for name in ('session_id', 'sequence', 'payload'))
                          for batch in batches for row in batch.to_pylist()], list(rows))
        self.assertTrue(all(batch['payload'].type == pa.large_binary() for batch in batches))
        self.assertEqual(batches[-1].get_total_buffer_size(), 8 + 24 + len(b'same'))
        for first, second in zip(batches, batches[1:]):
            self.assertNotEqual(first['sequence'].chunk(0).buffers()[1].address,
                                second['sequence'].chunk(0).buffers()[1].address)

    def test_arrow_validates_the_entire_sealed_window_before_yielding(self):
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            next(iter_payload_batches(((1, 0, b'ok'), (1, 0, b'duplicate')),
                 PayloadBatchLimits(2, 1024), batch_rows=1, backend='arrow'))
        with self.assertRaisesRegex(ValueError, 'byte count'):
            next(iter_payload_batches(((1, 0, b'a'), (1, 1, b'b')),
                 PayloadBatchLimits(2, 49), batch_rows=1, backend='arrow'))

    def test_unknown_backend_is_rejected_before_optional_imports(self):
        with self.assertRaisesRegex(ValueError, 'backend'):
            next(iter_payload_batches(((1, 0, b'ok'),), PayloadBatchLimits(1, 100),
                 batch_rows=1, backend='unknown'))

    def test_arrow_early_close_does_not_construct_the_next_batch(self):
        import pyarrow as pa
        from unittest.mock import patch
        stream = iter_payload_batches(tuple((1, i, b'data') for i in range(10)),
                                      PayloadBatchLimits(10, 1024), batch_rows=2, backend='arrow')
        with patch('pyarrow.array', wraps=pa.array) as arrays:
            self.assertEqual(next(stream).num_rows, 2)
            count = arrays.call_count
            stream.close()
            self.assertEqual(list(stream), [])
            self.assertEqual(arrays.call_count, count)
