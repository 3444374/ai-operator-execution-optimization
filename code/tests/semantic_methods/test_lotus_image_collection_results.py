"""Bounded NumPy image collection and RM scatter, without a model or SDK."""
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import numpy as np
from PIL import Image

from src.modalities.image.contracts import EmbeddingSemantics, ImageEmbeddingResult
from src.semantic_methods.lotus.image_backend import OrderedMatrix
from src.semantic_methods.lotus.image_rm import make_image_rm


SEMANTICS = EmbeddingSemantics("weights", "processor", 512)


def vectors(count):
    values = np.zeros((count, 512), dtype=np.float32)
    values[np.arange(count), np.arange(count) % 512] = 1
    return values


class OrderedMatrixTests(unittest.TestCase):
    def test_empty_input_keeps_original_finish_error(self):
        collector = OrderedMatrix((), SEMANTICS)
        collector.add((), vectors(0))
        with self.assertRaisesRegex(ValueError, "need at least one array to stack"):
            collector.finish()

    def test_collection_limit_checked_before_other_input(self):
        with self.assertRaisesRegex(ValueError, "exceeds 5000 rows"):
            OrderedMatrix(tuple(range(5001)), None)

    def test_unordered_batches_and_tail_keep_exact_float32_bytes(self):
        ids = tuple(str(i) for i in range(11))
        expected = vectors(11)
        collector = OrderedMatrix(ids, SEMANTICS)
        order = [7, 3, 10, 2, 0, 9, 5, 1, 8, 6, 4]
        for offset in range(0, len(order), 4):
            positions = order[offset:offset + 4]
            collector.add(tuple(ids[i] for i in positions), expected[positions])
        result = collector.finish()
        self.assertEqual(result.dtype, np.dtype("float32"))
        self.assertEqual(result.tobytes(), expected.tobytes())

    def test_input_and_each_finished_matrix_have_independent_ownership(self):
        upstream = vectors(2)
        collector = OrderedMatrix(("a", "b"), SEMANTICS)
        collector.add(("a", "b"), upstream)
        upstream[:] = 0
        first, second = collector.finish(), collector.finish()
        self.assertFalse(np.shares_memory(upstream, first))
        self.assertFalse(np.shares_memory(first, second))
        first[:] = 0
        self.assertEqual(second.tobytes(), vectors(2).tobytes())
        self.assertEqual(collector.finish().tobytes(), vectors(2).tobytes())

    def test_noncontiguous_input_is_checked_and_copied(self):
        values = np.asfortranarray(vectors(3))
        collector = OrderedMatrix(("a", "b", "c"), SEMANTICS)
        collector.add(("a", "b", "c"), values)
        result = collector.finish()
        self.assertTrue(result.flags.c_contiguous)
        self.assertFalse(np.shares_memory(result, values))
        self.assertEqual(result.tobytes(), values.tobytes())

    def test_missing_results_and_duplicate_expected_ids_remain_incomplete(self):
        for ids in (("a", "b"), ("a", "a")):
            with self.subTest(ids=ids):
                collector = OrderedMatrix(ids, SEMANTICS)
                collector.add(("a",), vectors(1))
                with self.assertRaisesRegex(ValueError, "incomplete"):
                    collector.finish()

    def test_duplicate_keeps_valid_prefix_and_stops_before_suffix(self):
        collector = OrderedMatrix(("a", "b", "c"), SEMANTICS)
        with self.assertRaisesRegex(ValueError, "unexpected or duplicate ID"):
            collector.add(("a", "a", "b"), vectors(3))
        collector.add(("b", "c"), vectors(2)[::-1])
        expected = np.stack((vectors(1)[0], vectors(2)[1], vectors(2)[0]))
        self.assertEqual(collector.finish().tobytes(), expected.tobytes())
        with self.assertRaisesRegex(ValueError, "unexpected or duplicate ID"):
            collector.add(("a",), vectors(1))

    def test_unknown_keeps_valid_prefix_and_can_finish_after_missing_arrives(self):
        collector = OrderedMatrix(("a", "b"), SEMANTICS)
        with self.assertRaisesRegex(ValueError, "unexpected or duplicate ID"):
            collector.add(("a", "unknown"), vectors(2))
        with self.assertRaisesRegex(ValueError, "incomplete"):
            collector.finish()
        collector.add(("b",), vectors(2)[1:])
        self.assertEqual(collector.finish().tobytes(), vectors(2).tobytes())

    def test_invalid_matrix_wins_over_unknown_or_duplicate_ids(self):
        cases = [
            (np.zeros((2, 511), np.float32), "complete finite float32"),
            (vectors(2).astype(np.float64), "complete finite float32"),
            (np.full((2, 512), np.nan, np.float32), "complete finite float32"),
            (np.full((2, 512), np.inf, np.float32), "complete finite float32"),
            (vectors(2) * 2, "not L2 normalized"),
        ]
        for values, error in cases:
            with self.subTest(error=error, dtype=values.dtype):
                collector = OrderedMatrix(("a", "b"), SEMANTICS)
                with self.assertRaisesRegex(ValueError, error):
                    collector.add(("unknown", "unknown"), values)
                collector.add(("a", "b"), vectors(2))
                with self.assertRaisesRegex(ValueError, error):
                    collector.add(("a", "a"), values)

    def test_semantics_check_precedes_norm_and_id_check(self):
        for semantics in (
                EmbeddingSemantics("w", "p", 512, dtype="float64"),
                EmbeddingSemantics("w", "p", 512, projected=False),
                EmbeddingSemantics("w", "p", 512, normalized=False)):
            collector = OrderedMatrix(("a",), semantics)
            with self.assertRaisesRegex(ValueError, "projected normalized float32"):
                collector.add(("unknown",), vectors(1) * 2)


class RMScatterTests(unittest.TestCase):
    def setUp(self):
        class RM:
            def __call__(self, docs):
                return self._embed(docs)
        self.module_patch = patch.dict(sys.modules, {"lotus.models.rm": SimpleNamespace(RM=RM)})
        self.module_patch.start()
        self.addCleanup(self.module_patch.stop)

    def check_scatter(self, *, mixed, reverse):
        source = vectors(2)
        text_source = vectors(4)[3:]
        class Executor:
            semantics = SEMANTICS
            input_kind = "rgb_pil"
            def embed_batch(self, ids, images):
                return ImageEmbeddingResult(ids[::-1] if reverse else ids,
                    source[::-1] if reverse else source, self.semantics, 0)
        docs = [Image.new("RGB", (2, 3)), Image.new("RGB", (3, 2))]
        if mixed:
            docs.insert(1, "query")
        rm = make_image_rm(Executor(), lambda docs: text_source)
        result = rm(docs)
        expected = np.stack((source[0], text_source[0], source[1])) if mixed else source.copy()
        self.assertEqual(result.tobytes(), expected.tobytes())
        self.assertTrue(result.flags.owndata)
        self.assertFalse(np.shares_memory(result, source))
        self.assertFalse(np.shares_memory(result, text_source))
        source[:] = 0
        text_source[:] = 0
        self.assertEqual(result.tobytes(), expected.tobytes())

    def test_ordered_all_images_use_owned_matrix(self):
        self.check_scatter(mixed=False, reverse=False)

    def test_reverse_all_images_use_owned_matrix(self):
        self.check_scatter(mixed=False, reverse=True)

    def test_mixed_input_scatter_keeps_native_row_order(self):
        for reverse in (False, True):
            with self.subTest(reverse=reverse):
                self.check_scatter(mixed=True, reverse=reverse)


if __name__ == "__main__":
    unittest.main()
