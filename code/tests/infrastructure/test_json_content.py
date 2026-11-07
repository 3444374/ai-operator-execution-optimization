"""JSON receipt comparisons preserve types without normalizing raw values."""
import unittest

from src.infrastructure.json_content import json_content_equal


class JsonContentTests(unittest.TestCase):
    def test_reordered_objects_and_shared_subtrees_keep_the_same_content(self):
        shared = [None, True, 3, 4.5, '文字']
        self.assertTrue(json_content_equal(
            {'second': shared, 'first': shared}, {'first': list(shared), 'second': list(shared)}))
        self.assertTrue(json_content_equal([], []))
        self.assertTrue(json_content_equal({}, {}))

    def test_boolean_integer_float_and_signed_zero_do_not_alias(self):
        for left, right in ((False, 0), (True, 1), (0, 0.0), (1, 1.0), (0.0, -0.0)):
            with self.subTest(left=left, right=right):
                self.assertFalse(json_content_equal({'value': [left]}, {'value': [right]}))
                self.assertFalse(json_content_equal(right, left))
                self.assertTrue(json_content_equal(left, left))

    def test_array_order_keys_and_missing_values_are_significant(self):
        for left, right in (([1, 2], [2, 1]), ([1], [1, 2]),
                            ({'a': None}, {}), ({'a': 1}, {'b': 1}), ('1', 1)):
            with self.subTest(left=left, right=right):
                self.assertFalse(json_content_equal(left, right))

    def test_nonfinite_and_nonjson_values_fail_even_when_the_same_object(self):
        class Integer(int):
            pass
        for value in (float('nan'), float('inf'), -float('inf'), (1,), b'1',
                      object(), {1: 'value'}, Integer(1)):
            with self.subTest(value_type=type(value).__name__):
                self.assertFalse(json_content_equal(value, value))
                self.assertFalse(json_content_equal({'value': value}, {'value': value}))
        self.assertFalse(json_content_equal([1], (1,)))

    def test_cyclic_or_too_deep_content_is_not_accepted(self):
        cyclic_list = []
        cyclic_list.append(cyclic_list)
        cyclic_dict = {}
        cyclic_dict['self'] = cyclic_dict
        for value in (cyclic_list, cyclic_dict):
            with self.subTest(value_type=type(value).__name__):
                self.assertFalse(json_content_equal(value, value))
        left, right = [], []
        for _ in range(1000):
            left, right = [left], [right]
        self.assertFalse(json_content_equal(left, right))
