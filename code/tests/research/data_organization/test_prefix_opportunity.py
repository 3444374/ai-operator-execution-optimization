"""Complete-prefix identity and occurrence-preserving offline order comparisons."""

import unittest

from src.research.data_organization.prefix_opportunity import analyze


def request(name, tokens):
    return dict(call_id=name, token_ids=tokens)


class PrefixOpportunityTests(unittest.TestCase):
    def test_identical_later_block_after_different_prefix_is_not_shared(self):
        result = analyze([request('a', [1, 2, 7, 8]), request('b', [3, 4, 7, 8])],
                         block_sizes=[2], window_rows=2)
        profile = result['block_profiles'][0]
        self.assertEqual(profile['distinct_prefix_blocks'], 4)
        self.assertEqual(profile['repeated_full_block_content_tokens'], 0)
        self.assertEqual(result['orders']['token_prefix']['adjacent_full_block_content_tokens'], {'2': 0})

    def test_duplicate_inputs_keep_two_occurrences_and_partial_tail_is_not_a_full_block(self):
        result = analyze([request('a', [1, 2, 3]), request('b', [1, 2, 3]),
                          request('c', [1, 2, 4, 5])], block_sizes=[2, 4], window_rows=3)
        self.assertEqual(result['duplicate_token_sequence_counts'], [2, 1])
        small, large = result['block_profiles']
        self.assertEqual(small['repeated_full_block_content_tokens'], 4)
        self.assertEqual(small['partial_tail_tokens'], 2)
        self.assertEqual(large['repeated_full_block_content_tokens'], 0)
        self.assertEqual(large['partial_tail_tokens'], 6)
        for order in result['orders'].values():
            self.assertCountEqual(order['call_ids'], ['a', 'b', 'c'])
        self.assertIsNone(result['cache_hits'])
        self.assertIsNone(result['avoided_model_work'])

    def test_prefix_sort_changes_locality_without_moving_members_across_source_windows(self):
        rows = [request('a', [1, 1, 0]), request('b', [8, 8, 0]), request('c', [1, 1, 1]),
                request('d', [0, 0, 0]), request('e', [9, 9, 9])]
        result = analyze(rows, block_sizes=[2], window_rows=3)
        self.assertEqual(result['orders']['token_prefix']['call_ids'], ['a', 'c', 'b', 'd', 'e'])
        self.assertEqual(result['orders']['source']['adjacent_full_block_content_tokens'], {'2': 0})
        self.assertEqual(result['orders']['token_prefix']['adjacent_full_block_content_tokens'], {'2': 2})
        self.assertEqual(rows[1]['call_id'], 'b')

    def test_full_prefix_can_be_reconstructed_from_parent_blocks(self):
        result = analyze([request('a', [1, 2, 3, 4]), request('b', [1, 2, 3, 5])],
                         block_sizes=[2], window_rows=1)
        nodes = result['block_profiles'][0]['nodes']
        self.assertEqual(nodes[0]['occurrences'], 2)
        for node, suffix in zip(nodes[1:], ([3, 4], [3, 5]), strict=True):
            self.assertEqual(nodes[node['parent']]['block_token_ids'] + node['block_token_ids'], [1, 2] + suffix)
        self.assertEqual(result['orders']['token_prefix']['call_ids'], ['a', 'b'])

    def test_empty_operator_has_unknown_quantiles_and_no_invented_work(self):
        result = analyze([], block_sizes=[16], window_rows=4)
        self.assertEqual(result['requests'], 0)
        self.assertIsNone(result['prompt_tokens']['p95'])
        self.assertEqual(result['block_profiles'][0]['nodes'], [])
        self.assertEqual(result['orders']['source']['call_ids'], [])

    def test_invalid_token_shape_duplicate_id_and_invalid_capacity_are_rejected(self):
        for values in ([True], [-1], [[1]], [], [1.5]):
            with self.subTest(values=values), self.assertRaises(ValueError):
                analyze([request('a', values)], block_sizes=[2], window_rows=2)
        with self.assertRaisesRegex(ValueError, 'duplicate call_id'):
            analyze([request('a', [1]), request('a', [1])], block_sizes=[2], window_rows=2)
        for sizes, window in (([0], 2), ([True], 2), ([2, 2], 2), ([], 2), ([2], 0)):
            with self.subTest(sizes=sizes, window=window), self.assertRaises(ValueError):
                analyze([], block_sizes=sizes, window_rows=window)


if __name__ == '__main__':
    unittest.main()
