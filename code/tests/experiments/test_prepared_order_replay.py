from copy import deepcopy
import unittest

from src.experiments.prepared_order_replay import replay
from src.infrastructure.private_artifacts import content_digest
from src.serving.backends.vllm_protocol import IDENTITY_FIELDS


def capture():
    rows = []
    for i, tokens in enumerate(([1, 2, 3, 4], [8, 7, 6, 5], [1, 2, 3, 4], [8, 7, 6, 5])):
        row = dict(query_id='q', algorithm_stage='map', model_role='main', row_id=str(i),
                   source_position=i, call_index=0, call_id='call-' + str(i), messages_json='[{"role":"user","content":"fixture"}]')
        rows.append(dict(row=row, generation=dict(max_tokens=4), token_ids=list(tokens), prompt_tokens=len(tokens)))
    digest = content_digest([dict(row={k: r['row'][k] for k in (*IDENTITY_FIELDS, 'messages_json')},
                                  generation=r['generation'], token_ids=r['token_ids']) for r in rows])
    return dict(schema='semloom.semantic_request_capture.v1', requests=rows, request_digest=digest,
                complete_initial_operator=True)


class ReplayTests(unittest.TestCase):
    def test_real_membership_is_preserved_while_fixture_shared_content_changes(self):
        value = capture(); before = deepcopy(value)
        result = replay(value, candidate_limit=4, group_requests=2, prefix_token_limit=4, block_tokens=2, repeats=2)
        arms = {row['mode']: row for row in result['arms']}
        self.assertEqual(arms['fifo']['within_group_shared_full_block_tokens'], 0)
        self.assertEqual(arms['prefix']['within_group_shared_full_block_tokens'], 8)
        self.assertEqual(arms['prefix']['selected_groups'], [[0, 2], [1, 3]])
        self.assertEqual(value, before)
        self.assertIn('simulated_ready_window', result['source_type'])

    def test_group_of_four_does_not_inherit_the_adjacent_pair_metric(self):
        result = replay(capture(), candidate_limit=4, group_requests=4, prefix_token_limit=4, block_tokens=2)
        prefix = next(row for row in result['arms'] if row['mode'] == 'prefix')
        self.assertGreater(prefix['adjacent_shared_full_block_tokens'], 0)
        self.assertEqual(prefix['within_group_shared_full_block_tokens'], 0)

    def test_tampered_tokens_are_rejected_before_replay(self):
        value = capture(); value['requests'][0]['token_ids'][0] = 99
        with self.assertRaisesRegex(ValueError, 'digest'):
            replay(value, candidate_limit=4, group_requests=2, prefix_token_limit=4, block_tokens=2)


if __name__ == '__main__':
    unittest.main()
