import types
import unittest

from src.experiments.request_layout_study import prefix_tree_opportunity, text_layout


class PrefixTreeOpportunityTest(unittest.TestCase):
    def test_subgroups_below_a_short_global_prefix_are_counted(self):
        report = prefix_tree_opportunity([[1, 2, 3, 4], [1, 2, 3, 4], [1, 2, 9, 8], [1, 2, 9, 8]], 2)
        self.assertEqual(report['all_member_common_tokens'], 2)
        self.assertEqual(report['shared_prefix_tree_tokens'], 10)
        self.assertEqual(report['unique_prefix_block_tokens'], 6)

    def test_layout_keeps_both_segments_and_restores_original_after_error(self):
        original = lambda data, instruction: dict(role='user', content='Context:\n' + data + '\n\n' + instruction)
        instructions = types.SimpleNamespace(user_message_formatter=original, context_formatter=lambda value: (value, []))
        columns = ['body']
        method = types.SimpleNamespace(instructions=instructions, columns=columns)
        with self.assertRaisesRegex(RuntimeError, 'stop'):
            with text_layout(method, instruction_first=True, columns=['body']):
                result = instructions.user_message_formatter('literal Context:\n inside data', 'Instruction: score')
                self.assertEqual(result['content'], 'Instruction: score\n\nContext:\nliteral Context:\n inside data')
                raise RuntimeError('stop')
        self.assertIs(instructions.user_message_formatter, original)
        self.assertIs(method.columns, columns)


if __name__ == '__main__':
    unittest.main()
