import types
import unittest
from unittest.mock import Mock

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

    def test_each_layout_formats_the_context_once_and_keeps_complete_messages(self):
        for first, instruction in ((False, 'Instruction: score'),
                                   (True, 'Instruction: score'), (True, None)):
            with self.subTest(instruction_first=first, instruction=instruction):
                instructions = types.SimpleNamespace(context_formatter=Mock(return_value=('body', [])))
                def original(data, instruction=None):
                    text, _ = instructions.context_formatter(data)
                    return dict(role='user', content=f'Context:\n{text}\n\n{instruction}')
                instructions.user_message_formatter = original
                method = types.SimpleNamespace(instructions=instructions, columns=['body'])
                with text_layout(method, instruction_first=first, columns=['body']):
                    message = instructions.user_message_formatter('input', instruction)
                expected = (f'{instruction}\n\nContext:\nbody' if first and instruction is not None
                            else f'Context:\nbody\n\n{instruction}')
                self.assertEqual(message, dict(role='user', content=expected))
                instructions.context_formatter.assert_called_once_with('input')
                self.assertIs(instructions.user_message_formatter, original)


if __name__ == '__main__':
    unittest.main()
