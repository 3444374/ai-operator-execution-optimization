"""The smoke audit compares actual outbound JSON without erasing value types."""
import copy
import unittest

from src.experiments.choice_service_checks import verify_choice_pair, verify_completion


class ChoiceServiceChecksTests(unittest.TestCase):
    def test_only_choice_may_differ_in_actual_requests(self):
        old = {'model': 'fixture', 'temperature': 0, 'max_tokens': 8,
               'messages': [{'role': 'user', 'content': 'text'}]}
        choice = {**old, 'structured_outputs': {'choice': ['TRUE', 'FALSE', 'UNKNOWN']}}
        verify_choice_pair(old, choice)
        for field, value in (('temperature', False), ('temperature', 0.0),
                             ('max_tokens', 9), ('model', 'other')):
            changed = copy.deepcopy(choice)
            changed[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                verify_choice_pair(old, changed)
        with self.assertRaises(ValueError):
            verify_choice_pair(old, {**choice, 'structured_outputs': {'choice': ['FALSE', 'TRUE', 'UNKNOWN']}})

    def test_sql_disposition_and_usage_must_match_raw_completion(self):
        for raw, rows in (('TRUE', 1), ('FALSE', 0), ('UNKNOWN', 0)):
            completion = {'raw_output': raw, 'response_model_id': 'fixture',
                          'prompt_tokens': 17, 'output_tokens': 2, 'finish_reason': 'stop'}
            plan = {'Model Calls': 1, 'Actual Rows': rows, 'Emitted Rows': rows,
                    'Prompt Tokens': 17, 'Output Tokens': 2}
            self.assertTrue(verify_completion(completion, plan, None, 'fixture'))
            for key in plan:
                with self.subTest(raw=raw, key=key), self.assertRaises(ValueError):
                    verify_completion(completion, {**plan, key: 99}, None, 'fixture')
            with self.assertRaises(ValueError):
                verify_completion(completion, plan, None, 'other')
        invalid = {**completion, 'raw_output': 'yes'}
        self.assertFalse(verify_completion(invalid, None, '22000', 'fixture'))
        with self.assertRaises(ValueError):
            verify_completion(invalid, plan, None, 'fixture')
