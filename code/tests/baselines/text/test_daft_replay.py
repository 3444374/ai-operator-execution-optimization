"""Saved-answer replay must preserve occurrences, including duplicate prompts."""

from copy import deepcopy
import json
import unittest

from src.infrastructure.private_artifacts import content_digest
from tests.baselines.text.replay_daft_sembench import saved_reply_bindings


class SavedReplyTests(unittest.TestCase):
    def setUp(self):
        self.calls = [dict(call_id=str(i), source_position=i,
            messages_json=json.dumps([dict(role='user', content=text)]))
            for i, text in enumerate(('same', 'other', 'same'))]
        self.frozen = dict(calls=deepcopy(self.calls), texts=['4', '2', '4'])

    def test_equal_messages_keep_distinct_occurrence_bindings(self):
        by_call, by_message = saved_reply_bindings(self.calls, self.frozen)
        self.assertEqual(by_call, {'0': '4', '1': '2', '2': '4'})
        self.assertEqual(len(by_message), 2)
        self.assertEqual(by_message[content_digest(json.loads(self.calls[0]['messages_json']))], '4')

    def test_conflicting_duplicate_answers_are_not_silently_overwritten(self):
        self.frozen['texts'][2] = '5'
        with self.assertRaisesRegex(ValueError, 'different saved replies'):
            saved_reply_bindings(self.calls, self.frozen)

    def test_equal_message_cannot_hide_wrong_source_occurrence(self):
        self.frozen['calls'][0], self.frozen['calls'][2] = self.frozen['calls'][2], self.frozen['calls'][0]
        with self.assertRaisesRegex(ValueError, 'source occurrence'):
            saved_reply_bindings(self.calls, self.frozen)

    def test_duplicate_call_id_is_rejected(self):
        self.calls[2]['call_id'] = self.calls[0]['call_id']
        with self.assertRaisesRegex(ValueError, 'duplicate replay call ID'):
            saved_reply_bindings(self.calls, self.frozen)


if __name__ == '__main__':
    unittest.main()
