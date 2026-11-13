"""Saved-answer replay must preserve occurrences, including duplicate prompts."""
from copy import deepcopy
import json
import importlib.util
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.infrastructure.private_artifacts import content_digest
from tests.baselines.text.replay_daft_sembench import saved_reply_bindings
from tests.experiments import test_sembench_text_original as original


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


@unittest.skipUnless(original.READY and importlib.util.find_spec('daft'),
                     'requires pinned original query and separate Daft CPU SDK, no model')
class CompleteDaftReplayTests(unittest.TestCase):
    def replay(self, scenario, key, answer):
        from src.baselines.text.sembench_prepared import prepare, result_value
        from src.infrastructure.private_artifacts import json_metric
        from tests.baselines.text.replay_daft_sembench import replay

        source = original.OriginalSemBenchTextTests()
        source.setUp()
        self.addCleanup(source.doCleanups)
        query = source._query(scenario, key)
        texts = [answer] * len(query.operation.records)
        expected = (source._join_reference(query, texts)[0] if key == '5' else source._reference(query, texts)[0])
        tokenizer = SimpleNamespace(
            apply_chat_template=lambda messages, **kwargs: json.dumps(messages),
            encode=lambda text, **kwargs: list(text.encode()))
        _, calls, requests = prepare(query, tokenizer)
        frozen = dict(calls=calls, texts=texts, token_ids=[tokenizer.encode(row['prompt']) for row in requests],
            result=result_value(expected), quality=json_metric(query.evaluate(expected)))
        config = dict(schema='semloom.native_daft_sembench.v1', sembench_checkout=str(query.checkout),
            data_dir=str(source.data), scenario=scenario, query=key, query_id=query.query_id,
            model_name=query.model_name, model='CPU-software-tokenizer', sampling_params=query.generation.kwargs(),
            engine_kwargs={}, provider_options={}, query_timeout_s=30, cleanup_s=3, stop_grace_s=.5,
            max_model_calls=len(texts))
        with patch('transformers.AutoTokenizer.from_pretrained', return_value=tokenizer):
            result = replay(config, frozen, source.data/('replay-'+scenario+'-'+key))
        self.assertEqual(result['model_calls'], 0)
        self.assertEqual(result['requests'], len(texts))
        self.assertTrue(result['full_messages_and_tokens_equal'])

    def test_map_saved_occurrences_migrate_to_full_query_ids(self):
        self.replay('movie', '8', 'POSITIVE')

    def test_extract_saved_json_keeps_original_full_query_and_parser(self):
        self.replay('mmqa', '4', '{"genres":"comedy"}')

    def test_exact_join_saved_pair_occurrences_keep_original_sink(self):
        self.replay('movie', '5', 'True')



if __name__ == '__main__':
    unittest.main()
