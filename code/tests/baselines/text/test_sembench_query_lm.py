"""Original SDK kwargs at the native prediction seam, without a model."""

from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import lotus
import pandas as pd

from src.baselines.text.sembench_prepared import make_query_lm
from src.baselines.text.frameworks.ray_data_sembench_native import _sampling_params
from src.serving.backends.vllm_protocol import GenerationSpec


class OriginalQueryLMParameterTests(unittest.TestCase):
    def model(self, generation, texts):
        observed = []

        def predict(requests, *, sampling_params):
            native = _sampling_params(sampling_params)
            observed.append(deepcopy(native))
            return dict(predictions=[dict(call_id=row['call_id'], generated_text=text)
                for row, text in reversed(list(zip(requests, texts, strict=True)))])

        query = SimpleNamespace(query_id='original-kwargs', model_name='openai/CPU-only', generation=generation)
        lm, batches = make_query_lm(query, predict)
        return lm, batches, observed

    def test_original_filter_false_disables_native_logprobs_and_keeps_sdk_kwargs(self):
        lm, batches, observed = self.model(GenerationSpec(max_tokens=128, stop=()), ['True', 'False'])
        frame = pd.DataFrame({'text': ['same text', 'same text']}, index=[7, 7])
        with patch('socket.socket.connect', side_effect=AssertionError('fixture attempted network')), \
                lotus.settings.context(lm=lm, enable_cache=False):
            result = frame.sem_filter('Keep {text}')
        pd.testing.assert_frame_equal(result, frame.iloc[[0]])
        self.assertIsNone(observed[0]['logprobs'])
        self.assertIsNone(batches[0]['sampling_params']['logprobs'])
        self.assertIs(batches[0]['operator_kwargs']['logprobs'], False)
        self.assertEqual(batches[0]['status'], 'completed')

    def test_explicit_json_generation_and_original_extract_share_one_constraint(self):
        lm, batches, observed = self.model(GenerationSpec(max_tokens=128, stop=(),
            response_format={'type': 'json_object'}), ['{"color":"Blue"}', '{"color":"Red"}'])
        frame = pd.DataFrame({'text': ['one item', 'another item']}, index=[4, 9])
        with patch('socket.socket.connect', side_effect=AssertionError('fixture attempted network')), \
                lotus.settings.context(lm=lm, enable_cache=False):
            result = frame.sem_extract(['text'], {'color': 'the item color'})
        self.assertEqual(list(result['color']), ['Blue', 'Red'])
        self.assertEqual(list(result.index), [4, 9])
        self.assertEqual(observed[0]['structured_outputs'], {'json_object': True})
        self.assertNotIn('response_format', batches[0]['sampling_params'])
        self.assertEqual(batches[0]['operator_kwargs'], {'response_format': {'type': 'json_object'}})
        self.assertEqual(batches[0]['status'], 'completed')

    def test_numeric_zero_logprobs_remains_an_explicit_native_request(self):
        lm, batches, observed = self.model(GenerationSpec(logprobs=0), ['raw'])
        with patch('socket.socket.connect', side_effect=AssertionError('fixture attempted network')):
            result = lm([[{'role': 'user', 'content': 'same input'}]])
        self.assertEqual(result.outputs, ['raw'])
        self.assertEqual(observed[0]['logprobs'], 0)
        self.assertIs(type(observed[0]['logprobs']), int)
        self.assertEqual(batches[0]['operator_kwargs'], {})

    def test_conflicting_constraints_still_fail_and_preserve_original_kwargs(self):
        lm, batches, observed = self.model(GenerationSpec(), ['unused'])
        structured = {'regex': 'True|False'}
        with patch('socket.socket.connect', side_effect=AssertionError('fixture attempted network')), \
                self.assertRaisesRegex(ValueError, 'unambiguous JSON object'):
            lm([[{'role': 'user', 'content': 'same input'}]],
               response_format={'type': 'json_object'}, structured_outputs=structured)
        structured['regex'] = 'changed after failure'
        self.assertEqual(observed, [])
        self.assertEqual(batches[0]['status'], 'failed')
        self.assertEqual(batches[0]['operator_kwargs'], {
            'response_format': {'type': 'json_object'}, 'structured_outputs': {'regex': 'True|False'}})


if __name__ == '__main__':
    unittest.main()
