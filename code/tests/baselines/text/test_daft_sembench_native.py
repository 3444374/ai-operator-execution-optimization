"""Real Daft CPU relations; native AI expression and SamplingParams are doubles."""

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.frameworks.daft_sembench_native import execute_prepared_map
from tests.baselines.text.native_sdk_fixtures import module_entries


class UnusedEngine:
    @classmethod
    def from_engine_args(cls, *args, **kwargs):
        raise AssertionError('literal CPU expression must not construct a model')


class NativeDaftTests(unittest.TestCase):
    def requests(self):
        return [dict(call_id=str(index),prompt='same complete prompt') for index in range(2)]

    def test_actual_native_ai_expression_builds_without_vllm_import_or_execution(self):
        import daft
        from daft.functions.ai import prompt

        with module_entries({'vllm': None}):
            expression = prompt(daft.col('prompt'), provider='vllm-prefix-caching',
                model='selected-model', engine_args={'dtype': 'bfloat16'}, generate_args={}, batch_size=17)
        self.assertIsInstance(expression, daft.Expression)

    def test_native_cpu_raw_predictions_and_provider_options_keep_occurrence_association(self):
        import daft

        sampling = dict(max_tokens=8192, temperature=0, stop=[])
        engines = dict(dtype='bfloat16')
        constructor = Mock(side_effect=lambda **kwargs: SimpleNamespace(**kwargs))
        with module_entries({'vllm': SimpleNamespace(SamplingParams=constructor, AsyncLLMEngine=UnusedEngine)}), \
                patch('daft.functions.ai.prompt', return_value=daft.lit('Reasoning:\nTrue\nAnswer: False')) as prompt:
            result = execute_prepared_map(self.requests(), model='selected-model',
                sampling_params=sampling, engine_kwargs=engines, provider_options={'batch_size': 17})
        self.assertEqual(len(result['predictions']), 2)
        self.assertEqual([row['call_id'] for row in result['predictions']],['0','1'])
        self.assertTrue(all(row['generated_text']=='Reasoning:\nTrue\nAnswer: False' for row in result['predictions']))
        self.assertNotIn('positive_review_cnt',result)
        self.assertEqual(prompt.call_args.kwargs['provider'], 'vllm-prefix-caching')
        self.assertEqual(prompt.call_args.kwargs['engine_args'], engines)
        self.assertEqual(prompt.call_args.kwargs['batch_size'], 17)
        constructor.assert_called_once_with(**sampling)
        self.assertEqual(sampling, dict(max_tokens=8192, temperature=0, stop=[]))
        self.assertEqual(result['metadata']['native_api']['generate_calls'],0)

    def test_ignored_num_gpus_and_unknown_options_are_rejected(self):
        for options in ({'num_gpus': 2}, {'native_threads': 1}, {'concurrency': 4},
                        {'gpus_per_actor': 2}, {'load_balance_threshold': 512}):
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, 'unsupported'):
                execute_prepared_map(self.requests(), model='selected-model',
                           sampling_params={}, provider_options=options)

    def test_model_expression_failure_is_not_retried_or_replaced(self):
        error = RuntimeError('native AI expression construction failed')
        with module_entries({'vllm': SimpleNamespace(SamplingParams=lambda **kwargs: kwargs,
                AsyncLLMEngine=UnusedEngine)}), \
                patch('daft.functions.ai.prompt', side_effect=error) as prompt:
            with self.assertRaises(RuntimeError) as caught:
                execute_prepared_map(self.requests(), model='selected-model',
                    sampling_params={})
        self.assertIs(caught.exception, error)
        prompt.assert_called_once()


if __name__ == '__main__':
    unittest.main()
