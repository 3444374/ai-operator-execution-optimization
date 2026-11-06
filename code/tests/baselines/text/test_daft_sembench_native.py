"""Real Daft CPU relations; native AI expression and SamplingParams are doubles."""

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.text.frameworks.daft_sembench_native import execute_q3
from src.baselines.text.sembench_movie_native import q3_messages
from tests.baselines.text.native_sdk_fixtures import module_entries


class UnusedEngine:
    @classmethod
    def from_engine_args(cls, *args, **kwargs):
        raise AssertionError('literal CPU expression must not construct a model')


class NativeDaftTests(unittest.TestCase):
    def rows(self):
        return [dict(occurrence_id=f'occurrence-{position}', reviewId='duplicate', id=movie,
                     reviewText=text) for position, (movie, text) in enumerate([
                         ('other', 'not sent'), ('taken_3', 'first'), ('taken_3', 'second')])]

    def test_actual_native_ai_expression_builds_without_vllm_import_or_execution(self):
        import daft
        from daft.functions.ai import prompt

        with module_entries({'vllm': None}):
            expression = prompt(daft.col('prompt'), provider='vllm-prefix-caching',
                model='selected-model', engine_args={'dtype': 'bfloat16'}, generate_args={}, batch_size=17)
        self.assertIsInstance(expression, daft.Expression)

    def test_native_cpu_filter_count_and_provider_options_keep_occurrence_association(self):
        import daft

        tokenizer = SimpleNamespace(apply_chat_template=Mock(side_effect=lambda messages, **kwargs: str(messages)))
        sampling = dict(max_tokens=8192, temperature=0, stop=[])
        engines = dict(dtype='bfloat16')
        constructor = Mock(side_effect=lambda **kwargs: SimpleNamespace(**kwargs))
        with module_entries({'vllm': SimpleNamespace(SamplingParams=constructor, AsyncLLMEngine=UnusedEngine)}), \
                patch('daft.functions.ai.prompt', return_value=daft.lit('Reasoning:\nTrue\nAnswer: False')) as prompt:
            result = execute_q3(self.rows(), model='selected-model', tokenizer=tokenizer,
                sampling_params=sampling, engine_kwargs=engines, provider_options={'batch_size': 17})
        self.assertEqual(len(result['predictions']), 2)
        self.assertEqual({row['occurrence_id'] for row in result['predictions']}, {'occurrence-1', 'occurrence-2'})
        self.assertEqual(result['positive_review_cnt'], 0)
        self.assertTrue(all(row['decision'] is False for row in result['predictions']))
        self.assertEqual(tokenizer.apply_chat_template.call_count, 2)
        self.assertEqual(tokenizer.apply_chat_template.call_args_list[0].args, (q3_messages('first'),))
        self.assertEqual(tokenizer.apply_chat_template.call_args_list[0].kwargs,
                         dict(tokenize=False, add_generation_prompt=True))
        self.assertEqual(prompt.call_args.kwargs['provider'], 'vllm-prefix-caching')
        self.assertEqual(prompt.call_args.kwargs['engine_args'], engines)
        self.assertEqual(prompt.call_args.kwargs['batch_size'], 17)
        constructor.assert_called_once_with(**sampling)
        self.assertEqual(sampling, dict(max_tokens=8192, temperature=0, stop=[]))
        self.assertIsNone(result['metadata']['resolved_engine_capacity'])

    def test_empty_eligibility_does_not_construct_framework_or_model(self):
        with module_entries({'daft': None, 'vllm': None}):
            for rows in ([], [self.rows()[0]]):
                result = execute_q3(rows, model='selected-model', tokenizer=None, sampling_params={})
                self.assertEqual(result['predictions'], [])
                self.assertEqual(result['positive_review_cnt'], 0)

    def test_ignored_num_gpus_and_unknown_options_are_rejected(self):
        for options in ({'num_gpus': 2}, {'native_threads': 1}, {'concurrency': 4},
                        {'gpus_per_actor': 2}, {'load_balance_threshold': 512}):
            with self.subTest(options=options), self.assertRaisesRegex(ValueError, 'unsupported'):
                execute_q3(self.rows(), model='selected-model', tokenizer=None,
                           sampling_params={}, provider_options=options)

    def test_model_expression_failure_is_not_retried_or_replaced(self):
        error = RuntimeError('native AI expression construction failed')
        with module_entries({'vllm': SimpleNamespace(SamplingParams=lambda **kwargs: kwargs,
                AsyncLLMEngine=UnusedEngine)}), \
                patch('daft.functions.ai.prompt', side_effect=error) as prompt:
            with self.assertRaises(RuntimeError) as caught:
                execute_q3(self.rows(), model='selected-model',
                    tokenizer=SimpleNamespace(apply_chat_template=lambda *args, **kwargs: 'prompt'),
                    sampling_params={})
        self.assertIs(caught.exception, error)
        prompt.assert_called_once()


if __name__ == '__main__':
    unittest.main()
