"""Native quick ranking bounds and identity; all responses here are software."""
from copy import deepcopy
from contextlib import contextmanager
import importlib
import unittest
from unittest.mock import patch

import lotus
from lotus.types import LMOutput
import numpy as np
import pandas as pd

from src.baselines.text.sembench_text import _operation
from src.baselines.text.frameworks.lotus_native_topk import comparisons, make_lm, execute_native_topk
from src.serving.backends.vllm_protocol import GenerationSpec


INSTRUCTION = 'Which review shows the most positive sentiment about the movie? Review: {reviewText}'


def operation(frame):
    return _operation('topk', frame, (INSTRUCTION,),
        dict(K=len(frame), method='quick', return_stats=True), model_name='openai/local-qwen2.5-7b',
        generation=GenerationSpec(max_tokens=8192, stop=()), query_id='cpu-topk')


class NativeTopKTests(unittest.TestCase):
    def setUp(self):
        self.state = np.random.get_state()
        self.addCleanup(np.random.set_state, self.state)
        self.frame = pd.DataFrame(dict(reviewText=['same'] * 8, reviewId=list(range(8))))

    def test_original_quick_worst_case_is_28_independent_comparisons_in_seven_batches(self):
        op = operation(self.frame)
        lm = make_lm(op, 4)
        pairs, batch_sizes, messages = [], [], []

        def batch(source_pairs, invoke):
            pairs.extend(source_pairs)
            batch_sizes.append(len(source_pairs))
            return invoke()

        def answer(current, prompts, **kwargs):
            messages.extend(deepcopy(prompts))
            return LMOutput(outputs=['Document 1'] * len(prompts))

        with lotus.settings.context(lm=lm, enable_cache=False), patch.object(type(lm), '__call__', answer), \
                comparisons(op, lm, batch):
            ranked, stats = self.frame.sem_topk(INSTRUCTION, K=8, method='quick', return_stats=True)
        self.assertEqual(batch_sizes, [7, 6, 5, 4, 3, 2, 1])
        self.assertEqual(len(messages), stats['total_llm_calls'])
        self.assertEqual(len(set(tuple(sorted(pair)) for pair in pairs)), 28)
        self.assertEqual(sorted(ranked.reviewId), list(range(8)))
        self.assertEqual(op.describe()['expected_model_calls'], 28)
        self.assertEqual(len(set(str(m) for m in messages)), 1)  # identical text is not deduplicated

    def test_native_parser_fallback_is_preserved_without_comparison_retry(self):
        topk = importlib.import_module('lotus.sem_ops.sem_topk')
        lm = make_lm(operation(self.frame), 4)
        with patch.object(type(lm), '__call__', return_value=LMOutput(outputs=['not a number'])) as call:
            answer, _, _ = topk.compare_batch_binary([({'text': 'a'}, {'text': 'b'})], lm, 'Which?')
        self.assertEqual(answer, [True])
        self.assertEqual(call.call_count, 1)

    def test_native_quick_may_finish_below_the_bound_and_keeps_full_rank_order(self):
        op = operation(self.frame)
        lm = make_lm(op, 4)
        observed, answers = [], []

        def batch(source_pairs, invoke):
            observed.extend(source_pairs)
            answers[:] = ['Document 1' if left < right else 'Document 2' for left, right in source_pairs]
            return invoke()

        np.random.seed(0)  # Fixture reproducibility only; production retains the author's RNG.
        with lotus.settings.context(lm=lm, enable_cache=False), \
                patch.object(type(lm), '__call__', side_effect=lambda *a, **k: LMOutput(outputs=list(answers))), \
                comparisons(op, lm, batch):
            ranked, stats = self.frame.sem_topk(INSTRUCTION, K=8, method='quick', return_stats=True)
        self.assertEqual(list(ranked.reviewId), list(range(8)))
        self.assertEqual(stats['total_llm_calls'], len(observed))
        self.assertLess(len(observed), 28)

    def test_partial_ranking_or_different_algorithm_is_not_this_author_profile(self):
        for change in (dict(K=1, method='quick', return_stats=True),
                       dict(K=8, method='heap', return_stats=True),
                       dict(K=8, method='quick', return_stats=True, strategy='zs-cot')):
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, 'original Movie Q9'):
                _operation('topk', self.frame, (INSTRUCTION,), change,
                    model_name='openai/local', generation=GenerationSpec(), query_id='cpu')

    def test_incomplete_allowance_stops_before_native_model_start(self):
        from types import SimpleNamespace
        op = operation(self.frame)
        owner = SimpleNamespace(generation=op.generation)
        with self.assertRaisesRegex(ValueError, 'worst-case'):
            execute_native_topk(op, invoke=lambda: self.fail('accessor must not run'),
                shared_owner=owner, query_id='cpu', budget=SimpleNamespace(remaining=27))

    def test_primary_model_failure_survives_accounting_failure_and_keeps_pair_evidence(self):
        from types import SimpleNamespace
        op = operation(self.frame)
        lm = make_lm(op, 4)
        binding = SimpleNamespace(capture_prompts=lambda value: None,
            report=lambda: (_ for _ in ()).throw(RuntimeError('accounting failed')))

        @contextmanager
        def scope(*args, **kwargs):
            yield binding

        owner = SimpleNamespace(generation=op.generation, concurrency=4, start=lambda: None, query=scope)
        with patch('src.baselines.text.frameworks.lotus_native_topk.make_lm', return_value=lm), \
                patch.object(type(lm), '__call__', side_effect=ValueError('primary model failure')):
            with self.assertRaisesRegex(ValueError, 'primary model failure') as caught:
                execute_native_topk(op, invoke=lambda: self.frame.sem_topk(
                    INSTRUCTION, K=8, method='quick', return_stats=True), shared_owner=owner,
                    query_id='cpu', budget=SimpleNamespace(remaining=28))
        report = caught.exception.operator_report
        self.assertEqual(len(report['pairs']), 7)
        self.assertTrue(all('messages' in pair for pair in report['pairs']))
        self.assertEqual(report['accounting_error_type'], 'RuntimeError')
        self.assertEqual(report['status'], 'failed')


if __name__ == '__main__':
    unittest.main()
