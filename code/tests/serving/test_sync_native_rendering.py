"""Rendered-input checks and native baseline separation; explicit CPU API stubs."""
from contextlib import contextmanager
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.semantic_methods.lotus.records import identity
from src.serving.backends import vllm_engine as sync
from tests.serving.test_sync_model_context import native_api


def output(index, tokens):
    return SimpleNamespace(request_id='CPU-request-'+str(index), finished=True,
        prompt_token_ids=tokens, outputs=[SimpleNamespace(index=0, text='fixed', token_ids=[3], finish_reason='stop')])


def rows(count=1):
    return [dict(identity('CPU-render-query', 'row-'+str(i), i),
        messages_json=json.dumps([dict(role='user', content='complete '+str(i))])) for i in range(count)]


@contextmanager
def kernel(tokens, *, maximum=64, reference=False):
    if reference:
        from src.baselines.text.frameworks.sync_reference_engine import ReferenceVllmEngine
        engine_type = ReferenceVllmEngine
    else:
        engine_type = sync.VllmEngine
    spec = sync.EngineSpec('/tmp/CPU-render-model', '/tmp/model-python', '/tmp/driver-python',
        max_model_len=maximum if reference else None)
    with native_api(actual=maximum):
        owner = engine_type(spec, sync.GenerationSpec(max_tokens=1))
        llm = owner._llm
        llm._preprocess_chat = Mock(return_value=[dict(type='token', prompt_token_ids=t) for t in tokens])
        llm._render_and_add_requests = Mock()
        llm.wait_for_completion = Mock(return_value=[output(i,t) for i,t in enumerate(tokens)])
        llm.chat = Mock(return_value=[output(i,t) for i,t in enumerate(tokens)])
        try:
            yield owner
        finally:
            owner.close(timeout=1)


class SyncNativeRenderingTests(unittest.TestCase):
    def test_exact_total_length_is_allowed_and_native_rendering_occurs_once(self):
        with kernel([list(range(60))]) as owner:
            result = owner.run_prepared(rows(), generation=sync.GenerationSpec(max_tokens=4))
            self.assertEqual(result[0]['status'], 'succeeded')
            self.assertEqual(owner._llm._preprocess_chat.call_count, 1)
            self.assertEqual(owner._llm._render_and_add_requests.call_count, 1)
            submitted = list(owner._llm._render_and_add_requests.call_args.kwargs['prompts'])
            self.assertEqual(submitted[0]['prompt_token_ids'], list(range(60)))
            self.assertEqual(owner._reserved_model_calls, 1)
            owner._llm.chat.assert_not_called()

    def test_one_token_over_is_known_unsubmitted_without_spending_a_call(self):
        with kernel([list(range(61))]) as owner:
            result = owner.run_prepared(rows(), generation=sync.GenerationSpec(max_tokens=4))
            self.assertEqual(result[0]['status'], 'failed')
            self.assertEqual(result[0]['error_type'], 'ModelContextLengthExceeded')
            self.assertFalse(result[0]['submission_confirmed'])
            self.assertEqual(owner._reserved_model_calls, 0)
            owner._llm._render_and_add_requests.assert_not_called()
            owner._llm.wait_for_completion.assert_not_called()

    def test_later_oversize_member_cannot_enqueue_an_earlier_valid_member(self):
        with kernel([[1,2], list(range(61))]) as owner:
            result = owner.run_prepared(rows(2), generation=sync.GenerationSpec(max_tokens=4))
            self.assertEqual([r['status'] for r in result], ['failed', 'failed'])
            self.assertEqual(len({r['call_id'] for r in result}), 2)
            self.assertEqual(owner._reserved_model_calls, 0)
            owner._llm._render_and_add_requests.assert_not_called()

    def test_unavailable_rendering_is_known_unsubmitted(self):
        for prompts in ([], [dict(type='embeds')], [dict(type='token',prompt_token_ids=[])],
                        [dict(type='token',prompt_token_ids=[True])]):
            with self.subTest(prompts=prompts), kernel([[1,2]]) as owner:
                owner._llm._preprocess_chat.return_value = prompts
                result = owner.run_prepared(rows())
                self.assertEqual(result[0]['status'], 'failed')
                self.assertFalse(result[0]['submission_confirmed'])
                self.assertEqual(owner._reserved_model_calls, 0)
                owner._llm._render_and_add_requests.assert_not_called()

    def test_native_renderer_exception_does_not_reserve_or_submit(self):
        with kernel([[1,2]]) as owner:
            owner._llm._preprocess_chat.side_effect = RuntimeError('CPU template error')
            result = owner.run_prepared(rows())
            self.assertEqual(result[0]['error_type'], 'RuntimeError')
            self.assertEqual(result[0]['status'], 'failed')
            self.assertEqual(owner._reserved_model_calls, 0)
            owner._llm._render_and_add_requests.assert_not_called()

    def test_native_submission_exception_retains_unknown_and_call_reservations(self):
        with kernel([[1,2], [3,4]]) as owner:
            owner._llm._render_and_add_requests.side_effect = RuntimeError('CPU enqueue outcome unknown')
            result = owner.run_prepared(rows(2))
            self.assertEqual([r['status'] for r in result], ['unknown', 'unknown'])
            self.assertEqual(owner._reserved_model_calls, 2)
            self.assertTrue(all(r['native_submission_invoked'] and not r['chat_invoked'] for r in result))
            owner._llm.wait_for_completion.assert_not_called()

    def test_returned_prompt_mismatch_keeps_observed_usage_and_fails(self):
        with kernel([[1,2]]) as owner:
            owner._llm.wait_for_completion.return_value = [output(0,[999])]
            result = owner.run_prepared(rows())
            self.assertEqual(result[0]['error_type'], 'PromptTokenMismatch')
            self.assertEqual(result[0]['status'], 'failed')
            self.assertEqual(result[0]['prompt_tokens'], 1)
            self.assertEqual(result[0]['completion_tokens'], 1)
            self.assertEqual(owner._reserved_model_calls, 1)

    def test_native_reference_keeps_original_chat_API_and_messages(self):
        with kernel([[1,2]], reference=True) as owner:
            result = owner.run_prepared(rows(), generation=sync.GenerationSpec(max_tokens=4))
            self.assertEqual(result[0]['status'], 'succeeded')
            self.assertEqual(owner._llm.chat.call_args.args[0],
                [json.loads(row['messages_json']) for row in rows()])
            owner._llm._preprocess_chat.assert_not_called()
            owner._llm._render_and_add_requests.assert_not_called()
            owner._llm.wait_for_completion.assert_not_called()

    def test_deadline_after_rendering_prevents_submission(self):
        with kernel([[1,2]]) as owner, patch.object(sync.time, 'monotonic', side_effect=(10.,12.)):
            result = owner.run_prepared(rows(), deadline_monotonic=11.)
            self.assertEqual(result[0]['error_type'], 'DeadlineExceeded')
            self.assertTrue(result[0]['deadline_expired'])
            self.assertEqual(owner._reserved_model_calls, 0)
            owner._llm._render_and_add_requests.assert_not_called()


if __name__ == '__main__':
    unittest.main()
