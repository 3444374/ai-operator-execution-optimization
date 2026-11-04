"""Every synchronous call checks its actual owner context; native CPU API stub."""
from contextlib import contextmanager
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from src.semantic_methods.lotus.records import identity
from src.serving.backends import vllm_engine as sync
from tests.serving import test_sync_model_context as fixture


@contextmanager
def kernel(actual, *, explicit=None, model_config=True, max_calls=None):
    spec = sync.EngineSpec('/tmp/CPU-sync-generation', '/tmp/model-python', '/tmp/driver-python',
        max_model_len=explicit, max_model_calls=max_calls)
    with fixture.native_api(actual=actual, model_config=model_config):
        owner = sync.VllmEngine(spec, sync.GenerationSpec(max_tokens=1))
        output = SimpleNamespace(request_id='CPU-request', finished=True, prompt_token_ids=[1, 2],
            outputs=[SimpleNamespace(index=0, text='fixed', token_ids=[3], finish_reason='stop')])
        owner._llm._preprocess_chat = Mock(side_effect=lambda conversations:
            [dict(type='token', prompt_token_ids=[1, 2]) for _ in conversations])
        owner._llm._render_and_add_requests = Mock()
        owner._llm.wait_for_completion = Mock(return_value=[output])
        try:
            yield owner
        finally:
            owner.close(timeout=1)


def rows():
    return [dict(identity('CPU-query', 'row', 0),
        messages_json=json.dumps([dict(role='user', content='complete')]))]


class SyncGenerationContextTests(unittest.TestCase):
    def test_actual_large_context_accepts_a_stage_output_above_the_legacy_value(self):
        with kernel(8192) as owner:
            result = owner.run_prepared(rows(), generation=sync.GenerationSpec(max_tokens=4097))
            self.assertEqual(result[0]['status'], 'succeeded')
            self.assertEqual(owner._llm._render_and_add_requests.call_args.kwargs['params'][0]['max_tokens'], 4097)
            self.assertEqual(owner._reserved_model_calls, 1)
            self.assertEqual(owner.ready()['model_context']['max_model_len'], 8192)

    def test_actual_small_context_rejects_before_native_chat_or_call_reservation(self):
        with kernel(64) as owner:
            with self.assertRaisesRegex(ValueError, 'resolved model context'):
                owner.run_prepared(rows(), generation=sync.GenerationSpec(max_tokens=65))
            owner._llm._render_and_add_requests.assert_not_called()
            self.assertEqual(owner._reserved_model_calls, 0)

    def test_readiness_and_later_calls_use_the_same_actual_context(self):
        with kernel(64) as owner:
            observed = owner.ready()['model_context']
            with self.assertRaisesRegex(ValueError, 'resolved model context'):
                owner.run_prepared(rows(), generation=sync.GenerationSpec(max_tokens=65))
            self.assertEqual(owner.ready()['model_context'], observed)
            owner._llm._render_and_add_requests.assert_not_called()

    def test_missing_or_invalid_context_is_never_treated_as_a_legacy_allowance(self):
        for actual in (None, 0, True, 64., '64'):
            with self.subTest(actual=actual), kernel(actual) as owner:
                with self.assertRaisesRegex((RuntimeError, ValueError), 'native model context'):
                    owner.run_prepared(rows())
                owner._llm._render_and_add_requests.assert_not_called()
                self.assertEqual(owner._reserved_model_calls, 0)
        with kernel(64, model_config=False) as owner:
            with self.assertRaisesRegex(RuntimeError, 'native model context'):
                owner.run_prepared(rows())
            owner._llm._render_and_add_requests.assert_not_called()

    def test_explicit_context_mismatch_is_rejected_without_a_prior_ready_call(self):
        with kernel(128, explicit=64) as owner:
            with self.assertRaisesRegex(ValueError, 'differs from explicit'):
                owner.run_prepared(rows())
            owner._llm._render_and_add_requests.assert_not_called()
            self.assertEqual(owner._reserved_model_calls, 0)

    def test_context_change_after_ready_rejects_the_next_call(self):
        with kernel(64) as owner:
            owner.ready()
            owner._llm.model_config.max_model_len = 128
            with self.assertRaisesRegex(RuntimeError, 'changed during this engine lifetime'):
                owner.run_prepared(rows())
            owner._llm._render_and_add_requests.assert_not_called()
            self.assertEqual(owner._reserved_model_calls, 0)

    def test_rejected_stage_does_not_spend_the_next_valid_call(self):
        with kernel(64, max_calls=1) as owner:
            with self.assertRaises(ValueError):
                owner.run_prepared(rows(), generation=sync.GenerationSpec(max_tokens=65))
            result = owner.run_prepared(rows(), generation=sync.GenerationSpec(max_tokens=4))
            self.assertEqual(result[0]['status'], 'succeeded')
            self.assertEqual(owner._reserved_model_calls, 1)
            with self.assertRaisesRegex(RuntimeError, 'max_model_calls exceeded'):
                owner.run_prepared(rows())
            self.assertEqual(owner._llm._render_and_add_requests.call_count, 1)

    def test_closed_uncertain_and_busy_states_still_reject_without_chat(self):
        for state in ('closed', 'uncertain', 'busy'):
            with self.subTest(state=state), kernel(64) as owner:
                if state == 'closed':
                    owner.close(timeout=1)
                elif state == 'uncertain':
                    owner._shutdown_failed = True
                else:
                    owner._lock.acquire()
                try:
                    with self.assertRaises(RuntimeError):
                        owner.run_prepared(rows())
                    owner._llm._render_and_add_requests.assert_not_called()
                finally:
                    if state == 'busy':
                        owner._lock.release()
                    if state == 'uncertain':
                        owner._shutdown_failed = False


if __name__ == '__main__':
    unittest.main()
