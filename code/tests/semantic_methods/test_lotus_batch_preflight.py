"""Whole image-batch rejection before Core flow creation, without SDK or HTTP."""

from contextlib import contextmanager
import json
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch
import weakref

from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import NativeTaskSession
from src.scheduling.core.session_contract import Usage
from src.semantic_methods.lotus.batch import LotusBatchExecutor
from src.semantic_methods.lotus.limits import LotusCallLimits
from src.semantic_methods.lotus.sdk import IMAGE_TEXT_PROFILE, PreparedLotusCall, check_model_config, validate_batch
from tests.execution_provider.test_native_tasks import fixture


def rows(profile=IMAGE_TEXT_PROFILE):
    return [([dict(role='user', content=str(index) if profile == 'text' else [
        dict(type='text', text=str(index)),
        dict(type='image_url', image_url=dict(url='data:image/png;base64,AA=='))])], None)
        for index in range(6)]


def payload(messages, *, model='fixture', padding=''):
    return json.dumps(dict(model=model, messages=messages, padding=padding)).encode()


class LotusBatchPreflightTests(unittest.TestCase):
    @contextmanager
    def batch(self, *, held, fault=None, profile=IMAGE_TEXT_PROFILE, prevalidate=False):
        execution, backend, _ = fixture(held_tasks=held, offer_tasks=held,
            input_bytes=held * 1024, result_bytes=held * 512,
            active_requests=held, active_work=held, metadata_bytes=held * 512)
        model = FixedModelConfig('http://localhost/v1/chat/completions', 'fixture', 1000, 'fixture')
        data = rows(profile)
        normal_bytes = len(payload(data[0][0]))
        limits = LotusCallLimits(1., max_request_bytes=normal_bytes,
            max_response_bytes=512, max_batch_input_bytes=normal_bytes * (4 if fault == 'batch' else 6))
        prepared, sent, parsed = [], [], []

        def prepare(lm, messages, kwargs, **options):
            index = int(messages[0]['content'] if profile == 'text' else messages[0]['content'][0]['text'])
            call = PreparedLotusCall(payload(messages,
                model='foreign' if fault == 'model' and index == 4 else 'fixture',
                padding='oversize' if fault == 'request' and index == 4 else ''),
                'http://localhost/v1', 'fixture', 1., profile)
            prepared.append(index)
            return call

        class Flow(NativeTaskSession):
            def wait(self, progress, timeout_s=None):
                for key in tuple(backend.pending):
                    sent.append(backend.pending[key][1].task.payload)
                    backend.complete(key, encode_full_response(FullModelResponse(200, (), b'{}')))

        def parse(lm, messages, kwargs, call, response, **options):
            parsed.append(call.payload)
            return SimpleNamespace(content=messages)

        openai = ModuleType('openai')
        openai.OpenAIError = type('OpenAIError', (Exception,), {})
        selected = LotusBatchExecutor(execution, model, query_id='preflight', operator_id='map',
            call_limits=limits, message_profile=profile, prevalidate_batch=prevalidate)
        with patch.dict('sys.modules', {'openai': openai}), \
             patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=prepare), \
             patch('src.semantic_methods.lotus.batch.prepare_call', side_effect=prepare), \
             patch('src.semantic_methods.lotus.batch.NativeTaskSession', side_effect=Flow) as opening, \
             patch('src.semantic_methods.lotus.batch.parse_lotus_response', side_effect=parse):
            yield selected, data, execution, backend, opening, prepared, sent, parsed

    def rejects_before_flow(self, fault, error):
        for held in (2, 4):
            with self.subTest(held=held), self.batch(held=held, fault=fault) as values:
                selected, data, execution, backend, opening, prepared, sent, parsed = values
                with self.assertRaisesRegex(ValueError, error):
                    selected(None, data, {}, False, 'fixture')
                opening.assert_not_called()
                self.assertEqual(backend.routes, [])
                self.assertEqual(sent, [])
                self.assertEqual(parsed, [])
                self.assertEqual(execution.engine.jobs.jobs, {})
                self.assertEqual(execution.engine.capacity.usage(), Usage())

    def test_fifth_image_request_over_capacity_rejects_before_flow(self):
        self.rejects_before_flow('request', 'request exceeds')

    def test_image_batch_crossing_input_capacity_at_fifth_row_rejects_before_flow(self):
        self.rejects_before_flow('batch', 'batch input exceeds')

    def test_fifth_image_call_with_foreign_model_rejects_before_flow(self):
        self.rejects_before_flow('model', 'model configuration')

    def test_cancel_or_deadline_during_image_guards_stops_before_flow(self):
        for phase in ('model', 'capacity'):
            for expired in (False, True):
                with self.subTest(phase=phase, expired=expired), self.batch(held=2) as values:
                    selected, data, execution, backend, opening, prepared, sent, parsed = values
                    state = dict(cancelled=False, now=0.)
                    selected.cancelled = lambda: state['cancelled']
                    actual = check_model_config if phase == 'model' else LotusCallLimits.check_prepared
                    target = ('src.semantic_methods.lotus.sdk.check_model_config' if phase == 'model'
                              else 'src.semantic_methods.lotus.limits.LotusCallLimits.check_prepared')
                    def checking(*args):
                        actual(*args)
                        call = args[0] if phase == 'model' else args[1]
                        if json.loads(call.payload)['messages'][0]['content'][0]['text'] == '4':
                            state['now'] = 121. if expired else 0.
                            state['cancelled'] = not expired
                    with patch(target, autospec=True, side_effect=checking), \
                         patch('src.semantic_methods.lotus.batch.time',
                               SimpleNamespace(monotonic=lambda: state['now'])):
                        with self.assertRaises(TimeoutError if expired else RuntimeError):
                            selected(None, data, {}, False, 'fixture')
                    opening.assert_not_called()
                    self.assertEqual(backend.routes, [])
                    self.assertEqual(execution.engine.capacity.usage(), Usage())
                    self.assertEqual(execution.engine.jobs.jobs, {})

    def test_whole_preflight_retains_only_first_call(self):
        data, references, live_suffixes = rows(), [], []
        def prepare(lm, messages, kwargs, **options):
            live_suffixes.append(sum(reference() is not None for reference in references[1:]))
            call = PreparedLotusCall(payload(messages), 'http://localhost/v1', 'fixture', 1., IMAGE_TEXT_PROFILE)
            references.append(weakref.ref(call))
            return call
        limits = LotusCallLimits(1., max_request_bytes=1024)
        model = FixedModelConfig('http://localhost/v1/chat/completions', 'fixture', 1000, 'fixture')
        with patch('src.semantic_methods.lotus.sdk.prepare_call', side_effect=prepare):
            first = validate_batch(None, data, {}, message_profile=IMAGE_TEXT_PROFILE,
                                   model_config=model, call_limits=limits)
        self.assertIs(references[0](), first)
        self.assertEqual(live_suffixes, [0] * len(data))
        self.assertTrue(all(reference() is None for reference in references[1:]))

    def test_valid_image_batch_preserves_dispatched_calls_and_consumed_order(self):
        with self.batch(held=2) as values:
            selected, data, execution, backend, opening, prepared, sent, parsed = values
            results = selected(None, data, {}, False, 'fixture')
            self.assertEqual([result.content for result in results], [item[0] for item in data])
            self.assertEqual(sent, [payload(item[0]) for item in data])
            self.assertEqual(parsed, sent)
            self.assertEqual(prepared, list(range(6)) + list(range(1, 6)))
            self.assertEqual(opening.call_count, 1)
            self.assertEqual(execution.engine.capacity.usage(), Usage())
            self.assertEqual(execution.engine.jobs.jobs, {})

    def test_text_default_and_explicit_light_prevalidation_keep_one_preparation_per_row(self):
        for prevalidate in (False, True):
            with self.subTest(prevalidate=prevalidate), self.batch(
                    held=2, profile='text', prevalidate=prevalidate) as values:
                selected, data, execution, backend, opening, prepared, sent, parsed = values
                selected(None, data, {}, False, 'fixture')
                self.assertEqual(prepared, list(range(6)))
                self.assertEqual(sent, [payload(item[0]) for item in data])
                self.assertEqual(parsed, sent)
                self.assertEqual(execution.engine.capacity.usage(), Usage())


if __name__ == '__main__':
    unittest.main()
