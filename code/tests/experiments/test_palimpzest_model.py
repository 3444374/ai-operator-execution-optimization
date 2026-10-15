"""Local model identity guards; no external service or model requests."""
from dataclasses import replace
import io
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.experiments.palimpzest.model import (
    LocalModelBinding, PZ_VLLM_MODEL, bind_local_completion, verify_model_metadata,
)
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError


def binding_fixture():
    binding = LocalModelBinding(FixedModelConfig('http://127.0.0.1:8000/v1/chat/completions',
                                                'Qwen2.5-7B-Instruct', 1000))
    metadata = dict(object='list', data=[dict(id=binding.model.model_id, object='model', owned_by='vllm')])
    return binding, metadata


class LocalModelTests(unittest.TestCase):
    def test_literal_served_model_and_same_api_base_are_required_before_original_call(self):
        binding, metadata = binding_fixture()
        original = Mock(return_value=SimpleNamespace(model=binding.model.model_id))
        completion = bind_local_completion(original, binding, verify_model_metadata(binding, metadata))
        kwargs = dict(model=PZ_VLLM_MODEL, api_base=binding.api_base,
                      messages=[dict(role='user', content='fixture')], temperature=0.0)
        for overrides in ({'model': 'gemini/gemini-2.5-flash'}, {'model': 'Qwen2.5-7B-Instruct'},
                          {'api_base': 'http://localhost:9999/v1'}, {'temperature': .5},
                          {'fallbacks': ['gemini-2.5-flash']}, {'api_key': 'undesired'}):
            with self.subTest(overrides=tuple(overrides)), self.assertRaisesRegex(ValueError, 'request profile'):
                completion(**dict(kwargs, **overrides))
        original.assert_not_called()
        response = completion(**kwargs)
        self.assertIs(response, original.return_value)
        actual = original.call_args.kwargs
        self.assertEqual(actual['model'], 'hosted_vllm/Qwen2.5-7B-Instruct')
        self.assertIs(actual['messages'], kwargs['messages'])
        self.assertEqual(actual['api_base'], binding.api_base)
        self.assertEqual(actual['num_retries'], 0)
        self.assertEqual(actual['fallbacks'], [])
        self.assertEqual(actual['timeout'], 1)

    def test_metadata_mismatch_duplicate_alias_or_wrong_owner_cannot_authorize_a_model(self):
        binding, metadata = binding_fixture()
        for value in ({}, dict(object='list', data=[]),
                      dict(object='list', data=[dict(id='Qwen2.5-7B', object='model', owned_by='vllm')]),
                      dict(object='list', data=[dict(id=binding.model.model_id, object='model', owned_by='other')]),
                      dict(object='list', data=metadata['data']*2)):
            with self.subTest(metadata=value), self.assertRaises(ValueError):
                verify_model_metadata(binding, value)
        verified = verify_model_metadata(binding, metadata)
        other = LocalModelBinding(replace(binding.model, model_id='another-local-model'))
        with self.assertRaisesRegex(ValueError, 'matching verified'):
            bind_local_completion(Mock(), other, verified)
        self.assertIsNone(verified.summary()['monetary_cost_usd'])
        self.assertFalse(verified.summary()['upstream_model_card_is_actual_price'])

    def test_loopback_binding_rejects_remote_query_redirect_and_provider_names(self):
        binding, _ = binding_fixture()
        for endpoint in ('https://127.0.0.1:8000/v1/chat/completions',
                         'http://service.invalid/v1/chat/completions',
                         'http://127.0.0.1:8000/chat/completions',
                         binding.model.endpoint_url+'?model=other'):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                LocalModelBinding(replace(binding.model, endpoint_url=endpoint))
        with self.assertRaisesRegex(ValueError, 'without a provider'):
            LocalModelBinding(replace(binding.model, model_id='openai/model'))

    def test_completion_response_must_retain_exact_verified_model_identity(self):
        binding, metadata = binding_fixture()
        completion = bind_local_completion(Mock(return_value=SimpleNamespace(model='different')),
                                            binding, verify_model_metadata(binding, metadata))
        with self.assertRaisesRegex(PalimpzestAdapterError, 'response model differs'):
            completion(model=PZ_VLLM_MODEL, api_base=binding.api_base,
                       messages=[dict(role='user', content='fixture')], temperature=0)


@unittest.skipUnless(os.environ.get('SEMLOOM_PALIMPZEST_TEST') == '1',
                     'requires pinned original Generator; no HTTP or model requests')
class LocalModelGeneratorTests(unittest.TestCase):
    def test_response_identity_error_escapes_original_default_but_provider_error_keeps_it(self):
        from src.experiments.palimpzest.ledger import PalimpzestLedger
        from src.experiments.palimpzest.source import load_palimpzest
        from src.semantic_methods.palimpzest import completion_bridge
        pz = load_palimpzest(Path(os.environ['SEMLOOM_TEST_PALIMPZEST_ROOT']))
        from palimpzest.constants import PromptStrategy
        from palimpzest.core.elements.records import DataRecord
        from palimpzest.query.generators.generators import Generator
        from pydantic.fields import FieldInfo
        source = pz.MemoryDataset('contract-fixture', [{'text': 'a deterministic fixture review'}])
        candidate = DataRecord(source.schema, source_indices=['contract-fixture-0'])
        candidate.text = 'a deterministic fixture review'
        binding, metadata = binding_fixture()
        verified = verify_model_metadata(binding, metadata)
        generator = Generator(pz.Model.VLLM_QWEN_1_5_0_5B_CHAT, PromptStrategy.COT_BOOL,
                              api_base=binding.api_base)
        fields = {'passed_operator': FieldInfo(annotation=bool)}
        arguments = dict(filter_condition='the review is positive', project_cols=['text'])
        ledger = PalimpzestLedger(max_calls=1)
        wrong = bind_local_completion(Mock(return_value=SimpleNamespace(model='unknown-response-model')),
                                      binding, verified)
        with patch('sys.stdout', new_callable=io.StringIO), \
                completion_bridge(completion=wrong, observer=ledger) as bridge:
            with self.assertRaisesRegex(PalimpzestAdapterError, 'response model differs'):
                generator(candidate, fields, **arguments)
        self.assertEqual(ledger.snapshot()['completion_failed'], 1)
        self.assertEqual(ledger.snapshot()['generator_failed'], 1)
        self.assertTrue(bridge.snapshot()['patches_restored'])
        provider_failure = TimeoutError('ordinary provider failure fixture')
        ordinary = bind_local_completion(Mock(side_effect=provider_failure), binding, verified)
        provider_ledger = PalimpzestLedger(max_calls=1)
        with patch('sys.stdout', new_callable=io.StringIO), \
                completion_bridge(completion=ordinary, observer=provider_ledger):
            result = generator(candidate, fields, **arguments)
        self.assertEqual(result[0], {'passed_operator': False})
        self.assertEqual(provider_ledger.snapshot()['completion_failed'], 1)
        self.assertEqual(provider_ledger.snapshot()['generator_failed'], 0)
