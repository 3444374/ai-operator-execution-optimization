"""Local model identity guards; no external service or model requests."""
import base64
from copy import deepcopy
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.execution_provider.adapters.model_config import FixedModelConfig
from src.experiments.palimpzest.model import (
    LocalModelBinding, PZ_VLLM_MODEL, bind_local_completion, fetch_model_metadata, verify_model_metadata,
)
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError


def binding_fixture():
    binding = LocalModelBinding(FixedModelConfig('http://127.0.0.1:8000/v1/chat/completions',
                                                'Qwen2.5-7B-Instruct', 1000))
    metadata = dict(object='list', data=[dict(id=binding.model.model_id, object='model', owned_by='vllm')])
    return binding, metadata


def model_card_fixture():
    binding, metadata = binding_fixture()
    metadata['data'][0].update(created=100, root='/fixture/checkpoint', parent=None, max_model_len=8192,
        permission=[dict(id='modelperm-fixture-first', object='model_permission', created=100,
            allow_create_engine=False, allow_sampling=True, allow_logprobs=True, allow_search_indices=False,
            allow_view=True, allow_fine_tuning=False, organization='*', group=None, is_blocking=False)])
    return binding, metadata


class LocalModelTests(unittest.TestCase):
    def test_only_three_response_generated_values_leave_stable_identity_unchanged(self):
        binding, metadata = model_card_fixture()
        before = deepcopy(metadata)
        first = verify_model_metadata(binding, metadata)
        for path, value in ((('created',), 101), (('permission', 0, 'id'), 'modelperm-fixture-next'),
                            (('permission', 0, 'created'), 101)):
            changed = deepcopy(metadata)
            target = changed['data'][0]
            for step in path[:-1]:
                target = target[step]
            target[path[-1]] = value
            with self.subTest(path=path):
                second = verify_model_metadata(binding, changed)
                self.assertNotEqual(first, second)
                self.assertNotEqual(first.metadata_sha256, second.metadata_sha256)
                self.assertEqual(first.stable_identity(), second.stable_identity())
        self.assertEqual(metadata, before)
        self.assertNotEqual(first.stable_identity(), replace(first, stable_metadata_profile='other-profile').stable_identity())

    def test_nonvolatile_fields_presence_types_and_order_cannot_inherit_stable_identity(self):
        binding, metadata = model_card_fixture()
        original = verify_model_metadata(binding, metadata).stable_identity()
        changes = []
        for key, value in dict(id='other-model', object='other-object', owned_by='other-provider',
                root='/other/checkpoint', parent='other-parent', max_model_len=4096, unknown_extension={'version':2}).items():
            changed = deepcopy(metadata)
            changed['data'][0][key] = value
            changes.append((key,changed))
        for key, value in dict(object='other-permission', allow_create_engine=True, allow_sampling=False,
                allow_logprobs=False, allow_search_indices=True, allow_view=False, allow_fine_tuning=True,
                organization='other', group='other-group', is_blocking=True, unknown_permission_extension=1).items():
            changed = deepcopy(metadata)
            changed['data'][0]['permission'][0][key] = value
            changes.append(('permission.'+key,changed))
        for path in (('created',), ('permission', 0, 'id'), ('permission', 0, 'created')):
            changed = deepcopy(metadata)
            target = changed['data'][0]
            for step in path[:-1]:
                target = target[step]
            target.pop(path[-1])
            changes.append(('missing '+str(path),changed))
        changed = deepcopy(metadata); changed['data'][0]['permission'] *= 2
        changes.append(('permission count',changed))
        changed = deepcopy(metadata); changed['data'].append(dict(id='extra', object='model', owned_by='vllm'))
        changes.append(('model count',changed))
        reordered = deepcopy(changed); reordered['data'].reverse()
        self.assertNotEqual(verify_model_metadata(binding, changed).stable_identity(),
                            verify_model_metadata(binding, reordered).stable_identity())
        changed = deepcopy(metadata); changed['new_top_level'] = 'new'
        changes.append(('unknown top level',changed))
        changed = deepcopy(metadata)
        changed['data'][0]['permission'].append(dict(id='modelperm-second', created=101, object='model_permission', allow_view=False))
        reordered = deepcopy(changed); reordered['data'][0]['permission'].reverse()
        self.assertNotEqual(verify_model_metadata(binding, changed).stable_identity(),
                            verify_model_metadata(binding, reordered).stable_identity())
        for label, changed in changes:
            with self.subTest(change=label):
                try:
                    actual = verify_model_metadata(binding, changed).stable_identity()
                except ValueError:
                    continue
                self.assertNotEqual(actual, original)
        for path, value in ((('created',), True), (('created',), -1), (('permission',0,'created'), '100'),
                            (('permission',0,'id'), None), (('permission',0,'id'), 'wrong-prefix')):
            changed = deepcopy(metadata)
            target = changed['data'][0]
            for step in path[:-1]:
                target = target[step]
            target[path[-1]] = value
            with self.subTest(bad_type=path), self.assertRaises(ValueError):
                verify_model_metadata(binding, changed)

    def test_each_bounded_get_retains_separate_raw_evidence_while_stable_identity_matches(self):
        import httpx
        from src.baselines.common.private_artifacts import write_private_json
        binding, metadata = model_card_fixture()
        actual_readiness = os.environ.get('SEMLOOM_TEST_VLLM_METADATA_JSON')
        if actual_readiness:
            metadata = json.loads(Path(actual_readiness).read_bytes())
            binding = LocalModelBinding(replace(binding.model, model_id=metadata['data'][0]['id']))
        changed = deepcopy(metadata)
        changed['data'][0]['created'] += 1
        changed['data'][0]['permission'][0].update(id='modelperm-fixture-next', created=metadata['data'][0]['permission'][0]['created']+1)
        payloads = [json.dumps(value).encode() for value in (metadata,changed)]
        remaining, requests = iter(payloads), []
        original_init = httpx.Client.__init__
        def handler(request):
            requests.append((request.method,str(request.url)))
            return httpx.Response(200, stream=httpx.ByteStream(next(remaining)), headers={'content-type':'application/json'})
        def initialize(client,*args,**kwargs):
            kwargs['transport'] = httpx.MockTransport(handler)
            original_init(client,*args,**kwargs)
        observations = [{},{}]
        with patch.object(httpx.Client,'__init__',initialize):
            first = fetch_model_metadata(binding,evidence=observations[0])
            second = fetch_model_metadata(binding,evidence=observations[1])
        self.assertNotEqual(first,second)
        self.assertEqual(first.stable_identity(),second.stable_identity())
        self.assertEqual(requests,[('GET',binding.api_base+'/models')]*2)
        for body, observation in zip(payloads,observations,strict=True):
            self.assertEqual(base64.b64decode(observation['raw_body_base64']),body)
            self.assertEqual(observation['raw_body_sha256'],hashlib.sha256(body).hexdigest())
            self.assertEqual(observation['raw_body_bytes'],len(body))
            self.assertTrue(observation['body_complete'])
            self.assertEqual(observation['status'],'verified')
            self.assertNotIn('Authorization',json.dumps(observation))
        self.assertNotEqual(observations[0]['raw_body_sha256'],observations[1]['raw_body_sha256'])
        retained = os.environ.get('SEMLOOM_PZ_METADATA_OUTPUT')
        if retained:
            root = Path(retained); root.mkdir(parents=True,exist_ok=False)
            for index, observation in enumerate(observations):
                write_private_json(root/f'metadata-{index}.json',observation)
            write_private_json(root/'qualification.json',dict(status='passed',actual_http_posts=0, model_calls=0,
                fixture='two MockTransport metadata responses; supplied readiness copied with three changed values',
                actual_failed_R2_second_GET='not retained; this test does not reconstruct it',
                selected=first.summary(), repeated=second.summary()))

    def test_metadata_failures_preserve_body_completeness_and_original_http_error(self):
        import httpx
        binding, metadata = model_card_fixture()
        original_init = httpx.Client.__init__
        class Partial(httpx.SyncByteStream):
            def __iter__(self):
                yield b'{"object":'
                raise httpx.ReadTimeout('metadata fixture timeout')
        cases = [(200,httpx.ByteStream(b'not json'),json.JSONDecodeError,True),
                 (200,httpx.ByteStream(json.dumps(dict(object='list',data=[])).encode()),ValueError,True),
                 (200,httpx.ByteStream(b'x'*1_048_577),ValueError,False),
                 (200,Partial(),httpx.ReadTimeout,False),
                 (503,httpx.ByteStream(b'provider unavailable'),httpx.HTTPStatusError,True),
                 (503,Partial(),httpx.HTTPStatusError,False)]
        for status,stream,expected,complete in cases:
            observation = {}
            def initialize(client,*args,**kwargs):
                kwargs['transport'] = httpx.MockTransport(lambda _request:httpx.Response(status,stream=stream))
                original_init(client,*args,**kwargs)
            with self.subTest(status=status,error=expected.__name__), patch.object(httpx.Client,'__init__',initialize):
                with self.assertRaises(expected):
                    fetch_model_metadata(binding,evidence=observation)
            self.assertEqual(observation['status'],'failed')
            self.assertEqual(observation['http_status'],status)
            self.assertEqual(observation['body_complete'],complete)
            self.assertLessEqual(observation['retained_body_bytes'],1_048_576)
            if not complete:
                self.assertIsNone(observation['raw_body_sha256'])
                self.assertIsNone(observation['raw_body_base64'])
                self.assertIsNotNone(observation['partial_body_sha256'])
            if status==503 and not complete:
                self.assertEqual(observation['error_type'],'HTTPStatusError')
                self.assertEqual(observation['body_observation_error_type'],'ReadTimeout')
        observation = {}
        with patch.object(httpx.Client,'stream',side_effect=httpx.ConnectError('fixture refused')):
            with self.assertRaises(httpx.ConnectError):
                fetch_model_metadata(binding,evidence=observation)
        self.assertEqual(observation['body_status'],'unavailable')
        self.assertFalse(observation['body_complete'])
        self.assertIsNone(observation['raw_body_sha256'])

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
