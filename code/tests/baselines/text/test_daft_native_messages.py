"""Actual Daft 0.7.21 builtin graph with a software OpenAI client, no service.

The optional frozen proof contains four original formatter batches already
produced by complete queries. Replay them without rerunning the queries.
"""
import asyncio
import base64
from collections import Counter
from copy import deepcopy
import hashlib
from io import BytesIO
import importlib.util
import inspect
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import traceback
from types import SimpleNamespace as NS
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import daft_sembench_native as binding
from src.infrastructure.private_artifacts import content_digest


PROOF = os.environ.get('SEMLOOM_DAFT_MESSAGES_PROOF')
PROOF_SHA256 = '5b7cb315bb9a83fe2e25b036a9c276fda81f83223504610c7146f2b3ed30f85f'


class EffectiveMessageTests(unittest.TestCase):
    def test_encoding_is_explicit_nonmutating_and_does_not_import_the_native_SDK(self):
        script = '''
import importlib.abc,sys
class RejectSDK(importlib.abc.MetaPathFinder):
    def find_spec(self,name,*args):
        if name.split('.')[0] in {'daft','openai','lotus','vllm','torch'}:
            raise AssertionError('pure message encoding imported '+name)
sys.meta_path.insert(0,RejectSDK())
from src.baselines.text.frameworks.daft_sembench_native import effective_openai_messages
messages=[{'role':'system','content':'system'}, {'role':'user','content':'  original \\n文本  '}]
effective=effective_openai_messages(messages)
assert messages[1]['content']=='  original \\n文本  '
assert effective==[messages[0], {'role':'user','content':[{'type':'text','text':messages[1]['content']}]}]
effective[1]['content'][0]['text']='changed'
assert messages[1]['content']=='  original \\n文本  '
array=[{'role':'system','content':'system'}, {'role':'user','content':[{'type':'text','text':'original'}]}]
assert effective_openai_messages(array)==array
assert effective_openai_messages([messages[0],{'role':'user','content':''}])[1]['content']==[{'type':'text','text':''}]
single=[{'role':'user','content':'original aggregate'}]
assert effective_openai_messages(single)==[{'role':'user','content':[{'type':'text','text':'original aggregate'}]}]
assert single==[{'role':'user','content':'original aggregate'}]
'''
        completed = subprocess.run([sys.executable, '-B', '-c', script], capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)


class SoftwareClient:
    lock = threading.Lock()
    calls, completed, created, closed = [], [], [], []
    expected = None
    failure = None

    def __init__(self, **options):
        self.created.append(dict(options))
        self.chat = NS(completions=NS(create=self.create))

    async def create(self, **kwargs):
        self.signature.bind(None, **kwargs)
        with self.lock:
            index = len(self.calls)
            self.calls.append(deepcopy(kwargs))
        await asyncio.sleep(.003 if index % 2 == 0 else 0)
        if self.failure is not None:
            raise self.failure
        text = (self.expected[content_digest(kwargs['messages'])] if self.expected is not None
                else 'software-response-' + str(index))
        self.completed.append(index)
        return self.response_type.model_validate(dict(id='software-' + str(index), object='chat.completion',
            created=1, model=kwargs['model'], choices=[dict(index=0, finish_reason='stop',
                message=dict(role='assistant', content=text))],
            usage=dict(prompt_tokens=11, completion_tokens=2, total_tokens=13)))

    async def close(self):
        self.closed.append(True)


@unittest.skipUnless(importlib.util.find_spec('daft') and importlib.util.find_spec('openai'),
                     'requires the existing Daft/OpenAI SDKs')
class NativeMessageBatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.inet_attempts = []
        original_init = socket.socket.__init__
        original_connect, original_bind = socket.socket.connect, socket.socket.bind

        def initialize(sock, family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0, fileno=None):
            if family in (socket.AF_INET, socket.AF_INET6):
                cls.inet_attempts.append(dict(event='construct', stack=traceback.format_stack()))
                raise RuntimeError('native CPU fixture forbids INET socket construction')
            return original_init(sock, family, type, proto, fileno)

        def connect(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                cls.inet_attempts.append(dict(event='connect', stack=traceback.format_stack()))
                raise RuntimeError('native CPU fixture forbids INET connections')
            return original_connect(sock, address)

        def bind(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                cls.inet_attempts.append(dict(event='bind', stack=traceback.format_stack()))
                raise RuntimeError('native CPU fixture forbids INET listeners')
            return original_bind(sock, address)

        for guard in (patch.object(socket.socket, '__init__', initialize),
                      patch.object(socket.socket, 'connect', connect), patch.object(socket.socket, 'bind', bind)):
            guard.start()
            cls.addClassCleanup(guard.stop)
        import daft
        if daft.__version__ != '0.7.21':
            raise unittest.SkipTest('requires the pinned existing Daft 0.7.21 SDK')
        from daft.ai.openai.protocols import prompter
        from openai.resources.chat.completions.completions import AsyncCompletions
        from openai.types.chat import ChatCompletion
        cls.prompter = prompter
        SoftwareClient.signature = inspect.signature(AsyncCompletions.create)
        SoftwareClient.response_type = ChatCompletion

    @classmethod
    def tearDownClass(cls):
        # urllib3 may probe ::1 capability while importing. Construction is
        # blocked before any socket exists; preserve that distinct observation.
        for attempt in cls.inet_attempts:
            if (attempt['event'] != 'construct'
                    or not any('urllib3/util/connection.py' in line for line in attempt['stack'])):
                raise AssertionError('unexpected blocked INET attempt: ' + repr(attempt))

    def setUp(self):
        SoftwareClient.calls, SoftwareClient.completed = [], []
        SoftwareClient.created, SoftwareClient.closed = [], []
        SoftwareClient.expected, SoftwareClient.failure = None, None
        guard = patch.object(self.prompter, 'AsyncOpenAI', SoftwareClient)
        guard.start()
        self.addCleanup(guard.stop)

    @staticmethod
    def request(call_id='occurrence-0', *, text='original text'):
        return dict(call_id=call_id, messages=[dict(role='system', content='original system'),
                    dict(role='user', content=[dict(type='text', text=text)])])

    def execute(self, requests, sampling=None, **kwargs):
        return binding.execute_prepared_messages(requests, model='local-native-fixture',
            sampling_params=dict(max_tokens=32, repetition_penalty=1, logprobs=None) if sampling is None else sampling,
            provider_options=dict(api_key='software-only', base_url='http://localhost:18473/v1', max_retries=0),
            **kwargs)

    def test_duplicate_messages_keep_distinct_occurrences_and_original_JSON_options(self):
        rows = [self.request('duplicate-0'), self.request('duplicate-1')]
        sampling = dict(max_tokens=32, repetition_penalty=1.0, logprobs=None,
                        response_format=dict(type='json_object'))
        original = deepcopy((rows, sampling))
        seen = []
        result = self.execute(rows, sampling, on_result_ready=lambda value: seen.append(deepcopy(value)))
        self.assertEqual(result['predictions'], [dict(call_id='duplicate-0', generated_text='software-response-0'),
                                                dict(call_id='duplicate-1', generated_text='software-response-1')])
        self.assertEqual((rows, sampling), original)
        self.assertEqual(len(SoftwareClient.calls), 2)
        self.assertCountEqual(SoftwareClient.completed, [0, 1])
        for call in SoftwareClient.calls:
            self.assertEqual(call['messages'], rows[0]['messages'])
            self.assertEqual(call['response_format'], sampling['response_format'])
            self.assertEqual(call['extra_body'], dict(repetition_penalty=1.0))
            self.assertIsNone(call['logprobs'])
        metadata = result['metadata']
        self.assertEqual(metadata['sampling_params'], sampling)
        self.assertEqual(metadata['native_sampling_params'], dict(max_tokens=32, logprobs=None,
            response_format=dict(type='json_object'), extra_body=dict(repetition_penalty=1.0)))
        self.assertEqual(metadata['daft_version'], '0.7.21')
        self.assertFalse(metadata['model_process_owned'])
        self.assertIsNone(metadata['model_close_receipt'])
        self.assertEqual(SoftwareClient.closed, [])
        self.assertIsInstance(metadata['result_ready_monotonic_ns'], int)
        self.assertEqual(seen, [result])
        self.assertNotIn('software-only', json.dumps(result))

    def test_string_and_array_users_keep_text_and_distinct_original_effective_identities(self):
        text = '  Context:\noriginal 文本\n\nClaim: unchanged?  '
        rows = [self.request('string', text=text), self.request('array', text=text)]
        rows[0]['messages'][1]['content'] = text
        original = deepcopy(rows)
        effective = binding.effective_openai_messages(rows[0]['messages'])
        SoftwareClient.expected = {content_digest(effective): 'same raw response'}
        result = self.execute(rows)
        self.assertEqual(rows, original)
        self.assertEqual(result['predictions'], [dict(call_id=row['call_id'], generated_text='same raw response')
                                                for row in rows])
        self.assertEqual([call['messages'] for call in SoftwareClient.calls], [effective, effective])
        metadata = result['metadata']
        self.assertEqual(metadata['message_encoding'], 'daft.openai.system_single_user.parts.v1')
        self.assertIsNone(metadata['target_renderer_equivalence_verified'])
        identities = [dict(call_id=row['call_id'],
            original_messages_sha256=content_digest(row['messages']),
            effective_messages_sha256=content_digest(binding.effective_openai_messages(row['messages'])),
            user_string_wrapped=index == 0) for index, row in enumerate(rows)]
        self.assertNotEqual(identities[0]['original_messages_sha256'], identities[0]['effective_messages_sha256'])
        self.assertEqual(identities[1]['original_messages_sha256'], identities[1]['effective_messages_sha256'])
        self.assertNotIn('message_identities', metadata)

    def test_single_user_aggregation_has_no_invented_system_message(self):
        rows = [dict(call_id=str(index), messages=[dict(role='user', content=text)])
                for index, text in enumerate(('original first document', 'original second document'))]
        original = deepcopy(rows)
        result = self.execute(rows)
        self.assertEqual(rows, original)
        self.assertEqual(result['metadata']['message_encoding'], 'daft.openai.single_user.parts.v1')
        self.assertCountEqual([call['messages'] for call in SoftwareClient.calls],
                             [binding.effective_openai_messages(row['messages']) for row in rows])
        self.assertTrue(all(len(call['messages']) == 1 for call in SoftwareClient.calls))

    def test_original_formatters_keep_plain_text_then_image_JSON_batches(self):
        from PIL import Image
        from lotus.templates.task_instructions import filter_formatter, extract_formatter

        stream = BytesIO()
        Image.new('RGB', (3, 2), 'red').save(stream, 'PNG')
        uri = 'data:image/png;base64,' + base64.b64encode(stream.getvalue()).decode('ascii')
        filters = [dict(call_id='filter-' + str(i), messages=filter_formatter(NS(),
            dict(text='[Description]: original ' + str(i), image={}), 'Keep the described product.'))
            for i in range(2)]
        extracts = [dict(call_id='extract-survivor', messages=extract_formatter(NS(),
            dict(text='[Id]: 1\n[Description]: original survivor', image=dict(Images=uri)),
            dict(generated_json='Extract the unchanged product identity.')))]
        original = deepcopy((filters, extracts))
        self.assertTrue(all(isinstance(row['messages'][1]['content'], str) for row in filters))
        self.assertIsInstance(extracts[0]['messages'][1]['content'], list)
        plain = dict(max_tokens=32, repetition_penalty=1, logprobs=None, response_format=None)
        structured = dict(plain, response_format=dict(type='json_object'))
        expected_text = ['True', 'False', '{"generated_json":"original survivor"}']
        SoftwareClient.expected = {content_digest(binding.effective_openai_messages(row['messages'])): value
            for row, value in zip(filters + extracts, expected_text, strict=True)}
        first = self.execute(filters, plain)
        second = self.execute(extracts, structured)
        self.assertEqual((filters, extracts), original)
        self.assertEqual([row['generated_text'] for result in (first, second) for row in result['predictions']],
                         expected_text)
        calls = SoftwareClient.calls
        self.assertEqual(Counter(content_digest(call['messages']) for call in calls),
                         Counter(content_digest(binding.effective_openai_messages(row['messages']))
                                 for row in filters + extracts))
        self.assertTrue(all(call['response_format'] is None for call in calls[:2]))
        self.assertEqual(calls[2]['response_format'], {'type': 'json_object'})
        self.assertEqual([part['image_url']['url'] for part in calls[2]['messages'][1]['content']
                          if part['type'] == 'image_url'], [uri])
        self.assertEqual(first['metadata']['message_encoding'], second['metadata']['message_encoding'])

    @unittest.skipUnless(PROOF, 'requires the explicitly bound frozen original-batch proof JSON')
    def test_four_frozen_original_visual_batches_keep_messages_bytes_and_raw_replies(self):
        from PIL import Image
        raw = Path(PROOF).read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), PROOF_SHA256)
        proof = json.loads(raw)
        uris = {}
        for color in ('red', 'green', 'blue', 'yellow'):
            stream = BytesIO()
            Image.new('RGB', (3, 2), color).save(stream, 'PNG')
            uri = 'data:image/png;base64,' + base64.b64encode(stream.getvalue()).decode('ascii')
            uris[hashlib.sha256(uri.encode()).hexdigest()] = uri
        shapes = []
        for case in proof['cases']:
            for index, batch in enumerate(case['batches']):
                with self.subTest(scenario=case['scenario'], query=case['query'], batch=index):
                    messages = deepcopy(batch['original_messages'])
                    for conversation in messages:
                        for message in conversation:
                            if isinstance(message['content'], list):
                                for part in message['content']:
                                    if part['type'] == 'image_url':
                                        uri = uris[part['url_sha256']]
                                        self.assertEqual(len(uri.encode()), part['url_bytes'])
                                        part['image_url']['url'] = uri
                                        for field in ('url_sha256', 'url_bytes', 'representation'):
                                            del part[field]
                    expected = batch['generated_texts']
                    rows = [dict(call_id=case['scenario'] + '-' + case['query'] + '-' + str(index) + '-' + str(i),
                                 messages=message) for i, message in enumerate(messages)]
                    SoftwareClient.expected = {}
                    for message, text in zip(messages, expected, strict=True):
                        digest = content_digest(message)
                        self.assertEqual(SoftwareClient.expected.get(digest, text), text)
                        SoftwareClient.expected[digest] = text
                    start = len(SoftwareClient.calls)
                    sampling = dict(batch['sampling'], repetition_penalty=1)
                    result = self.execute(rows, sampling)
                    self.assertEqual(result['predictions'], [dict(call_id=row['call_id'], generated_text=text)
                        for row, text in zip(rows, expected, strict=True)])
                    returned = SoftwareClient.calls[start:]
                    self.assertEqual(Counter(content_digest(call['messages']) for call in returned),
                                     Counter(content_digest(message) for message in messages))
                    self.assertTrue(all(call.get('response_format') == sampling.get('response_format')
                                        and call['extra_body'] == dict(repetition_penalty=1) for call in returned))
                    shapes.append((case['scenario'], case['query'], len(rows)))
        self.assertEqual(shapes, [('animals', 'Q1', 4), ('medical', 'Q9', 10),
                                  ('mmqa', 'Q2b', 4), ('mmqa', 'Q2b', 4)])

    def test_unsupported_shapes_and_duplicate_ids_fail_before_constructing_a_client(self):
        values = []
        row = self.request(); row['messages'][1]['content'] = {'unsupported': 'content'}; values.append([row])
        row = self.request(); row['messages'][0]['role'] = 'assistant'; values.append([row])
        values.append([self.request(), self.request()])
        single = dict(call_id='single-user', messages=[dict(role='user', content='original')])
        values.extend(([single, self.request()], [self.request(), single]))
        row = self.request('other'); row['messages'][0]['content'] = 'different system'; values.append([self.request(), row])
        row = self.request('other'); row['messages'][1]['content'].append(dict(type='text', text='another part'))
        values.append([self.request(), row])
        values.append([self.request(text='http://localhost/image.png')])
        row = self.request(); row['messages'][1]['content'] = 'http://localhost/image.png'; values.append([row])
        for image in (dict(url='http://localhost/image.png'), dict(url='data:image/jpeg;base64,AA=='),
                      dict(url='data:image/png;base64,AA=='), dict(url='data:image/png;base64,!'),
                      dict(url='data:image/png;base64,AA==', detail='high')):
            row = self.request(); row['messages'][1]['content'] = [dict(type='image_url', image_url=image)]
            values.append([row])
        for rows in values:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                self.execute(rows)
        self.assertEqual(SoftwareClient.created, [])
        self.assertEqual(SoftwareClient.calls, [])

    def test_empty_batch_has_no_client_and_callback_observes_the_complete_result(self):
        seen = []
        result = self.execute([], on_result_ready=seen.append)
        self.assertEqual(result['predictions'], [])
        self.assertEqual(result['metadata']['message_encoding'], 'daft.openai.system_single_user.parts.v1')
        self.assertEqual(seen, [result])
        self.assertIsInstance(result['metadata']['result_ready_monotonic_ns'], int)
        self.assertEqual(SoftwareClient.created, [])

    def test_conflicting_extra_body_is_rejected_and_other_extra_fields_are_preserved(self):
        for extra in ({'repetition_penalty': 1}, 'ambiguous'):
            with self.subTest(extra=extra), self.assertRaisesRegex(ValueError, 'unambiguous'):
                self.execute([self.request()], dict(repetition_penalty=1, extra_body=extra))
        self.assertEqual(SoftwareClient.created, [])
        self.execute([self.request()], dict(repetition_penalty=1.2, extra_body=dict(min_p=0.01)))
        self.assertEqual(SoftwareClient.calls[0]['extra_body'], dict(min_p=0.01, repetition_penalty=1.2))

    def test_native_API_error_keeps_failure_and_does_not_publish_a_ready_result(self):
        SoftwareClient.failure = RuntimeError('software native API failure')
        seen = []
        with self.assertRaises(Exception) as raised:
            self.execute([self.request()], on_result_ready=seen.append)
        self.assertIn('software native API failure', str(raised.exception))
        report = raised.exception.native_query_report
        self.assertEqual(report['predictions'], [])
        self.assertNotIn('result_ready_monotonic_ns', report['metadata'])
        self.assertEqual(seen, [])
        self.assertEqual(len(SoftwareClient.calls), 1)
        self.assertFalse(report['metadata']['model_process_owned'])
        self.assertIsNone(report['metadata']['model_close_receipt'])


if __name__ == '__main__':
    unittest.main()
