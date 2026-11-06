"""Real HTTPX with software transports; no model, service or framework starts."""

import asyncio
import base64
import builtins
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import httpx

from integrations.imlane_duckdb import model_provider as provider
from src.baselines.text.sembench_movie_native import q3_decision, q3_messages
from src.infrastructure.private_artifacts import content_digest


REAL_CLIENT = httpx.AsyncClient


class RecordingTransport(httpx.MockTransport):
    def __init__(self, handler, close_error=None):
        super().__init__(handler)
        self.close_error = close_error
        self.closes = 0

    async def aclose(self):
        self.closes += 1
        if self.close_error is not None:
            raise self.close_error


class IMLaneModelProviderTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='semloom-imlane-model-provider-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.receipts = self.root / 'receipts'
        self.receipts.mkdir(mode=0o700)
        self.path = self.root / 'provider.json'
        self.config = dict(schema=provider.CONFIG_SCHEMA,
            url='http://127.0.0.1:9000/v1/chat/completions', model='fixture-model',
            sampling_params=dict(max_tokens=8192, temperature=1.0, top_p=0.8,
                                 stop=[], repetition_penalty=1.05, seed=1),
            request_timeout_s=1, receipt_dir=str(self.receipts), api_key_env=None,
            provider_mode='software_http')
        self.path.write_text(json.dumps(self.config), encoding='utf-8')
        self.messages = [q3_messages('雪 «review»\nfirst'), q3_messages('second')]
        self.requests, self.clients, self.client_kwargs = [], [], []
        self.texts = ['Answer: True', 'Answer: False']
        provider._configuration.cache_clear()
        self.addCleanup(provider._configuration.cache_clear)
        original_import = builtins.__import__

        def cpu_import(name, *args, **kwargs):
            if name.split('.', 1)[0] in {'ray', 'daft', 'vllm', 'torch', 'lotus', 'transformers', 'duckdb'}:
                raise AssertionError('unexpected framework/model import: ' + name)
            return original_import(name, *args, **kwargs)

        for guard in (
            patch.dict(os.environ, {provider.CONFIG_ENV: str(self.path)}),
            patch.object(builtins, '__import__', side_effect=cpu_import),
            patch.object(subprocess, 'Popen', side_effect=AssertionError('process launch forbidden')),
            patch.object(socket.socket, 'connect', side_effect=AssertionError('network connect forbidden')),
            patch.object(socket.socket, 'bind', side_effect=AssertionError('network bind forbidden')),
        ):
            guard.start()
            self.addCleanup(guard.stop)

    def configure(self, **changes):
        self.config.update(changes)
        self.path.write_text(json.dumps(self.config), encoding='utf-8')

    def reply(self, position, *, text=None, finish='stop', model='fixture-model', status=200):
        return httpx.Response(status, json=dict(id=f'reply-{position}', model=model,
            choices=[dict(index=0, message=dict(role='assistant',
                content=self.texts[position] if text is None else text), finish_reason=finish)],
            usage=dict(prompt_tokens=20, completion_tokens=position + 1)))

    def install(self, handler=None, *, close_error=None):
        async def default_handler(request):
            position = len(self.requests)
            self.requests.append(request)
            return self.reply(position)

        transport = RecordingTransport(handler or default_handler, close_error=close_error)

        def client(**kwargs):
            self.client_kwargs.append(kwargs)
            value = REAL_CLIENT(transport=transport, **kwargs)
            self.clients.append(value)
            return value

        guard = patch.object(provider.httpx, 'AsyncClient', side_effect=client)
        guard.start()
        self.addCleanup(guard.stop)
        return transport

    def saved(self, phase):
        values = [json.loads(path.read_text()) for path in self.receipts.glob(f'*.{phase}.json')]
        return sorted(values, key=lambda value: value['started_monotonic_ns'])

    def test_batch_preserves_messages_generation_order_and_complete_private_receipts(self):
        transport = self.install()
        original = deepcopy(self.messages)
        self.assertEqual(provider.infer_batch(self.messages), self.texts)
        self.assertEqual(self.messages, original)
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(transport.closes, 1)
        self.assertTrue(self.clients[0].is_closed)
        started, = self.saved('started')
        terminal, = self.saved('terminal')
        self.assertEqual(started['schema'], provider.RECEIPT_SCHEMA)
        self.assertEqual(started['batch_id'], terminal['batch_id'])
        self.assertEqual((started['status'], started['http_post_attempts']), ('unknown', None))
        self.assertEqual((terminal['status'], terminal['http_post_attempts']), ('succeeded', 2))
        self.assertTrue(terminal['client_closed'])
        self.assertTrue(terminal['client_is_closed'])
        self.assertEqual(terminal['provider_mode'], 'software_http')
        self.assertIsNone(terminal['requested_config']['deadline_monotonic'])
        self.assertIsNone(terminal['physical_model_attempts'])
        for position, (request, call) in enumerate(zip(self.requests, terminal['calls'], strict=True)):
            payload = json.loads(request.content)
            self.assertEqual(payload, dict(self.config['sampling_params'], model='fixture-model',
                messages=original[position], stream=False, n=1))
            self.assertEqual(call['position'], position)
            self.assertEqual(call['messages_sha256'], content_digest(original[position]))
            self.assertEqual(call['request_payload_sha256'], content_digest(payload))
            self.assertEqual(call['requested_payload'], payload)
            raw = base64.b64decode(call['response_body_base64'])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), call['response_body_sha256'])
            self.assertEqual(json.loads(raw), call['response'])
            self.assertEqual(call['usage']['prompt_tokens'], 20)
            self.assertEqual(call['finish_reason'], 'stop')
            self.assertFalse(call['truncated'])
            self.assertTrue(call['attempted'])
            self.assertEqual(call['effective_request_timeout_s'], 1)
            self.assertLessEqual(call['http_attempt_started_monotonic_ns'], call['http_attempt_finished_monotonic_ns'])
        self.assertTrue(all(path.stat().st_mode & 0o777 == 0o600 for path in self.receipts.iterdir()))

    def test_129_requests_start_together_without_eight_or_hundred_request_gate(self):
        count = 129
        self.messages = [q3_messages(str(index)) for index in range(count)]
        self.texts = ['Answer: True'] * count
        entered = 0
        all_entered = asyncio.Event()

        async def handler(request):
            nonlocal entered
            position = entered
            entered += 1
            self.requests.append(request)
            if entered == count:
                all_entered.set()
            await all_entered.wait()
            return self.reply(position)

        self.install(handler)
        self.assertEqual(provider.infer_batch(self.messages), self.texts)
        self.assertEqual(entered, count)
        kwargs, = self.client_kwargs
        self.assertIsNone(kwargs['limits'].max_connections)
        self.assertIsNone(kwargs['limits'].max_keepalive_connections)
        self.assertFalse(kwargs['trust_env'])
        self.assertFalse(kwargs['follow_redirects'])

    def test_default_httpx_transport_has_zero_retries_without_any_network_send(self):
        async def inspect_default_transport():
            async with REAL_CLIENT(limits=httpx.Limits(max_connections=None, max_keepalive_connections=None),
                                   trust_env=False, follow_redirects=False) as client:
                self.assertIsInstance(client._transport, httpx.AsyncHTTPTransport)
                self.assertEqual(client._transport._pool._retries, 0)
        asyncio.run(inspect_default_transport())

    def test_out_of_order_completion_keeps_input_order_and_drains_before_first_error(self):
        self.messages.append(q3_messages('third'))
        self.texts.append('third reply')
        first = httpx.ReadError('first input failed')
        second = RuntimeError('second input failed first in wall time')
        completed = []

        async def handler(request):
            position = len(self.requests)
            self.requests.append(request)
            if position == 0:
                await asyncio.sleep(.02)
                completed.append(0)
                raise first
            if position == 1:
                completed.append(1)
                raise second
            await asyncio.sleep(.04)
            completed.append(2)
            return self.reply(position)

        self.install(handler)
        with self.assertRaises(httpx.ReadError) as caught:
            provider.infer_batch(self.messages)
        self.assertIs(caught.exception, first)
        self.assertEqual(completed, [1, 0, 2])
        terminal, = self.saved('terminal')
        self.assertEqual(terminal['status'], 'unknown')
        self.assertEqual([call['status'] for call in terminal['calls']], ['unknown', 'failed', 'succeeded'])
        self.assertTrue(terminal['client_closed'])
        self.assertEqual(terminal['http_post_attempts'], 3)

    def test_successful_out_of_order_http_responses_return_in_input_order(self):
        completed = []

        async def handler(request):
            position = len(self.requests)
            self.requests.append(request)
            await asyncio.sleep(.02 if position == 0 else 0)
            completed.append(position)
            return self.reply(position)

        self.install(handler)
        self.assertEqual(provider.infer_batch(self.messages), self.texts)
        self.assertEqual(completed, [1, 0])

    def test_cancelled_gather_drains_started_requests_and_records_completed_client_close(self):
        completed = []

        async def handler(request):
            self.requests.append(request)
            try:
                await asyncio.sleep(1)
            finally:
                completed.append(True)

        self.install(handler)
        original_gather = asyncio.gather

        def cancel_group(*coroutines, **kwargs):
            group = original_gather(*coroutines, **kwargs)
            asyncio.get_running_loop().call_later(.01, group.cancel)
            return group

        with patch.object(provider.asyncio, 'gather', side_effect=cancel_group):
            with self.assertRaises(asyncio.CancelledError):
                provider.infer_batch(self.messages)
        self.assertEqual(completed, [True, True])
        terminal, = self.saved('terminal')
        self.assertEqual(terminal['status'], 'unknown')
        self.assertTrue(terminal['client_closed'])
        self.assertTrue(terminal['client_is_closed'])
        self.assertTrue(all(call['error_type'] == 'CancelledError' for call in terminal['calls']))

    def test_length_and_empty_text_retain_original_parser_semantics(self):
        async def handler(request):
            position = len(self.requests)
            self.requests.append(request)
            return self.reply(position, text='' if position == 0 else 'Answer: False', finish='length')

        self.install(handler)
        texts = provider.infer_batch(self.messages)
        self.assertEqual(texts, ['', 'Answer: False'])
        self.assertEqual([q3_decision(text) for text in texts], [True, False])
        terminal, = self.saved('terminal')
        self.assertTrue(all(call['truncated'] and call['finish_reason'] == 'length' for call in terminal['calls']))

    def test_http_error_does_not_retry_or_discard_its_original_response(self):
        async def handler(request):
            self.requests.append(request)
            return httpx.Response(503, json=dict(error=dict(type='fixture', message='unavailable')))

        self.install(handler)
        with self.assertRaises(httpx.HTTPStatusError) as caught:
            provider.infer_batch(self.messages[:1])
        self.assertEqual(len(self.requests), 1)
        terminal, = self.saved('terminal')
        call, = terminal['calls']
        self.assertEqual(call['response'], caught.exception.response.json())
        self.assertEqual(call['http_status'], 503)
        self.assertEqual(call['error_type'], 'HTTPStatusError')
        self.assertEqual(terminal['status'], 'failed')

    def test_invalid_json_error_json_model_identity_and_nontext_responses_fail(self):
        bad = [b'not json', b'{"model":"fixture-model","choices":[],"value":NaN}',
            b'{"model":"fixture-model","choices":[],"value":1e999}',
            json.dumps(dict(error=dict(type='fixture'))).encode(),
            self.reply(0, model='another-model').content,
            self.reply(0, finish='abort').content, self.reply(0, finish='error').content,
            self.reply(0, finish='tool_calls').content,
            json.dumps(dict(model='fixture-model', choices=[])).encode(),
            json.dumps(dict(model='fixture-model', choices=[dict(message=dict(content=None), finish_reason='stop')])).encode(),
            json.dumps(dict(model='fixture-model', choices=[dict(message=dict(content='True'), finish_reason='stop')] * 2)).encode()]
        for body in bad:
            with self.subTest(body=body):
                async def handler(request, value=body):
                    self.requests.append(request)
                    return httpx.Response(200, content=value)
                self.install(handler)
                with self.assertRaises((ValueError, UnicodeError)):
                    provider.infer_batch(self.messages[:1])
                call = self.saved('terminal')[-1]['calls'][0]
                self.assertEqual(base64.b64decode(call['response_body_base64']), body)
                self.assertEqual(call['status'], 'failed')

    def test_timeout_is_unknown_and_client_still_closes(self):
        self.configure(request_timeout_s=.01)
        finished = []

        async def handler(request):
            self.requests.append(request)
            try:
                await asyncio.sleep(1)
            finally:
                finished.append(True)

        self.install(handler)
        with self.assertRaises(TimeoutError):
            provider.infer_batch(self.messages[:1])
        self.assertEqual(finished, [True])
        terminal, = self.saved('terminal')
        self.assertEqual(terminal['status'], 'unknown')
        self.assertTrue(terminal['client_closed'])
        self.assertEqual(terminal['calls'][0]['error_type'], 'TimeoutError')

    def test_expired_or_exact_deadline_sends_zero_posts_and_records_not_started(self):
        self.configure(deadline_monotonic=100)
        self.install()
        for now in (100, 101):
            with self.subTest(now=now), patch.object(provider, 'monotonic', return_value=now):
                with self.assertRaisesRegex(TimeoutError, 'before HTTP POST'):
                    provider.infer_batch(self.messages)
            self.assertEqual(self.requests, [])
            terminal = self.saved('terminal')[-1]
            self.assertEqual(terminal['requested_config']['deadline_monotonic'], 100)
            self.assertEqual(terminal['http_post_attempts'], 0)
            self.assertEqual(terminal['status'], 'failed')
            self.assertTrue(terminal['client_closed'])
            for call in terminal['calls']:
                self.assertFalse(call['attempted'])
                self.assertEqual(call['status'], 'not_started')
                self.assertEqual(call['effective_request_timeout_s'], 0)
                self.assertIsNone(call['http_attempt_started_monotonic_ns'])
                self.assertIsNone(call['http_attempt_finished_monotonic_ns'])

    def test_deadline_is_rechecked_per_request_and_caps_the_positive_remaining_timeout(self):
        self.configure(deadline_monotonic=100)
        self.install()
        with patch.object(provider, 'monotonic', side_effect=[99.75, 100]):
            with self.assertRaisesRegex(TimeoutError, 'before HTTP POST'):
                provider.infer_batch(self.messages)
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.requests[0].extensions['timeout'],
                         dict(connect=.25, read=.25, write=.25, pool=.25))
        terminal, = self.saved('terminal')
        self.assertEqual(terminal['http_post_attempts'], 1)
        self.assertEqual([call['status'] for call in terminal['calls']], ['succeeded', 'not_started'])
        self.assertEqual([call['effective_request_timeout_s'] for call in terminal['calls']], [.25, 0])

    def test_deadline_larger_than_request_timeout_does_not_extend_the_request(self):
        self.configure(deadline_monotonic=100)
        self.install()
        with patch.object(provider, 'monotonic', return_value=90):
            self.assertEqual(provider.infer_batch(self.messages), self.texts)
        terminal, = self.saved('terminal')
        self.assertTrue(all(call['effective_request_timeout_s'] == 1 for call in terminal['calls']))

    def test_deadline_remaining_bounds_the_entire_post_not_just_admission(self):
        self.configure(deadline_monotonic=100)

        async def handler(request):
            self.requests.append(request)
            await asyncio.sleep(1)

        self.install(handler)
        with patch.object(provider, 'monotonic', return_value=99.99):
            with self.assertRaises(TimeoutError):
                provider.infer_batch(self.messages[:1])
        terminal, = self.saved('terminal')
        call, = terminal['calls']
        self.assertEqual(terminal['status'], 'unknown')
        self.assertTrue(call['attempted'])
        self.assertAlmostEqual(call['effective_request_timeout_s'], .01)
        self.assertTrue(terminal['client_closed'])

    def test_close_error_preserves_primary_and_does_not_claim_transport_closed(self):
        first = httpx.ReadError('request fixture failed')
        cleanup = RuntimeError('transport close fixture failed')

        async def handler(request):
            raise first

        self.install(handler, close_error=cleanup)
        with self.assertRaises(httpx.ReadError) as caught:
            provider.infer_batch(self.messages[:1])
        self.assertIs(caught.exception, first)
        self.assertIn('HTTP batch client cleanup also failed: RuntimeError', first.__notes__)
        terminal, = self.saved('terminal')
        self.assertTrue(terminal['client_is_closed'])  # HTTPX sets its flag before transport close.
        self.assertIsNone(terminal['client_closed'])

    def test_close_error_after_success_propagates_with_all_raw_responses(self):
        cleanup = RuntimeError('close after success fixture')
        self.install(close_error=cleanup)
        with self.assertRaises(RuntimeError) as caught:
            provider.infer_batch(self.messages)
        self.assertIs(caught.exception, cleanup)
        terminal, = self.saved('terminal')
        self.assertEqual(terminal['status'], 'failed')
        self.assertTrue(all(call['status'] == 'succeeded' for call in terminal['calls']))
        self.assertIsNone(terminal['client_closed'])

    def test_terminal_write_failure_preserves_primary_and_started_remains_unknown(self):
        first = httpx.ReadError('request fixture failure')

        async def handler(request):
            raise first

        self.install(handler)
        actual_publish = provider.publish_private_json

        def publish(path, value):
            if path.name.endswith('.terminal.json'):
                raise OSError('terminal fixture failure')
            actual_publish(path, value)

        with patch.object(provider, 'publish_private_json', side_effect=publish):
            with self.assertRaises(httpx.ReadError) as caught:
                provider.infer_batch(self.messages[:1])
        self.assertIs(caught.exception, first)
        self.assertIn('model-provider terminal receipt also failed: OSError', first.__notes__)
        self.assertEqual(self.saved('terminal'), [])
        started, = self.saved('started')
        self.assertEqual(started['status'], 'unknown')
        self.assertIsNone(started['http_post_attempts'])
        self.assertIsNone(started['physical_model_attempts'])

    def test_terminal_write_failure_after_success_raises_and_retains_report(self):
        self.install()
        actual_publish = provider.publish_private_json

        def publish(path, value):
            if path.name.endswith('.terminal.json'):
                raise OSError('terminal fixture failure')
            actual_publish(path, value)

        with patch.object(provider, 'publish_private_json', side_effect=publish):
            with self.assertRaises(OSError) as caught:
                provider.infer_batch(self.messages)
        self.assertEqual(caught.exception.imlane_model_provider_report['http_post_attempts'], 2)
        self.assertEqual(len(self.saved('started')), 1)
        self.assertEqual(self.saved('terminal'), [])

    def test_started_write_failure_prevents_client_allocation(self):
        self.install()
        with patch.object(provider, 'publish_private_json', side_effect=OSError('started fixture failure')):
            with self.assertRaises(OSError):
                provider.infer_batch(self.messages)
        self.assertEqual(self.clients, [])

    def test_credentials_are_environment_only_and_never_written_to_receipts(self):
        self.configure(api_key_env='SEMLOOM_MODEL_PROVIDER_FIXTURE_CREDENTIAL')
        secret = 'private-model-provider-fixture-credential'

        async def handler(request):
            self.assertEqual(request.headers['Authorization'], 'Bearer ' + secret)
            self.requests.append(request)
            return self.reply(0)

        self.install(handler)
        with patch.dict(os.environ, {'SEMLOOM_MODEL_PROVIDER_FIXTURE_CREDENTIAL': secret}):
            provider.infer_batch(self.messages[:1])
        self.assertTrue(all(secret not in path.read_text() and 'Authorization' not in path.read_text()
                            for path in self.receipts.iterdir()))

    def test_missing_credential_has_no_http_attempts_and_no_client(self):
        self.configure(api_key_env='SEMLOOM_MODEL_PROVIDER_ABSENT_FIXTURE')
        self.install()
        with self.assertRaisesRegex(ValueError, 'credential'):
            provider.infer_batch(self.messages)
        self.assertEqual(self.clients, [])
        terminal, = self.saved('terminal')
        self.assertEqual(terminal['http_post_attempts'], 0)
        self.assertEqual([call['status'] for call in terminal['calls']], ['not_started', 'not_started'])

    def test_configuration_is_resource_free_and_accepts_a_not_yet_created_receipt_directory(self):
        value = dict(self.config, receipt_dir=str(self.root / 'future'), api_key_env='ABSENT_PREFLIGHT_KEY')
        checked = provider.configuration(value)
        self.assertFalse(Path(checked['receipt_dir']).exists())
        self.assertEqual(checked['api_key_env'], 'ABSENT_PREFLIGHT_KEY')
        json.dumps(checked, allow_nan=False)
        value['sampling_params']['max_tokens'] = 7
        self.assertEqual(checked['sampling_params']['max_tokens'], 8192)

    def test_configuration_is_cached_once_and_has_no_model_call_default(self):
        self.install()
        provider.infer_batch(self.messages[:1])
        self.configure(sampling_params=dict(max_tokens=8, temperature=0, top_p=1, stop=['\n']))
        self.texts.append('unused')
        provider.infer_batch(self.messages[:1])
        self.assertEqual(provider._configuration.cache_info().misses, 1)
        self.assertTrue(all(json.loads(request.content)['max_tokens'] == 8192 for request in self.requests))
        self.assertTrue(all(json.loads(request.content)['stop'] == [] for request in self.requests))

    def test_invalid_configuration_is_rejected_before_any_client(self):
        invalid = [dict(schema='another'), dict(url='http://example.com/v1/chat/completions'),
            dict(url='http://localhost:invalid/v1/chat/completions'), dict(url='http://localhost:9000/health'),
            dict(provider_mode='unspecified'), dict(request_timeout_s=0), dict(request_timeout_s=float('inf')),
            dict(deadline_monotonic=0), dict(deadline_monotonic=-1), dict(deadline_monotonic=True),
            dict(deadline_monotonic=float('inf')), dict(deadline_monotonic=float('nan')),
            dict(sampling_params=dict(max_tokens=1, temperature=0)),
            dict(sampling_params=dict(max_tokens=1, temperature=0, top_p=1, model='override')),
            dict(sampling_params=dict(max_tokens=1, temperature=0, top_p=1, stream=False)),
            dict(sampling_params=dict(max_tokens=1, temperature=0, top_p=1, n=2)),
            dict(sampling_params=dict(max_tokens=1, temperature=float('nan'), top_p=1)),
            dict(api_key='credential-must-not-be-in-config')]
        self.install()
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                provider.configuration(dict(self.config, **changes))
        self.assertEqual(self.clients, [])

    def test_empty_input_and_active_event_loop_do_not_start_http(self):
        self.install()
        with patch.dict(os.environ, {provider.CONFIG_ENV: ''}):
            self.assertEqual(provider.infer_batch([]), [])

        async def call():
            with self.assertRaisesRegex(RuntimeError, 'active event loop'):
                provider.infer_batch(self.messages)
        asyncio.run(call())
        self.assertEqual(self.clients, [])
        self.assertEqual(self.saved('started'), [])

    def test_existing_arrow_q3_udf_loads_the_sync_provider_and_keeps_business_id_occurrences(self):
        import pyarrow as pa
        from integrations.imlane_duckdb import q3_udf

        self.install()
        q3_receipts = self.root / 'q3-receipts'
        q3_receipts.mkdir(mode=0o700)
        q3_config = self.root / 'q3.json'
        q3_config.write_text(json.dumps(dict(provider_path=str(Path(provider.__file__).resolve()),
            provider_function='infer_batch', provider_mode='software_http', receipt_dir=str(q3_receipts))))
        q3_udf._configuration.cache_clear()
        self.addCleanup(q3_udf._configuration.cache_clear)
        previous = set(sys.modules)
        self.addCleanup(lambda: [sys.modules.pop(name) for name in tuple(sys.modules)
            if name.startswith('_imlane_duckdb_q3_provider_') and name not in previous])
        reviews = ['first original 雪', 'second original review']
        table = pa.Table.from_arrays([pa.array(values, type=pa.string()) for values in
            (['occ-1', 'occ-2'], ['taken_3', 'taken_3'], ['business', 'business'], reviews)],
            schema=q3_udf.INPUT)
        with patch.dict(os.environ, {q3_udf.CONFIG_ENV: str(q3_config)}):
            output = q3_udf.infer_q3(table)
        self.assertEqual(output.schema, q3_udf.OUTPUT)
        self.assertEqual(output.column(0).to_pylist(), [1, 0])
        self.assertEqual(output.column(0).num_chunks, 1)
        self.assertEqual([json.loads(request.content)['messages'] for request in self.requests],
                         [q3_messages(review) for review in reviews])
        receipt, = [json.loads(path.read_text()) for path in q3_receipts.glob('q3-batch-*.json')]
        self.assertEqual([row['occurrence_id'] for row in receipt['predictions']], ['occ-1', 'occ-2'])
        self.assertEqual(receipt['provider_mode'], 'software_http')
        self.assertEqual(len(self.saved('terminal')), 1)


if __name__ == '__main__':
    unittest.main()
