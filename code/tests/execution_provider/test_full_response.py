"""Read complete native HTTP responses without changing existing PG error mapping."""

import json
import unittest

import httpx

from src.execution_provider.adapters.async_fixed_model import AsyncFixedModelTransport
from src.execution_provider.adapters.full_response import (
    FullModelResponse, FullResponseTransport, decode_full_response, encode_full_response, response_prefix,
)
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.completion_response import decode_backend_completion
from src.execution_provider.adapters.ray_map_transport import _HttpActor
from src.execution_provider.completion import CompletionAdapterError
from src.scheduling.core.session_contract import BackendTask, OfferedTask, SessionSpec, TaskKey


class FullResponseTests(unittest.IsolatedAsyncioTestCase):
    async def run_transport(self, cls, status, body, headers=(), bound=2048):
        calls = []
        async def serve(request):
            calls.append(request.content)
            return httpx.Response(status, headers=headers, stream=httpx.ByteStream(body))
        transport = cls(FixedModelConfig('http://localhost/fixture', 'fixture', 1000), 1)
        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(serve))
        payload = b'{ "messages": [{"role":"user","content":"x"}], "model":"fixture", "max_tokens":7 }'
        task = BackendTask(TaskKey(0, 0), SessionSpec('q', 'op', 'fixture'), OfferedTask(0, payload, 1, bound))
        try:
            result = await transport.execute(task, 'model')
            self.assertEqual(calls, [payload])
            return result
        finally:
            await transport.close()

    async def test_usage_logprobs_and_extension_fields_survive_exact_body(self):
        body = b'{"model":"fixture","choices":[{"message":{"content":"yes"},"finish_reason":"stop",' \
               b'"logprobs":{"content":[{"token":"yes","logprob":-0.2}]}}],"usage":' \
               b'{"prompt_tokens":7,"completion_tokens":1,"total_tokens":8},"extension":[1,2]}'
        payload = await self.run_transport(FullResponseTransport, 200, body, [('x-request-id', 'id-1')])
        response = decode_full_response(payload)
        self.assertEqual(response.body, body)
        self.assertIn(('x-request-id', 'id-1'), response.headers)
        self.assertEqual(response.status_code, 200)
        pg_payload = await self.run_transport(AsyncFixedModelTransport, 200, body)
        self.assertEqual(pg_payload, body)
        self.assertEqual(decode_backend_completion(pg_payload).raw_output, 'yes')

    async def test_every_error_status_keeps_body_and_headers_default_mapping_is_unchanged(self):
        for status, pg_code in ((302, 'MODEL_RESPONSE_INVALID'), (400, 'MODEL_REQUEST_REJECTED'),
                                (401, 'MODEL_REQUEST_REJECTED'), (429, 'MODEL_REQUEST_REJECTED'),
                                (503, 'MODEL_UNAVAILABLE')):
            with self.subTest(status=status):
                body = b'{"error":{"code":"native-error","message":"original"},"extra":true}'
                payload = await self.run_transport(FullResponseTransport, status, body,
                                                   [('retry-after', '9'), ('x-native', 'one'), ('x-native', 'two')])
                raw = decode_full_response(payload)
                self.assertEqual(raw.status_code, status)
                self.assertEqual(raw.body, body)
                self.assertEqual([v for k,v in raw.headers if k == 'x-native'], ['one', 'two'])
                old = await self.run_transport(AsyncFixedModelTransport, status, body)
                self.assertEqual(json.loads(old), {'bridge_error': pg_code})

    async def test_non_json_and_binary_response_are_not_parsed_by_common_layer(self):
        body = b'\xff\x00<html>unavailable</html>'
        self.assertEqual(decode_full_response(await self.run_transport(FullResponseTransport, 502, body)).body, body)

    async def test_envelope_counts_towards_the_result_reservation(self):
        body = b'x' * 100
        with self.assertRaisesRegex(ValueError, 'response exceeds bound'):
            await self.run_transport(FullResponseTransport, 200, body, bound=len(body))
        self.assertEqual(await self.run_transport(AsyncFixedModelTransport, 200, body, bound=len(body)), body)

    async def test_oversized_headers_fail_before_result_buffering(self):
        with self.assertRaisesRegex(ValueError, 'headers exceed bound'):
            await self.run_transport(FullResponseTransport, 200, b'x', [('x-extra', 'a' * 8193)])

    def test_corrupt_header_length_does_not_create_a_response(self):
        raw = encode_full_response(FullModelResponse(200, (), b'hello'))
        for invalid in (b'SLHTTP\n\x01', raw[:12], raw[:8] + b'\xff' * 4 + raw[12:], b'garbage'):
            with self.subTest(invalid=invalid[:12]), self.assertRaises(ValueError): decode_full_response(invalid)
        self.assertEqual(decode_full_response(raw).body, b'hello')

    def test_local_failure_cannot_claim_an_upstream_http_status(self):
        with self.assertRaises(CompletionAdapterError) as raised:
            decode_full_response(b'{"bridge_error":"MODEL_UNAVAILABLE"}')
        self.assertEqual(raised.exception.code, 'MODEL_UNAVAILABLE')

    async def test_worker_pool_identity_includes_response_mode(self):
        config = FixedModelConfig('http://localhost/fixture', 'fixture', 1000)
        old = _HttpActor(config, 2, managed=True)
        full = _HttpActor(config, 2, managed=True, response_mode='full')
        try:
            self.assertNotEqual(old.identity, full.identity)
            with self.assertRaisesRegex(ValueError, 'differs'):
                await old.claim('a' * 32, full.identity, 2)
            with self.assertRaisesRegex(ValueError, 'differs'):
                await full.claim('b' * 32, old.identity, 2)
            self.assertTrue(await full.claim('c' * 32, full.identity, 2))
            await full.release('c' * 32)
        finally:
            await old.close();await full.close()
