"""Cookie scope for one fixed-model request, using the actual HTTPX client."""

import asyncio
from contextlib import asynccontextmanager
import unittest
from unittest import mock

import httpx

from src.execution_provider.adapters.async_fixed_model import AsyncFixedModelTransport
from src.execution_provider.adapters.full_response import FullResponseTransport, decode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.scheduling.core.session_contract import BackendTask, OfferedTask, SessionSpec, TaskKey


def task(sequence=0):
    return BackendTask(TaskKey(0, sequence), SessionSpec('query', 'flow', 'fixture'),
                       OfferedTask(sequence, str(sequence).encode(), 1, 4096))


@asynccontextmanager
async def transport_client(handler, cls=FullResponseTransport, redirects=False):
    transport = cls(FixedModelConfig('http://localhost/start', 'fixture', 1000), 2)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=redirects)
    with mock.patch('httpx.AsyncClient', return_value=client) as create:
        transport.prepare()
        try:
            yield transport, client, create
        finally:
            await transport.close()


class HttpxRequestCookieTests(unittest.IsolatedAsyncioTestCase):
    async def test_sequential_calls_share_one_client_without_previous_cookie(self):
        for cls in (FullResponseTransport, AsyncFixedModelTransport):
            with self.subTest(transport=cls.__name__):
                sent = []

                def respond(request):
                    sent.append(request.headers.get('cookie'))
                    return httpx.Response(200, headers={'set-cookie': 'previous=leaked; Path=/'},
                                          stream=httpx.ByteStream(b'body'))

                async with transport_client(respond, cls) as (transport, client, create):
                    await transport.execute(task(0), 'model')
                    await transport.execute(task(1), 'model')
                    self.assertEqual(sent, [None, None])
                    self.assertEqual(list(client.cookies.jar), [])
                    create.assert_called_once()
                self.assertTrue(client.is_closed)

    async def test_concurrent_redirect_chains_keep_each_calls_own_cookie_and_payload(self):
        reached = asyncio.Event()
        redirected, count = [], 0

        async def respond(request):
            nonlocal count
            if request.url.path == '/start':
                self.assertNotIn('cookie', request.headers)
                return httpx.Response(307, headers=[('location', '/finish'),
                    ('set-cookie', 'current=' + request.content.decode() + '; Path=/')],
                    stream=httpx.ByteStream(b'redirect body'))
            count += 1
            if count == 2:
                reached.set()
            await reached.wait()
            redirected.append((request.method, request.content, request.headers.get('cookie')))
            return httpx.Response(200, headers=[('x-repeat', 'one'), ('x-repeat', 'two')],
                                  stream=httpx.ByteStream(b'{"usage":{"total_tokens":2},"logprobs":[-0.1]}'))

        async with transport_client(respond, redirects=True) as (transport, client, create):
            replies = await asyncio.gather(*(transport.execute(task(i), 'model') for i in (0, 1)))
            self.assertEqual(sorted(redirected), [('POST', b'0', 'current=0'), ('POST', b'1', 'current=1')])
            for reply in replies:
                full = decode_full_response(reply)
                self.assertEqual([v for k, v in full.headers if k == 'x-repeat'], ['one', 'two'])
                self.assertEqual(full.body, b'{"usage":{"total_tokens":2},"logprobs":[-0.1]}')
            self.assertEqual(list(client.cookies.jar), [])
            create.assert_called_once()
        self.assertTrue(client.is_closed)

    async def test_default_redirect_status_headers_and_body_are_returned_without_following(self):
        calls = []
        raw = b' {"detail":"original redirect"}\n'

        def respond(request):
            calls.append(request.url.path)
            return httpx.Response(307, headers=[('location', '/next'), ('set-cookie', 'one=1; Path=/'),
                ('x-repeat', 'first'), ('x-repeat', 'second')], stream=httpx.ByteStream(raw))

        async with transport_client(respond) as (transport, client, _):
            full = decode_full_response(await transport.execute(task(), 'model'))
            self.assertEqual((full.status_code, full.body), (307, raw))
            self.assertEqual([v for k, v in full.headers if k == 'x-repeat'], ['first', 'second'])
            self.assertEqual(calls, ['/start'])
            self.assertEqual(list(client.cookies.jar), [])

    async def test_cancellation_discards_only_cancelled_calls_cookie_scope(self):
        arrived, release = asyncio.Event(), asyncio.Event()
        sent = []

        async def respond(request):
            sent.append((request.url.path, request.headers.get('cookie')))
            if request.url.path == '/start' and request.content == b'0':
                return httpx.Response(307, headers=[('location', '/wait'),
                    ('set-cookie', 'cancelled=one; Path=/')], stream=httpx.ByteStream(b''))
            if request.url.path == '/wait':
                arrived.set()
                await release.wait()
            return httpx.Response(200, stream=httpx.ByteStream(b'ok'))

        async with transport_client(respond, redirects=True) as (transport, client, _):
            first = asyncio.create_task(transport.execute(task(0), 'model'))
            await asyncio.wait_for(arrived.wait(), 0.5)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            await transport.execute(task(1), 'model')
            self.assertEqual(sent, [('/start', None), ('/wait', 'cancelled=one'), ('/start', None)])
            self.assertEqual(list(client.cookies.jar), [])
        self.assertTrue(client.is_closed)

    async def test_303_keeps_normal_domain_path_and_secure_cookie_rules(self):
        redirected = []

        def respond(request):
            if request.url.path == '/start':
                return httpx.Response(303, headers=[('location', '/target'),
                    ('set-cookie', 'valid=mine; Path=/target'),
                    ('set-cookie', 'offpath=hidden; Path=/else'),
                    ('set-cookie', 'otherdomain=hidden; Domain=invalid.local; Path=/'),
                    ('set-cookie', 'secure_only=hidden; Secure; Path=/')], stream=httpx.ByteStream(b''))
            redirected.append((request.method, request.content, request.headers.get('cookie')))
            return httpx.Response(200, stream=httpx.ByteStream(b'ok'))

        async with transport_client(respond, redirects=True) as (transport, client, _):
            await transport.execute(task(), 'model')
            self.assertEqual(redirected, [('GET', b'', 'valid=mine')])
            self.assertEqual(list(client.cookies.jar), [])

    async def test_cancelling_one_chain_keeps_another_active_chains_redirect_cookie(self):
        entered = [asyncio.Event(), asyncio.Event()]
        release = asyncio.Event()
        finished = []

        async def respond(request):
            if request.url.path == '/start':
                value = request.content.decode()
                return httpx.Response(307, headers=[('location', '/hold/' + value),
                    ('set-cookie', 'current=' + value + '; Path=/')], stream=httpx.ByteStream(b''))
            if request.url.path.startswith('/hold/'):
                entered[int(request.url.path[-1])].set()
                await release.wait()
                return httpx.Response(307, headers={'location': '/done'}, stream=httpx.ByteStream(b''))
            finished.append((request.content, request.headers.get('cookie')))
            return httpx.Response(200, stream=httpx.ByteStream(b'ok'))

        async with transport_client(respond, redirects=True) as (transport, client, _):
            calls = [asyncio.create_task(transport.execute(task(i), 'model')) for i in (0, 1)]
            try:
                await asyncio.wait_for(asyncio.gather(*(event.wait() for event in entered)), 0.5)
                calls[0].cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await calls[0]
                release.set()
                await calls[1]
                self.assertEqual(finished, [(b'1', 'current=1')])
                self.assertEqual(list(client.cookies.jar), [])
            finally:
                release.set()
                for call in calls:
                    call.cancel()
                await asyncio.gather(*calls, return_exceptions=True)
