"""HTTP client ownership on real event loops, using session/response stand-ins."""
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import native_calls as native


class NativeHttpFailureAndCookieTests(unittest.TestCase):
    def call(self, index=0, **options):
        return native.NativeCall(f'failure:{index}', index, b'{}', 'http://localhost/fixture', (), 1., **options)

    def test_standalone_capacity_error_survives_pool_close_failure(self):
        created = []
        class BrokenClose(Session):
            async def close(self):
                await super().close()
                raise OSError('pool close fixture')
        def new():
            client = native._HttpClient(asyncio.get_running_loop(), BrokenClose())
            created.append(client)
            return client
        with patch.object(native, '_new_http_client', new):
            with self.assertRaisesRegex(ValueError, 'byte allowance') as observed:
                asyncio.run(native._request(self.call(max_response_bytes=1)))
        self.assertEqual(created[0].session.closes, 1)
        self.assertIn('OSError', '\n'.join(observed.exception.__notes__))

    def test_transport_error_retains_its_cause_and_close_diagnostic(self):
        class BrokenSession(Session):
            def post(self, *args, **kwargs):
                raise ConnectionResetError('request fixture')
            async def close(self):
                await super().close()
                raise RuntimeError('pool close fixture')
        with patch.object(native, '_new_http_client', lambda: native._HttpClient(
                asyncio.get_running_loop(), BrokenSession())):
            outcome = asyncio.run(native._request(self.call()))
        self.assertIsNone(outcome.response)
        self.assertTrue(outcome.transport_error.startswith('ConnectionResetError: request fixture'))
        self.assertIn('pool close also failed: RuntimeError', outcome.transport_error)

    def test_pooled_redirect_cookies_match_request_local_default_jar(self):
        import aiohttp
        from yarl import URL
        async def run():
            client = native._new_http_client()
            try:
                for index in range(2):
                    reference = aiohttp.CookieJar()
                    async with native._http_session(client) as session:
                        jar = session.cookie_jar
                        for endpoint in ('http://localhost/fixture', 'http://127.0.0.1/fixture'):
                            url = URL(endpoint)
                            self.assertEqual(jar.filter_cookies(url), reference.filter_cookies(url))
                            cookie = {'redirect': str(index)}
                            jar.update_cookies(cookie, url)
                            reference.update_cookies(cookie, url)
                            self.assertEqual(jar.filter_cookies(url), reference.filter_cookies(url))
                        self.assertEqual(jar.filter_cookies(URL('http://localhost/final'))['redirect'].value, str(index))
                    self.assertFalse(jar.filter_cookies(URL('http://localhost/final')))
            finally:
                await client.close()
        asyncio.run(run())

    def test_concurrent_calls_keep_distinct_redirect_cookies_on_one_session(self):
        from yarl import URL
        async def run():
            client = native._new_http_client()
            both_ready, ready = asyncio.Event(), []
            url = URL('http://localhost/fixture')
            async def request(index):
                async with native._http_session(client) as session:
                    self.assertIs(session, client.session)
                    self.assertFalse(session.cookie_jar.filter_cookies(url))
                    session.cookie_jar.update_cookies({'redirect': str(index)}, url)
                    ready.append(index)
                    if len(ready) == 2:
                        both_ready.set()
                    await both_ready.wait()
                    self.assertEqual(session.cookie_jar.filter_cookies(url)['redirect'].value, str(index))
            try:
                await asyncio.gather(request(0), request(1))
                self.assertFalse(client.session.cookie_jar.filter_cookies(url))
            finally:
                await client.close()
        asyncio.run(run())

    def test_raw_set_cookie_headers_match_default_jar_and_stay_call_local(self):
        import aiohttp
        from yarl import URL
        async def run():
            client=native._new_http_client()
            both_ready,ready=asyncio.Event(),[]
            url=URL('http://localhost/redirect')
            async def request(index):
                async with native._http_session(client) as session:
                    reference=aiohttp.CookieJar()
                    headers=[f'sid={index}; Path=/', 'quoted="two words"; Path=/']
                    session.cookie_jar.update_cookies_from_headers(headers,url)
                    reference.update_cookies_from_headers(headers,url)
                    self.assertEqual(session.cookie_jar.filter_cookies(url),reference.filter_cookies(url))
                    ready.append(index)
                    if len(ready)==2:both_ready.set()
                    await both_ready.wait()
                    self.assertEqual(session.cookie_jar.filter_cookies(url)['sid'].value,str(index))
            try:
                await asyncio.gather(request(0),request(1))
                self.assertFalse(client.session.cookie_jar.filter_cookies(url))
                async with native._http_session(client) as session:
                    ip=URL('http://127.0.0.1/redirect')
                    session.cookie_jar.update_cookies_from_headers(['sid=ip; Path=/'],ip)
                    self.assertFalse(session.cookie_jar.filter_cookies(ip))
            finally:await client.close()
        asyncio.run(run())


class Session:
    def __init__(self):
        self.closed, self.requests, self.closes = False, [], 0

    def post(self, endpoint, **kwargs):
        self.requests.append(kwargs)
        return Response(kwargs['data'])

    async def close(self):
        self.closes += 1
        self.closed = True


class Response:
    status, raw_headers = 200, ()
    version = SimpleNamespace(major=1, minor=1)

    def __init__(self, body):
        self.body, self.content = body, self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *error):
        return False

    async def iter_chunked(self, size):
        yield self.body


class NativeHttpLifecycleTests(unittest.TestCase):
    def test_real_keepalive_preserves_occurrences_and_reuses_connection(self):
        requests = []
        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def do_POST(self):
                raw = self.rfile.read(int(self.headers['Content-Length']))
                requests.append((self.client_address, raw))
                self.send_response(200)
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        control = native._GraphControl()
        values = ('first', 'duplicate', 'tail', 'duplicate')
        async def run():
            try:
                for index, text in enumerate(values):
                    call = native.NativeCall(f'keepalive:{index}', index,
                        json.dumps(dict(text=text)).encode(), f'http://127.0.0.1:{server.server_port}/fixture', (), 1.)
                    outcome = await native._request(call, control)
                    self.assertEqual(outcome.row_id, index)
                    self.assertEqual(outcome.call_id, call.call_id)
                    self.assertEqual(json.loads(outcome.response.body)['text'], text)
            finally:
                control.stop()
                await asyncio.gather(*(asyncio.wrap_future(f) for f in control._client_closes.values()))
        try:
            asyncio.run(run())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        control.wait_clients_closed(0)
        self.assertEqual(len(requests), 4)
        self.assertEqual(len({address for address, _ in requests}), 1)
        self.assertEqual(len(control._clients), 1)

    def call(self, index):
        return native.NativeCall(f'lifecycle:{index}', index, json.dumps(dict(index=index)).encode(),
            'http://localhost/fixture', (), .1 + index * .1)

    def factory(self, created):
        def new():
            session = Session()
            client = native._HttpClient(asyncio.get_running_loop(), session)
            created.append(client)
            return client
        return new

    def test_graph_reuses_one_client_with_per_call_timeouts_and_closes(self):
        created, control = [], native._GraphControl()
        async def run():
            for i in range(2):
                outcome = await native._request(self.call(i), control)
                self.assertEqual(json.loads(outcome.response.body), dict(index=i))
            self.assertEqual(len(created), 1)
            self.assertEqual([r['timeout'].total for r in created[0].session.requests], [.1,.2])
            self.assertFalse(created[0].closed)
            control.stop()
            await asyncio.gather(*(asyncio.wrap_future(f) for f in control._client_closes.values()))
        with patch.object(native, '_new_http_client', self.factory(created)):
            asyncio.run(run())
        control.wait_clients_closed(0)
        self.assertTrue(created[0].closed)
        self.assertEqual(created[0].session.closes, 1)

    def test_ray_batch_factory_reuses_client_and_closes_on_error(self):
        created = []
        primary = ValueError('fixture parser')
        async def run():
            try:
                async with native._ProcessorSession() as session:
                    for i in range(2):
                        carrier = await session.post(native._PROCESSOR_URL,
                            data=json.dumps(dict(native_call=native._call_wire(self.call(i)))), headers={})
                        outcome = native._outcome_from_wire((await carrier.json())['native_outcome'])
                        self.assertEqual(json.loads(outcome.response.body), dict(index=i))
                    raise primary
            except ValueError as error:
                self.assertIs(error, primary)
        with patch.object(native, '_new_http_client', self.factory(created)):
            asyncio.run(run())
        self.assertEqual(len(created), 1)
        self.assertTrue(created[0].closed)
        self.assertEqual(created[0].session.closes, 1)

    def test_no_client_crosses_event_loops(self):
        created = []
        async def create():
            return self.factory(created)()
        client = asyncio.run(create())
        async def use():
            with self.assertRaisesRegex(RuntimeError, 'different event loop'):
                await native._request(self.call(0), client=client)
        asyncio.run(use())
        self.assertEqual(client.session.requests, [])

    def test_stopped_control_sends_no_request(self):
        created, control = [], native._GraphControl()
        control.stop()
        with patch.object(native, '_new_http_client', self.factory(created)):
            outcome = asyncio.run(native._request(self.call(0), control))
        self.assertEqual(outcome.transport_error, 'NativeCallCancelled')
        self.assertEqual(created, [])

    def test_owner_loop_exit_before_close_is_reported(self):
        control, created = native._GraphControl(), []
        async def run():
            control.client()
        with patch.object(native, '_new_http_client', self.factory(created)):
            asyncio.run(run())
        with self.assertRaisesRegex(RuntimeError, 'owner loop stopped'):
            control.stop()
        with self.assertRaisesRegex(RuntimeError, 'unconfirmed'):
            control.wait_clients_closed(0)


if __name__ == '__main__':
    unittest.main()
