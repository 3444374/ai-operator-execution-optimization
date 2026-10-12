"""HTTP client ownership on real event loops, using session/response stand-ins."""
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.baselines.text.frameworks import native_calls as native


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
