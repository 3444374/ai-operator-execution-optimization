"""Model transport failures retain diagnostic type without private request data."""
import json
import unittest

import httpx

from src.execution_provider.adapters.async_fixed_model import AsyncFixedModelTransport
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.scheduling.core.session_contract import BackendTask, OfferedTask, SessionSpec, TaskKey


class AsyncTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_failure_reports_type_once_without_message_or_body(self):
        events = []
        attempts = []
        error = httpx.RemoteProtocolError('private endpoint and response details')
        async def fail(request):
            attempts.append(request)
            raise error
        transport = AsyncFixedModelTransport(FixedModelConfig('http://localhost/model', 'fixture', 1000), 1, events.append)
        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
        task = BackendTask(TaskKey(0, 1), SessionSpec('job', 'flow', 'fixture'),
                           OfferedTask(1, b'private prompt', 1, 1024))
        try:
            with self.assertRaises(httpx.RemoteProtocolError) as raised:
                await transport.execute(task, 'model')
            self.assertIs(raised.exception, error)
            self.assertEqual(len(attempts), 1)
            failures = [e for e in events if e['event'] == 'http_error']
            self.assertEqual(len(failures), 1)
            self.assertEqual(failures[0]['reason']['exception_type'], 'httpx.RemoteProtocolError')
            self.assertEqual(failures[0]['key'], {'session_id': 0, 'sequence': 1})
            self.assertNotIn('private', json.dumps(failures))
        finally:
            await transport.close()

    async def test_error_causes_are_bounded_and_observer_failure_preserves_original(self):
        events = []
        causes = [OSError('private cause') for _ in range(6)]
        for left, right in zip(causes, causes[1:]):
            left.__cause__ = right
        causes[-1].__cause__ = causes[0]
        error = httpx.ReadError('private top-level message')
        error.__cause__ = causes[0]
        def observe(event):
            if event['event'] == 'http_error':
                events.append(event)
                raise RuntimeError('secondary observer failure')
        async def fail(request):
            raise error
        transport = AsyncFixedModelTransport(FixedModelConfig('http://localhost/model', 'fixture', 1000), 1, observe)
        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
        task = BackendTask(TaskKey(0, 1), SessionSpec('job', 'flow', 'fixture'),
                           OfferedTask(1, b'private prompt', 1, 1024))
        try:
            with self.assertRaises(httpx.ReadError) as raised:
                await transport.execute(task, 'model')
            self.assertIs(raised.exception, error)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]['reason']['cause_types'], ['builtins.OSError'] * 4)
            self.assertNotIn('private', json.dumps(events))
            self.assertNotIn('/', events[0]['reason']['origin']['file'])
        finally:
            await transport.close()

    async def test_success_keeps_response_and_has_no_error_event(self):
        events = []
        async def reply(request):
            return httpx.Response(200, stream=httpx.ByteStream(b'fixture reply'))
        transport = AsyncFixedModelTransport(FixedModelConfig('http://localhost/model', 'fixture', 1000), 1, events.append)
        transport._client = httpx.AsyncClient(transport=httpx.MockTransport(reply))
        task = BackendTask(TaskKey(0, 1), SessionSpec('job', 'flow', 'fixture'),
                           OfferedTask(1, b'input', 1, 1024))
        try:
            self.assertEqual(await transport.execute(task, 'model'), b'fixture reply')
            self.assertEqual([e['event'] for e in events], ['http_started', 'http_finished'])
        finally:
            await transport.close()
