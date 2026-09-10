"""Native SDK observation preserves request content and first transport errors."""
import asyncio
import unittest
import httpx
from src.experiments.native_http_observer import observe_native_httpx


class Budget:
    def __init__(self):self.calls=[]
    def reserve(self,digest):self.calls.append(digest);return len(self.calls)


class NativeHttpObserverTests(unittest.TestCase):
    def test_sync_async_body_and_effective_deadline(self):
        events=[];budget=Budget();seen=[]
        def receive(request):
            seen.append(request)
            return httpx.Response(200,json={'choices':[]})
        with observe_native_httpx(budget,events.append,'http://localhost/v1/chat/completions',timeout_s=1):
            with httpx.Client(transport=httpx.MockTransport(receive)) as client:
                client.post('http://localhost/v1/chat/completions',json={'messages':['original']})
            async def call():
                async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
                    await client.post('http://localhost/v1/chat/completions',json={'messages':['original']})
            asyncio.run(call())
        self.assertEqual(len(budget.calls),2)
        self.assertEqual(seen[0].content,seen[1].content)
        self.assertLessEqual(seen[0].extensions['timeout']['read'],1)
        self.assertEqual(sum(e['event']=='http_finished' for e in events),2)

    def test_recording_error_does_not_replace_transport_failure(self):
        original=httpx.ConnectError('first transport error')
        def receive(request):raise original
        def record(event):
            if event['event']=='http_finished':raise OSError('secondary recording failure')
        with observe_native_httpx(Budget(),record,'http://localhost/v1/chat/completions'):
            with httpx.Client(transport=httpx.MockTransport(receive)) as client:
                with self.assertRaises(httpx.ConnectError) as caught:
                    client.post('http://localhost/v1/chat/completions',json={})
        self.assertIs(caught.exception,original)

    def test_unexpected_destination_and_expired_deadline_do_not_spend(self):
        budget=Budget()
        with httpx.Client(transport=httpx.MockTransport(lambda _:self.fail('no I/O'))) as client:
            with observe_native_httpx(budget,lambda _:None,'http://localhost/v1/chat/completions',timeout_s=0):
                with self.assertRaises(TimeoutError):client.post('http://localhost/v1/chat/completions',json={})
                with self.assertRaises(ValueError):client.post('http://localhost/other',json={})
        self.assertEqual(budget.calls,[])
