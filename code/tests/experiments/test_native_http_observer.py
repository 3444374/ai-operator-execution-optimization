"""Native SDK observation preserves request content and first transport errors."""
import asyncio
import unittest
import httpx
from src.experiments.native_http_observer import observe_native_httpx
from src.experiments.native_service_query import NativeServiceCalls
from types import SimpleNamespace


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

    def test_optional_response_recording_defers_parsing_without_copying_or_replacing_native_body(self):
        events=[];budget=Budget();native=[];captured=[]
        audit=NativeServiceCalls(SimpleNamespace(record=events.append),'openai/model')
        class Response(httpx.Response):
            def json(self, **kwargs):
                raise AssertionError('the optional observer parsed the native response')
        def receive(request):
            value=Response(200,json=dict(id='response-'+str(len(native)),model='model',choices=[{}]))
            native.append(value)
            return value
        def capture(event, content):
            captured.append((event,content))
            audit.record_response(event,content)
        with observe_native_httpx(budget,audit.record,'http://localhost/v1/chat/completions',record_response=capture):
            with httpx.Client(transport=httpx.MockTransport(receive)) as client:
                self.assertIs(client.post('http://localhost/v1/chat/completions',json={}),native[-1])
            async def call():
                async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
                    self.assertIs(await client.post('http://localhost/v1/chat/completions',json={}),native[-1])
            asyncio.run(call())
        self.assertEqual(len(captured),2)
        self.assertFalse(any(e['event']=='http_response' for e in events))
        for (event,content),response in zip(captured,native,strict=True):
            self.assertIs(content,response.content)
            finished=next(e for e in events if e['event']=='http_finished' and e['attempt']==event['attempt'])
            self.assertLessEqual(event['monotonic_ns'],finished['monotonic_ns'])
        self.assertEqual(audit.snapshot(),dict(attempts=2,succeeded=2,failed=0,unknown=0,unfinished=0))
        self.assertEqual([e['response']['id'] for e in events if e['event']=='http_response'],['response-0','response-1'])

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
