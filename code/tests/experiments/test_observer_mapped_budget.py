"""Descriptor accounting opens at the SDK caller and survives errors without refunds."""
import asyncio
import hashlib
import json
from pathlib import Path
import pickle
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.mapped_request_budget import MappedUnitOwner
from src.experiments.native_http_observer import NativeSessionFactory, observe_native_httpx
from src.experiments.request_budget_client import open_request_budget
from src.experiments.request_identity import RAY_IDENTITY_FIELD


class MappedObserverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.owner = None
        self.addCleanup(self.close_owner)

    def close_owner(self):
        if self.owner is not None:
            self.owner.close()

    def prepare(self, body, requests=2):
        budget = AttemptBudget('mapped.observer.fixture', requests)
        self.ledger = CellBudgetLedger.create(self.root/'ledger', budget, deadline_utc=time.time()+60)
        self.ledger.reserve_unit('query', requests)
        self.owner = MappedUnitOwner.prepare(self.ledger, 'query',
            [hashlib.sha256(body).hexdigest()]*requests, self.root/'mapped')
        return pickle.loads(pickle.dumps(self.owner.descriptor))

    def test_httpx_sync_async_share_prepaid_identity_and_return_handles(self):
        body = b'{"messages":["original"]}'
        descriptor = self.prepare(body)
        events, seen = [], []
        def receive(request):
            seen.append(request.content)
            return httpx.Response(200, json={'choices':[]})
        with patch('sqlite3.connect', side_effect=AssertionError('send-time SQLite')):
            with observe_native_httpx(descriptor, events.append, 'http://localhost/fixture'):
                with httpx.Client(transport=httpx.MockTransport(receive)) as client:
                    client.post('http://localhost/fixture', content=body)
                async def call():
                    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
                        await client.post('http://localhost/fixture', content=body)
                asyncio.run(call())
        self.assertEqual(seen, [body, body])
        self.assertEqual([e['attempt'] for e in events if e['event']=='request'], [1,2])
        self.assertEqual(self.owner.client.attempts, 2)
        self.assertEqual(self.ledger.snapshot()['allocated_requests'], 2)

    def test_close_stops_an_existing_observer_without_refund(self):
        body = b'{}'
        descriptor = self.prepare(body)
        sent=[]
        with observe_native_httpx(descriptor, lambda _:None, 'http://localhost/fixture'):
            with httpx.Client(transport=httpx.MockTransport(lambda r:(sent.append(r.content),httpx.Response(200,json={}))[1])) as client:
                client.post('http://localhost/fixture',content=body)
                self.owner.client.close_budget()
                with self.assertRaises(BudgetExhausted):
                    client.post('http://localhost/fixture',content=body)
        self.assertEqual(sent,[body])
        self.assertEqual(self.owner.client.attempts,1)
        self.assertEqual(self.ledger.snapshot()['allocated_requests'],2)

    def test_cancellation_keeps_the_grant_and_original_error(self):
        descriptor=self.prepare(b'{}')
        original=asyncio.CancelledError('fixture cancellation')
        def receive(_):raise original
        async def call():
            with observe_native_httpx(descriptor,lambda _:None,'http://localhost/fixture'):
                async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
                    await client.post('http://localhost/fixture',content=b'{}')
        with self.assertRaises(asyncio.CancelledError) as caught:
            asyncio.run(call())
        self.assertIs(caught.exception,original)
        self.assertEqual(self.owner.client.attempts,1)

    def test_aiohttp_factory_opens_only_in_session_and_returns_handles_after_failure(self):
        body=b'{"model":"model"}'
        descriptor=self.prepare(body)
        (self.root/'events').mkdir()
        factory=pickle.loads(pickle.dumps(NativeSessionFactory(descriptor,str(self.root/'events'),'http://localhost/fixture',1)))
        original=ConnectionError('fixture transport error')
        session=AsyncMock();session.post.side_effect=original
        opened=[]
        async def call():
            with patch('aiohttp.ClientSession',return_value=session):
                async with factory() as observer:
                    opened.append(observer.budget)
                    data=json.dumps({'model':'model',RAY_IDENTITY_FIELD:{'row_id':'a','source_position':0}})
                    await observer.post('http://localhost/fixture',data=data,headers={})
        with self.assertRaises(ConnectionError) as caught:asyncio.run(call())
        self.assertIs(caught.exception,original)
        self.assertIsNone(opened[0]._state_fd)
        self.assertIsNone(opened[0]._lease_fd)
        self.assertIsNone(opened[0]._mapping)
        session.close.assert_awaited_once()
        self.assertEqual(self.owner.client.attempts,1)
        events=[json.loads(line) for p in (self.root/'events').glob('*.jsonl') for line in p.read_text().splitlines()]
        self.assertEqual(sum(e['event']=='request' for e in events),1)

    def test_session_startup_failure_closes_opened_mapping(self):
        descriptor=self.prepare(b'{}');(self.root/'events').mkdir()
        observed=[]
        from src.experiments.mapped_request_budget import MappedUnitClient
        def open_client(d):
            client=MappedUnitClient(d);observed.append(client);return client
        async def call():
            with patch('src.experiments.mapped_request_budget.MappedUnitClient',side_effect=open_client), \
                 patch('aiohttp.ClientSession',side_effect=OSError('fixture startup')):
                async with NativeSessionFactory(descriptor,str(self.root/'events'),'http://localhost/fixture',1)():
                    self.fail('startup should fail')
        with self.assertRaises(OSError):asyncio.run(call())
        self.assertEqual(len(observed),1)
        self.assertIsNone(observed[0]._state_fd)
        self.assertEqual(self.owner.client.attempts,0)

    def test_session_close_error_does_not_replace_cancellation_or_keep_handles(self):
        descriptor=self.prepare(b'{}');(self.root/'events').mkdir()
        original=asyncio.CancelledError('fixture cancellation')
        session=AsyncMock();session.close.side_effect=OSError('fixture cleanup')
        opened=[]
        async def call():
            with patch('aiohttp.ClientSession',return_value=session):
                async with NativeSessionFactory(descriptor,str(self.root/'events'),'http://localhost/fixture',1)() as observer:
                    opened.append(observer.budget)
                    raise original
        with self.assertRaises(asyncio.CancelledError) as caught:asyncio.run(call())
        self.assertIs(caught.exception,original)
        self.assertIsNone(opened[0]._state_fd)
        self.assertIn('OSError',original.__notes__[0])

    def test_existing_live_budget_stays_caller_owned(self):
        class Budget:
            reserve=lambda self,digest:1
            close=lambda self: (_ for _ in ()).throw(AssertionError('caller owns this budget'))
        budget=Budget()
        with open_request_budget(budget) as opened:self.assertIs(opened,budget)


if __name__=='__main__':unittest.main()
