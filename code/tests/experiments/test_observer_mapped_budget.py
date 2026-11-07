"""Descriptor accounting opens at the SDK caller and survives errors without refunds."""
import asyncio
import hashlib
from pathlib import Path
import pickle
import tempfile
import time
import unittest
from unittest.mock import patch

import httpx

from src.experiments.attempt_ledger import AttemptBudget, BudgetExhausted
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.mapped_request_budget import MappedUnitOwner
from src.experiments.native_http_observer import observe_native_httpx
from src.experiments.request_budget_client import open_request_budget


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


    def test_existing_live_budget_stays_caller_owned(self):
        class Budget:
            reserve=lambda self,digest:1
            close=lambda self: (_ for _ in ()).throw(AssertionError('caller owns this budget'))
        budget=Budget()
        with open_request_budget(budget) as opened:self.assertIs(opened,budget)


if __name__=='__main__':unittest.main()
