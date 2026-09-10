"""Verify request identity before transport and separate private/public event values."""

import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import time
from unittest.mock import patch

from src.baselines.common.private_artifacts import content_digest
from src.experiments.attempt_ledger import AttemptBudget, AttemptLedger, BudgetError
from src.experiments.choice_gateway_observer import main
from src.experiments.expected_requests import ExpectedRequests, expected_request_manifest
from src.experiments.cell_budget import CellBudgetLedger


class ExpectedRequestTests(unittest.TestCase):
    def test_buffered_cell_observer_never_opens_sqlite_during_send(self):
        import httpx
        body = {'model': 'fixture', 'messages': []}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = CellBudgetLedger.create(root / 'budget.sqlite', AttemptBudget('fixture', 2), deadline_utc=time.time()+60)
            ledger.reserve_unit('two', 2)
            (root / 'expected.json').write_text(json.dumps(expected_request_manifest([body, body])))
            async def send():
                async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200))) as client:
                    await asyncio.gather(*(client.post('http://localhost/fixture', json=body) for _ in range(2)))
            def serve(*_args, **_kwargs):
                with patch('sqlite3.connect', side_effect=AssertionError('SQLite in send path')):
                    asyncio.run(send())
                return 0
            args = ['--events', str(root/'events.jsonl'), '--cell-budget', str(ledger.path), '--unit-id', 'two',
                    '--budget-id', 'fixture', '--max-attempts', '2', '--event-mode', 'compact-buffered',
                    '--observer-summary', str(root/'observer.json'), '--expected-request-hashes', str(root/'expected.json'),
                    '--', '--incremental-map']
            with patch('src.experiments.choice_gateway_observer.server.main', side_effect=serve):
                self.assertEqual(main(args), 0)
            summary = json.loads((root/'observer.json').read_text())
            self.assertEqual(summary['observed_attempts'], 2)
            self.assertTrue(summary['events']['closed'])

    def test_multiset_allows_reorder_but_not_extra_calls(self):
        a, b = {"messages": [{"role": "user", "content": "a"}]}, {"messages": [{"role": "user", "content": "b"}]}
        guard = ExpectedRequests(expected_request_manifest([a, a, b]), available_attempts=3)
        for body in (b, a, a):
            guard.accept(body)
        self.assertEqual(guard.remaining, 0)
        with self.assertRaises(BudgetError):
            guard.accept(a)

    def test_budget_and_complete_values_are_checked(self):
        body = {"messages": [{"role": "user", "content": "raw"}], "max_tokens": 128}
        manifest = expected_request_manifest([body])
        with self.assertRaises(ValueError):
            ExpectedRequests(manifest, available_attempts=0)
        for changed in (dict(body, max_tokens=256), dict(body, messages=[{"role": "user", "content": "cut"}])):
            with self.subTest(changed=changed), self.assertRaises(BudgetError):
                ExpectedRequests(manifest, available_attempts=1).accept(changed)

    def test_observer_guards_before_send_and_preserves_private_payload(self):
        try:
            import httpx
        except ImportError:
            self.skipTest("httpx unavailable")
        original = {"model": "fixture", "messages": [{"role": "user", "content": "password=fixture-only"}], "max_tokens": 128}
        for changed in (False, True):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ledger = AttemptLedger.create(root / "ledger.jsonl", AttemptBudget("fixture", 1))
                manifest = root / "expected.json"
                manifest.write_text(json.dumps(expected_request_manifest([original])))
                received = []
                actual = dict(original, max_tokens=256) if changed else original
                payload = json.dumps(actual).encode()

                async def send():
                    def receive(request):
                        received.append(request.content)
                        return httpx.Response(200, json={})
                    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
                        await client.post("http://localhost/fixture", content=payload)

                def serve(*_args, **_kwargs):
                    asyncio.run(send())
                    return 0

                args = ["--events", str(root / "public.jsonl"), "--private-events", str(root / "private.jsonl"),
                    "--ledger", str(root / "ledger.jsonl"), "--budget-id", "fixture", "--max-attempts", "1",
                    "--expected-request-hashes", str(manifest), "--", "--incremental-map"]
                with patch("src.experiments.choice_gateway_observer.server.main", side_effect=serve):
                    if changed:
                        with self.assertRaises(BudgetError):
                            main(args)
                    else:
                        self.assertEqual(main(args), 0)
                self.assertEqual(ledger.attempts, 1)
                self.assertEqual(len(received), 0 if changed else 1)
                public = [json.loads(line) for line in (root / "public.jsonl").read_text().splitlines()]
                private = [json.loads(line) for line in (root / "private.jsonl").read_text().splitlines()]
                self.assertEqual(public[0]["request_bytes_sha256"], hashlib.sha256(payload).hexdigest())
                self.assertEqual(public[0]["request_values_sha256"], content_digest(actual))
                self.assertNotIn("fixture-only", (root / "public.jsonl").read_text())
                if not changed:
                    self.assertEqual(private[0]["body"], original)


if __name__ == "__main__":
    unittest.main()
