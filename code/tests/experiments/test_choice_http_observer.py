"""Real HTTP dispatch is downstream of durable attempt reservation."""

import http.client
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from src.experiments.attempt_ledger import (
    AttemptBudget,
    AttemptLedger,
    BudgetError,
    observe_http_posts,
)


BUDGET = AttemptBudget("fixture.request-budget", 100)


class GatewayEventTimingTests(unittest.TestCase):
    def test_incremental_events_have_observer_timestamps(self):
        from src.experiments.choice_gateway_observer import main

        def emit(_argv, **options):
            options["incremental_observer"]({"event": "http_started", "key": {"sequence": 0}})
            return 0

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            before = time.monotonic_ns()
            with patch("src.experiments.choice_gateway_observer.server.main", side_effect=emit):
                self.assertEqual(main(["--events", str(path), "--fixture-only", "--", "--incremental-map"]), 0)
            event = json.loads(path.read_text())
            self.assertEqual(event["event"], "core_http_started")
            self.assertGreaterEqual(event["monotonic_ns"], before)
            self.assertLessEqual(event["monotonic_ns"], time.monotonic_ns())


class ChoiceHttpObserverTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        requests = self.requests

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                requests.append(self.rfile.read(int(self.headers["Content-Length"])))
                self.send_response(500)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *_):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.directory = tempfile.TemporaryDirectory()
        self.ledger = AttemptLedger.create(Path(self.directory.name) / "attempts.jsonl", BUDGET)

    def tearDown(self):
        self.server.shutdown()
        self.worker.join()
        self.server.server_close()
        self.directory.cleanup()

    def test_reserves_before_dispatch_and_preserves_actual_body(self):
        recorded = []

        def record(attempt, body):
            self.assertEqual(self.ledger.attempts, 1)
            self.assertEqual(self.requests, [])
            recorded.append((attempt, body))

        body = b'{"structured_outputs":{"choice":["TRUE","FALSE","UNKNOWN"]}}'
        connection = http.client.HTTPConnection(*self.server.server_address)
        try:
            with observe_http_posts(self.ledger, record):
                connection.request("POST", "/", body=body)
                self.assertEqual(connection.getresponse().status, 500)
        finally:
            connection.close()
        self.assertEqual(recorded, [(1, body)])
        self.assertEqual(self.requests, [body])
        self.assertEqual(self.ledger.attempts, 1)

    def test_async_transport_uses_the_same_durable_budget(self):
        import asyncio

        try:
            import httpx
        except ImportError:
            self.skipTest("httpx unavailable")
        from src.experiments.attempt_ledger import observe_async_http_posts

        recorded = []

        async def run():
            async with httpx.AsyncClient(trust_env=False) as client:
                with observe_async_http_posts(
                    self.ledger, lambda n, body: recorded.append((n, body))
                ):
                    response = await client.post(
                        f"http://127.0.0.1:{self.server.server_port}/", content=b"body"
                    )
                    self.assertEqual(response.status_code, 500)
                with observe_async_http_posts(self.ledger, lambda *_: None):
                    with patch.object(self.ledger, "reserve", side_effect=BudgetError("failed")):
                        with self.assertRaises(BudgetError):
                            await client.post(
                                f"http://127.0.0.1:{self.server.server_port}/", content=b"blocked"
                            )

        asyncio.run(run())
        self.assertEqual(recorded, [(1, b"body")])
        self.assertEqual(self.requests, [b"body"])

    def test_persistence_failure_prevents_dispatch(self):
        connection = http.client.HTTPConnection(*self.server.server_address)
        try:
            with observe_http_posts(self.ledger, lambda *_: self.fail("should not observe a send")):
                with patch("os.fsync", side_effect=OSError("injected persistence failure")):
                    with self.assertRaises(BudgetError):
                        connection.request("POST", "/", body=b"{}")
        finally:
            connection.close()
        self.assertEqual(self.requests, [])
