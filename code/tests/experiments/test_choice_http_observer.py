"""Real HTTP dispatch is downstream of durable attempt reservation."""
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from src.experiments.attempt_ledger import (
    AttemptBudget, AttemptLedger, BudgetError, observe_http_posts,
)


BUDGET = AttemptBudget("fixture.request-budget", 100)


class ChoiceHttpObserverTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        requests = self.requests

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                requests.append(self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(500)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def log_message(self, *_):
                pass

        self.server = HTTPServer(('127.0.0.1', 0), Handler)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.directory = tempfile.TemporaryDirectory()
        self.ledger = AttemptLedger.create(Path(self.directory.name) / 'attempts.jsonl', BUDGET)

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
                connection.request('POST', '/', body=body)
                self.assertEqual(connection.getresponse().status, 500)
        finally:
            connection.close()
        self.assertEqual(recorded, [(1, body)])
        self.assertEqual(self.requests, [body])
        self.assertEqual(self.ledger.attempts, 1)

    def test_persistence_failure_prevents_dispatch(self):
        connection = http.client.HTTPConnection(*self.server.server_address)
        try:
            with observe_http_posts(self.ledger, lambda *_: self.fail('should not observe a send')):
                with patch('os.fsync', side_effect=OSError('injected persistence failure')):
                    with self.assertRaises(BudgetError):
                        connection.request('POST', '/', body=b'{}')
        finally:
            connection.close()
        self.assertEqual(self.requests, [])
