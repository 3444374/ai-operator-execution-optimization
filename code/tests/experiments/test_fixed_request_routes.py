"""Real localhost transport checks for the declared common replica mapping."""
import gzip
import json
import socket
import struct
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection, RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlsplit

from src.experiments.fixed_request_routes import FixedRequestRoutes, request_values_sha256
from src.observability.request_gateway import GatewayRoute, ObservationGateway


class FixedRequestRoutesTest(unittest.TestCase):
    def test_equivalent_values_keep_assignment_and_duplicate_keys_are_rejected(self):
        value = {"messages": [{"content": "中文", "role": "user"}], "model": "fixture"}
        original = GatewayRoute("job", "model", "http://localhost:1")
        routes = FixedRequestRoutes({"card-a": "http://localhost:2"}, {request_values_sha256(value): "card-a"})
        self.assertEqual(routes(original, json.dumps(value).encode()).endpoint_id, "card-a")
        self.assertEqual(routes(original, json.dumps(value, ensure_ascii=False).encode()).job_id, "job")
        with self.assertRaises(ValueError):
            routes(original, b'{"model":"fixture","model":"other"}')
        with self.assertRaises(KeyError):
            routes(original, b'{"model":"other"}')

    def test_manifest_rejects_invalid_or_aliased_replica(self):
        digest = request_values_sha256({"model": "fixture"})
        for endpoints, assignments in [
            ({"a": "http://localhost:1", "b": "http://localhost:1"}, {digest: "a"}),
            ({"a": "http://localhost:1"}, {digest: "b"}),
            ({"a": "http://<user>:<secret>@<replica-host>:1"}, {digest: "a"}),
        ]:
            with self.assertRaises(ValueError):
                FixedRequestRoutes(endpoints, assignments)


class FixedReplicaTransportTest(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.lock = threading.Lock()
        self.received, self.release = threading.Event(), threading.Event()
        self.mode = "complete"
        self.payload = gzip.compress(b'{"choices":[{"message":{"content":"ok"}}],"opaque":"all bytes"}')
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                with owner.lock:
                    owner.calls.append((self.server.replica_id, body))
                owner.received.set()
                self.send_response(422 if owner.mode == "http_error" else 200)
                self.send_header("Content-Encoding", "gzip")
                self.send_header("X-Repeated", "first")
                self.send_header("X-Repeated", "second")
                self.send_header("Content-Length", str(len(owner.payload)))
                self.end_headers()
                if owner.mode in ("blocked", "timeout"):
                    owner.release.wait(5)
                try:
                    self.wfile.write(owner.payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass
            def log_message(self, *args):
                pass
        self.servers, self.threads = [], []
        for name in ("card-a", "card-b"):
            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            server.replica_id = name
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start(); self.servers.append(server); self.threads.append(thread)
        self.values = [{"model": "fixture", "messages": [{"role": "user", "content": str(i)}]}
                       for i in (0, 1, 1, 3, 4, 5, 6, 7)]
        self.assignments = {request_values_sha256(v): "card-a" if i < 4 else "card-b"
                            for i, v in enumerate(self.values)}
        self.routes = FixedRequestRoutes({s.replica_id: f"http://127.0.0.1:{s.server_port}/v1/chat/completions"
                                         for s in self.servers}, self.assignments)

    def tearDown(self):
        self.release.set()
        for server in self.servers:
            server.shutdown(); server.server_close()
        for thread in self.threads:
            thread.join(5)
            self.assertFalse(thread.is_alive())

    def gateway(self, path, timeout=2):
        return ObservationGateway(routes=(GatewayRoute("job", "model", "http://localhost:1"),),
            trace_path=path, preserve_http=True, request_timeout_s=timeout, select_upstream=self.routes)

    def post(self, url, value):
        target = urlsplit(url); client = HTTPConnection(target.hostname, target.port, timeout=5)
        body = json.dumps(value, ensure_ascii=False).encode()
        try:
            client.request("POST", target.path, body, {"Content-Type": "application/json"})
            response = client.getresponse()
            return response.status, response.getheaders(), response.read(), body
        finally:
            client.close()

    def test_all_eight_occurrences_keep_fixed_assignment_complete_bytes_and_headers(self):
        with TemporaryDirectory() as directory:
            trace = Path(directory)/"trace.jsonl"
            with self.gateway(trace) as gateway:
                url = gateway.endpoint_url("job", "model")
                with ThreadPoolExecutor(max_workers=8) as pool:
                    results = list(pool.map(lambda v: self.post(url, v), reversed(self.values)))
                rows = gateway.snapshot()
            self.assertEqual(len(rows), 8)
            self.assertEqual([sum(name == card for name, _ in self.calls) for card in ("card-a", "card-b")], [4, 4])
            for status, headers, body, encoded in results:
                self.assertEqual(status, 200); self.assertEqual(body, self.payload)
                self.assertEqual([v for k, v in headers if k.lower() == "x-repeated"], ["first", "second"])
                self.assertIn((self.assignments[request_values_sha256(json.loads(encoded))], encoded), self.calls)
            self.assertTrue(all(t["retry_count"] == 0 and t["selected_replica_id"] in ("card-a", "card-b")
                                and t["request_body_sha256"] == t["forwarded_body_sha256"] for t in rows))
            self.assertIsNone(gateway._thread)

    def test_remote_http_error_is_complete_and_never_moves_to_another_replica(self):
        self.mode = "http_error"
        with TemporaryDirectory() as directory, self.gateway(Path(directory)/"trace.jsonl") as gateway:
            status, headers, payload, _ = self.post(gateway.endpoint_url("job", "model"), self.values[0])
            self.assertEqual(status, 422); self.assertEqual(payload, self.payload)
            self.assertEqual(len(self.calls), 1); self.assertEqual(self.calls[0][0], "card-a")
            self.assertEqual(gateway.snapshot()[0]["retry_count"], 0)

    def test_timeout_keeps_single_send_and_does_not_claim_remote_stop(self):
        self.mode = "timeout"
        with TemporaryDirectory() as directory, self.gateway(Path(directory)/"trace.jsonl", .15) as gateway:
            with self.assertRaises(RemoteDisconnected):
                self.post(gateway.endpoint_url("job", "model"), self.values[0])
            row = gateway.snapshot()[0]
            self.assertEqual(row["transport_error"], "TimeoutError")
            self.assertEqual(row["retry_count"], 0); self.assertEqual(len(self.calls), 1)
            self.assertFalse(self.release.is_set())  # The fixture owner still owns the blocked request.
            self.release.set()

    def test_caller_cancel_after_send_keeps_remote_responsibility_and_no_replay(self):
        self.mode = "blocked"
        with TemporaryDirectory() as directory, self.gateway(Path(directory)/"trace.jsonl") as gateway:
            target = urlsplit(gateway.endpoint_url("job", "model"))
            client = HTTPConnection(target.hostname, target.port, timeout=3)
            client.request("POST", target.path, json.dumps(self.values[0]))
            self.assertTrue(self.received.wait(3))
            client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            client.close()
            self.assertEqual(len(self.calls), 1); self.assertFalse(self.release.is_set())
            self.release.set()
            rows = gateway.snapshot()
            self.assertEqual(rows[0]["retry_count"], 0)
            self.assertEqual(len(self.calls), 1)


if __name__ == "__main__":
    unittest.main()
