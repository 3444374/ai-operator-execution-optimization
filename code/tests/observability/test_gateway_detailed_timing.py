"""Optional gateway clocks use localhost HTTP, never a model service."""
import asyncio
from http.client import RemoteDisconnected
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib import error, request
from unittest.mock import patch

from src.experiments import execution_timing
from src.observability import request_gateway
from tests.observability.test_request_gateway import _UpstreamHandler


class GatewayDetailedTimingTest(unittest.TestCase):
    def setUp(self):
        _UpstreamHandler.calls = []
        _UpstreamHandler.response_status = 200
        _UpstreamHandler.response_gate = None
        _UpstreamHandler.headers_sent = None
        _UpstreamHandler.two_requests_received = None
        _UpstreamHandler.response_padding_bytes = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _UpstreamHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def gateway(self, path, **kwargs):
        return request_gateway.ObservationGateway(
            routes=(request_gateway.GatewayRoute("job", "model",
                f"http://127.0.0.1:{self.server.server_port}/v1/chat/completions"),),
            trace_path=path, **kwargs)

    @staticmethod
    def send(gateway):
        with request.urlopen(request.Request(gateway.endpoint_url("job", "model"),
                                             data=b'{"fixture":"original bytes"}'), timeout=5) as response:
            return response.status, response.read()

    @staticmethod
    def read_events(root):
        return [json.loads(line) for path in Path(root).glob("timing-*.jsonl")
                for line in path.read_text().splitlines()]

    def test_default_off_preserves_trace_fields_and_http_bytes(self):
        with TemporaryDirectory() as root, \
                patch.dict(os.environ, {"SEMLOOM_DETAILED_TIMING_DIR": ""}):
            path = Path(root) / "gateway.jsonl"
            with self.gateway(path) as gateway:
                status, body = self.send(gateway)
            row = json.loads(path.read_text())
            self.assertEqual(self.read_events(root), [])
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["choices"][0]["message"]["content"], "ok")
        self.assertEqual(_UpstreamHandler.calls[0][1], b'{"fixture":"original bytes"}')
        self.assertEqual(len(_UpstreamHandler.calls), 1)
        self.assertFalse({"pid", "clock_domain", "run_id", "owner_id",
                          "detailed_timing_request_id", "detailed_timing_recorded",
                          "upstream_response_headers_received_monotonic_ns"} & row.keys())

    def test_enabled_reuses_exact_clocks_and_live_query_identity_after_callbacks(self):
        query = ["query-a"]
        observed = []
        with TemporaryDirectory() as root, \
                patch.dict(os.environ, {"SEMLOOM_DETAILED_TIMING_DIR": root,
                                        "SEMLOOM_TIMING_RUN_ID": "fixture-run"}), \
                patch.object(execution_timing, "_shared", None):
            def before(_route, _body):
                # No diagnostic events are published while the callback may
                # own a transaction lock.
                observed.append(execution_timing._shared.writer.snapshot()["accepted_events"])

            path = Path(root) / "gateway.jsonl"
            try:
                with self.gateway(path, query_identity=lambda: query[0], before_forward=before) as gateway:
                    first = self.send(gateway)
                    query[0] = "query-b"
                    second = self.send(gateway)
            finally:
                execution_timing.close_timing()
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            events = self.read_events(root)
        self.assertEqual(first, second)
        self.assertEqual(observed[0], 0)
        self.assertEqual(len(_UpstreamHandler.calls), 2)
        for row in rows:
            selected = [event for event in events if event["gateway_request_id"] == row["gateway_request_id"]]
            self.assertTrue(all(event["query_id"] == row["query_id"] for event in selected))
            self.assertTrue(all(event["request_id"] == row["detailed_timing_request_id"] for event in selected))
            self.assertTrue(all(event["clock_domain"] == row["clock_domain"] and event["pid"] == row["pid"]
                                and event["run_id"] == "fixture-run" for event in selected))
            intervals = {}
            for event in selected:
                if event["event"].startswith("span_"):
                    intervals.setdefault(event["operation"], []).append(event["monotonic_ns"])
            self.assertEqual(intervals["gateway.handler_to_terminal"],
                             [row["received_monotonic_ns"], row["request_terminal_monotonic_ns"]])
            self.assertEqual(intervals["gateway.upstream_dispatch_to_body_read"],
                             [row["upstream_dispatch_started_monotonic_ns"],
                              row["upstream_response_body_read_completed_monotonic_ns"]])
            self.assertEqual(intervals["gateway.local_response_write"],
                             [row["response_write_started_monotonic_ns"], row["response_write_completed_monotonic_ns"]])
            self.assertLessEqual(row["upstream_headers_send_started_monotonic_ns"],
                                 row["upstream_response_headers_received_monotonic_ns"])
            self.assertLessEqual(row["upstream_response_headers_received_monotonic_ns"],
                                 row["upstream_response_body_read_completed_monotonic_ns"])
        self.assertNotEqual(rows[0]["detailed_timing_request_id"], rows[1]["detailed_timing_request_id"])

    def test_timeout_keeps_unfinished_upstream_span_and_original_http_failure(self):
        _UpstreamHandler.response_gate = threading.Event()
        with TemporaryDirectory() as root, \
                patch.dict(os.environ, {"SEMLOOM_DETAILED_TIMING_DIR": root}), \
                patch.object(execution_timing, "_shared", None):
            path = Path(root) / "gateway.jsonl"
            try:
                with self.gateway(path, request_timeout_s=.1) as gateway:
                    with self.assertRaises(error.HTTPError) as raised:
                        self.send(gateway)
                    self.assertEqual(raised.exception.code, 502)
            finally:
                _UpstreamHandler.response_gate.set()
                execution_timing.close_timing()
            row = json.loads(path.read_text())
            events = self.read_events(root)
        upstream = [event for event in events if event["operation"] == "gateway.upstream_dispatch_to_body_read"]
        self.assertEqual(len(upstream), 1)
        self.assertEqual(upstream[0]["event"], "span_started")
        self.assertEqual(upstream[0]["error_type"], row["error_type"])
        self.assertEqual(upstream[0]["status"], "failed")
        self.assertIsNone(row["upstream_response_body_read_completed_monotonic_ns"])
        self.assertEqual(row["response_write_status"], "completed")
        self.assertEqual(row["retry_count"], 0)
        self.assertEqual(len(_UpstreamHandler.calls), 1)

    def test_cancellation_keeps_terminal_identity_without_a_response_completion(self):
        def cancel(_route, _request, _response, _status):
            raise asyncio.CancelledError("fixture cancellation")

        with TemporaryDirectory() as root, \
                patch.dict(os.environ, {"SEMLOOM_DETAILED_TIMING_DIR": root}), \
                patch.object(execution_timing, "_shared", None):
            path = Path(root) / "gateway.jsonl"
            try:
                with self.gateway(path, after_forward=cancel, query_identity=lambda: "cancelled-query") as gateway:
                    with self.assertRaises(RemoteDisconnected):
                        self.send(gateway)
            finally:
                execution_timing.close_timing()
            row = json.loads(path.read_text())
            events = self.read_events(root)
        terminal = [event for event in events if event["operation"] == "gateway.request_terminal"]
        self.assertEqual(len(terminal), 1)
        self.assertEqual((terminal[0]["query_id"], terminal[0]["status"], terminal[0]["error_type"]),
                         ("cancelled-query", "cancelled", "CancelledError"))
        self.assertIsNone(row["response_write_completed_monotonic_ns"])
        self.assertFalse(any(event["operation"] == "gateway.local_response_write" for event in events))
        self.assertEqual(len(_UpstreamHandler.calls), 1)

    def test_observation_failure_preserves_active_original_exception_and_cleanup(self):
        class FailedTiming:
            pid = os.getpid()
            clock_domain, clock_verified, run_id = "fixture-clock", False, "fixture-run"
            def record_interval(self, *_args, **_kwargs):
                raise OSError("fixture observation failure")

        for primary in (None, ValueError("fixture original failure")):
            with self.subTest(primary=type(primary).__name__), TemporaryDirectory() as root, \
                    patch.object(request_gateway, "get_timing", return_value=FailedTiming()):
                path = Path(root) / "gateway.jsonl"
                with self.assertRaises(type(primary) if primary else RuntimeError) as raised:
                    with self.gateway(path) as gateway:
                        self.assertEqual(self.send(gateway)[0], 200)
                        if primary:
                            raise primary
                row = json.loads(path.read_text())
            if primary:
                self.assertIs(raised.exception, primary)
                self.assertTrue(any("Gateway cleanup" in note for note in primary.__notes__))
            else:
                self.assertIsInstance(raised.exception.__cause__, OSError)
            self.assertEqual(row["status"], "completed")
            self.assertEqual(row["detailed_timing_error_type"], "OSError")


if __name__ == "__main__":
    unittest.main()
