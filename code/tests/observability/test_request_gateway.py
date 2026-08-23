from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib import error, request

from src.observability.request_gateway import (
    GatewayRoute,
    ObservationGateway,
)


class _UpstreamHandler(BaseHTTPRequestHandler):
    calls: list[tuple[str, bytes]] = []
    response_status = 200

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        body = self.rfile.read(int(self.headers["Content-Length"]))
        type(self).calls.append((self.path, body))
        payload = json.dumps(
            {
                "choices": [
                    {
                        "message": {"content": "ok"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 7,
                    "total_tokens": 18,
                },
            }
        ).encode("utf-8")
        self.send_response(type(self).response_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class ObservationGatewayTest(unittest.TestCase):
    def setUp(self) -> None:
        _UpstreamHandler.calls = []
        _UpstreamHandler.response_status = 200
        self.upstream = ThreadingHTTPServer(
            ("127.0.0.1", 0), _UpstreamHandler
        )
        self.thread = threading.Thread(
            target=self.upstream.serve_forever, daemon=True
        )
        self.thread.start()

    def tearDown(self) -> None:
        self.upstream.shutdown()
        self.upstream.server_close()
        self.thread.join(timeout=5)

    def _upstream_url(self) -> str:
        return (
            f"http://127.0.0.1:{self.upstream.server_port}"
            "/v1/chat/completions"
        )

    def test_forwards_exact_body_once_and_records_actual_usage(self) -> None:
        with TemporaryDirectory() as directory:
            trace = Path(directory) / "gateway.jsonl"
            body = json.dumps(
                {"model": "m", "messages": [{"role": "user", "content": "p"}]}
            ).encode("utf-8")
            with ObservationGateway(
                routes=(GatewayRoute("job0", "endpoint-0", self._upstream_url()),),
                trace_path=trace,
            ) as gateway:
                endpoint = gateway.endpoint_url("job0", "endpoint-0")
                response = request.urlopen(
                    request.Request(
                        endpoint,
                        data=body,
                        headers={"Content-Type": "application/json"},
                        method="POST",
                    ),
                    timeout=5,
                )
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read())["usage"]["total_tokens"], 18)

            self.assertEqual(_UpstreamHandler.calls, [("/v1/chat/completions", body)])
            rows = [json.loads(line) for line in trace.read_text().splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["job_id"], "job0")
            self.assertEqual(rows[0]["endpoint_id"], "endpoint-0")
            self.assertEqual(rows[0]["retry_count"], 0)
            self.assertEqual(rows[0]["request_body_sha256"], rows[0]["forwarded_body_sha256"])
            self.assertEqual(rows[0]["actual_total_tokens"], 18)
            self.assertGreaterEqual(rows[0]["dispatch_delay_s"], 0.0)

    def test_upstream_failure_is_forwarded_without_retry(self) -> None:
        _UpstreamHandler.response_status = 503
        with TemporaryDirectory() as directory:
            trace = Path(directory) / "gateway.jsonl"
            with ObservationGateway(
                routes=(GatewayRoute("job1", "endpoint-0", self._upstream_url()),),
                trace_path=trace,
            ) as gateway:
                with self.assertRaises(error.HTTPError) as raised:
                    request.urlopen(
                        request.Request(
                            gateway.endpoint_url("job1", "endpoint-0"),
                            data=b"{}",
                            headers={"Content-Type": "application/json"},
                            method="POST",
                        ),
                        timeout=5,
                    )
                self.assertEqual(raised.exception.code, 503)

            self.assertEqual(len(_UpstreamHandler.calls), 1)
            row = json.loads(trace.read_text().strip())
            self.assertEqual(row["upstream_status"], 503)
            self.assertEqual(row["retry_count"], 0)


if __name__ == "__main__":
    unittest.main()
