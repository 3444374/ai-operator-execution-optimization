"""Bounded localhost full-response fixture; no model or shared runtime."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time


@contextmanager
def fixture_server(*, http_status=200, delay=0.005, content="ok"):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            time.sleep(delay)
            value = dict(
                id="fixture-response",
                object="chat.completion",
                created=0,
                model="fixture",
                choices=[
                    dict(
                        index=0,
                        message=dict(role="assistant", content=content),
                        finish_reason="stop",
                    )
                ],
                usage=dict(prompt_tokens=2, completion_tokens=1, total_tokens=3),
            )
            raw = json.dumps(value).encode()
            self.send_response(http_status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield "http://127.0.0.1:" + str(server.server_port) + "/v1/chat/completions", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)
