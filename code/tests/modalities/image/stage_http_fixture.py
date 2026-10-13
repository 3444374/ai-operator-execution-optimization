"""Bounded localhost observations without retaining image request bodies."""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import threading
import time


class RequestReceipts(list):
    def __init__(self):
        self.condition = threading.Condition()
        self.inflight = 0
        self.errors = []

    def wait_idle(self, timeout=10):
        with self.condition:
            if not self.condition.wait_for(lambda: self.inflight == 0, timeout):
                raise TimeoutError("fixture requests did not finish")
            if self.errors:
                raise RuntimeError("fixture handler failed: " + ",".join(self.errors))


@contextmanager
def stage_http_fixture(reply, identity, *, max_posts, decode=json.loads):
    receipts = RequestReceipts()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = parts = None
            with receipts.condition:
                receipts.inflight += 1
            try:
                length = int(self.headers["Content-Length"])
                if not 0 < length <= 1048576:
                    raise ValueError("fixture request body exceeds its byte limit")
                body = decode(self.rfile.read(length))
                parts = [part for message in body.get("messages", [])
                         if isinstance(message.get("content"), list) for part in message["content"]]
                row = dict(identity=identity(body), json_sha256=hashlib.sha256(
                    json.dumps(body, sort_keys=True).encode()).hexdigest(), request_bytes=length,
                    uri_bytes=sum(len(p["image_url"]["url"]) for p in parts if p["type"] == "image_url"),
                    received_ns=time.monotonic_ns())
                with receipts.condition:
                    if len(receipts) >= max_posts:
                        raise ValueError("fixture exceeded its total POST limit")
                    receipts.append(row)
                status, response = reply(body)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)
                self.wfile.flush()
                row.update(status=status, completed_ns=time.monotonic_ns(),
                           response_sha256=hashlib.sha256(response).hexdigest())
            except BaseException as error:
                with receipts.condition:
                    receipts.errors.append(type(error).__name__)
            finally:
                # wait_idle observes this only after the complete body lost its
                # last handler-owned reference; receipts contain metadata only.
                body = parts = None
                with receipts.condition:
                    receipts.inflight -= 1
                    receipts.condition.notify_all()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1", receipts
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        receipts.wait_idle()
        if thread.is_alive():
            raise RuntimeError("fixture server did not stop")
