"""Deterministic local OpenAI-compatible HTTP fixture for TAP tests."""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


EXPECTED_FIELDS = {
    "model",
    "messages",
    "temperature",
    "top_p",
    "max_tokens",
    "n",
    "stream",
    "stop",
}


class FixtureServer(HTTPServer):
    model_id: str
    response_model_id: str
    raw_outputs: tuple[str, ...]
    response_status: int
    delay_ms: int
    invalid_json: bool
    request_index: int
    require_choice: bool
    allow_choice: bool
    request_log: Path | None


class FixtureHandler(BaseHTTPRequestHandler):
    server: FixtureServer

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler interface
        length_text = self.headers.get("Content-Length")
        try:
            length = int(length_text or "")
            body = self.rfile.read(length)
            request_value = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            self._send(400, {"error": "invalid request"})
            return
        if self.server.request_log is not None:
            with self.server.request_log.open('a', encoding='utf-8') as handle:
                handle.write(json.dumps(request_value) + '\n')
        if self.server.delay_ms > 0:
            time.sleep(self.server.delay_ms / 1000)
        if self.server.response_status != 200:
            self._send(self.server.response_status, {"error": "fixture failure"})
            return
        with_choice = self.server.require_choice or (
            self.server.allow_choice and isinstance(request_value, dict)
            and 'structured_outputs' in request_value)
        if not _valid_request(request_value, self.server.model_id, with_choice):
            self._send(400, {"error": "invalid request"})
            return
        if self.server.invalid_json:
            self._send_bytes(200, b"{")
            return
        output_index = min(
            self.server.request_index,
            len(self.server.raw_outputs) - 1,
        )
        self.server.request_index += 1
        self._send(
            200,
            {
                "model": self.server.response_model_id,
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": self.server.raw_outputs[output_index],
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 17, "completion_tokens": 1},
            },
        )

    def _send(self, status: int, value: object) -> None:
        self._send_bytes(
            status,
            json.dumps(value, separators=(",", ":")).encode("utf-8"),
        )

    def _send_bytes(self, status: int, payload: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except BrokenPipeError:
            pass

    def log_message(self, _format: str, *_args: object) -> None:
        return


def _valid_request(value: object, model_id: str, require_choice: bool = False) -> bool:
    return (
        isinstance(value, dict)
        and set(value) == EXPECTED_FIELDS | ({"structured_outputs"} if require_choice else set())
        and (not require_choice or value.get("structured_outputs") == {"choice": ["TRUE", "FALSE", "UNKNOWN"]})
        and value.get("model") == model_id
        and isinstance(value.get("messages"), list)
        and len(value["messages"]) == 2
        and value.get("temperature") == 0
        and type(value.get("temperature")) is int
        and value.get("top_p") == 1
        and type(value.get("top_p")) is int
        and value.get("max_tokens") == 8
        and type(value.get("max_tokens")) is int
        and value.get("n") == 1
        and type(value.get("n")) is int
        and value.get("stream") is False
        and value.get("stop") == ["\n"]
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port-file", type=Path, required=True)
    parser.add_argument("--model-id", default="fixed-model-v1")
    parser.add_argument("--response-model-id")
    parser.add_argument("--raw-outputs", default="TRUE")
    parser.add_argument("--response-status", type=int, default=200)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--invalid-json", action="store_true")
    parser.add_argument("--max-requests", type=int, default=1)
    parser.add_argument("--require-choice", action="store_true")
    parser.add_argument("--allow-choice", action="store_true")
    parser.add_argument("--request-log", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_outputs = tuple(args.raw_outputs.split(","))
    if (
        not raw_outputs
        or any(not output for output in raw_outputs)
        or args.delay_ms < 0
        or args.max_requests <= 0
        or not (100 <= args.response_status <= 599)
    ):
        raise SystemExit("invalid fixture configuration")
    if args.port_file.exists():
        raise SystemExit("refusing to replace existing port file")
    if args.request_log is not None and args.request_log.exists():
        raise SystemExit("refusing to replace existing request log")
    server = FixtureServer(("127.0.0.1", 0), FixtureHandler)
    server.model_id = args.model_id
    server.response_model_id = args.response_model_id or args.model_id
    server.raw_outputs = raw_outputs
    server.response_status = args.response_status
    server.delay_ms = args.delay_ms
    server.invalid_json = args.invalid_json
    server.request_index = 0
    server.require_choice = args.require_choice
    server.allow_choice = args.allow_choice
    server.request_log = args.request_log
    args.port_file.write_text(str(server.server_port), encoding="ascii")
    try:
        for _ in range(args.max_requests):
            server.handle_request()
    finally:
        server.server_close()
        try:
            args.port_file.unlink()
        except FileNotFoundError:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
