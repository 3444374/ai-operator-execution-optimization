"""Contract tests for the fixed OpenAI-compatible completion adapter."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import tempfile
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from src.execution_provider.adapters.openai_compatible_fixed import (
    FIXED_EXECUTION_ID,
    FixedModelConfig,
    OpenAICompatibleFixedAdapter,
    load_fixed_model_config,
)
from src.execution_provider.adapters.v3_session import (
    CompletionAdapterError,
    V3CompletionRequest,
)
from src.execution_provider.wire.framing import encode_frame, read_frame
from src.execution_provider.wire.v3 import (
    GENERATION_CONSTRAINTS,
    SemanticFilterPlan,
    build_open_message,
    build_task_message,
)


CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
CANONICAL_GATEWAY_CLI = (
    CODE_ROOT / "scripts" / "services" / "run_execution_provider_gateway.py"
)


class _CompletionHandler(BaseHTTPRequestHandler):
    request_body: bytes | None = None
    authorization: str | None = None
    request_count = 0
    response_status = 200
    response_headers: dict[str, str] = {}
    response_delay_seconds = 0.0
    response_chunk_size = 0
    response_chunk_delay_seconds = 0.0
    response_value: object = {
        "model": "fixed-model-v1",
        "choices": [
            {
                "message": {"role": "assistant", "content": "TRUE"},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 17, "completion_tokens": 1},
    }

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler interface
        length = int(self.headers["Content-Length"])
        type(self).request_body = self.rfile.read(length)
        self._send_configured_response()

    def do_GET(self) -> None:  # noqa: N802 - redirect characterization
        type(self).request_body = b""
        self._send_configured_response()

    def _send_configured_response(self) -> None:
        type(self).authorization = self.headers.get("Authorization")
        type(self).request_count += 1
        if type(self).response_delay_seconds > 0:
            time.sleep(type(self).response_delay_seconds)
        response = json.dumps(type(self).response_value, separators=(",", ":")).encode(
            "utf-8"
        )
        self.send_response(type(self).response_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        for key, value in type(self).response_headers.items():
            self.send_header(key, value)
        self.end_headers()
        try:
            if type(self).response_chunk_size > 0:
                for offset in range(0, len(response), type(self).response_chunk_size):
                    self.wfile.write(
                        response[offset : offset + type(self).response_chunk_size]
                    )
                    self.wfile.flush()
                    time.sleep(type(self).response_chunk_delay_seconds)
            else:
                self.wfile.write(response)
        except OSError:
            pass

    def log_message(self, _format: str, *_args: object) -> None:
        return


class FixedModelAdapterDeadlineTests(unittest.TestCase):
    def test_repeated_timeouts_share_one_bounded_dns_resolution(self) -> None:
        release_resolver = threading.Event()

        def slow_getaddrinfo(*_args: object, **_kwargs: object) -> object:
            release_resolver.wait(timeout=1)
            return [
                (
                    socket.AF_INET,
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP,
                    "",
                    ("127.0.0.1", 9),
                )
            ]

        adapter = OpenAICompatibleFixedAdapter(
            FixedModelConfig(
                endpoint_url="http://model.test/v1/chat/completions",
                model_id="fixed-model-v1",
                timeout_ms=100,
            )
        )
        request = V3CompletionRequest(
            semantic_payload_digest="a" * 64,
            model_id="fixed-model-v1",
            canonical_messages=(
                {"role": "system", "content": "instruction"},
                {"role": "user", "content": "input"},
            ),
            generation_constraints=dict(GENERATION_CONSTRAINTS),
        )

        with mock.patch(
            "socket.getaddrinfo",
            side_effect=slow_getaddrinfo,
        ) as getaddrinfo:
            try:
                started = time.monotonic()
                for _ in range(2):
                    with self.assertRaises(CompletionAdapterError) as raised:
                        adapter.complete(request)
                    self.assertEqual(raised.exception.code, "MODEL_TIMEOUT")
                elapsed = time.monotonic() - started
            finally:
                release_resolver.set()

        self.assertEqual(getaddrinfo.call_count, 1)
        self.assertLess(elapsed, 0.35)


class FixedModelAdapterTests(unittest.TestCase):
    def test_map_session_uses_fixed_identity_and_verbatim_generation_request(self) -> None:
        from src.execution_provider.adapters.semantic_session import run_v5_session
        from src.execution_provider.semantic_map import SemanticMapPlan
        from src.execution_provider.wire import v5

        plan = SemanticMapPlan('原样返回输入。\n"instruction"', "fixed-model-v1", 128)
        task = v5.build_task_message(plan, sequence=0, input_value='TRUE\n{}\\中文',
                                     provider_execution_id=v5.FIXED_EXECUTION_ID)
        raw = " TRUE\n世界\t "
        _CompletionHandler.response_value = {
            "model": plan.model_id,
            "choices": [{"message": {"role": "assistant", "content": raw}, "finish_reason": "length"}],
            "usage": {"prompt_tokens": 27, "completion_tokens": 9},
        }
        adapter = OpenAICompatibleFixedAdapter(FixedModelConfig(
            endpoint_url=f"http://127.0.0.1:{self.httpd.server_port}/v1/chat/completions",
            model_id=plan.model_id, timeout_ms=1000))
        client, server = socket.socketpair()
        client.settimeout(3)
        worker = threading.Thread(target=run_v5_session, args=(server, adapter), daemon=True)
        worker.start()
        try:
            opened = v5.build_open_message(plan, provider_execution_id=v5.FIXED_EXECUTION_ID)
            client.sendall(encode_frame(opened))
            self.assertEqual(read_frame(client)["type"], "opened")
            client.sendall(encode_frame(task))
            completion = v5.validate_completion(read_frame(client), expected_sequence=0,
                payload_digest=task["semantic_payload_digest"],
                open_context=v5.validate_open(opened, provider_execution_id=v5.FIXED_EXECUTION_ID))
            self.assertEqual((completion.raw_output, completion.finish_reason, completion.output_tokens), (raw, "length", 9))
        finally:
            client.close()
            worker.join(3)
            server.close()
        self.assertFalse(worker.is_alive())
        self.assertEqual(_CompletionHandler.request_count, 1)
        self.assertEqual(json.loads(_CompletionHandler.request_body), {
            "model": "fixed-model-v1",
            "messages": [{"role": "system", "content": '原样返回输入。\n"instruction"'},
                         {"role": "user", "content": 'TRUE\n{}\\中文'}],
            "temperature": 0, "top_p": 1, "max_tokens": 128, "n": 1, "stream": False, "stop": None,
        })
        self.assertIsNone(adapter.execution_id_for(6))

    def setUp(self) -> None:
        _CompletionHandler.request_body = None
        _CompletionHandler.authorization = None
        _CompletionHandler.request_count = 0
        _CompletionHandler.response_status = 200
        _CompletionHandler.response_headers = {}
        _CompletionHandler.response_delay_seconds = 0.0
        _CompletionHandler.response_chunk_size = 0
        _CompletionHandler.response_chunk_delay_seconds = 0.0
        _CompletionHandler.response_value = {
            "model": "fixed-model-v1",
            "choices": [
                {
                    "message": {"role": "assistant", "content": "TRUE"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 17, "completion_tokens": 1},
        }
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _CompletionHandler)
        self.thread = threading.Thread(target=self.httpd.serve_forever)
        self.thread.start()
        self.addCleanup(self._stop_server)

    def _stop_server(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=1)

    def test_adapter_forwards_the_exact_v3_request_once(self) -> None:
        endpoint_url = f"http://127.0.0.1:{self.httpd.server_port}/v1/chat/completions"
        adapter = OpenAICompatibleFixedAdapter(
            FixedModelConfig(
                endpoint_url=endpoint_url,
                model_id="fixed-model-v1",
                timeout_ms=1_000,
                bearer_token="test-token",
            )
        )
        messages = (
            {"role": "system", "content": "fixed instruction"},
            {"role": "user", "content": "PostgreSQL is a database."},
        )

        completion = adapter.complete(
            V3CompletionRequest(
                semantic_payload_digest="a" * 64,
                model_id="fixed-model-v1",
                canonical_messages=messages,
                generation_constraints=dict(GENERATION_CONSTRAINTS),
            )
        )

        self.assertEqual(
            json.loads(_CompletionHandler.request_body or b"null"),
            {
                "model": "fixed-model-v1",
                "messages": list(messages),
                **GENERATION_CONSTRAINTS,
            },
        )
        self.assertEqual(_CompletionHandler.authorization, "Bearer test-token")
        self.assertEqual(completion.raw_output, "TRUE")
        self.assertEqual(completion.response_model_id, "fixed-model-v1")
        self.assertEqual(completion.prompt_tokens, 17)
        self.assertEqual(completion.output_tokens, 1)
        self.assertEqual(completion.finish_reason, "stop")
        self.assertEqual(_CompletionHandler.request_count, 1)

    def test_adapter_rejects_redirect_without_contacting_target(self) -> None:
        class RedirectTargetHandler(_CompletionHandler):
            pass

        RedirectTargetHandler.request_body = None
        RedirectTargetHandler.authorization = None
        RedirectTargetHandler.request_count = 0
        RedirectTargetHandler.response_status = 200
        RedirectTargetHandler.response_headers = {}
        target_server = ThreadingHTTPServer(
            ("127.0.0.1", 0),
            RedirectTargetHandler,
        )
        target_thread = threading.Thread(target=target_server.serve_forever)
        target_thread.start()

        def stop_target() -> None:
            target_server.shutdown()
            target_server.server_close()
            target_thread.join(timeout=1)

        self.addCleanup(stop_target)
        _CompletionHandler.response_headers = {
            "Location": (
                f"http://127.0.0.1:{target_server.server_port}"
                "/v1/chat/completions"
            )
        }
        adapter = OpenAICompatibleFixedAdapter(
            FixedModelConfig(
                endpoint_url=(
                    f"http://127.0.0.1:{self.httpd.server_port}"
                    "/v1/chat/completions"
                ),
                model_id="fixed-model-v1",
                timeout_ms=1_000,
                bearer_token="redirect-secret",
            )
        )

        for status in (301, 302, 303, 307, 308):
            with self.subTest(status=status):
                _CompletionHandler.response_status = status
                with self.assertRaises(CompletionAdapterError) as raised:
                    adapter.complete(self._completion_request())
                self.assertEqual(raised.exception.code, "MODEL_RESPONSE_INVALID")

        self.assertEqual(_CompletionHandler.request_count, 5)
        self.assertEqual(RedirectTargetHandler.request_count, 0)
        self.assertIsNone(RedirectTargetHandler.authorization)

    def test_adapter_timeout_is_a_total_deadline_for_slow_response(self) -> None:
        _CompletionHandler.response_chunk_size = 4
        _CompletionHandler.response_chunk_delay_seconds = 0.03
        adapter = OpenAICompatibleFixedAdapter(
            FixedModelConfig(
                endpoint_url=(
                    f"http://127.0.0.1:{self.httpd.server_port}"
                    "/v1/chat/completions"
                ),
                model_id="fixed-model-v1",
                timeout_ms=100,
            )
        )

        started = time.monotonic()
        with self.assertRaises(CompletionAdapterError) as raised:
            adapter.complete(self._completion_request())
        elapsed = time.monotonic() - started

        self.assertEqual(raised.exception.code, "MODEL_TIMEOUT")
        self.assertLess(elapsed, 0.75)
        self.assertEqual(_CompletionHandler.request_count, 1)

    def test_config_loader_reads_auth_from_a_named_environment_variable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            config_path = Path(temporary_directory, "fixed-model.json")
            config_path.write_text(
                json.dumps(
                    {
                        "endpoint_url": "https://model.invalid/v1/chat/completions",
                        "model_id": "fixed-model-v1",
                        "timeout_ms": 2_000,
                        "bearer_token_env": "SEMLOOM_TEST_MODEL_TOKEN",
                    }
                ),
                encoding="utf-8",
            )
            config = load_fixed_model_config(
                config_path,
                environ={"SEMLOOM_TEST_MODEL_TOKEN": "private-token"},
            )

        self.assertEqual(config.model_id, "fixed-model-v1")
        self.assertEqual(config.timeout_ms, 2_000)
        self.assertEqual(config.bearer_token, "private-token")
        self.assertNotIn("private-token", repr(config))

    def test_config_loader_rejects_unknown_fields_and_missing_auth(self) -> None:
        cases = (
            {
                "endpoint_url": "https://model.invalid/v1/chat/completions",
                "model_id": "fixed-model-v1",
                "timeout_ms": 2_000,
                "future": True,
            },
            {
                "endpoint_url": "https://model.invalid/v1/chat/completions",
                "model_id": "fixed-model-v1",
                "timeout_ms": 2_000,
                "bearer_token_env": "MISSING_MODEL_TOKEN",
            },
            {
                "endpoint_url": "http://127.0.0.1:99999/v1/chat/completions",
                "model_id": "fixed-model-v1",
                "timeout_ms": 2_000,
            },
            {
                "endpoint_url": "http://127.0.0.1:0/v1/chat/completions",
                "model_id": "fixed-model-v1",
                "timeout_ms": 2_000,
            },
        )
        for value in cases:
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                path = Path(directory, "fixed-model.json")
                path.write_text(json.dumps(value), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_fixed_model_config(path, environ={})

    def test_adapter_maps_remote_failures_to_redacted_wire_codes_without_retry(self) -> None:
        cases = (
            (400, 0.0, {}, "MODEL_REQUEST_REJECTED", 1_000),
            (503, 0.0, {}, "MODEL_UNAVAILABLE", 1_000),
            (200, 0.0, [], "MODEL_RESPONSE_INVALID", 1_000),
            (200, 0.05, {}, "MODEL_TIMEOUT", 1),
        )
        for status, delay, response, expected_code, timeout_ms in cases:
            with self.subTest(code=expected_code):
                _CompletionHandler.response_status = status
                _CompletionHandler.response_delay_seconds = delay
                _CompletionHandler.response_value = response
                _CompletionHandler.request_count = 0
                endpoint_url = (
                    f"http://127.0.0.1:{self.httpd.server_port}/v1/chat/completions"
                )
                adapter = OpenAICompatibleFixedAdapter(
                    FixedModelConfig(
                        endpoint_url=endpoint_url,
                        model_id="fixed-model-v1",
                        timeout_ms=timeout_ms,
                    )
                )
                with self.assertRaises(CompletionAdapterError) as raised:
                    adapter.complete(
                        V3CompletionRequest(
                            semantic_payload_digest="a" * 64,
                            model_id="fixed-model-v1",
                            canonical_messages=(
                                {"role": "system", "content": "instruction"},
                                {"role": "user", "content": "input"},
                            ),
                            generation_constraints=dict(GENERATION_CONSTRAINTS),
                        )
                    )
                self.assertEqual(raised.exception.code, expected_code)
                self.assertEqual(_CompletionHandler.request_count, 1)
                self.assertNotIn(endpoint_url, str(raised.exception))

    @staticmethod
    def _completion_request() -> V3CompletionRequest:
        return V3CompletionRequest(
            semantic_payload_digest="a" * 64,
            model_id="fixed-model-v1",
            canonical_messages=(
                {"role": "system", "content": "instruction"},
                {"role": "user", "content": "input"},
            ),
            generation_constraints=dict(GENERATION_CONSTRAINTS),
        )

    def test_canonical_gateway_routes_wire_v3_through_the_fixed_adapter(self) -> None:
        plan = SemanticFilterPlan(
            instruction="The input describes a database system.",
            model_id="fixed-model-v1",
        )
        with tempfile.TemporaryDirectory(prefix="sf4b-", dir="/tmp") as directory:
            root = Path(directory)
            socket_path = root / "gateway.sock"
            config_path = root / "fixed-model.json"
            config_path.write_text(
                json.dumps(
                    {
                        "endpoint_url": (
                            f"http://127.0.0.1:{self.httpd.server_port}"
                            "/v1/chat/completions"
                        ),
                        "model_id": plan.model_id,
                        "timeout_ms": 1_000,
                        "bearer_token_env": "SEMLOOM_TEST_MODEL_TOKEN",
                    }
                ),
                encoding="utf-8",
            )
            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            environment["SEMLOOM_TEST_MODEL_TOKEN"] = "test-token"
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(CANONICAL_GATEWAY_CLI),
                    "--socket",
                    str(socket_path),
                    "--fixed-model-config",
                    str(config_path),
                    "--once",
                ],
                cwd=tempfile.gettempdir(),
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.addCleanup(process.kill)
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.addCleanup(client.close)
            for _ in range(200):
                try:
                    client.connect(str(socket_path))
                    break
                except (FileNotFoundError, ConnectionRefusedError):
                    time.sleep(0.01)
            else:
                _stdout, stderr = process.communicate(timeout=2)
                self.fail(f"fixed gateway did not accept a connection: {stderr}")

            client.sendall(
                encode_frame(
                    build_open_message(
                        plan,
                        provider_execution_id=FIXED_EXECUTION_ID,
                    )
                )
            )
            self.assertEqual(read_frame(client)["type"], "opened")
            client.sendall(
                encode_frame(
                    build_task_message(
                        plan,
                        sequence=0,
                        input_value="PostgreSQL is a database.",
                        provider_execution_id=FIXED_EXECUTION_ID,
                    )
                )
            )
            completion = read_frame(client)
            self.assertEqual(completion["raw_output"], "TRUE")
            self.assertEqual(completion["response_model_id"], plan.model_id)
            self.assertEqual(completion["prompt_tokens"], "17")
            self.assertEqual(completion["output_tokens"], "1")
            client.close()
            stdout, stderr = process.communicate(timeout=2)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertEqual(stdout, "")
            self.assertEqual(stderr, "")
            self.assertFalse(socket_path.exists())
            self.assertEqual(_CompletionHandler.request_count, 1)


if __name__ == "__main__":
    unittest.main()
