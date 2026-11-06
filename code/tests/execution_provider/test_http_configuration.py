"""Explicit HTTP result capacity with deterministic in-memory responses only."""

from dataclasses import replace
import asyncio
import http.client
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import httpx

from src.execution_provider import server
from src.execution_provider.adapters.full_response import FullResponseTransport, decode_full_response
from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig, load_fixed_model_config
from src.execution_provider.adapters.native_tasks import (
    NativeTaskSession, build_native_execution, prepare_native_task,
)
from src.execution_provider.adapters.openai_compatible_fixed import OpenAICompatibleFixedAdapter
from src.execution_provider.completion import CompletionAdapterError, CompletionRequest
from src.execution_provider.request_admission import RequestAdmission, RequestCapacity
from src.scheduling.core.session_contract import Usage
from src.execution_provider.wire.framing import MAX_FRAME_BYTES
from src.execution_provider.wire.v3 import GENERATION_CONSTRAINTS


MIB = 1024 * 1024
MODEL = FixedModelConfig("http://localhost/v1/chat/completions", "fixture", 180000)
REQUEST = CompletionRequest(
    semantic_payload_digest="a" * 64,
    model_id="fixture",
    canonical_messages=({"role": "user", "content": "input"},),
    generation_constraints=dict(GENERATION_CONSTRAINTS),
)


def response_body(size):
    value = dict(model="fixture", choices=[dict(
        message=dict(role="assistant", content="TRUE"), finish_reason="stop")],
        usage=dict(prompt_tokens=1, completion_tokens=1), padding="")
    raw = json.dumps(value, separators=(",", ":")).encode()
    value["padding"] = "x" * (size - len(raw))
    return json.dumps(value, separators=(",", ":")).encode()


def response(raw):
    class MemorySocket:
        def makefile(self, mode):
            return io.BytesIO(b"HTTP/1.1 200 OK\r\nContent-Length: " +
                              str(len(raw)).encode() + b"\r\n\r\n" + raw)

    result = http.client.HTTPResponse(MemorySocket())
    result.begin()
    return result


class HttpConfigurationTests(unittest.TestCase):
    def test_explicit_request_timeout_has_no_five_minute_ceiling(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "model.json")
            path.write_text(json.dumps(dict(endpoint_url=MODEL.endpoint_url,
                model_id=MODEL.model_id, timeout_ms=600_000)))
            model = load_fixed_model_config(path)
        self.assertEqual(model.timeout_ms, 600_000)
        execution = build_native_execution(model, physical=None)
        try:
            self.assertEqual(execution.engine.capacity.limits.timeouts.backend_s, 601)
        finally:
            self.assertTrue(execution.close())

    def test_request_timeout_requires_explicit_positive_integer(self):
        for timeout in (None, True, 0, -1, 1.5, "600000"):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                replace(MODEL, timeout_ms=timeout)

    def test_strict_external_config_loads_optional_response_capacity(self):
        for limit in (511, MIB, MIB + 1, 8 * MIB):
            with self.subTest(limit=limit), tempfile.TemporaryDirectory() as directory:
                path = Path(directory, "model.json")
                path.write_text(json.dumps(dict(endpoint_url=MODEL.endpoint_url,
                    model_id=MODEL.model_id, timeout_ms=180000, max_response_bytes=limit)))
                self.assertEqual(load_fixed_model_config(path).max_response_bytes, limit)
        self.assertEqual(MODEL.max_response_bytes, MIB)

    def test_invalid_response_capacities_reject(self):
        for limit in (True, 0, -1, 1.5, "1024", None, 8 * MIB + 1):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                replace(MODEL, max_response_bytes=limit)

    def test_native_item_and_total_results_follow_explicit_configuration(self):
        model = replace(MODEL, max_response_bytes=2 * MIB)
        execution = build_native_execution(model, physical=None, result_bytes=3 * MIB)
        try:
            limits = execution.engine.capacity.limits
            self.assertEqual((limits.item_result_bytes, limits.result_bytes), (2 * MIB, 3 * MIB))
        finally:
            self.assertTrue(execution.close())

    def test_large_native_item_requires_explicit_fitting_total_before_transport(self):
        model = replace(MODEL, max_response_bytes=2 * MIB)
        for options in ({}, dict(result_bytes=None), dict(result_bytes=MIB),
                        dict(result_bytes=True)):
            with self.subTest(options=options), patch(
                "src.execution_provider.adapters.native_tasks.FullResponseTransport"
            ) as transport:
                with self.assertRaises(ValueError):
                    build_native_execution(model, physical=None, **options)
                transport.assert_not_called()

    def test_complete_http_response_capacity_includes_metadata_without_truncation(self):
        async def check(size, item_limit):
            raw = b"x" * size
            transport = FullResponseTransport(replace(MODEL, max_response_bytes=2 * MIB), 1)
            transport._client = httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=httpx.ByteStream(raw))))
            task = prepare_native_task(b"{}", 0, row_sequence=0, call_id="result",
                                       max_result_bytes=item_limit)
            try:
                return await transport.execute(Mock(task=task), "model")
            finally:
                await transport.close()

        for size in (MIB - 1, MIB, MIB + 1):
            with self.subTest(size=size):
                full = decode_full_response(asyncio.run(check(size, 2 * MIB)))
                self.assertEqual(full.body, b"x" * size)
        with self.assertRaisesRegex(ValueError, "response exceeds bound"):
            asyncio.run(check(512, 512))

    def test_total_budget_retains_large_result_until_consumer_release(self):
        def transport_factory(config, concurrency, observer):
            transport = FullResponseTransport(config, concurrency, observer)
            transport._client = httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, stream=httpx.ByteStream(b"x" * (MIB + 1)))))
            return transport

        execution = build_native_execution(replace(MODEL, max_response_bytes=2 * MIB),
            physical=None, max_tasks=2, max_active_requests=1, result_bytes=2 * MIB,
            transport_factory=transport_factory)
        flow = NativeTaskSession(execution, "large-result", "map")
        tasks = tuple(prepare_native_task(b"{}", i, row_sequence=i, call_id="result",
                                          max_result_bytes=2 * MIB) for i in range(2))
        try:
            self.assertEqual(flow.offer(tasks).accepted_prefix_count, 1)
            until = time.monotonic() + 3
            while time.monotonic() < until:
                progress = flow.advance(1)
                if progress.deliveries:
                    break
                flow.wait(progress)
            self.assertEqual(len(progress.deliveries), 1)
            delivery = progress.deliveries[0]
            self.assertEqual(decode_full_response(delivery.result).body, b"x" * (MIB + 1))
            self.assertEqual(execution.engine.capacity.usage().result_bytes, 2 * MIB)
            self.assertEqual(flow.offer(tasks[1:]).accepted_prefix_count, 0)
            flow.release((delivery.lease_id,))
            self.assertEqual(flow.offer(tasks[1:]).accepted_prefix_count, 1)
        finally:
            flow.close()
            self.assertTrue(execution.close())
        self.assertEqual(execution.engine.capacity.usage(), Usage())

    def test_pg_factory_retains_frame_limit_and_honors_small_item_limit(self):
        with patch("src.execution_provider.adapters.incremental_execution.AsyncFixedModelTransport") as transport:
            with self.assertRaisesRegex(ValueError, "PG.*1 MiB"):
                build_fixed_model_execution(replace(MODEL, max_response_bytes=MIB + 1),
                                            result_bytes=4 * MIB)
            transport.assert_not_called()
        execution = build_fixed_model_execution(replace(MODEL, max_response_bytes=511),
                                                result_bytes=1024)
        try:
            task = execution.prepare_task(REQUEST, 0)
            self.assertEqual(task.max_result_bytes, 511)
            self.assertEqual(execution.engine.capacity.limits.item_result_bytes, 511)
            self.assertEqual(MAX_FRAME_BYTES, MIB)
        finally:
            self.assertTrue(execution.close())

    def test_pg_cli_rejects_larger_profile_before_socket_or_transport(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "model.json")
            path.write_text(json.dumps(dict(endpoint_url=MODEL.endpoint_url,
                model_id=MODEL.model_id, timeout_ms=180000, max_response_bytes=MIB + 1)))
            for extra in ([], ["--incremental-map"]):
                with self.subTest(extra=extra), patch.object(server.socket, "socket") as socket:
                    with self.assertRaisesRegex(SystemExit, "PG.*1 MiB"):
                        server.main(["--socket", str(Path(directory, "gateway.sock")),
                                     "--fixed-model-config", str(path), *extra])
                    socket.assert_not_called()

    def complete(self, model, raw):
        adapter = OpenAICompatibleFixedAdapter(model)
        gate = RequestAdmission(adapter, 1)
        connection = Mock(sock=Mock())
        connection.getresponse.return_value = response(raw)
        return adapter, gate, connection

    def test_fixed_http_reads_complete_json_around_one_mib(self):
        for size in (MIB - 1, MIB, MIB + 1):
            model = replace(MODEL, max_response_bytes=2 * MIB)
            adapter, gate, connection = self.complete(model, response_body(size))
            with self.subTest(size=size), patch.object(adapter._resolver, "resolve", return_value=[]), patch(
                "http.client.HTTPConnection", return_value=connection
            ):
                result = gate.complete(REQUEST)
            self.assertEqual(result.raw_output, "TRUE")
            self.assertEqual(gate.capacity(), RequestCapacity(0, 0))
            connection.request.assert_called_once()

    def test_explicit_small_or_default_overflow_keeps_unknown_request(self):
        for limit in (511, MIB):
            model = replace(MODEL, max_response_bytes=limit)
            adapter, gate, connection = self.complete(model, response_body(limit + 1))
            with self.subTest(limit=limit), patch.object(adapter._resolver, "resolve", return_value=[]), patch(
                "http.client.HTTPConnection", return_value=connection
            ), self.assertLogs("src.execution_provider.request_admission", level="WARNING"):
                with self.assertRaises(CompletionAdapterError) as caught:
                    gate.complete(REQUEST)
            self.assertEqual(caught.exception.code, "MODEL_RESPONSE_INVALID")
            self.assertTrue(caught.exception.remote_outcome_unknown)
            self.assertEqual(gate.capacity(), RequestCapacity(0, 1))
            connection.request.assert_called_once()

    def test_malformed_complete_json_is_known_terminal(self):
        adapter, gate, connection = self.complete(MODEL, b'{"broken":')
        with patch.object(adapter._resolver, "resolve", return_value=[]), patch(
            "http.client.HTTPConnection", return_value=connection
        ):
            with self.assertRaises(CompletionAdapterError) as caught:
                gate.complete(REQUEST)
        self.assertFalse(caught.exception.remote_outcome_unknown)
        self.assertEqual(gate.capacity(), RequestCapacity(0, 0))


if __name__ == "__main__":
    unittest.main()
