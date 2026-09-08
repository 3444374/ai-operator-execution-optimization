"""Window-one v6 lifecycle qualification over actual framed RPCs and a controlled backend."""

import asyncio
import json
import socket
import threading
import unittest

from src.execution_provider.adapters.incremental_session import IncrementalMapSessionAdapter
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.wire import v5, v6
from src.execution_provider.wire.framing import encode_frame, read_frame


def response():
    return json.dumps(
        {
            "model": "model",
            "choices": [{"message": {"content": "hello"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 1},
        }
    ).encode()


def exchange(adapter, *, interrupt=None, before_drain=None):
    peer, server = socket.socketpair()
    peer.settimeout(3)
    results, failures = [], []

    def client():
        try:
            plan = SemanticMapPlan("Echo.", "model", 8)
            peer.sendall(encode_frame(v6.build_open_message(plan)))
            assert read_frame(peer)["max_inflight_tasks"] == 1
            task = v6.build_task_message(plan, sequence=0, input_value="hello")
            peer.sendall(encode_frame(task))
            assert read_frame(peer)["accepted_prefix_count"] == 1
            peer.sendall(encode_frame({"type": "poll", "protocol_version": 6}))
            if interrupt:
                interrupt(peer)
            if peer.fileno() >= 0:
                result = read_frame(peer)
                if result and result["type"] == "completion":
                    v6.validate_completion(
                        result,
                        expected_sequence=0,
                        payload_digest=task["semantic_payload_digest"],
                        open_context=v6.validate_open(v6.build_open_message(plan)),
                    )
                results.append(result)
        except OSError as exc:
            if not interrupt:
                failures.append(exc)
        except BaseException as exc:
            failures.append(exc)
        finally:
            peer.close()

    def handle(connection):
        adapter.run_incremental(connection, read_frame(connection))
        if before_drain:
            before_drain()

    worker = threading.Thread(target=client)
    worker.start()
    try:
        adapter.serve_connection(server, handle)
    finally:
        server.close()
        worker.join(3)
        peer.close()
    assert not worker.is_alive()
    if failures:
        raise failures[0]
    return results[0] if results else None


class IncrementalWindowOneTests(unittest.TestCase):
    def adapter(self, execute, **kwargs):
        return IncrementalMapSessionAdapter(
            FixedModelConfig("http://localhost/v1/chat/completions", "model", 1000),
            execute=execute,
            max_tasks=1,
            max_active_requests=1,
            **kwargs,
        )

    def test_consecutive_queries_share_engine_and_keep_task_identity(self):
        submitted, observed = [], []

        async def execute(task, endpoint):
            submitted.append(task)
            await asyncio.sleep(0.001)
            return response()

        adapter = self.adapter(execute, observer=observed.append)
        try:
            for _ in range(2):
                self.assertEqual(exchange(adapter)["raw_output"], "hello")
            self.assertEqual([r.key.session_id for r in submitted], [0, 1])
            self.assertEqual([r.member.size for r in submitted], [1, 1])
            drained = [e for e in observed if e["event"] == "drained"]
            self.assertEqual(len(drained), 2)
            self.assertTrue(all(not any(e["usage"].values()) for e in drained))
        finally:
            self.assertTrue(adapter.close())

    def test_disconnect_retains_credit_until_authoritative_response(self):
        self.assert_interrupted_peer_drains(pipelined=False)

    def test_pipelined_rpc_is_rejected_without_releasing_remote_credit(self):
        self.assert_interrupted_peer_drains(pipelined=True)

    def assert_interrupted_peer_drains(self, *, pipelined):
        started, allow_response = threading.Event(), threading.Event()

        async def execute(task, endpoint):
            started.set()
            while not allow_response.is_set():
                await asyncio.sleep(0.001)
            return response()

        def interrupt(peer):
            self.assertTrue(started.wait(2))
            if pipelined:
                peer.sendall(b"\x00")
            else:
                peer.close()

        adapter = self.adapter(execute)
        retained = []

        def before_drain():
            retained.append(adapter.engine.capacity.usage().active_requests)
            allow_response.set()

        try:
            exchange(adapter, interrupt=interrupt, before_drain=before_drain)
            self.assertEqual(retained, [1])
            self.assertEqual(adapter.engine.capacity.usage().active_requests, 0)
            self.assertEqual(exchange(adapter)["raw_output"], "hello")
        finally:
            allow_response.set()
            adapter.close()

    def test_unknown_result_quarantines_engine(self):
        async def execute(task, endpoint):
            raise OSError("transport failed")

        adapter = self.adapter(execute)
        try:
            with self.assertRaisesRegex(RuntimeError, "quarantined"):
                exchange(adapter)
            self.assertEqual(adapter.engine.capacity.usage().active_requests, 1)
            self.assertTrue(adapter.engine.error)
        finally:
            adapter.close()

    def test_invalid_response_returns_error_and_releases_lease(self):
        async def execute(task, endpoint):
            return b"{}"

        adapter = self.adapter(execute)
        try:
            self.assertEqual(
                exchange(adapter), v6.build_error_message("MODEL_RESPONSE_INVALID", sequence=0)
            )
            self.assertEqual(adapter.engine.capacity.usage().held_tasks, 0)
        finally:
            adapter.close()

    def test_gateway_setup_failure_closes_transport(self):
        import tempfile
        from pathlib import Path
        from src.execution_provider import server as gateway

        before = {t.ident for t in threading.enumerate() if t.name == "semloom-async-io"}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "occupied").write_text("owned")
            config = root / "model.json"
            config.write_text(
                json.dumps(
                    {
                        "model_id": "model",
                        "endpoint_url": "http://localhost/v1/chat/completions",
                        "timeout_ms": 1000,
                    }
                )
            )
            with self.assertRaises(SystemExit):
                gateway.main(
                    [
                        "--socket",
                        str(root / "occupied"),
                        "--fixed-model-config",
                        str(config),
                        "--incremental-map",
                        "--max-held-tasks",
                        "1",
                    ]
                )
            self.assertEqual((root / "occupied").read_text(), "owned")
        self.assertEqual(
            {t.ident for t in threading.enumerate() if t.name == "semloom-async-io"}, before
        )

    def test_retired_cli_and_execution_identity_are_rejected(self):
        from contextlib import redirect_stderr
        from io import StringIO
        from src.execution_provider.server import parse_args

        error = StringIO()
        with redirect_stderr(error), self.assertRaises(SystemExit) as caught:
            parse_args(["--socket", "/unused", "--incremental-map-window-one"])
        self.assertEqual(caught.exception.code, 2)
        self.assertIn("unrecognized arguments", error.getvalue())
        with self.assertRaisesRegex(ValueError, "unsupported Map execution identity"):
            v5.provider_execution_digest(
                "model", provider_execution_id="semloom.provider.incremental-map-window-one.uds.v5"
            )

    def test_v6_identity_is_distinct_from_synchronous_v5(self):
        self.assertEqual(
            len(
                {
                    v6.provider_execution_digest("model"),
                    v5.provider_execution_digest(
                        "model", provider_execution_id=v5.FIXED_EXECUTION_ID
                    ),
                    v5.provider_execution_digest(
                        "model", provider_execution_id=v5.GOLDEN_EXECUTION_ID
                    ),
                }
            ),
            3,
        )
