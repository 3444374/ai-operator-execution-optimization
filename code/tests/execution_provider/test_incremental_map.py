"""Window-one bridge ownership and strict identities with an asynchronous controlled backend."""

import asyncio
import json
import socket
import threading
import unittest

from src.execution_provider.adapters.incremental_map import IncrementalMapAdapter
from src.execution_provider.adapters.openai_compatible_fixed import FixedModelConfig
from src.execution_provider.adapters.semantic_session import (
    CompletionRequest,
    CompletionAdapterError,
)
from src.execution_provider.wire import v5


def request():
    return CompletionRequest(
        "a" * 64,
        "model",
        ({"role": "user", "content": "hello"},),
        {"max_tokens": 8, "temperature": 0},
        protocol_version=5,
    )


def response():
    return json.dumps(
        {
            "model": "model",
            "choices": [{"message": {"content": "hello"}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 2, "completion_tokens": 1},
        }
    ).encode()


class IncrementalMapTests(unittest.TestCase):
    def test_normal_queries_share_engine_and_use_organized_task_identity(self):
        submitted = []
        observed = []

        async def execute(task, endpoint):
            submitted.append(task)
            await asyncio.sleep(0.001)
            return response()

        adapter = IncrementalMapAdapter(
            FixedModelConfig("http://localhost/v1/chat/completions", "model", 1000),
            execute=execute,
            observer=observed.append,
        )
        try:
            for _ in range(2):
                peer, server = socket.socketpair()
                try:

                    def handle(connection):
                        self.assertEqual(adapter.complete(request()).raw_output, "hello")

                    adapter.serve_connection(server, handle)
                finally:
                    peer.close()
                    server.close()
            self.assertEqual([r.key.session_id for r in submitted], [0, 1])
            self.assertEqual([r.member.size for r in submitted], [1, 1])
            self.assertEqual(adapter.engine.capacity.usage().held_tasks, 0)
            drained = [e for e in observed if e["event"] == "drained"]
            self.assertEqual(len(drained), 2)
            self.assertTrue(all(not any(e["usage"].values()) for e in drained))
            self.assertNotEqual(adapter.execution_id_for(5), v5.FIXED_EXECUTION_ID)
            self.assertIsNone(adapter.execution_id_for(4))
        finally:
            self.assertTrue(adapter.close())

    def test_disconnect_retains_credit_until_late_authoritative_response(self):
        started = threading.Event()
        allow_response = threading.Event()

        async def execute(task, endpoint):
            started.set()
            while not allow_response.is_set():
                await asyncio.sleep(0.001)
            return response()

        adapter = IncrementalMapAdapter(
            FixedModelConfig("http://localhost/v1/chat/completions", "model", 1000), execute=execute
        )
        peer, server = socket.socketpair()
        observed = []

        def cancel_peer():
            started.wait(2)
            peer.close()

        worker = threading.Thread(target=cancel_peer)
        worker.start()
        try:

            def handle(connection):
                with self.assertRaises(ConnectionResetError):
                    adapter.complete(request())
                observed.append(adapter.engine.capacity.usage().active_requests)
                allow_response.set()

            adapter.serve_connection(server, handle)
            self.assertEqual(observed, [1])
            self.assertEqual(adapter.engine.capacity.usage().active_requests, 0)
            peer2, server2 = socket.socketpair()
            try:
                adapter.serve_connection(
                    server2,
                    lambda conn: self.assertEqual(adapter.complete(request()).raw_output, "hello"),
                )
            finally:
                peer2.close()
                server2.close()
        finally:
            allow_response.set()
            worker.join(2)
            server.close()
            adapter.close()

    def test_unknown_result_quarantines_shared_engine(self):
        async def execute(task, endpoint):
            raise OSError("transport failed")

        adapter = IncrementalMapAdapter(
            FixedModelConfig("http://localhost/v1/chat/completions", "model", 1000), execute=execute
        )
        peer, server = socket.socketpair()
        try:

            def handle(connection):
                with self.assertRaises(CompletionAdapterError):
                    adapter.complete(request())

            with self.assertRaisesRegex(RuntimeError, "quarantined"):
                adapter.serve_connection(server, handle)
            self.assertEqual(adapter.engine.capacity.usage().active_requests, 1)
            with self.assertRaises(RuntimeError):
                adapter.serve_connection(server, handle)
        finally:
            peer.close()
            server.close()
            adapter.close()

    def test_invalid_complete_response_returns_error_and_releases_lease(self):
        async def execute(task, endpoint):
            return b"{}"

        adapter = IncrementalMapAdapter(
            FixedModelConfig("http://localhost/v1/chat/completions", "model", 1000), execute=execute
        )
        peer, server = socket.socketpair()
        try:

            def handle(connection):
                with self.assertRaises(CompletionAdapterError) as caught:
                    adapter.complete(request())
                self.assertEqual(caught.exception.code, "MODEL_RESPONSE_INVALID")

            adapter.serve_connection(server, handle)
            self.assertEqual(adapter.engine.capacity.usage().held_tasks, 0)
        finally:
            peer.close()
            server.close()
            adapter.close()

    def test_gateway_setup_failure_closes_new_transport(self):
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
                        "--incremental-map-window-one",
                    ]
                )
            self.assertEqual((root / "occupied").read_text(), "owned")
        self.assertEqual(
            {t.ident for t in threading.enumerate() if t.name == "semloom-async-io"}, before
        )

    def test_new_execution_digest_is_distinct_and_old_values_stay_supported(self):
        values = {
            v5.provider_execution_digest("model", provider_execution_id=identity)
            for identity in (
                v5.INCREMENTAL_EXECUTION_ID,
                v5.FIXED_EXECUTION_ID,
                v5.GOLDEN_EXECUTION_ID,
            )
        }
        self.assertEqual(len(values), 3)
