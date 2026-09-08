"""Window-one lifecycle uses the same production service as multi-Job execution."""

import asyncio
import json
import threading
import unittest
from src.execution_provider.wire import v5, v6
from tests.execution_provider.test_multisession_gateway import service, Client, completion, wait_for


def single(execute, **kwargs):
    return service(execute, max_jobs=1, max_tasks=1, max_active_requests=1, **kwargs)


class IncrementalWindowOneTests(unittest.TestCase):
    def test_consecutive_queries_share_engine_and_keep_task_identity(self):
        submitted = []

        async def execute(task, endpoint):
            submitted.append(task)
            return completion("hello")

        with single(execute) as (path, gateway, events):
            for _ in range(2):
                client = Client(path, window=1)
                try:
                    client.offer("hello")
                    client.poll()
                    self.assertEqual(client.result()["raw_output"], "hello")
                finally:
                    client.close()
            wait_for(lambda: len([e for e in events if e["event"] == "job_drained"]) == 2)
            self.assertEqual([r.key.session_id for r in submitted], [0, 1])
            self.assertEqual([r.member.size for r in submitted], [1, 1])
            self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)

    def test_disconnect_retains_credit_until_authoritative_response(self):
        self.assert_interrupted_peer_drains(pipelined=False)

    def test_pipelined_rpc_is_rejected_without_releasing_remote_credit(self):
        self.assert_interrupted_peer_drains(pipelined=True)

    def assert_interrupted_peer_drains(self, *, pipelined):
        started, release = threading.Event(), threading.Event()

        async def execute(task, endpoint):
            started.set()
            while not release.is_set():
                await asyncio.sleep(0.001)
            return completion("hello")

        with single(execute) as (path, gateway, events):
            client = Client(path, window=1)
            try:
                client.offer("hello")
                client.poll()
                self.assertTrue(started.wait(2))
                if pipelined:
                    client.socket.sendall(b"\x00")
                else:
                    client.close()
                wait_for(lambda: any(e["event"] == "connection_closed" for e in events))
                closed = next(e for e in events if e["event"] == "connection_closed")
                self.assertEqual(closed["usage"]["active_requests"], 1)
            finally:
                release.set()
                client.close()
            wait_for(lambda: any(e["event"] == "job_drained" for e in events))
            recovered = Client(path, window=1)
            try:
                recovered.offer("hello")
                recovered.poll()
                self.assertEqual(recovered.result()["raw_output"], "hello")
            finally:
                recovered.close()

    def test_unknown_result_retains_job_capacity(self):
        async def execute(task, endpoint):
            raise OSError("transport failed")

        with single(execute, unknown=True) as (path, gateway, events):
            client = Client(path, window=1)
            try:
                client.offer("hello")
                client.poll()
                self.assertEqual(client.result()["code"], "MODEL_UNAVAILABLE")
                self.assertEqual(gateway.engine.capacity.usage().active_requests, 1)
                self.assertIsNone(gateway.engine.error)
            finally:
                client.close()

    def test_invalid_response_returns_error_and_releases_lease(self):
        async def execute(task, endpoint):
            return b"{}"

        with single(execute) as (path, gateway, events):
            client = Client(path, window=1)
            try:
                client.offer("hello")
                client.poll()
                self.assertEqual(
                    client.result(), v6.build_error_message("MODEL_RESPONSE_INVALID", sequence=0)
                )
            finally:
                client.close()
            wait_for(lambda: any(e["event"] == "job_drained" for e in events))
            self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)

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
