"""Exercise independent sessions and bounded admission through the public gateway."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from src.execution_provider.completion import Completion
from src.execution_provider.wire import v3
from src.execution_provider.wire.framing import encode_frame, read_frame

CODE = Path(__file__).resolve().parents[2]
CLI = CODE / "scripts/services/run_execution_provider_gateway.py"


class GatewaySessionsTests(unittest.TestCase):
    @contextmanager
    def gateway(self, *options):
        with tempfile.TemporaryDirectory(prefix="gw-sessions-", dir="/tmp") as directory:
            root = Path(directory)
            plans = [
                v3.SemanticFilterPlan(name, "golden-model-v1") for name in ("First.", "Second.")
            ]
            fixtures = {
                v3.build_task_message(plan, sequence=0, input_value="same")[
                    "semantic_payload_digest"
                ]: value
                for plan, value in zip(plans, ("TRUE", "FALSE"))
            }
            fixture = root / "fixture.json"
            fixture.write_text(json.dumps(fixtures))
            path = root / "gateway.sock"
            env = os.environ.copy()
            env.pop("PYTHONPATH", None)
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(CLI),
                    "--socket",
                    str(path),
                    "--golden-fixture",
                    str(fixture),
                    *options,
                ],
                cwd=root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                deadline = time.monotonic() + 3
                while not path.exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(path.exists(), "gateway must start")
                yield path, plans, process
            finally:
                if process.poll() is None:
                    process.terminate()
                try:
                    stdout, stderr = process.communicate(timeout=4)
                except subprocess.TimeoutExpired:
                    process.kill()
                    stdout, stderr = process.communicate(timeout=2)
                    self.fail("gateway did not stop after clients closed")
                self.assertEqual((process.returncode, stdout, stderr), (0, b"", b""))
                self.assertFalse(path.exists())

    def connect(self, path):
        connection = socket.socket(socket.AF_UNIX)
        connection.settimeout(1)
        connection.connect(str(path))
        return connection

    def open(self, connection, plan):
        connection.sendall(encode_frame(v3.build_open_message(plan)))
        self.assertEqual(read_frame(connection)["type"], "opened")

    def complete(self, connection, plan, sequence, expected):
        task = v3.build_task_message(plan, sequence=sequence, input_value="same")
        connection.sendall(encode_frame(task))
        expected_frame = v3.build_completion_message(
            v3.validate_open(v3.build_open_message(plan)),
            sequence=sequence,
            payload_digest=task["semantic_payload_digest"],
            completion=Completion(expected, plan.model_id, 0, 1, "stop"),
        )
        self.assertEqual(read_frame(connection), expected_frame)

    def test_idle_session_does_not_block_another_and_errors_are_isolated(self):
        with self.gateway() as (path, plans, process):
            with self.connect(path) as first, self.connect(path) as second:
                self.open(first, plans[0])
                self.open(second, plans[1])
                self.complete(second, plans[1], 0, "FALSE")
                self.complete(first, plans[0], 0, "TRUE")
                self.complete(second, plans[1], 1, "FALSE")
                first.sendall(
                    encode_frame(v3.build_task_message(plans[0], sequence=0, input_value="same"))
                )
                self.assertEqual(read_frame(first)["type"], "error")
                self.assertIsNone(read_frame(first))
                self.complete(second, plans[1], 2, "FALSE")
                self.assertIsNone(process.poll())

    def test_same_plan_and_payload_have_independent_sequences(self):
        with self.gateway() as (path, plans, _):
            with self.connect(path) as first, self.connect(path) as second:
                self.open(first, plans[0])
                self.open(second, plans[0])
                for connection, sequence in [
                    (first, 0),
                    (first, 1),
                    (second, 0),
                    (first, 2),
                    (second, 1),
                ]:
                    self.complete(connection, plans[0], sequence, "TRUE")

    def test_connection_limit_rejects_excess_without_blocking_existing_sessions(self):
        with self.gateway("--max-connections", "2") as (path, plans, _):
            with self.connect(path) as first, self.connect(path) as second:
                self.open(first, plans[0])
                self.open(second, plans[1])
                with self.connect(path) as excess:
                    try:
                        self.assertEqual(excess.recv(1), b"")
                    except ConnectionResetError:
                        pass
                self.complete(second, plans[1], 0, "FALSE")
                self.complete(first, plans[0], 0, "TRUE")

    @unittest.skipUnless(sys.platform == "linux", "requires Linux process counters")
    def test_repeated_sessions_release_fds_and_threads_while_gateway_stays_alive(self):
        with self.gateway() as (path, plans, process):
            process_root = Path("/proc") / str(process.pid)

            def counters():
                return (
                    len(list((process_root / "fd").iterdir())),
                    len(list((process_root / "task").iterdir())),
                )

            baseline = counters()
            for _ in range(40):
                with self.connect(path) as connection:
                    self.open(connection, plans[0])
                    self.complete(connection, plans[0], 0, "TRUE")
            deadline = time.monotonic() + 2
            while counters() != baseline and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(counters(), baseline)
            self.assertIsNone(process.poll())

    def test_shutdown_unblocks_idle_connections(self):
        with self.gateway() as (path, plans, process):
            with self.connect(path) as first, self.connect(path) as second:
                self.open(first, plans[0])
                self.open(second, plans[1])
                process.terminate()
                self.assertIsNone(read_frame(first))
                self.assertIsNone(read_frame(second))
                process.wait(timeout=3)

    def test_shutdown_also_unblocks_a_finite_session_drain(self):
        with self.gateway("--once") as (path, plans, process):
            with self.connect(path) as connection:
                self.open(connection, plans[0])
                process.terminate()
                self.assertIsNone(read_frame(connection))
                process.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()


class ConnectionLifecycleTests(unittest.TestCase):
    def test_capacity_is_rechecked_after_blocked_accept(self):
        from src.execution_provider.gateway_runtime import accept_connection

        class Connection:
            closed = False

            def settimeout(self, value):
                self.timeout = value

            def close(self):
                self.closed = True

        occupied = [1]
        connection = Connection()

        class Listener:
            def accept(self):
                # A previous handler can exit while the listener waits for a new peer.
                occupied[0] = 0
                return connection, None

        accepted = accept_connection(
            Listener(), occupied=lambda: occupied[0], maximum=1, timeout_s=1
        )
        self.assertIs(accepted, connection)
        self.assertFalse(connection.closed)
