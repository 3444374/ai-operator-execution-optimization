"""Behavior contracts for the recording gateway's canonical-path migration."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
LEGACY_GATEWAY_ROOT = CODE_ROOT / "postgres" / "semloom_pg" / "gateway"
LEGACY_GATEWAY_CLI = LEGACY_GATEWAY_ROOT / "recording_gateway.py"
CANONICAL_GATEWAY_CLI = CODE_ROOT / "scripts" / "services" / "run_execution_provider_gateway.py"


def _clean_python_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    return environment


class GatewayMigrationTests(unittest.TestCase):
    def test_canonical_wire_v2_preserves_frozen_digest_vectors(self) -> None:
        sys.path.insert(0, str(CODE_ROOT))
        self.addCleanup(sys.path.remove, str(CODE_ROOT))

        from src.execution_provider.wire.v2 import (  # noqa: PLC0415
            physical_algorithm_digest,
            provider_execution_digest,
            semantic_payload_digest,
            semantic_spec_digest,
        )

        self.assertEqual(
            semantic_spec_digest(),
            "83f62acc5bc7fcc92644d949d05c359f53ea610cda240fcff0f3a3938c7f0df1",
        )
        self.assertEqual(
            physical_algorithm_digest(),
            "3bfda6657ed427401fe64f723680caa18e9daf112bbb8694bf3efdd3c9344936",
        )
        self.assertEqual(
            provider_execution_digest(),
            "7154a5805b8ca4d5b56c4aa5401a592e636ae98a70aea448b8961fb0bbab528c",
        )
        self.assertEqual(
            semantic_payload_digest("héllo世界"),
            "2df0c970538d8ac3a604e88753aef3d587c6ae04bf5402d0798c951d810a4a30",
        )

    def test_legacy_protocol_reexports_canonical_public_api(self) -> None:
        command = (
            "import sys; "
            f"sys.path.insert(0, {str(LEGACY_GATEWAY_ROOT)!r}); "
            "import protocol; "
            "from src.execution_provider.wire import v2; "
            "from src.execution_provider.adapters.recording import run_recording_session; "
            "assert protocol.encode_frame is v2.encode_frame; "
            "assert protocol.run_recording_session is run_recording_session"
        )
        result = subprocess.run(
            [sys.executable, "-c", command],
            cwd=tempfile.gettempdir(),
            env=_clean_python_environment(),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_legacy_and_canonical_clis_are_self_locating(self) -> None:
        for cli_path in (LEGACY_GATEWAY_CLI, CANONICAL_GATEWAY_CLI):
            with self.subTest(cli_path=cli_path):
                result = subprocess.run(
                    [sys.executable, str(cli_path), "--help"],
                    cwd=tempfile.gettempdir(),
                    env=_clean_python_environment(),
                    capture_output=True,
                    text=True,
                    check=False,
                )

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--socket", result.stdout)

    def test_canonical_cli_serves_the_frozen_recording_contract(self) -> None:
        sys.path.insert(0, str(CODE_ROOT))
        self.addCleanup(sys.path.remove, str(CODE_ROOT))

        from src.execution_provider.wire.v2 import (  # noqa: PLC0415
            PROTOCOL_VERSION,
            RECORDING_ALGORITHM,
            RECORDING_SPEC_ID,
            RECORDING_SPEC_VERSION,
            UDS_EXECUTION_ID,
            encode_frame,
            physical_algorithm_digest,
            provider_execution_digest,
            read_frame,
            semantic_payload_digest,
            semantic_spec_digest,
        )

        with tempfile.TemporaryDirectory(prefix="sg-", dir="/tmp") as temporary_directory:
            socket_path = Path(temporary_directory) / "gateway.sock"
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(CANONICAL_GATEWAY_CLI),
                    "--socket",
                    str(socket_path),
                    "--once",
                ],
                cwd=tempfile.gettempdir(),
                env=_clean_python_environment(),
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
                self.fail(f"canonical gateway did not accept a connection: {stderr}")

            identity = {
                "semantic_spec_digest": semantic_spec_digest(),
                "physical_algorithm_digest": physical_algorithm_digest(),
                "provider_execution_digest": provider_execution_digest(),
            }
            client.sendall(
                encode_frame(
                    {
                        "type": "open",
                        "protocol_version": PROTOCOL_VERSION,
                        **identity,
                        "provider_execution_id": UDS_EXECUTION_ID,
                        "operator_kind": "SEM_MAP",
                        "semantic_spec_id": RECORDING_SPEC_ID,
                        "semantic_spec_version": RECORDING_SPEC_VERSION,
                        "physical_algorithm": RECORDING_ALGORITHM,
                        "null_policy": "PROPAGATE_NULL",
                        "error_policy": "FAIL_QUERY",
                        "input_type": "text",
                        "output_type": "text",
                    }
                )
            )
            self.assertEqual(read_frame(client)["type"], "opened")

            input_value = "héllo世界"
            client.sendall(
                encode_frame(
                    {
                        "type": "task",
                        "protocol_version": PROTOCOL_VERSION,
                        "sequence": "0",
                        **identity,
                        "payload_digest": semantic_payload_digest(input_value),
                        "is_null": False,
                        "input": input_value,
                    }
                )
            )
            completion = read_frame(client)
            self.assertEqual(completion["output"], "recorded:héllo世界")
            client.close()

            stdout, stderr = process.communicate(timeout=2)
            self.assertEqual(process.returncode, 0, stderr)
            self.assertEqual(stdout, "")
            self.assertEqual(stderr, "")
            self.assertFalse(socket_path.exists())

    def test_legacy_files_are_bootstrap_only(self) -> None:
        protocol_source = (LEGACY_GATEWAY_ROOT / "protocol.py").read_text(encoding="utf-8")
        gateway_source = LEGACY_GATEWAY_CLI.read_text(encoding="utf-8")

        for forbidden in (
            "import hashlib",
            "import json",
            "import socket",
            "def encode_frame",
            "def read_frame",
            "def run_recording_session",
        ):
            self.assertNotIn(forbidden, protocol_source)
        for forbidden in (
            "import argparse",
            "import socket",
            "listener.bind",
            "listener.accept",
            "def parse_args",
        ):
            self.assertNotIn(forbidden, gateway_source)


if __name__ == "__main__":
    unittest.main()
