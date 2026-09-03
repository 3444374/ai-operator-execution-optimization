"""Public gateway CLI checks for explicit Map fixture metadata and wire routing."""

import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest

from src.execution_provider.completion import Completion
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.wire import v5
from src.execution_provider.wire.framing import MAX_FRAME_BYTES, encode_frame, read_frame


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
CLI = CODE_ROOT / "scripts/services/run_execution_provider_gateway.py"


class MapGatewayTests(unittest.TestCase):
    def test_self_locating_cli_survives_invalid_open_and_preserves_map_metadata(self) -> None:
        plan = SemanticMapPlan("Echo the input.", "golden-map-v1", 128)
        task = v5.build_task_message(plan, sequence=0, input_value="hello")
        expected = Completion(' \nTRUE 世界\t ', plan.model_id, 23, 7, "stop")
        fixture = {task["semantic_payload_digest"]: {
            "raw_output": expected.raw_output, "response_model_id": plan.model_id,
            "prompt_tokens": 23, "output_tokens": 7, "finish_reason": "stop",
        }}
        deep_json = b'{"nested":' + b"[" * 10000 + b"0" + b"]" * 10000 + b"}"
        self.assertLess(len(deep_json), MAX_FRAME_BYTES)
        invalid_frames = (
            ("integer-open", b'{"protocol_version":5,"max_tokens":' + b"9" * 5000 + b"}", False),
            ("deep-open", deep_json, False),
            ("deep-task", deep_json, True),
        )
        with tempfile.TemporaryDirectory(prefix="map-gw-", dir="/tmp") as directory:
            path = Path(directory)
            fixture_path = path / "fixture.json"
            fixture_path.write_text(json.dumps(fixture), encoding="utf-8")
            socket_path = path / "provider.sock"
            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            process = subprocess.Popen([sys.executable, str(CLI), "--test-max-sessions", "6", "--socket", str(socket_path),
                                        "--golden-fixture", str(fixture_path)],
                                       cwd=directory, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.settimeout(3)
            try:
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline and process.poll() is None:
                    try:
                        client.connect(str(socket_path))
                        break
                    except (FileNotFoundError, ConnectionRefusedError):
                        time.sleep(0.01)
                else:
                    self.fail(f"gateway did not start: {process.communicate(timeout=1)[1]!r}")
                opened = v5.build_open_message(plan)
                for index, (label, payload, after_open) in enumerate(invalid_frames):
                    with self.subTest(invalid_frame=label):
                        if index > 0:
                            client.close()
                            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                            client.settimeout(3)
                            client.connect(str(socket_path))
                        if after_open:
                            client.sendall(encode_frame(opened))
                            self.assertEqual(read_frame(client)["type"], "opened")
                        client.sendall(struct.pack("!I", len(payload)) + payload)
                        if after_open:
                            self.assertEqual(read_frame(client), {"type": "error", "protocol_version": 5,
                                                                 "sequence": "0", "code": "INVALID_TASK"})
                        self.assertIsNone(read_frame(client))
                        client.close()
                        self.assertIsNone(process.poll())
                        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                        client.settimeout(3)
                        client.connect(str(socket_path))
                        client.sendall(encode_frame(opened))
                        self.assertEqual(read_frame(client)["type"], "opened")
                        for sequence in (0, 1):
                            task["sequence"] = str(sequence)
                            client.sendall(encode_frame(task))
                            actual = v5.validate_completion(read_frame(client), expected_sequence=sequence,
                                payload_digest=task["semantic_payload_digest"], open_context=v5.validate_open(opened))
                            self.assertEqual(actual, expected)
                        client.close()
                stdout, stderr = process.communicate(timeout=3)
                self.assertEqual((process.returncode, stdout, stderr), (0, b"", b""))
                self.assertFalse(socket_path.exists())
            finally:
                client.close()
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=3)


if __name__ == "__main__":
    unittest.main()
