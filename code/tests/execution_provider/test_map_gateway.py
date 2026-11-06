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
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.execution_provider.completion import Completion
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.session_dispatch import run_session
from src.execution_provider.wire import map_codec
from src.execution_provider.wire.framing import MAX_FRAME_BYTES, encode_frame, read_frame


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
CLI = CODE_ROOT / "scripts/services/run_execution_provider_gateway.py"


class MapGatewayTests(unittest.TestCase):
    def test_self_locating_cli_survives_invalid_open_and_preserves_map_metadata(self) -> None:
        plan = SemanticMapPlan("Echo the input.", "golden-map-v1", 128)
        task = map_codec.build_task_message(plan, sequence=0, input_value="hello")
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
                opened = map_codec.build_open_message(plan)
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
                            actual = map_codec.validate_completion(read_frame(client), expected_sequence=sequence,
                                payload_digest=task["semantic_payload_digest"], open_context=map_codec.validate_open(opened))
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


class ProviderServiceCliTests(unittest.TestCase):
    def test_golden_and_image_profiles_reach_the_existing_adapters(self):
        from src.execution_provider import server, image_gateway
        from src.execution_provider.adapters.golden import GoldenCompletionAdapter
        from src.execution_provider.adapters.image_execution import ImageServiceConfig

        for profile in ("golden", "image"):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as directory:
                args = ["--socket", str(Path(directory, "provider.sock"))]
                if profile == "image":
                    config_path = Path(directory, "image.json")
                    config_path.write_text("{}")
                    args.extend(["--image-config", str(config_path), "--max-held-tasks", "4"])
                image_config = SimpleNamespace(mode="reference", plan=SimpleNamespace(result_bytes=8))
                adapter = Mock()
                selected = []
                stopped_before_listener = RuntimeError("fixture stops after adapter selection")

                def observe_selection(value):
                    selected.append(value)
                    raise stopped_before_listener

                with patch.object(ImageServiceConfig, "from_record", return_value=image_config), \
                     patch.object(image_gateway, "ImageGateway", return_value=adapter) as build_image, \
                     patch.object(server.socket, "socket", side_effect=AssertionError("unexpected socket")):
                    with self.assertRaises(RuntimeError) as caught:
                        server.main(args, adapter_wrapper=observe_selection)
                self.assertIs(caught.exception, stopped_before_listener)
                if profile == "golden":
                    self.assertIsInstance(selected[0], GoldenCompletionAdapter)
                    build_image.assert_not_called()
                else:
                    self.assertIs(selected[0], adapter)
                    self.assertIs(build_image.call_args.args[0], image_config)
                    self.assertEqual(build_image.call_args.kwargs["max_tasks"], 4)
                    self.assertEqual(build_image.call_args.kwargs["result_bytes"], 32)
                    adapter.close.assert_called_once_with()


class DispatchCloseTests(unittest.TestCase):
    def test_invalid_or_exhausted_open_closes_once_without_dispatch(self):
        for opened in (None, ValueError('invalid fixture frame')):
            with self.subTest(opened=opened):
                connection = Mock()
                with patch('src.execution_provider.session_dispatch.read_frame', side_effect=[opened]), \
                        patch('src.execution_provider.session_dispatch.run_recording_session') as dispatch:
                    self.assertIsNone(run_session(connection, completion_adapter=object(), response_delay_ms=0,
                        tamper_evidence_digest=False, disconnect_on_task=False, completion_fixture=None))
                connection.close.assert_called_once_with()
                dispatch.assert_not_called()

if __name__ == "__main__":
    unittest.main()
