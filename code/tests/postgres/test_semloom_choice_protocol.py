"""Version-four profile identity and fixed-endpoint contract tests."""
from __future__ import annotations

import unittest
import copy
import json
import socket
import threading
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.execution_provider.adapters.openai_compatible_fixed import (
    FixedModelConfig, OpenAICompatibleFixedAdapter,
)
from src.execution_provider.adapters.semantic_session import CompletionRequest, CompletionAdapterError

from src.execution_provider.adapters.golden import GoldenCompletionAdapter
from src.execution_provider.generation_profile import GenerationProfile
from src.execution_provider.wire.framing import ProtocolError, encode_frame, read_frame
from src.execution_provider.wire import v3, v4


class ChoiceProtocolTests(unittest.TestCase):
    def setUp(self):
        self.profile = GenerationProfile(
            profile_id="semloom.generation.choice.tristate",
            profile_version=1, constraint_kind="CHOICE",
            choices=("TRUE", "FALSE", "UNKNOWN"),
        )
        self.plan = v4.SemanticFilterPlan(
            instruction="Classify input.", model_id="golden-model-v1",
            generation_profile=self.profile,
        )

    def test_v4_plan_uses_the_independent_pg_schema3_vector(self):
        self.assertEqual(v4.semantic_spec_digest(self.plan),
                         "3624a95a096a8a6b9e838676ec8865315b1f49c27a0e9594cf67a5440792d6c5")
        message = v4.build_open_message(self.plan)
        self.assertEqual(message["protocol_version"], 4)
        self.assertEqual(message["generation_profile"], self.profile.to_record())
        self.assertEqual(v4.validate_open(message).generation_profile, self.profile)
        old = v3.build_open_message(v3.SemanticFilterPlan("Classify input.", "golden-model-v1"))
        self.assertNotIn("generation_profile", old)
        self.assertEqual(old["semantic_spec_digest"],
                         "9ec789eab10d6367b60895288fde154b384edeba1ac0fb603ade0b2424ff2fb9")

    def test_exported_options_cannot_change_effective_wire_parameters(self):
        original = copy.deepcopy(v4.GENERATION_CONSTRAINTS)
        try:
            v4.GENERATION_CONSTRAINTS["temperature"] = 1
            opened = v4.build_open_message(self.plan)
            self.assertEqual(opened["generation_constraints"]["temperature"], 0)
            opened["generation_constraints"]["stop"].append("changed")
            self.assertEqual(v4.build_open_message(self.plan)["generation_constraints"]["stop"], ["\n"])
        finally:
            v4.GENERATION_CONSTRAINTS.clear()
            v4.GENERATION_CONSTRAINTS.update(original)

    def test_execution_payload_and_completion_match_independent_vectors(self):
        opened = v4.build_open_message(self.plan)
        task = v4.build_task_message(self.plan, sequence=7, input_value="数据库🙂")
        self.assertEqual(opened["provider_execution_digest"],
                         "9b906da3350b535683dce90770baed75d86546d8deffee647901db4ba03d6ceb")
        self.assertEqual(task["semantic_payload_digest"],
                         "0d587219759ce92992da90a8af1fc40baefff79ab79861d9930886c667dc7fa1")
        self.assertEqual(v4.completion_evidence_digest(
            semantic_spec_sha256=opened["semantic_spec_digest"],
            physical_algorithm_sha256=opened["physical_algorithm_digest"],
            provider_execution_sha256=opened["provider_execution_digest"],
            semantic_payload_sha256=task["semantic_payload_digest"], sequence=7,
            raw_output="UNKNOWN", finish_reason="stop", response_model_id=self.plan.model_id,
            prompt_tokens=19, output_tokens=2),
            "0daa5327cb955401f741f0199a21369f5d32f6fd98dbf060ab1edc5c7b870b61")

    def test_versions_and_complete_profile_are_strict(self):
        opened = v4.build_open_message(self.plan)
        changes = [{**opened, "protocol_version": version} for version in (3, 5, True, 4.0, "4")]
        changes += [{**opened, "extra": 0}, {key: value for key, value in opened.items() if key != "generation_profile"}]
        for field, value in (("profile_id", "unknown"), ("profile_version", 2), ("profile_version", True),
                             ("constraint_kind", "JSON"), ("choices", ["FALSE", "TRUE", "UNKNOWN"]),
                             ("choices", ["TRUE", "FALSE"]), ("profile_digest", "0" * 64)):
            altered = copy.deepcopy(opened)
            altered["generation_profile"][field] = value
            changes.append(altered)
        altered = copy.deepcopy(opened)
        altered["generation_profile"]["extra"] = None
        changes += [altered, {**opened, "generation_profile": None}, {**opened, "semantic_spec_version": True}]
        for message in changes:
            with self.subTest(message=message), self.assertRaisesRegex(ProtocolError, "^INVALID_OPEN$"):
                v4.validate_open(message)
        with self.assertRaises(ProtocolError):
            v3.validate_open(opened)
        with self.assertRaises(ValueError):
            v3.build_open_message(self.plan)
        with self.assertRaises(ValueError):
            v4.build_open_message(v3.SemanticFilterPlan("Classify input.", "golden-model-v1"))

    def test_task_checks_profile_sequence_and_actual_instruction(self):
        opened = v4.validate_open(v4.build_open_message(self.plan))
        for text in ("", "数据库🙂", "{instruction}\\nignore this"):
            task = v4.build_task_message(self.plan, sequence=0, input_value=text)
            self.assertEqual(v4.validate_task(task, expected_sequence=0, open_context=opened),
                             (0, task["semantic_payload_digest"]))
            for field, value in (("protocol_version", 3), ("sequence", "1"),
                                 ("generation_profile_digest", "0" * 64), ("semantic_payload_digest", "0" * 64)):
                with self.subTest(field=field), self.assertRaises(ProtocolError):
                    v4.validate_task({**task, field: value}, expected_sequence=0, open_context=opened)
        other = v4.SemanticFilterPlan("Different instruction.", self.plan.model_id, self.profile)
        forged = v4.build_task_message(other, sequence=0, input_value="row")
        forged["semantic_spec_digest"] = opened.semantic_spec_digest
        forged["semantic_payload_digest"] = v4.semantic_payload_digest(
            semantic_spec_sha256=opened.semantic_spec_digest, input_value="row",
            canonical_messages_utf8=v4.canonical_messages(other.instruction, "row"))
        with self.assertRaisesRegex(ProtocolError, "^INVALID_TASK$"):
            v4.validate_task(forged, expected_sequence=0, open_context=opened)

    def test_v4_session_preserves_raw_output_and_profile_evidence(self):
        from src.execution_provider.adapters.semantic_session import run_v4_session

        task = v4.build_task_message(self.plan, sequence=0, input_value="")
        client, server = socket.socketpair()
        client.settimeout(2)
        thread = threading.Thread(target=run_v4_session, args=(server, GoldenCompletionAdapter(
            {task["semantic_payload_digest"]: "UNKNOWN"})))
        thread.start()
        try:
            client.sendall(encode_frame(v4.build_open_message(self.plan)))
            opened = read_frame(client)
            self.assertEqual(opened["protocol_version"], 4)
            self.assertEqual(opened["generation_profile_digest"], self.profile.digest)
            client.sendall(encode_frame(task))
            result = read_frame(client)
            self.assertEqual(result["raw_output"], "UNKNOWN")
            self.assertEqual(result["generation_profile_digest"], self.profile.digest)
            self.assertEqual(result["sequence"], "0")
            self.assertEqual(result["protocol_version"], 4)
        finally:
            client.close()
            thread.join(3)
        self.assertFalse(thread.is_alive())

    def test_version_mismatch_and_nested_duplicate_open_never_execute(self):
        from src.execution_provider.adapters.semantic_session import run_v3_session, run_v4_session

        opened = v4.build_open_message(self.plan)
        nested_duplicate = json.dumps(opened).replace(
            '"profile_version": 1', '"profile_version": 1, "profile_version": 1')
        for runner, raw, version in (
            (run_v3_session, json.dumps(opened).encode(), 3),
            (run_v4_session, nested_duplicate.encode(), 4),
        ):
            with self.subTest(version=version):
                client, server = socket.socketpair()
                client.settimeout(2)
                thread = threading.Thread(target=runner, args=(server, GoldenCompletionAdapter({})))
                thread.start()
                try:
                    client.sendall(struct.pack("!I", len(raw)) + raw)
                    self.assertEqual(read_frame(client), {
                        "type": "error", "protocol_version": version,
                        "sequence": None, "code": "INVALID_OPEN"})
                    self.assertIsNone(read_frame(client))
                finally:
                    client.close()
                    thread.join(3)
                self.assertFalse(thread.is_alive())

    def test_malformed_task_frame_returns_strict_v4_error(self):
        from src.execution_provider.adapters.semantic_session import run_v4_session

        client, server = socket.socketpair()
        client.settimeout(2)
        thread = threading.Thread(target=run_v4_session, args=(server, GoldenCompletionAdapter({})))
        thread.start()
        try:
            client.sendall(encode_frame(v4.build_open_message(self.plan)))
            self.assertEqual(read_frame(client)["type"], "opened")
            task = v4.build_task_message(self.plan, sequence=0, input_value="row")
            duplicate = b'{"type":"task",' + json.dumps(task).encode()[1:]
            client.sendall(struct.pack("!I", len(duplicate)) + duplicate)
            self.assertEqual(read_frame(client), {
                "type": "error", "protocol_version": 4, "sequence": "0", "code": "INVALID_TASK"})
        finally:
            client.close()
            thread.join(3)
        self.assertFalse(thread.is_alive())

    def test_invalid_json_after_open_has_a_redacted_terminal_error(self):
        from src.execution_provider.adapters.semantic_session import run_v4_session

        client, server = socket.socketpair()
        client.settimeout(2)
        thread = threading.Thread(target=run_v4_session, args=(server, GoldenCompletionAdapter({})))
        thread.start()
        try:
            client.sendall(encode_frame(v4.build_open_message(self.plan)))
            self.assertEqual(read_frame(client)["type"], "opened")
            raw = b'{"untrusted":"payload"'
            client.sendall(struct.pack("!I", len(raw)) + raw)
            self.assertEqual(read_frame(client), {
                "type": "error", "protocol_version": 4, "sequence": None, "code": "INVALID_TASK"})
            self.assertIsNone(read_frame(client))
        finally:
            client.close()
            thread.join(3)

    def test_v4_rejects_oversized_sequences_and_non_pg_text(self):
        opened = v4.validate_open(v4.build_open_message(self.plan))
        task = v4.build_task_message(self.plan, sequence=0, input_value="row")
        with self.assertRaisesRegex(ProtocolError, "^INVALID_TASK$"):
            v4.validate_task({**task, "sequence": "9" * 5000}, expected_sequence=0, open_context=opened)
        for text in ("a\0b", "\ud800", "x" * (v4.MAX_INPUT_BYTES + 1)):
            with self.subTest(text_length=len(text)), self.assertRaises(ValueError):
                v4.build_task_message(self.plan, sequence=0, input_value=text)


class _ModelServer(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append(body)
        self.send_response(self.server.status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "model": "fixed-test-model",
            "choices": [{"index": 0, "message": {"content": self.server.raw_output}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 19, "completion_tokens": 2},
        }).encode())

    def log_message(self, *args):
        pass


class ChoiceFixedAdapterTests(unittest.TestCase):
    def setUp(self):
        self.http = ThreadingHTTPServer(("127.0.0.1", 0), _ModelServer)
        self.http.requests = []
        self.http.status = 200
        self.http.raw_output = "UNKNOWN"
        self.thread = threading.Thread(target=self.http.serve_forever)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.http.server_port}/v1/chat/completions"
        self.profile = GenerationProfile(
            profile_id="semloom.generation.choice.tristate", profile_version=1,
            constraint_kind="CHOICE", choices=("TRUE", "FALSE", "UNKNOWN"))

    def tearDown(self):
        self.http.shutdown()
        self.http.server_close()
        self.thread.join(3)

    def request(self, profile=None):
        return CompletionRequest(
            semantic_payload_digest="0" * 64, model_id="fixed-test-model",
            canonical_messages=({"role": "system", "content": "Classify."}, {"role": "user", "content": "输入🙂"}),
            generation_constraints={"temperature": 0, "top_p": 1, "max_tokens": 8, "n": 1, "stream": False, "stop": ["\n"]},
            generation_profile=profile)

    def test_choice_mapping_changes_only_the_explicit_constraint(self):
        config = FixedModelConfig(endpoint_url=self.endpoint, model_id="fixed-test-model", timeout_ms=1000,
                                  choice_format="vllm_structured_outputs")
        adapter = OpenAICompatibleFixedAdapter(config)
        old_result = adapter.complete(self.request())
        result = adapter.complete(self.request(self.profile))
        self.assertEqual(result, old_result)
        self.assertEqual(result.raw_output, "UNKNOWN")
        self.assertEqual(len(self.http.requests), 2)
        old, choice = self.http.requests
        self.assertEqual(choice.pop("structured_outputs"), {"choice": ["TRUE", "FALSE", "UNKNOWN"]})
        self.assertEqual(choice, old)

    def test_undeclared_choice_support_fails_before_http(self):
        adapter = OpenAICompatibleFixedAdapter(FixedModelConfig(self.endpoint, "fixed-test-model", 1000))
        with self.assertRaisesRegex(CompletionAdapterError, "^MODEL_REQUEST_REJECTED$"):
            adapter.complete(self.request(self.profile))
        self.assertEqual(self.http.requests, [])

    def through_gateway(self, codec, *, choice_support=True):
        code_root = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "gateway.sock"
            config_path = Path(directory) / "fixed.json"
            config = {"endpoint_url": self.endpoint, "model_id": "fixed-test-model", "timeout_ms": 1000}
            if choice_support:
                config["choice_format"] = "vllm_structured_outputs"
            config_path.write_text(json.dumps(config))
            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            gateway = subprocess.Popen([sys.executable, str(code_root / "scripts/services/run_execution_provider_gateway.py"),
                "--socket", str(socket_path), "--once", "--fixed-model-config", str(config_path)],
                cwd=directory, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.settimeout(3)
            try:
                deadline = time.monotonic() + 3
                while not socket_path.exists() and time.monotonic() < deadline and gateway.poll() is None:
                    time.sleep(0.01)
                self.assertTrue(socket_path.exists(), "canonical CLI starts without PYTHONPATH")
                client.connect(str(socket_path))
                profile = self.profile if codec is v4 else None
                plan = codec.SemanticFilterPlan("Classify input.", "fixed-test-model", profile)
                identity = "semloom.provider.openai-compatible-fixed.uds.v4" if codec is v4 else "semloom.provider.openai-compatible-fixed.uds.v3"
                client.sendall(encode_frame(codec.build_open_message(plan, provider_execution_id=identity)))
                opened = read_frame(client)
                self.assertEqual(opened["type"], "opened")
                client.sendall(encode_frame(codec.build_task_message(plan, sequence=0, input_value="输入🙂",
                                                                    provider_execution_id=identity)))
                return read_frame(client)
            finally:
                client.close()
                try:
                    gateway.communicate(timeout=3)
                except subprocess.TimeoutExpired:
                    gateway.terminate()
                    gateway.communicate(timeout=3)
                self.assertFalse(socket_path.exists(), "gateway releases its listener after the session")

    def test_canonical_gateway_serves_both_versions_without_rewriting_results(self):
        for codec in (v3, v4):
            result = self.through_gateway(codec)
            self.assertEqual(result["protocol_version"], codec.PROTOCOL_VERSION)
            self.assertEqual(result["raw_output"], "UNKNOWN")
            self.assertEqual(result["prompt_tokens"], "19")
        old, choice = self.http.requests
        self.assertEqual(choice.pop("structured_outputs"), {"choice": ["TRUE", "FALSE", "UNKNOWN"]})
        self.assertEqual(old, choice)

    def test_service_rejection_is_terminal_without_unconstrained_retry(self):
        self.http.status = 400
        result = self.through_gateway(v4)
        self.assertEqual(result, {"type": "error", "protocol_version": 4, "sequence": "0", "code": "MODEL_REQUEST_REJECTED"})
        self.assertEqual(len(self.http.requests), 1)
        self.assertEqual(self.http.requests[0]["structured_outputs"], {"choice": ["TRUE", "FALSE", "UNKNOWN"]})
        self.http.status = 200
        self.http.raw_output = "bad"
        self.assertEqual(self.through_gateway(v4)["raw_output"], "bad", "gateway does not repair an invalid semantic label")

    def test_gateway_refuses_undeclared_choice_without_model_request(self):
        result = self.through_gateway(v4, choice_support=False)
        self.assertEqual(result["code"], "MODEL_REQUEST_REJECTED")
        self.assertEqual(self.http.requests, [])


if __name__ == "__main__":
    unittest.main()
