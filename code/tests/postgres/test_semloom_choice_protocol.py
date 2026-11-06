"""Version-four profile identity and fixed-endpoint contract tests."""
from __future__ import annotations

import unittest
import copy
import json
import socket
import threading
import struct

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










if __name__ == "__main__":
    unittest.main()
