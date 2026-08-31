"""Contract tests for the version-3 exact SemFilter golden provider."""

from __future__ import annotations

import socket
import threading
import unittest

from src.execution_provider.adapters.golden import run_golden_session
from src.execution_provider.wire.framing import encode_frame, read_frame
from src.execution_provider.wire.v3 import (
    MAX_INPUT_BYTES,
    PHYSICAL_ALGORITHM,
    PROMPT_PROGRAM_DIGEST,
    PROTOCOL_VERSION,
    RESULT_PARSER_DIGEST,
    SemanticFilterPlan,
    build_open_message,
    build_task_message,
    canonical_messages,
    completion_evidence_digest,
    physical_algorithm_digest,
    provider_execution_digest,
    semantic_payload_digest,
    semantic_spec_digest,
)


class SemloomSemanticProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.plan = SemanticFilterPlan(
            instruction="输入描述数据库系统。",
            model_id="golden-model-v1",
        )

    def test_v3_digest_and_prompt_golden_vectors(self) -> None:
        messages = canonical_messages(
            self.plan.instruction,
            "PostgreSQL is a database.",
        )
        semantic_digest = semantic_spec_digest(self.plan)
        physical_digest = physical_algorithm_digest()
        execution_digest = provider_execution_digest(self.plan.model_id)
        payload_digest = semantic_payload_digest(
            semantic_spec_sha256=semantic_digest,
            input_value="PostgreSQL is a database.",
            canonical_messages_utf8=messages,
        )

        self.assertEqual(PROTOCOL_VERSION, 3)
        self.assertEqual(MAX_INPUT_BYTES, 163_840)
        self.assertEqual(PHYSICAL_ALGORITHM, "MODEL_REFERENCE_SYNC_V1")
        self.assertEqual(
            PROMPT_PROGRAM_DIGEST,
            "83540ad1cf326c0e92272b0cc9389a1bec6321c9213499f0ebf261e7a7313ae1",
        )
        self.assertEqual(
            RESULT_PARSER_DIGEST,
            "39ef25e06022452f7f97083785e01caaaf3acd2c70c44bea02ada7139e45263d",
        )
        self.assertEqual(
            semantic_digest,
            "f13186bba227d4d0c7d50f2ff32d4fa8beb5ed11e89595fba1f5af9c7f0aa8c7",
        )
        self.assertEqual(
            physical_digest,
            "558e50ae5e2716d2e699e09ddb8ffb953f772ba9a1be9dbb15379d9bfcf08d66",
        )
        self.assertEqual(
            execution_digest,
            "eb32f150809fa3f6e26c8e8936b9bb39f84a9ed07b196f5e3499e80be5b8f7f6",
        )
        self.assertEqual(
            payload_digest,
            "20ff2ba46f26fc9680a85978a0e130743a1a4e9b7edae13344262c7630ea443e",
        )
        self.assertEqual(
            completion_evidence_digest(
                semantic_spec_sha256=semantic_digest,
                physical_algorithm_sha256=physical_digest,
                provider_execution_sha256=execution_digest,
                semantic_payload_sha256=payload_digest,
                sequence=7,
                raw_output="TRUE",
                finish_reason="stop",
                response_model_id=self.plan.model_id,
                prompt_tokens=17,
                output_tokens=1,
            ),
            "359d52141cbb069a1e23c91acf0397063f08885a91404768208858b6ba0d2824",
        )
        self.assertEqual(
            messages.decode("utf-8"),
            '[{"role":"system","content":"Evaluate whether the input satisfies the '
            'instruction. Reply with exactly TRUE, FALSE, or UNKNOWN. Use UNKNOWN only '
            'when the input lacks enough information.\\nInstruction:\\n输入描述数据库系统。"},'
            '{"role":"user","content":"PostgreSQL is a database."}]',
        )

    def test_golden_adapter_returns_fixture_bound_completion(self) -> None:
        input_value = "PostgreSQL is a database."
        task = build_task_message(self.plan, sequence=0, input_value=input_value)
        fixtures = {task["semantic_payload_digest"]: "TRUE"}
        client, server = socket.socketpair()
        thread = threading.Thread(
            target=run_golden_session,
            args=(server, fixtures),
        )
        thread.start()
        self.addCleanup(client.close)

        client.sendall(encode_frame(build_open_message(self.plan)))
        opened = read_frame(client)
        self.assertEqual(opened["type"], "opened")
        self.assertEqual(opened["protocol_version"], PROTOCOL_VERSION)
        self.assertEqual(opened["max_input_bytes"], MAX_INPUT_BYTES)

        client.sendall(encode_frame(task))
        completion = read_frame(client)
        self.assertEqual(completion["type"], "completion")
        self.assertEqual(completion["sequence"], "0")
        self.assertEqual(completion["raw_output"], "TRUE")
        self.assertEqual(completion["response_model_id"], self.plan.model_id)
        self.assertEqual(completion["finish_reason"], "stop")
        self.assertEqual(completion["prompt_tokens"], "0")
        self.assertEqual(completion["output_tokens"], "1")

        client.close()
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

    def test_unknown_payload_digest_fails_closed_without_prompt_echo(self) -> None:
        task = build_task_message(self.plan, sequence=0, input_value="private payload")
        client, server = socket.socketpair()
        thread = threading.Thread(target=run_golden_session, args=(server, {}))
        thread.start()
        self.addCleanup(client.close)

        client.sendall(encode_frame(build_open_message(self.plan)))
        self.assertEqual(read_frame(client)["type"], "opened")
        client.sendall(encode_frame(task))
        error = read_frame(client)

        self.assertEqual(
            error,
            {
                "type": "error",
                "protocol_version": PROTOCOL_VERSION,
                "sequence": "0",
                "code": "GOLDEN_FIXTURE_MISSING",
            },
        )
        self.assertNotIn("private payload", repr(error))
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

    def test_strict_nested_fields_and_decimal_uint64_fail_closed(self) -> None:
        open_message = build_open_message(self.plan)
        open_message["generation_constraints"]["future"] = True
        client, server = socket.socketpair()
        thread = threading.Thread(target=run_golden_session, args=(server, {}))
        thread.start()
        self.addCleanup(client.close)
        client.sendall(encode_frame(open_message))
        self.assertEqual(
            read_frame(client),
            {
                "type": "error",
                "protocol_version": PROTOCOL_VERSION,
                "sequence": None,
                "code": "INVALID_OPEN",
            },
        )
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

        for invalid_sequence in (True, 0, "00", "-1", str(2**64)):
            with self.subTest(sequence=invalid_sequence):
                task = build_task_message(self.plan, sequence=0, input_value="x")
                task["sequence"] = invalid_sequence
                client2, server2 = socket.socketpair()
                thread2 = threading.Thread(target=run_golden_session, args=(server2, {}))
                thread2.start()
                client2.sendall(encode_frame(build_open_message(self.plan)))
                self.assertEqual(read_frame(client2)["type"], "opened")
                client2.sendall(encode_frame(task))
                error = read_frame(client2)
                self.assertEqual(error["code"], "INVALID_TASK")
                self.assertIsNone(error["sequence"])
                client2.close()
                thread2.join(timeout=1)
                self.assertFalse(thread2.is_alive())

    def test_completion_fault_fixtures_target_v3_evidence_fields(self) -> None:
        task = build_task_message(self.plan, sequence=0, input_value="fixture")
        fixtures = {task["semantic_payload_digest"]: "TRUE"}
        cases = {
            "v3-model-mismatch": ("response_model_id", "different-model"),
            "v3-invalid-usage": ("prompt_tokens", 0),
            "v3-finish-reason": ("finish_reason", "length"),
            "v3-extra-field": ("future_field", True),
        }
        for fixture, (field, expected) in cases.items():
            with self.subTest(fixture=fixture):
                client, server = socket.socketpair()
                thread = threading.Thread(
                    target=run_golden_session,
                    args=(server, fixtures),
                    kwargs={"completion_fixture": fixture},
                )
                thread.start()
                client.sendall(encode_frame(build_open_message(self.plan)))
                self.assertEqual(read_frame(client)["type"], "opened")
                client.sendall(encode_frame(task))
                completion = read_frame(client)
                self.assertEqual(completion[field], expected)
                client.close()
                thread.join(timeout=1)
                self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
