"""Contract tests for the SemLoom UDS recording protocol."""

from __future__ import annotations

import socket
import threading
import unittest
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
GATEWAY_ROOT = CODE_ROOT / "postgres" / "semloom_pg" / "gateway"

import sys

sys.path.insert(0, str(GATEWAY_ROOT))

from protocol import (  # noqa: E402
    MAX_FRAME_BYTES,
    MAX_INPUT_BYTES,
    PROTOCOL_VERSION,
    RECORDING_ALGORITHM,
    RECORDING_SPEC_ID,
    RECORDING_SPEC_VERSION,
    UDS_EXECUTION_ID,
    completion_evidence_digest,
    encode_frame,
    physical_algorithm_digest,
    provider_execution_digest,
    read_frame,
    run_recording_session,
    semantic_payload_digest,
    semantic_spec_digest,
)


def _identity_fields() -> dict[str, str]:
    return {
        "semantic_spec_digest": semantic_spec_digest(),
        "physical_algorithm_digest": physical_algorithm_digest(),
        "provider_execution_digest": provider_execution_digest(),
    }


def _open_message() -> dict[str, object]:
    return {
        "type": "open",
        "protocol_version": PROTOCOL_VERSION,
        **_identity_fields(),
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


def _filter_open_message() -> dict[str, object]:
    return {
        "type": "open",
        "protocol_version": PROTOCOL_VERSION,
        "semantic_spec_digest": (
            "8991bd426463415d86ea513ae5a58dd7f380bdbdf2b6d1fb1df7937513f93b0b"
        ),
        "physical_algorithm_digest": physical_algorithm_digest(),
        "provider_execution_digest": provider_execution_digest(),
        "provider_execution_id": UDS_EXECUTION_ID,
        "operator_kind": "SEM_FILTER",
        "semantic_spec_id": "semloom.recording.sem_filter.tristate",
        "semantic_spec_version": RECORDING_SPEC_VERSION,
        "physical_algorithm": RECORDING_ALGORITHM,
        "null_policy": "PROPAGATE_NULL",
        "error_policy": "FAIL_QUERY",
        "input_type": "text",
        "output_type": "tristate",
    }


class SemloomRecordingProtocolTests(unittest.TestCase):
    def test_digest_golden_vectors_cover_unicode_and_null(self) -> None:
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
        self.assertEqual(
            semantic_payload_digest(None),
            "47d39895a4f60f8caafaefd2317450ce7b663740cf43c9413de763b4ecf03912",
        )
        self.assertEqual(
            completion_evidence_digest(
                semantic_spec_sha256=semantic_spec_digest(),
                physical_algorithm_sha256=physical_algorithm_digest(),
                provider_execution_sha256=provider_execution_digest(),
                payload_sha256=semantic_payload_digest("héllo世界"),
                sequence=7,
                output="recorded:héllo世界",
            ),
            "2e24050b5a1bd18d3475d47d0e1f0cffaa50e17f088134c51a6a33f118bd32a0",
        )

    def test_fragmented_and_coalesced_frames_complete_a_unicode_task(self) -> None:
        client, server = socket.socketpair()
        thread = threading.Thread(target=run_recording_session, args=(server,))
        thread.start()
        self.addCleanup(client.close)

        payload_sha256 = semantic_payload_digest("héllo世界")
        opened = _open_message()
        task = {
            "type": "task",
            "protocol_version": PROTOCOL_VERSION,
            "sequence": "0",
            **_identity_fields(),
            "payload_digest": payload_sha256,
            "is_null": False,
            "input": "héllo世界",
        }
        wire = encode_frame(opened) + encode_frame(task)
        client.sendall(wire[:3])
        client.sendall(wire[3:])

        open_response = read_frame(client)
        completion = read_frame(client)
        self.assertEqual(open_response["type"], "opened")
        self.assertEqual(open_response["max_inflight_tasks"], 1)
        self.assertEqual(open_response["max_frame_bytes"], MAX_FRAME_BYTES)
        self.assertEqual(open_response["max_input_bytes"], MAX_INPUT_BYTES)
        self.assertEqual(open_response["semantic_spec_digest"], semantic_spec_digest())
        self.assertEqual(
            open_response["physical_algorithm_digest"], physical_algorithm_digest()
        )
        self.assertEqual(
            open_response["provider_execution_digest"], provider_execution_digest()
        )
        self.assertEqual(completion["type"], "completion")
        self.assertEqual(completion["output"], "recorded:héllo世界")
        self.assertEqual(completion["payload_digest"], payload_sha256)

        client.close()
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

    def test_filter_spec_returns_deterministic_tristate_completions(self) -> None:
        client, server = socket.socketpair()
        thread = threading.Thread(target=run_recording_session, args=(server,))
        thread.start()
        self.addCleanup(client.close)

        opened = _filter_open_message()
        client.sendall(encode_frame(opened))
        open_response = read_frame(client)
        self.assertEqual(open_response["type"], "opened")

        identity = {
            "semantic_spec_digest": opened["semantic_spec_digest"],
            "physical_algorithm_digest": opened["physical_algorithm_digest"],
            "provider_execution_digest": opened["provider_execution_digest"],
        }
        for sequence, decision in enumerate(("true", "false", "unknown")):
            client.sendall(
                encode_frame(
                    {
                        "type": "task",
                        "protocol_version": PROTOCOL_VERSION,
                        "sequence": str(sequence),
                        **identity,
                        "payload_digest": semantic_payload_digest(decision),
                        "is_null": False,
                        "input": decision,
                    }
                )
            )
            completion = read_frame(client)
            self.assertEqual(completion["type"], "completion")
            self.assertEqual(completion["sequence"], str(sequence))
            self.assertEqual(completion["output"], decision)

        client.close()
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

    def test_bad_payload_digest_fails_closed_without_echoing_input(self) -> None:
        client, server = socket.socketpair()
        thread = threading.Thread(target=run_recording_session, args=(server,))
        thread.start()
        self.addCleanup(client.close)

        client.sendall(encode_frame(_open_message()))
        self.assertEqual(read_frame(client)["type"], "opened")
        client.sendall(
            encode_frame(
                {
                    "type": "task",
                    "protocol_version": PROTOCOL_VERSION,
                    "sequence": "0",
                    **_identity_fields(),
                    "payload_digest": "0" * 64,
                    "is_null": False,
                    "input": "do-not-log-this",
                }
            )
        )

        error = read_frame(client)
        self.assertEqual(error, {"type": "error", "code": "payload_digest_mismatch"})
        self.assertNotIn("do-not-log-this", repr(error))
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

    def test_unknown_open_field_and_oversized_frame_fail_closed(self) -> None:
        client, server = socket.socketpair()
        thread = threading.Thread(target=run_recording_session, args=(server,))
        thread.start()
        self.addCleanup(client.close)

        message = _open_message()
        message["future_field"] = True
        client.sendall(encode_frame(message))
        self.assertEqual(
            read_frame(client),
            {"type": "error", "code": "invalid_open_fields"},
        )
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

        client2, server2 = socket.socketpair()
        thread2 = threading.Thread(target=run_recording_session, args=(server2,))
        thread2.start()
        self.addCleanup(client2.close)
        client2.sendall((MAX_FRAME_BYTES + 1).to_bytes(4, "big"))
        self.assertEqual(
            read_frame(client2),
            {"type": "error", "code": "invalid_frame_length"},
        )
        thread2.join(timeout=1)
        self.assertFalse(thread2.is_alive())

    def test_boolean_semantic_spec_version_fails_closed(self) -> None:
        client, server = socket.socketpair()
        thread = threading.Thread(target=run_recording_session, args=(server,))
        thread.start()
        self.addCleanup(client.close)

        message = _open_message()
        message["semantic_spec_version"] = True
        client.sendall(encode_frame(message))
        self.assertEqual(
            read_frame(client),
            {"type": "error", "code": "unsupported_semantic_spec_version"},
        )
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

    def test_disconnect_on_task_yields_no_completion(self) -> None:
        client, server = socket.socketpair()
        thread = threading.Thread(
            target=run_recording_session,
            args=(server,),
            kwargs={"disconnect_on_task": True},
        )
        thread.start()
        self.addCleanup(client.close)

        client.sendall(encode_frame(_open_message()))
        self.assertEqual(read_frame(client)["type"], "opened")
        client.sendall(
            encode_frame(
                {
                    "type": "task",
                    "protocol_version": PROTOCOL_VERSION,
                    "sequence": "0",
                    **_identity_fields(),
                    "payload_digest": semantic_payload_digest("alpha"),
                    "is_null": False,
                    "input": "alpha",
                }
            )
        )

        self.assertIsNone(read_frame(client))
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

    def test_null_and_oversized_tasks_are_rejected_before_execution(self) -> None:
        for input_value, is_null, expected_code in (
            (None, True, "null_task_not_allowed"),
            ("x" * (MAX_INPUT_BYTES + 1), False, "input_too_large"),
        ):
            with self.subTest(expected_code=expected_code):
                client, server = socket.socketpair()
                thread = threading.Thread(target=run_recording_session, args=(server,))
                thread.start()

                client.sendall(encode_frame(_open_message()))
                self.assertEqual(read_frame(client)["type"], "opened")
                client.sendall(
                    encode_frame(
                        {
                            "type": "task",
                            "protocol_version": PROTOCOL_VERSION,
                            "sequence": "0",
                            **_identity_fields(),
                            "payload_digest": semantic_payload_digest(input_value),
                            "is_null": is_null,
                            "input": input_value,
                        }
                    )
                )
                self.assertEqual(
                    read_frame(client),
                    {"type": "error", "code": expected_code},
                )
                client.close()
                thread.join(timeout=1)
                self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
