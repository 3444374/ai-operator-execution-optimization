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
    completion_evidence_digest,
    encode_frame,
    plan_digest,
    read_frame,
    run_recording_session,
    semantic_payload_digest,
)


class SemloomRecordingProtocolTests(unittest.TestCase):
    def test_digest_golden_vectors_cover_unicode_and_null(self) -> None:
        self.assertEqual(
            plan_digest(mapped_column=2),
            "83a8d707d851fb4fe2a1cef163b008d2f69a69bab4c4d1532d496094e4619de4",
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
                plan_sha256=plan_digest(mapped_column=2),
                payload_sha256=semantic_payload_digest("héllo世界"),
                sequence=7,
                output="recorded:héllo世界",
            ),
            "edf53b887924e6c1a5b3bf46f303a94bde5bf290f0d9692d7f4ab3527c4f2f1c",
        )

    def test_fragmented_and_coalesced_frames_complete_a_unicode_task(self) -> None:
        client, server = socket.socketpair()
        thread = threading.Thread(target=run_recording_session, args=(server,))
        thread.start()
        self.addCleanup(client.close)

        plan_sha256 = plan_digest(mapped_column=1)
        payload_sha256 = semantic_payload_digest("héllo世界")
        opened = {
            "type": "open",
            "protocol_version": PROTOCOL_VERSION,
            "plan_digest": plan_sha256,
            "mapped_column": 1,
            "null_policy": "PROPAGATE_NULL",
            "input_type": "text",
            "output_type": "text",
        }
        task = {
            "type": "task",
            "protocol_version": PROTOCOL_VERSION,
            "sequence": "0",
            "plan_digest": plan_sha256,
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
        self.assertEqual(completion["type"], "completion")
        self.assertEqual(completion["output"], "recorded:héllo世界")
        self.assertEqual(completion["payload_digest"], payload_sha256)

        client.close()
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

    def test_bad_payload_digest_fails_closed_without_echoing_input(self) -> None:
        client, server = socket.socketpair()
        thread = threading.Thread(target=run_recording_session, args=(server,))
        thread.start()
        self.addCleanup(client.close)

        plan_sha256 = plan_digest(mapped_column=1)
        client.sendall(
            encode_frame(
                {
                    "type": "open",
                    "protocol_version": PROTOCOL_VERSION,
                    "plan_digest": plan_sha256,
                    "mapped_column": 1,
                    "null_policy": "PROPAGATE_NULL",
                    "input_type": "text",
                    "output_type": "text",
                }
            )
        )
        self.assertEqual(read_frame(client)["type"], "opened")
        client.sendall(
            encode_frame(
                {
                    "type": "task",
                    "protocol_version": PROTOCOL_VERSION,
                    "sequence": "0",
                    "plan_digest": plan_sha256,
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

        message = {
            "type": "open",
            "protocol_version": PROTOCOL_VERSION,
            "plan_digest": plan_digest(mapped_column=1),
            "mapped_column": 1,
            "null_policy": "PROPAGATE_NULL",
            "input_type": "text",
            "output_type": "text",
            "future_field": True,
        }
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

    def test_disconnect_on_task_yields_no_completion(self) -> None:
        client, server = socket.socketpair()
        thread = threading.Thread(
            target=run_recording_session,
            args=(server,),
            kwargs={"disconnect_on_task": True},
        )
        thread.start()
        self.addCleanup(client.close)

        plan_sha256 = plan_digest(mapped_column=1)
        client.sendall(
            encode_frame(
                {
                    "type": "open",
                    "protocol_version": PROTOCOL_VERSION,
                    "plan_digest": plan_sha256,
                    "mapped_column": 1,
                    "null_policy": "PROPAGATE_NULL",
                    "input_type": "text",
                    "output_type": "text",
                }
            )
        )
        self.assertEqual(read_frame(client)["type"], "opened")
        client.sendall(
            encode_frame(
                {
                    "type": "task",
                    "protocol_version": PROTOCOL_VERSION,
                    "sequence": "0",
                    "plan_digest": plan_sha256,
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

                plan_sha256 = plan_digest(mapped_column=1)
                client.sendall(
                    encode_frame(
                        {
                            "type": "open",
                            "protocol_version": PROTOCOL_VERSION,
                            "plan_digest": plan_sha256,
                            "mapped_column": 1,
                            "null_policy": "PROPAGATE_NULL",
                            "input_type": "text",
                            "output_type": "text",
                        }
                    )
                )
                self.assertEqual(read_frame(client)["type"], "opened")
                client.sendall(
                    encode_frame(
                        {
                            "type": "task",
                            "protocol_version": PROTOCOL_VERSION,
                            "sequence": "0",
                            "plan_digest": plan_sha256,
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
