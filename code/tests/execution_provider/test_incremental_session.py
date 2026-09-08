"""Real framed v6 intake against a controlled async backend, without a model."""

import asyncio
import json
import socket
import threading
import unittest

from src.execution_provider.adapters.incremental_session import IncrementalMapSessionAdapter
from src.execution_provider.adapters.openai_compatible_fixed import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.wire import v5, v6
from src.execution_provider.wire.framing import encode_frame, read_frame, ProtocolError


class IncrementalSessionTests(unittest.TestCase):
    def test_two_inflight_reverse_completion_rejection_and_resubmission(self):
        peer, server = socket.socketpair()
        peer.settimeout(3)
        release_first = threading.Event()
        started_two = threading.Event()
        failures = []
        observed = []
        submitted = []
        members = []
        plan = SemanticMapPlan("Return the input.", "model", 8)

        async def execute(request, endpoint):
            submitted.append(request.key.sequence)
            members.append(request.member.size)
            if len(submitted) == 2:
                started_two.set()
            if request.key.sequence == 0:
                while not release_first.is_set():
                    await asyncio.sleep(0.001)
            return json.dumps(
                {
                    "model": "model",
                    "choices": [
                        {"message": {"content": str(request.key.sequence)}, "finish_reason": "stop"}
                    ],
                    "usage": {"prompt_tokens": 2, "completion_tokens": 1},
                }
            ).encode()

        def run():
            adapter = IncrementalMapSessionAdapter(
                FixedModelConfig("http://localhost/v1/chat/completions", "model", 2000),
                execute=execute,
                max_tasks=2,
                observer=observed.append,
            )
            try:
                adapter.serve_connection(
                    server, lambda conn: adapter.run_incremental(conn, read_frame(conn))
                )
                self.assertEqual(adapter.engine.capacity.usage().held_tasks, 0)
            except BaseException as exc:
                failures.append(exc)
            finally:
                adapter.close()
                server.close()

        worker = threading.Thread(target=run)
        worker.start()
        try:
            peer.sendall(encode_frame(v6.build_open_message(plan)))
            self.assertEqual(read_frame(peer)["max_inflight_tasks"], 2)
            context = v6.validate_open(v6.build_open_message(plan))
            tasks = [v6.build_task_message(plan, sequence=i, input_value=str(i)) for i in range(3)]
            for task in tasks[:2]:
                peer.sendall(encode_frame(task))
                self.assertEqual(read_frame(peer)["accepted_prefix_count"], 1)
            peer.sendall(encode_frame(tasks[2]))
            self.assertEqual(read_frame(peer)["accepted_prefix_count"], 0)
            peer.sendall(encode_frame({"type": "poll", "protocol_version": 6}))
            self.assertTrue(started_two.wait(2))
            self.assertFalse(release_first.is_set())
            completion = read_frame(peer)
            self.assertEqual(completion["sequence"], "1")
            v6.validate_completion(
                completion,
                expected_sequence=1,
                payload_digest=tasks[1]["semantic_payload_digest"],
                open_context=context,
            )
            peer.sendall(encode_frame(tasks[2]))
            self.assertEqual(read_frame(peer)["accepted_prefix_count"], 1)
            release_first.set()
            sequences = set()
            for _ in range(2):
                peer.sendall(encode_frame({"type": "poll", "protocol_version": 6}))
                sequences.add(read_frame(peer)["sequence"])
            self.assertEqual(sequences, {"0", "2"})
            self.assertEqual(submitted, [0, 1, 2])
            self.assertEqual(members, [2, 2, 1])
        finally:
            release_first.set()
            peer.close()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        if failures:
            raise failures[0]
        self.assertTrue(any(event["usage"]["active_requests"] == 2 for event in observed))

    def test_protocol_identity_cannot_be_relabelled(self):
        plan = SemanticMapPlan("Return input.", "model", 8)
        message = v5.build_open_message(plan)
        message["protocol_version"] = 6
        with self.assertRaises(ProtocolError):
            v6.validate_open(message)
        self.assertNotEqual(
            v5.provider_execution_digest("model"), v6.provider_execution_digest("model")
        )
