"""Real framed v6 intake against a controlled async backend, without a model."""

import asyncio
from dataclasses import replace
import json
import socket
import threading
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.incremental_session import IncrementalMapSessionAdapter
from src.execution_provider.adapters.openai_compatible_fixed import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.completion import CompletionRequest
from src.execution_provider.adapters.model_config import MAX_MODEL_RESPONSE_BYTES
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

    def test_invalid_backend_output_reports_error_and_releases_resources(self):
        for payload, code in (
            (b"not json", "MODEL_RESPONSE_INVALID"),
            (b"\xff", "MODEL_RESPONSE_INVALID"),
            (b"{}", "MODEL_RESPONSE_INVALID"),
            (b'{"bridge_error":"MODEL_REQUEST_REJECTED"}', "MODEL_REQUEST_REJECTED"),
        ):
            with self.subTest(payload=payload):
                peer, server = socket.socketpair()
                peer.settimeout(3)
                failures = []

                async def execute(request, endpoint):
                    return payload

                adapters = []

                def run():
                    adapter = IncrementalMapSessionAdapter(
                        FixedModelConfig("http://localhost/v1/chat/completions", "model", 2000),
                        execute=execute,
                    )
                    adapters.append(adapter)
                    try:
                        adapter.serve_connection(
                            server, lambda conn: adapter.run_incremental(conn, read_frame(conn))
                        )
                    except BaseException as exc:
                        failures.append(exc)
                    finally:
                        server.close()

                worker = threading.Thread(target=run)
                worker.start()
                try:
                    plan = SemanticMapPlan("Return input.", "model", 8)
                    peer.sendall(encode_frame(v6.build_open_message(plan)))
                    self.assertEqual(read_frame(peer)["type"], "opened")
                    peer.sendall(
                        encode_frame(v6.build_task_message(plan, sequence=0, input_value="x"))
                    )
                    self.assertEqual(read_frame(peer)["accepted_prefix_count"], 1)
                    peer.sendall(encode_frame({"type": "poll", "protocol_version": 6}))
                    self.assertEqual(read_frame(peer), v6.build_error_message(code, sequence=0))
                finally:
                    peer.close()
                    worker.join(3)
                    if adapters:
                        adapters[0].close()
                self.assertFalse(worker.is_alive())
                if failures:
                    raise failures[0]
                usage = adapters[0].engine.capacity.usage()
                self.assertEqual(usage.held_tasks, 0)
                self.assertEqual(usage.active_requests, 0)

    def test_result_budget_limits_intake_independently_of_request_capacity(self):
        adapter = IncrementalMapSessionAdapter(
            FixedModelConfig("http://localhost/v1/chat/completions", "model", 2000),
            max_tasks=65,
            max_active_requests=1,
            result_bytes=2 * MAX_MODEL_RESPONSE_BYTES,
        )
        peer, server = socket.socketpair()

        def offer(connection):
            request = CompletionRequest("a" * 64, "model", (), {}, protocol_version=6)
            tasks = tuple(adapter.prepare_task(request, i) for i in range(3))
            self.assertEqual(adapter._session.offer(tasks).accepted_prefix_count, 2)
            usage = adapter.engine.capacity.usage()
            self.assertEqual(usage.held_tasks, 2)
            self.assertEqual(usage.active_requests, 0)

        try:
            adapter.serve_connection(server, offer)
            self.assertEqual(adapter.engine.capacity.usage().held_tasks, 0)
        finally:
            peer.close()
            server.close()
            adapter.close()

    def test_invalid_budget_is_rejected_before_starting_transport(self):
        with patch(
            "src.execution_provider.adapters.incremental_execution.BoundedAsyncBackend"
        ) as backend:
            with self.assertRaises(ValueError):
                IncrementalMapSessionAdapter(
                    FixedModelConfig("http://localhost/v1/chat/completions", "model", 2000),
                    result_bytes=0,
                )
            backend.assert_not_called()

    def test_injected_execution_uses_alternative_work_and_selection(self):
        from src.execution_provider.adapters.incremental_execution import (
            build_fixed_model_execution,
        )
        from src.planning.work import StageWork, WorkDescriptor

        peer, server = socket.socketpair()
        peer.settimeout(3)
        observed, failures = [], []

        def describe(request):
            length = len(request.canonical_messages[-1]["content"])
            return WorkDescriptor(
                (StageWork("model", length, "tokens"),),
                "model",
                "synthetic-length",
                locality_key="shared-prefix",
            )

        async def execute(request, endpoint):
            observed.append(
                (
                    request.key.sequence,
                    request.task.estimated_work,
                    request.task.info.work.locality_key,
                    json.loads(request.task.payload)["messages"][-1]["content"],
                )
            )
            return json.dumps(
                {
                    "model": "model",
                    "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1},
                }
            ).encode()

        def factory(config, **kwargs):
            execution = build_fixed_model_execution(
                config, describe_work=describe, active_work=4, work_unit="tokens", **kwargs
            )
            execution.engine.policies = replace(
                execution.engine.policies,
                organize=None,
                choose_task=lambda candidates: candidates[-1].key,
            )
            return execution

        def run():
            adapter = IncrementalMapSessionAdapter(
                FixedModelConfig("http://localhost/v1/chat/completions", "model", 2000),
                max_tasks=2,
                max_active_requests=1,
                execute=execute,
                execution_factory=factory,
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
            plan = SemanticMapPlan("Return input.", "model", 8)
            peer.sendall(encode_frame(v6.build_open_message(plan)))
            read_frame(peer)
            for sequence, value in enumerate(("a", "abc")):
                peer.sendall(
                    encode_frame(v6.build_task_message(plan, sequence=sequence, input_value=value))
                )
                self.assertEqual(read_frame(peer)["accepted_prefix_count"], 1)
            for sequence in (1, 0):
                peer.sendall(encode_frame({"type": "poll", "protocol_version": 6}))
                self.assertEqual(read_frame(peer)["sequence"], str(sequence))
        finally:
            peer.close()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        if failures:
            raise failures[0]
        self.assertEqual(observed, [(1, 3, "shared-prefix", "abc"), (0, 1, "shared-prefix", "a")])

    def test_protocol_identity_cannot_be_relabelled(self):
        plan = SemanticMapPlan("Return input.", "model", 8)
        message = v5.build_open_message(plan)
        message["protocol_version"] = 6
        with self.assertRaises(ProtocolError):
            v6.validate_open(message)
        self.assertNotEqual(
            v5.provider_execution_digest("model"), v6.provider_execution_digest("model")
        )
