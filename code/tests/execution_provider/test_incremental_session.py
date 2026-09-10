"""Framed intake, resource budgets and injected execution on the production service."""

import asyncio
from dataclasses import replace
import threading
import unittest
from unittest.mock import patch
from src.execution_provider.adapters.incremental_execution import build_fixed_model_execution
from src.execution_provider.adapters.model_config import FixedModelConfig, MAX_MODEL_RESPONSE_BYTES
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.wire import v5, v6
from src.execution_provider.wire.framing import ProtocolError
from tests.execution_provider.test_multisession_gateway import service, Client, completion, wait_for


class IncrementalSessionTests(unittest.TestCase):
    def test_two_inflight_reverse_completion_rejection_and_resubmission(self):
        release, started_two = threading.Event(), threading.Event()
        submitted, members = [], []

        async def execute(task, endpoint):
            submitted.append(task.key.sequence)
            members.append(task.member.size)
            if len(submitted) == 2:
                started_two.set()
            if task.key.sequence == 0:
                while not release.is_set():
                    await asyncio.sleep(0.001)
            return completion(str(task.key.sequence))

        with service(execute, max_jobs=1, max_tasks=2) as (path, gateway, events):
            client = Client(path)
            try:
                self.assertEqual([client.offer(str(i)) for i in range(3)], [1, 1, 0])
                self.assertEqual([e['sequence'] for e in events if e['event'] == 'map_task'], [0, 1])
                self.assertEqual(submitted, [])
                client.poll()
                self.assertTrue(started_two.wait(2))
                result = client.result()
                task = v6.build_task_message(client.plan, sequence=1, input_value="1")
                v6.validate_completion(
                    result,
                    expected_sequence=1,
                    payload_digest=task["semantic_payload_digest"],
                    open_context=v6.validate_open(v6.build_open_message(client.plan)),
                )
                self.assertEqual(client.offer("2"), 1)
                release.set()
                observed = set()
                for _ in range(2):
                    client.poll()
                    observed.add(client.result()["sequence"])
                self.assertEqual(observed, {"0", "2"})
                self.assertEqual(submitted, [0, 1, 2])
                self.assertEqual(members, [2, 2, 1])
                self.assertEqual([e['sequence'] for e in events if e['event'] == 'map_task'], [0, 1, 2])
                wait_for(lambda: len([e for e in events if e['event'] == 'map_completion']) == 3)
                for sequence in range(3):
                    task_index = next(i for i, e in enumerate(events) if e['event'] == 'map_task' and e['sequence'] == sequence)
                    result_index = next(i for i, e in enumerate(events) if e['event'] == 'map_completion' and e['sequence'] == sequence)
                    self.assertLess(task_index, result_index)
            finally:
                release.set()
                client.close()
            self.assertTrue(any(e.get("usage", {}).get("active_requests") == 2 for e in events))

    def test_invalid_backend_output_reports_error_and_releases_resources(self):
        for payload, code in (
            (b"not json", "MODEL_RESPONSE_INVALID"),
            (b"\xff", "MODEL_RESPONSE_INVALID"),
            (b"{}", "MODEL_RESPONSE_INVALID"),
            (b'{"bridge_error":"MODEL_REQUEST_REJECTED"}', "MODEL_REQUEST_REJECTED"),
        ):
            with self.subTest(payload=payload):

                async def execute(task, endpoint):
                    return payload

                with service(execute, max_jobs=1) as (path, gateway, events):
                    client = Client(path, window=4)
                    try:
                        client.offer("x")
                        client.poll()
                        self.assertEqual(client.result(), v6.build_error_message(code, sequence=0))
                    finally:
                        client.close()
                    wait_for(lambda: any(e["event"] == "job_drained" for e in events))
                    self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)

    def test_result_budget_limits_intake_independently_of_request_capacity(self):
        async def execute(task, endpoint):
            raise AssertionError("intake alone must not dispatch")

        with service(
            execute,
            max_jobs=1,
            max_tasks=65,
            max_active_requests=1,
            result_bytes=2 * MAX_MODEL_RESPONSE_BYTES,
        ) as (path, gateway, events):
            client = Client(path, window=65)
            try:
                self.assertEqual([client.offer(str(i)) for i in range(3)], [1, 1, 0])
                self.assertEqual(gateway.engine.capacity.usage().active_requests, 0)
            finally:
                client.close()
            wait_for(lambda: any(e["event"] == "job_drained" for e in events))
            self.assertEqual(gateway.engine.capacity.usage().held_tasks, 0)

    def test_invalid_budget_is_rejected_before_starting_transport(self):
        with patch(
            "src.execution_provider.adapters.incremental_execution.BoundedAsyncBackend"
        ) as backend:
            with self.assertRaises(ValueError):
                build_fixed_model_execution(
                    FixedModelConfig("http://localhost/v1/chat/completions", "model", 2000),
                    result_bytes=0,
                )
            backend.assert_not_called()

    def test_injected_execution_uses_alternative_work_and_selection(self):
        import json
        from src.planning.work import StageWork, WorkDescriptor

        observed = []

        def describe(request):
            length = len(request.canonical_messages[-1]["content"])
            return WorkDescriptor(
                (StageWork("model", length, "tokens"),),
                "model",
                "synthetic-length",
                locality_key="shared-prefix",
            )

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

        async def execute(task, endpoint):
            observed.append(
                (
                    task.key.sequence,
                    task.task.estimated_work,
                    task.task.info.work.locality_key,
                    json.loads(task.task.payload)["messages"][-1]["content"],
                )
            )
            return completion("ok")

        with service(
            execute, max_jobs=1, max_tasks=2, max_active_requests=1, execution_factory=factory
        ) as (path, gateway, events):
            client = Client(path)
            try:
                client.offer("a")
                client.offer("abc")
                for sequence in (1, 0):
                    client.poll()
                    self.assertEqual(client.result()["sequence"], str(sequence))
            finally:
                client.close()
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
