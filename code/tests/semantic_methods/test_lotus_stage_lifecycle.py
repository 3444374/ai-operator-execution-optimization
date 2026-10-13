"""LOTUS consumer faults over real staged Core, with table/Ray/SDK substitutes.

No vendor library, HTTP, model or GPU is used. The existing preparation broker,
transport and asynchronous backend retain ownership; only their external seams
are substitutes. Actual SDK error interpretation requires the separate fixture.
"""

import asyncio
from contextlib import contextmanager
import json
import sys
import threading
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import NativeTaskSession, build_native_execution
from src.execution_provider.adapters.ray_map_transport import RayMapConfig, RayMapTransport
from src.scheduling.core.session_contract import Usage
from src.scheduling.runtime.stage_broker import StageBrokerLimits
from src.semantic_methods.lotus.batch import LotusBatchExecutor
from src.semantic_methods.lotus.limits import LotusCallLimits
from src.semantic_methods.lotus.sdk import PreparedLotusCall
from tests.execution_provider.test_map_preparation import batches, wait_until
from tests.execution_provider.test_ray_map_transport import FakeRay


class StubOpenAIError(Exception):
    pass


class LotusStageLifecycleTests(unittest.TestCase):
    @contextmanager
    def scenario(self, *, fault=None, unknown=False, cancel=False, undelivered_result=False):
        base = "http://localhost/v1"
        config = FixedModelConfig(base + "/chat/completions", "fixture", 1000, "fixture")
        physical = RayMapConfig("fixture-cluster", 1, 3, 2**21, 2**22,
            payload_backend="arrow", response_mode="full",
            preparation=StageBrokerLimits(2**21, 2**22, 4, 1, 4))
        first_results, late_result, unknown_after_consume = (
            threading.Event(), threading.Event(), threading.Event())
        cancelled = threading.Event()
        state = dict(started=set(), parsed=[], responses=[], flows=[], releases=[],
                     closes=[], peaks=dict(held_tasks=0, active_requests=0, active_work=0),
                     transports=[], armed=False)
        primary = ValueError("fixture " + str(fault) + " failed")

        async def execute(table, index, template):
            sequence = template.key.sequence
            self.assertEqual((table["session_id"][index].as_py(),
                              table["sequence"][index].as_py()),
                             (template.key.session_id, sequence))
            payload = json.loads(table["payload"][index].as_py())
            self.assertEqual(payload["messages"][-1]["content"], f"row-{sequence}")
            self.assertEqual(template.task.payload, b"")
            state["started"].add(sequence)
            gate = (late_result if sequence == 2 else unknown_after_consume
                    if unknown and sequence == 1 else first_results)
            while not gate.is_set():
                await asyncio.sleep(.001)
            if unknown and sequence == 1:
                raise OSError("fixture actor reply lost")
            body = json.dumps(dict(row=sequence, text=f"answer-{sequence}"),
                              separators=(",", ":")).encode()
            response = encode_full_response(FullModelResponse(200, (), body))
            return template.key, response, 1, 2

        ray = FakeRay(execute)

        def transport_factory(*args):
            transport = RayMapTransport(*args, physical=physical, ray_api=ray)
            state["transports"].append(transport)
            return transport

        def prepare(_lm, messages, _kwargs, **_profile):
            payload = json.dumps(dict(model="fixture", messages=messages),
                                 separators=(",", ":")).encode()
            return PreparedLotusCall(payload, base, "fixture", 1)

        def bounded(execution, label):
            usage = execution.engine.capacity.usage()
            for field in ("held_tasks", "active_requests", "active_work", "input_bytes", "result_bytes"):
                self.assertLessEqual(getattr(usage, field), getattr(execution.engine.capacity.limits, field),
                                     label + ": " + field)
                if field in state["peaks"]:
                    state["peaks"][field] = max(state["peaks"][field], getattr(usage, field))
            snapshot = state["transports"][0].preparation.snapshot()
            self.assertLessEqual(snapshot["held_tasks"], 4, label)
            self.assertLessEqual(snapshot["object_bytes"], physical.object_bytes, label)
            self.assertLessEqual(snapshot["stages"]["ready_held_bytes"], physical.preparation.ready_bytes, label)

        class Flow(NativeTaskSession):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                state["flows"].append(self)

            def advance(self, max_deliveries=1):
                progress = super().advance(1 if undelivered_result else max_deliveries)
                bounded(self.execution, "advance")
                return progress

            def wait(self, progress, timeout_s=None):
                if not state["armed"] and state["started"] == {0, 1, 2}:
                    # Freeze the fourth ready input until both early results
                    # are known. A delivery limit may leave one result in Core.
                    self.session.set_dispatch_enabled(False)
                    state["armed"] = True
                    first_results.set()

                    def early_finished():
                        backend = self.execution.engine.backend
                        with backend._lock:
                            early = (0,) if unknown else (0, 1)
                            return all(slot.future.done() for key, slot in backend._slots.items()
                                       if key.sequence in early)

                    wait_until(early_finished)
                super().wait(progress, timeout_s=.001)

            def release(self, leases):
                state["releases"].append(tuple(leases))
                super().release(leases)
                bounded(self.execution, "result-release")

            def close(self, *, clean=False):
                report = super().close(clean=clean)
                state["closes"].append((clean, report))
                bounded(self.execution, "consumer-close")
                return report

        with patch("src.execution_provider.adapters.map_preparation.iter_payload_batches", batches):
            execution = build_native_execution(config, physical=None, max_tasks=4,
                max_active_requests=3, active_work=3, transport_factory=transport_factory,
                preparation_factory=lambda transport, limits, notify: transport.prepare_inputs(limits, notify))

            def observe(index, full):
                bounded(execution, "observation")
                state["responses"].append(index)
                phases = {key.sequence: record.phase for key, record in execution.engine.capacity.records.items()}
                second_phase = "INFLIGHT" if unknown else "READY" if undelivered_result else "LEASED"
                self.assertEqual(phases, {0: "LEASED", 1: second_phase,
                                          2: "INFLIGHT", 3: "QUEUED"})
                if fault == "consumer" and index == 0:
                    raise primary
                if cancel and index == 0:
                    cancelled.set()

            def parse(_lm, messages, _kwargs, call, full, **_replay):
                bounded(execution, "parser")
                index = int(messages[-1]["content"].removeprefix("row-"))
                body = json.loads(full.body)
                self.assertEqual(body, dict(row=index, text=f"answer-{index}"))
                self.assertEqual(json.loads(call.payload)["messages"], messages)
                state["parsed"].append(index)
                if fault == "parser" and index == 1:
                    raise primary
                if unknown and index == 0:
                    unknown_after_consume.set()
                return body

            selected = LotusBatchExecutor(execution, config, query_id="stage-lifecycle", operator_id="map",
                on_response=observe, cancelled=cancelled.is_set,
                call_limits=LotusCallLimits(1, max_request_bytes=8192, max_response_bytes=8192,
                                           max_batch_rows=4, batch_timeout_s=10))
            module = ModuleType("openai")
            module.OpenAIError = StubOpenAIError
            rows = [([dict(role="user", content=f"row-{index}")], index) for index in range(4)]

            def run():
                return selected(SimpleNamespace(model="openai/fixture"), rows, {}, False, "fixture")

            try:
                with patch.dict(sys.modules, openai=module), \
                     patch("src.semantic_methods.lotus.batch.NativeTaskSession", Flow), \
                     patch("src.semantic_methods.lotus.batch.prepare_call", prepare), \
                     patch("src.semantic_methods.lotus.batch.parse_lotus_response", parse):
                    yield selected, execution, state, primary, run
            finally:
                first_results.set()
                late_result.set()
                unknown_after_consume.set()
                for flow in state["flows"]:
                    flow.close()
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    execution.engine.advance()
                    bounded(execution, "teardown-reap")
                    remaining = execution.engine.capacity.records
                    if not remaining or (unknown and {key.sequence for key in remaining} == {1}):
                        break
                    execution.engine.wake.wait(execution.engine.wake.generation, .001)
                if unknown:
                    self.assertEqual({key.sequence for key in remaining}, {1})
                    self.assertEqual(execution.engine.capacity.usage().active_requests, 1)
                    self.assertEqual(execution.engine.capacity.usage().active_work, 1)
                    self.assertEqual(execution.engine.capacity.usage().held_tasks, 1)
                    preparation = state["transports"][0].preparation
                    self.assertEqual(preparation.snapshot()["held_tasks"], 1)
                    self.assertEqual(preparation.snapshot()["stages"]["model_inflight"], 1)
                    self.assertEqual(preparation.snapshot()["stages"]["ready_held_work"], 3)
                    self.assertEqual({key.sequence for key in next(iter(preparation.blocks.values())).members}, {1})
                    self.assertFalse(execution.close())
                    # The fake actor owns no external work; stop only this local
                    # test's I/O thread. This must still report unconfirmed close.
                    with self.assertRaisesRegex(RuntimeError, "finalization failed"):
                        execution.engine.backend.close()
                    self.assertFalse(execution.engine.backend._thread.is_alive())
                else:
                    self.assertEqual(execution.engine.capacity.usage(), Usage())
                    self.assertTrue(execution.close())
                self.assertEqual(state["peaks"], dict(held_tasks=4, active_requests=3, active_work=3))

    def assert_late_owner(self, execution, state):
        def reaped_local_inputs():
            execution.engine.advance()
            return {key.sequence for key in execution.engine.capacity.records} == {2}
        wait_until(reaped_local_inputs)
        usage = execution.engine.capacity.usage()
        self.assertEqual((usage.held_tasks, usage.active_requests, usage.active_work), (1, 1, 1))
        self.assertEqual({key.sequence for key in execution.engine.capacity.records}, {2})
        preparation = state["transports"][0].preparation
        snapshot = preparation.snapshot()
        self.assertEqual(snapshot["held_tasks"], 1)
        self.assertEqual(snapshot["stages"]["model_inflight"], 1)
        self.assertEqual(snapshot["stages"]["ready_held_work"], 3)
        self.assertEqual(len(preparation.blocks), 1)
        block = next(iter(preparation.blocks.values()))
        self.assertEqual({key.sequence for key in block.members}, {2})
        self.assertIsNotNone(block.reference)
        self.assertFalse(execution.close())
        self.assertEqual(state["started"], {0, 1, 2})
        self.assertEqual(state["closes"][0][1].uncertain_requests, 1)

    def test_consumer_failure_releases_unprocessed_delivery_but_not_shared_inflight_member(self):
        with self.scenario(fault="consumer") as (selected, execution, state, primary, run):
            with self.assertRaises(ValueError) as caught:
                run()
            self.assertIs(caught.exception, primary)
            self.assertEqual(state["responses"], [0])
            self.assertEqual(state["parsed"], [])
            self.assertEqual([len(leases) for leases in state["releases"] if leases], [2])
            self.assertEqual(selected.last_responses, (None,) * 4)
            self.assertEqual(selected.last_cleanup_errors, ())
            self.assert_late_owner(execution, state)

    def test_consumer_failure_discards_undelivered_core_result_without_refunding_inflight(self):
        with self.scenario(fault="consumer", undelivered_result=True) as (selected, execution, state, primary, run):
            with self.assertRaises(ValueError) as caught:
                run()
            self.assertIs(caught.exception, primary)
            self.assertEqual(state["responses"], [0])
            self.assertEqual(state["parsed"], [])
            self.assertEqual([len(leases) for leases in state["releases"] if leases], [1])
            self.assertEqual(selected.last_responses, (None,) * 4)
            self.assert_late_owner(execution, state)

    def test_parser_failure_keeps_previous_response_and_shared_inflight_member(self):
        with self.scenario(fault="parser") as (selected, execution, state, primary, run):
            with self.assertRaises(ValueError) as caught:
                run()
            self.assertIs(caught.exception, primary)
            self.assertEqual(state["responses"], [0, 1])
            self.assertEqual(state["parsed"], [0, 1])
            self.assertEqual([len(leases) for leases in state["releases"] if leases], [2])
            self.assertEqual([response is not None for response in selected.last_responses], [True, True, False, False])
            self.assertEqual(selected.last_cleanup_errors, ())
            self.assert_late_owner(execution, state)

    def test_cancel_after_delivery_keeps_late_member_and_prevents_ready_row_send(self):
        with self.scenario(cancel=True) as (selected, execution, state, _primary, run):
            with self.assertRaisesRegex(RuntimeError, "cancelled"):
                run()
            self.assertEqual(state["parsed"], [0, 1])
            self.assertEqual([response is not None for response in selected.last_responses], [True, True, False, False])
            self.assert_late_owner(execution, state)

    def test_actor_unknown_keeps_shared_block_after_success_delivery_and_consumer_close(self):
        with self.scenario(unknown=True) as (selected, execution, state, _primary, run):
            with self.assertRaisesRegex(RuntimeError, "MODEL_UNAVAILABLE"):
                run()
            self.assertEqual(state["parsed"], [0])
            self.assertEqual([response is not None for response in selected.last_responses], [True, False, False, False])
            self.assertFalse(execution.close())
            transport = state["transports"][0]
            self.assertEqual({key.sequence for key in transport.unknown}, {1})
            self.assertFalse(transport.preparation.release(next(iter(transport.unknown))))
            self.assertGreater(transport.used_bytes, 0)
            self.assertEqual(execution.engine.capacity.usage().active_requests, 2)
            self.assertEqual(execution.engine.capacity.usage().active_work, 2)
            block = next(iter(transport.preparation.blocks.values()))
            self.assertEqual({key.sequence for key in block.members}, {1, 2})
            self.assertEqual(state["closes"][0][1].uncertain_requests, 2)


if __name__ == "__main__":
    unittest.main()
