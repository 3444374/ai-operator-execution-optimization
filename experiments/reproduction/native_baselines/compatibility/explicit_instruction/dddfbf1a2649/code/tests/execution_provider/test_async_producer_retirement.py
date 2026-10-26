"""Resource retirement is independent of unknown call results; real CPU IPC only."""

import time
import unittest
from copy import deepcopy
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
import asyncio
import json
import threading
from unittest.mock import patch

from src.execution_provider.adapters import lotus_records_core as core
from src.execution_provider.adapters import lotus_records_actor_owner as actors
from src.semantic_methods.lotus.records import CallLedger, FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.serving import test_async_engine_process as ipc_fixture
from tests.execution_provider import test_lotus_records_async_core as fixture
from tests.execution_provider import test_lotus_records_async_actor as actor_fixture
from tests.scheduling import test_async_backend as backend_fixture
from src.scheduling.core.session_contract import TaskKey, Terminal, Uncertain
from src.scheduling.runtime.async_backend import BoundedAsyncBackend


class IPCProducerRetirementTests(unittest.TestCase):
    def fixture(self):
        case = ipc_fixture.AsyncProcessTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def test_unknown_result_does_not_hold_resources_after_owned_producer_stops(self):
        case = self.fixture()
        owner = case.owner()
        request = case.submit(owner, text="hold-unknown")
        case.until(owner, lambda: owner.snapshot()["accepted_requests"] == 1)
        self.assertTrue(owner.abort(request))
        case.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        event = owner.poll()[0]
        self.assertEqual(event.result["status"], "unknown")
        self.assertFalse(event.engine_terminal_confirmed)
        self.assertEqual(owner.snapshot()["active_requests"], 1)
        report = owner.close(timeout=3)
        self.assertEqual(report["status"], "closed")
        self.assertTrue(report["producer_quiescence"]["confirmed"])
        self.assertEqual(report["active_requests"], 0)
        self.assertEqual(report["held_requests"], 0)
        self.assertEqual(report["retired_unknown_requests"], 1)
        self.assertEqual(event.result["prompt_tokens"], 11)
        self.assertEqual(event.result["status"], "unknown")
        self.assertFalse(owner.abort(request))
        repeated = owner.close(timeout=1)
        self.assertEqual(repeated["producer_quiescence"], report["producer_quiescence"])
        self.assertEqual(repeated["retired_unknown_requests"], 1)

    def test_unconsumed_known_and_unknown_receipts_survive_resource_quiescence(self):
        case = self.fixture()
        owner = case.owner()
        case.submit(owner, 0, "fast")
        case.submit(owner, 1, "unknown")
        case.until(owner, lambda: owner.snapshot()["ready_results"] == 2)
        report = owner.close(timeout=3)
        self.assertEqual(report["status"], "closed")
        self.assertEqual(report["active_requests"], 0)
        self.assertEqual(report["held_requests"], 2)  # Buffered evidence is still owned.
        events = owner.poll(2)
        self.assertEqual({event.result["status"] for event in events}, {"succeeded", "unknown"})
        self.assertEqual(sum(event.result["prompt_tokens"] for event in events), 22)
        self.assertEqual(owner.snapshot()["held_requests"], 0)
        self.assertEqual(owner.snapshot()["input_bytes"], 0)

    def test_incomplete_or_foreign_close_cannot_retire_unknown(self):
        for defect in ("owner_uid", "engine_uid", "received_submissions", "input_reader_stopped",
                       "engine_api_shutdown_confirmed", "missing-proof", "owned-process", "io-thread"):
            with self.subTest(defect=defect):
                case = self.fixture()
                owner = case.owner()
                case.submit(owner, text="unknown")
                case.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
                event = owner.poll()[0]
                original = owner._receive
                def receive(value, **kwargs):
                    if value.get("op") == "closed":
                        value = deepcopy(value)
                        if defect == "missing-proof": value.pop("producer_stopped")
                        elif defect == "engine_api_shutdown_confirmed": value["report"][defect] = False
                        elif defect in ("owner_uid", "engine_uid"): value["producer_stopped"][defect] = "other-owner"
                        elif defect == "received_submissions": value["producer_stopped"][defect] = 0
                        elif defect == "input_reader_stopped": value["producer_stopped"][defect] = False
                    return original(value, **kwargs)
                with patch.object(owner, "_receive", side_effect=receive):
                    if defect == "owned-process": owner._owned_process = None
                    thread_state = (patch.object(owner._thread, "is_alive", return_value=True)
                                    if defect == "io-thread" else nullcontext())
                    with thread_state:
                        report = owner.close(timeout=2)
                self.assertEqual(report["status"], "unknown")
                self.assertIsNone(report["producer_quiescence"])
                self.assertEqual(owner.snapshot()["active_requests"], 1)
                self.assertEqual(owner.snapshot()["held_requests"], 1)
                self.assertEqual(event.result["prompt_tokens"], 11)
                self.assertFalse(event.engine_terminal_confirmed)


class CoreProducerRetirementTests(unittest.TestCase):
    def test_prepare_not_yet_in_actor_retains_until_local_delivery_stops(self):
        for preparation_fails in (False, True):
            with self.subTest(preparation_fails=preparation_fails):
                rows = fixture.AsyncCoreCompositionTests().rows(["fast"])
                entered, release = threading.Event(), threading.Event()
                original = actors._PrepareActor.prepare
                def blocked_prepare(actor, values, deadline):
                    entered.set()
                    if not release.wait(3): raise TimeoutError("fixture prepare not released")
                    if preparation_fails: raise OSError("fixture preparation failed")
                    return original(actor, values, deadline)
                with fixture.ThreadedRuntime(rows), patch.object(actors._PrepareActor, "prepare", blocked_prepare):
                    owner = fixture.AsyncCoreCompositionTests().owner()
                    try:
                        stream = owner.evaluate(fixture.Source(rows), query_id="pending-prepare")
                        stream._fill()
                        deadline = time.monotonic() + 2
                        while not entered.is_set():
                            stream.session.advance(1)
                            if time.monotonic() >= deadline: self.fail("prepare did not start")
                            owner.core.wake.wait(owner.core.wake.generation, 0.005)
                        initial = owner.close(timeout=2)
                        self.assertEqual(initial["status"], "unknown")
                        self.assertFalse(initial["transport_closed"])
                        self.assertEqual(initial["core_usage"]["held_tasks"], 1)
                        release.set()
                        backend_fixture.AsyncBackendTests().wait_for(
                            lambda: all(slot.future.done() for slot in owner.transport.backend._slots.values()))
                        final = owner.close(timeout=2)
                        # A failed dependency never reached the actor. Its key
                        # is absent from the proof, so it cannot be inferred away.
                        self.assertEqual(final["status"], "unknown" if preparation_fails else "closed")
                        self.assertEqual(final["core_usage"]["held_tasks"], int(preparation_fails))
                    finally:
                        release.set()
                        owner.close(timeout=2)

    def test_cancel_keeps_unknown_usage_and_primary_error_but_releases_closed_producer(self):
        rows = fixture.AsyncCoreCompositionTests().rows(["hold-unknown"])
        cancelled = [False]
        with fixture.ThreadedRuntime(rows) as runtime:
            owner = core.create_records_core(FilterMethod("fixture"),
                EngineSpec("/tmp/fixture-model", "/tmp/model-python", "/tmp/driver-python"),
                GenerationSpec(), RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                    max_active_batches=1, startup_timeout_s=5, query_timeout_s=8),
                profile=dict(mode="async", max_inflight=1))
            try:
                stream = owner.evaluate(fixture.Source(rows), query_id="cancel-retire", cancelled=lambda: cancelled[0])
                stream._fill()
                deadline = time.monotonic() + 3
                actor = runtime.async_actors[0].instance
                while actor.engine.snapshot()["accepted_requests"] != 1:
                    stream.session.advance(1)
                    if time.monotonic() >= deadline:
                        self.fail("CPU model did not accept the original request")
                    owner.core.wake.wait(owner.core.wake.generation, 0.005)
                cancelled[0] = True
                with self.assertRaises(InterruptedError) as failure:
                    next(stream)
                closed = owner.close(timeout=3)
                self.assertEqual(closed["status"], "closed")
                self.assertTrue(closed["producer_retirement"]["confirmed"])
                self.assertEqual(closed["core_usage"]["held_tasks"], 0)
                self.assertEqual(closed["core_usage"]["active_requests"], 0)
                self.assertEqual(closed["retained_remote_inputs"], 0)
                self.assertIsNone(closed["gpu_exit_confirmed"])
                self.assertEqual(str(failure.exception), "LOTUS Core records cancelled")
                ledger = CallLedger(fixture.Source(rows).expected("cancel-retire"), 1)
                for value in owner.last_query_report["unconsumed_call_results"]:
                    ledger.record(value)
                result = ledger.report()
                self.assertEqual(result["counts"]["unknown"], 1)
                self.assertEqual(result["usage"]["known_prompt_tokens"], 11)
                self.assertEqual(actor.snapshot()["held_requests"], 0)
                repeated = owner.close(timeout=1)
                self.assertEqual(repeated["producer_retirement"], closed["producer_retirement"])
                self.assertEqual(repeated["core_usage"]["held_tasks"], 0)
            finally:
                owner.close(timeout=2)

    def test_consumed_unknown_receipt_keeps_identity_until_resource_retirement(self):
        rows = fixture.AsyncCoreCompositionTests().rows(["unknown"])
        with fixture.ThreadedRuntime(rows):
            owner = fixture.AsyncCoreCompositionTests().owner()
            try:
                stream = owner.evaluate(fixture.Source(rows), query_id="unknown-result")
                value = next(stream)
                self.assertEqual(value["status"], "unknown")
                with self.assertRaisesRegex(RuntimeError, "Core records execution failed"):
                    next(stream)
                self.assertEqual(owner.core.capacity.usage().active_requests, 1)
                report = owner.close(timeout=3)
                self.assertEqual(report["status"], "closed")
                self.assertEqual(report["core_usage"]["held_tasks"], 0)
                self.assertEqual(report["retained_remote_inputs"], 0)
                self.assertEqual(value["prompt_tokens"], 11)
            finally:
                owner.close(timeout=2)

    def test_foreign_proof_or_changed_call_keeps_core_borrowing(self):
        for defect in ("owner", "call", "key"):
            with self.subTest(defect=defect):
                rows = fixture.AsyncCoreCompositionTests().rows(["unknown"])
                with fixture.ThreadedRuntime(rows):
                    owner = fixture.AsyncCoreCompositionTests().owner()
                    try:
                        stream = owner.evaluate(fixture.Source(rows), query_id="wrong-proof")
                        self.assertEqual(next(stream)["status"], "unknown")
                        with self.assertRaises(RuntimeError): next(stream)
                        close = owner._actors.close
                        def wrong_close():
                            report = deepcopy(close())
                            proof = report["producer_quiescence"]
                            if defect == "owner": proof["ipc"]["owner_uid"] = "other-owner"
                            elif defect == "call": proof["requests"][0]["call_id"] = "other-call"
                            else: proof["requests"][0]["request_key"][0] += 1
                            return report
                        with patch.object(owner._actors, "close", side_effect=wrong_close):
                            report = owner.close(timeout=3)
                        self.assertEqual(report["status"], "unknown")
                        self.assertEqual(report["core_usage"]["held_tasks"], 1)
                        self.assertEqual(report["retained_remote_inputs"], 1)
                        self.assertIn("producer_retirement: ValueError", report["cleanup_errors"])
                    finally:
                        owner.close(timeout=2)


class LocalProducerRetirementTests(unittest.TestCase):
    def test_actor_reader_still_delivering_prevents_retirement_until_it_exits(self):
        case = actor_fixture.AsyncActorTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        actor = case.actor()
        ready, release = threading.Event(), threading.Event()
        original = actor._run_core
        def held_delivery(*args):
            value = original(*args)
            ready.set()
            if not release.wait(3): raise TimeoutError("fixture delivery was not released")
            return value
        with ThreadPoolExecutor(max_workers=1) as pool, patch.object(actor, "_run_core", side_effect=held_delivery):
            try:
                result = case.submit(pool, actor, text="unknown")
                self.assertTrue(ready.wait(2))
                report = actor.close(0.5)
                self.assertEqual(report["status"], "unknown")
                self.assertNotIn("producer_quiescence", report)
                self.assertEqual(actor.snapshot()["held_requests"], 1)
                self.assertIn("async_actor_readers_still_active", report["cleanup_errors"])
            finally:
                release.set()
            self.assertEqual(json.loads(result.result(2))["status"], "unknown")
        final = actor.close(2)
        self.assertEqual(final["status"], "closed")
        self.assertEqual(actor.snapshot()["held_requests"], 0)
        self.assertEqual(final["unconsumed_core_results"][0]["result"]["prompt_tokens"], 11)

    def test_known_close_race_wins_and_unselected_unknown_stays_owned(self):
        async def execute(task, endpoint):
            if task.key.sequence == 1: return b"ok"
            raise OSError("fixture unknown")
        backend = BoundedAsyncBackend(execute, max_tasks=3, notify=lambda: None, isolate_failures=True)
        submitted = [backend.try_submit(backend_fixture.task(i), "local") for i in range(3)]
        keys = tuple(TaskKey(0, i) for i in range(3))
        handles = tuple(zip(keys, (item.handle for item in submitted)))
        try:
            backend_fixture.AsyncBackendTests().wait_for(
                lambda: all(slot.completed_order is not None for slot in backend._slots.values()))
            self.assertTrue(backend.close())
            self.assertEqual(backend.retire_producer(keys[:2]), 2)
            events = {event.key: event for event in backend.poll(handles, 3)}
            self.assertEqual(events[keys[0]], Terminal(keys[0], submitted[0].handle, "cancelled"))
            self.assertEqual(events[keys[1]], Terminal(keys[1], submitted[1].handle, "completed", b"ok"))
            self.assertIsInstance(events[keys[2]], Uncertain)
            self.assertEqual(set(backend._slots), {keys[2]})
            self.assertEqual(backend.retire_producer(keys[:2]), 0)
            self.assertEqual(backend.poll(handles, 3), ())
        finally:
            backend.close()

    def test_live_local_callback_cannot_be_retired(self):
        entered = threading.Event()
        gates = []
        async def execute(task, endpoint):
            gate = asyncio.Event()
            gates.append(gate)
            entered.set()
            await gate.wait()
            return b"ok"
        backend = BoundedAsyncBackend(execute, max_tasks=1, notify=lambda: None)
        ticket = backend.try_submit(backend_fixture.task(0), "local")
        try:
            self.assertTrue(entered.wait(2))
            self.assertFalse(backend.close(0.01))
            with self.assertRaisesRegex(RuntimeError, "not stopped"):
                backend.retire_producer((TaskKey(0, 0),))
            self.assertEqual(len(backend._slots), 1)
        finally:
            backend._loop.call_soon_threadsafe(gates[0].set)
            backend._slots[TaskKey(0, 0)].future.result(2)
            self.assertTrue(backend.close())
        self.assertEqual(backend.retire_producer((TaskKey(0, 0),)), 1)
        self.assertEqual(backend.poll(((TaskKey(0, 0), ticket.handle),), 1)[0].result, b"ok")


if __name__ == "__main__":
    unittest.main()
