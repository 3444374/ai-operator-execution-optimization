"""Cancellation delivery at close, using production IPC and a CPU-only child."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.serving.backends.async_engine_process import AsyncEngineProcess, _frame
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from src.execution_provider.adapters.lotus_records_async_actor import AsyncCoreModelActor
from tests.execution_provider.test_lotus_records_async_actor import prepared


class WitnessProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        fixture = Path(__file__).with_name("async_cancel_order_fixture.py")
        return [sys.executable, str(fixture), *map(str, descriptors)]


class WitnessActor(AsyncCoreModelActor):
    def _process_type(self):
        return WitnessProcess


class CancelCloseOrderTests(unittest.TestCase):
    def test_close_freeze_rejects_new_cancel_instead_of_claiming_late_acceptance(self):
        with tempfile.TemporaryDirectory(prefix="async-cancel-order-") as temporary, \
                patch.object(EngineSpec, "validate_driver_environment", return_value={}):
            spec = EngineSpec(temporary, sys.executable, "/tmp/fixture-driver-python")
            limits = AsyncEngineLimits(max_inflight=1, result_slots=1, command_slots=1)
            process = WitnessProcess(spec, GenerationSpec(), limits=limits, startup_timeout_s=5)
            try:
                submitted = process.submit(prepared(text="hold-unknown"),
                    deadline_monotonic=time.monotonic() + 5)
                with process._condition:
                    # The same state close atomically publishes before its IO
                    # reader can send close; no later control is admissible.
                    process._closing = True
                    self.assertFalse(process.abort(submitted.request_id))
                    self.assertEqual(len(process._controls), 0)
            finally:
                process.close(timeout=2)

    def test_queued_abort_reaches_child_before_immediate_shutdown(self):
        with tempfile.TemporaryDirectory(prefix="async-cancel-order-") as temporary, \
                patch.object(EngineSpec, "validate_driver_environment", return_value={}):
            spec = EngineSpec(temporary, sys.executable, "/tmp/fixture-driver-python")
            limits = AsyncEngineLimits(max_inflight=1, result_slots=1, command_slots=1)
            process = WitnessProcess(spec, GenerationSpec(), limits=limits, startup_timeout_s=5)
            try:
                submitted = process.submit(prepared(text="hold-unknown"),
                    deadline_monotonic=time.monotonic() + 5)
                self.assertEqual(submitted.status, "queued")
                events_file = Path(temporary) / "events.jsonl"
                deadline = time.monotonic() + 3
                while not events_file.exists() or not events_file.read_text().strip():
                    if time.monotonic() >= deadline:
                        self.fail("CPU request did not reach fixture native admission")
                    time.sleep(0.005)
                # Hold the real parent IO condition until both cancel and
                # close are owned. close's Condition.wait then releases it.
                with process._condition:
                    self.assertTrue(process.abort(submitted.request_id))
                    self.assertEqual(len(process._controls), 1)
                    report = process.close(timeout=2)
                events = [json.loads(line) for line in events_file.read_text().splitlines()]
                self.assertTrue(report["process_exit_confirmed"])
                receipts = process.poll(1)
                self.assertEqual(receipts[0].result["status"], "unknown")
                self.assertEqual(receipts[0].result["prompt_tokens"], 11)
                self.assertEqual(process.snapshot()["held_requests"], 1)
                self.assertIn(dict(event="fixture_native_abort_requested",
                                   request_id=submitted.request_id), events,
                    "accepted cancel intent disappeared before shutdown: " + str(events))
            finally:
                process.close(timeout=2)

    def test_close_hands_off_active_abort_when_cancel_rpc_enters_after_closing(self):
        with tempfile.TemporaryDirectory(prefix="async-cancel-order-") as temporary, \
                patch.object(EngineSpec, "validate_driver_environment", return_value={}), \
                ThreadPoolExecutor(max_workers=3) as pool:
            spec = EngineSpec(temporary, sys.executable, "/tmp/fixture-driver-python")
            limits = AsyncEngineLimits(max_inflight=1, result_slots=1, command_slots=1)
            actor = WitnessActor(asdict(spec), asdict(GenerationSpec()), 1, asdict(limits))
            delayed_rpc, cancel_scheduled = threading.Event(), threading.Event()

            def cancel_after_close_enters():
                cancel_scheduled.set()
                if not delayed_rpc.wait(4):
                    raise TimeoutError("test did not release the delayed cancel RPC")
                return actor.abort_core((0, 0))

            try:
                actor.ready(time.monotonic() + 5)
                result = pool.submit(actor.run_core, [prepared(text="hold-unknown")],
                                     time.monotonic() + 5, 4096, (0, 0))
                events_file = Path(temporary) / "events.jsonl"
                deadline = time.monotonic() + 3
                while not events_file.exists() or not events_file.read_text().strip():
                    if time.monotonic() >= deadline:
                        self.fail("CPU request did not reach fixture native admission")
                    time.sleep(0.005)
                cancel = pool.submit(cancel_after_close_enters)
                self.assertTrue(cancel_scheduled.wait(1))
                report = actor.close(2)
                delayed_rpc.set()
                self.assertFalse(cancel.result(2)["requested"])
                self.assertEqual(json.loads(result.result(2))["status"], "unknown")
                self.assertEqual(actor.snapshot()["held_requests"], 1)
                self.assertEqual(report["unconsumed_core_results"][0]["result"]["prompt_tokens"], 11)
                events = [json.loads(line) for line in events_file.read_text().splitlines()]
                self.assertTrue(any(row["event"] == "fixture_native_abort_requested" for row in events),
                    "late cancel RPC was rejected and close never handed off an explicit abort: " + str(events))
                self.assertEqual(report["abort_handoff"]["adopted_request_keys"], [[0, 0]])
                self.assertEqual(report["abort_handoff"]["queued_request_keys"], [[0, 0]])
                self.assertFalse(report["abort_handoff"]["engine_terminal_confirmed"])
            finally:
                delayed_rpc.set()
                actor.close(2)

    def test_stalled_native_abort_keeps_peer_unconfirmed_and_shutdown_available(self):
        with tempfile.TemporaryDirectory(prefix="async-cancel-order-") as temporary, \
                patch.object(EngineSpec, "validate_driver_environment", return_value={}):
            spec = EngineSpec(temporary, sys.executable, "/tmp/fixture-driver-python")
            limits = AsyncEngineLimits(max_inflight=2, result_slots=2, command_slots=2)
            process = WitnessProcess(spec, GenerationSpec(), limits=limits, startup_timeout_s=5)
            events_file = Path(temporary) / "events.jsonl"

            def events():
                return [] if not events_file.exists() else [json.loads(line)
                    for line in events_file.read_text().splitlines()]

            def wait_for(predicate):
                deadline = time.monotonic() + 3
                while not predicate():
                    if time.monotonic() >= deadline:
                        self.fail("CPU native witness did not progress: " + str(events()))
                    time.sleep(0.005)

            try:
                first = process.submit(prepared(0, "hold-stall-abort"),
                    deadline_monotonic=time.monotonic() + 5)
                second = process.submit(prepared(1, "hold-unknown"),
                    deadline_monotonic=time.monotonic() + 5)
                wait_for(lambda: sum(item["event"] == "fixture_native_admitted" for item in events()) == 2)
                self.assertTrue(process.abort(first.request_id))
                wait_for(lambda: any(item["event"] == "fixture_native_abort_requested" for item in events()))
                with process._condition:
                    self.assertTrue(process.abort(second.request_id))
                    report = process.close(timeout=2)
                observed = events()
                self.assertFalse(any(item["event"] == "fixture_native_abort_requested"
                    and item["request_id"] == second.request_id for item in observed))
                self.assertTrue(any(item["event"] == "fixture_shutdown_entered" for item in observed))
                self.assertTrue(report["process_exit_confirmed"])
                receipts = {item.request_id: item for item in process.poll(2)}
                self.assertEqual(receipts[second.request_id].result["status"], "unknown")
                self.assertFalse(receipts[second.request_id].engine_terminal_confirmed)
                self.assertEqual(sum(item.result["prompt_tokens"] for item in receipts.values()), 22)
                self.assertEqual(process.snapshot()["held_requests"], 1)
            finally:
                process.close(timeout=2)

    def test_close_retries_existing_cancel_intents_after_control_queue_backpressure(self):
        with tempfile.TemporaryDirectory(prefix="async-cancel-order-") as temporary, \
                patch.object(EngineSpec, "validate_driver_environment", return_value={}), \
                ThreadPoolExecutor(max_workers=3) as pool:
            spec = EngineSpec(temporary, sys.executable, "/tmp/fixture-driver-python")
            limits = AsyncEngineLimits(max_inflight=2, result_slots=2, command_slots=2, control_slots=1)
            actor = WitnessActor(asdict(spec), asdict(GenerationSpec()), 1, asdict(limits))
            try:
                actor.ready(time.monotonic() + 5)
                results = [pool.submit(actor.run_core, [prepared(index, "hold-unknown")],
                    time.monotonic() + 5, 4096, (0, index)) for index in range(2)]
                events_file = Path(temporary) / "events.jsonl"
                deadline = time.monotonic() + 3
                while not events_file.exists() or len(events_file.read_text().splitlines()) < 2:
                    if time.monotonic() >= deadline:
                        self.fail("both CPU requests did not reach native admission")
                    time.sleep(0.005)
                with actor._condition, actor.engine._condition:
                    actor.engine._controls.append(_frame(dict(op="drain"), limits.control_frame_bytes))
                    self.assertFalse(actor.abort_core((0, 0))["requested"])
                    self.assertFalse(actor.abort_core((0, 1))["requested"])
                report = actor.close(2)
                self.assertEqual(report["abort_handoff"]["unqueued_request_keys"], [])
                self.assertEqual(report["abort_handoff"]["adopted_request_keys"], [])
                self.assertEqual(sum(json.loads(result.result(2))["prompt_tokens"] for result in results), 22)
                self.assertEqual(actor.snapshot()["held_requests"], 2)
                events = [json.loads(line) for line in events_file.read_text().splitlines()]
                self.assertEqual(sum(row["event"] == "fixture_native_abort_requested" for row in events), 2)
            finally:
                actor.close(2)

    def test_abort_handoff_timeout_is_bounded_and_keeps_cleanup_responsibility(self):
        with tempfile.TemporaryDirectory(prefix="async-cancel-order-") as temporary, \
                patch.object(EngineSpec, "validate_driver_environment", return_value={}), \
                ThreadPoolExecutor(max_workers=2) as pool:
            spec = EngineSpec(temporary, sys.executable, "/tmp/fixture-driver-python")
            limits = AsyncEngineLimits(max_inflight=1, result_slots=1, command_slots=1)
            actor = WitnessActor(asdict(spec), asdict(GenerationSpec()), 1, asdict(limits))
            try:
                actor.ready(time.monotonic() + 5)
                result = pool.submit(actor.run_core, [prepared(text="hold-unknown")],
                                     time.monotonic() + 5, 4096, (0, 0))
                events_file = Path(temporary) / "events.jsonl"
                deadline = time.monotonic() + 3
                while not events_file.exists() or not events_file.read_text().strip():
                    if time.monotonic() >= deadline:
                        self.fail("CPU request did not reach fixture native admission")
                    time.sleep(0.005)
                engine = actor.engine
                with patch.object(engine, "abort", return_value=False):
                    started = time.monotonic()
                    report = actor.close(0.05)
                    elapsed = time.monotonic() - started
                self.assertLess(elapsed, 0.5)
                self.assertIs(actor.engine, engine)
                self.assertEqual(report["status"], "unknown")
                self.assertEqual(report["abort_handoff"]["unqueued_request_keys"], [[0, 0]])
                self.assertFalse(report["engine_api_shutdown_confirmed"])
                self.assertEqual(actor.snapshot()["held_requests"], 1)
                self.assertEqual(json.loads(result.result(2))["status"], "unknown")
            finally:
                actor.close(2)

    def test_abort_handoff_exception_still_closes_same_child_and_preserves_receipt(self):
        with tempfile.TemporaryDirectory(prefix="async-cancel-order-") as temporary, \
                patch.object(EngineSpec, "validate_driver_environment", return_value={}), \
                ThreadPoolExecutor(max_workers=2) as pool:
            spec = EngineSpec(temporary, sys.executable, "/tmp/fixture-driver-python")
            limits = AsyncEngineLimits(max_inflight=1, result_slots=1, command_slots=1)
            actor = WitnessActor(asdict(spec), asdict(GenerationSpec()), 1, asdict(limits))
            try:
                actor.ready(time.monotonic() + 5)
                result = pool.submit(actor.run_core, [prepared(text="hold-unknown")],
                                     time.monotonic() + 5, 4096, (0, 0))
                events_file = Path(temporary) / "events.jsonl"
                deadline = time.monotonic() + 3
                while not events_file.exists() or not events_file.read_text().strip():
                    if time.monotonic() >= deadline:
                        self.fail("CPU request did not reach fixture native admission")
                    time.sleep(0.005)
                original_abort, failed = actor.engine.abort, []

                def fail_once(key):
                    if not failed:
                        failed.append(True)
                        raise OSError("fixture abort handoff failed")
                    return original_abort(key)

                with patch.object(actor.engine, "abort", side_effect=fail_once):
                    report = actor.close(2)
                self.assertEqual(report["status"], "unknown")
                self.assertIn("abort_handoff: OSError", report["cleanup_errors"])
                self.assertTrue(report["ipc_close"]["process_exit_confirmed"])
                self.assertEqual(report["unconsumed_core_results"][0]["result"]["prompt_tokens"], 11)
                self.assertEqual(json.loads(result.result(2))["status"], "unknown")
            finally:
                actor.close(2)


if __name__ == "__main__":
    unittest.main()
