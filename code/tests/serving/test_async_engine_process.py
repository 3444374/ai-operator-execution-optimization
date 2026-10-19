"""Real bounded sockets and CPU child; model execution is explicitly fixed."""

import json
from pathlib import Path
import sys
import socket
import time
import unittest
from unittest.mock import patch

from src.serving.backends.async_engine_process import AsyncEngineProcess
from src.serving.backends.vllm_async_engine import AsyncEngineLimits
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec


def row(index=0, text="fast", query="q"):
    return dict(query_id=query, model_role="main", algorithm_stage="map", row_id="row-" + str(index),
        source_position=index, call_index=0, call_id=query + ":" + str(index),
        messages_json=json.dumps([dict(role="user", content=text)]))


class FixtureProcess(AsyncEngineProcess):
    @staticmethod
    def _command(spec, descriptors):
        return [sys.executable, str(Path(__file__).with_name("async_engine_fixture.py")), *map(str, descriptors)]


class AsyncProcessTests(unittest.TestCase):
    def setUp(self):
        self.patch = patch.object(EngineSpec, "validate_driver_environment", return_value={})
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.owners = []
        self.addCleanup(self.close_all)

    def close_all(self):
        for owner in self.owners:
            owner.close(timeout=2)

    def owner(self, **limits):
        capacity = AsyncEngineLimits(max_inflight=2, result_slots=2, item_result_bytes=4096,
                                    result_bytes=8192, **limits)
        owner = FixtureProcess(EngineSpec("/tmp/cpu-fixture", sys.executable, "/tmp/driver-python"),
            GenerationSpec(), limits=capacity, startup_timeout_s=10)
        self.owners.append(owner)
        return owner

    def submit(self, owner, index=0, text="fast", query="q"):
        result = owner.submit(row(index, text, query), deadline_monotonic=time.monotonic() + 5)
        self.assertEqual(result.status, "queued", result)
        return result.request_id

    def until(self, owner, predicate, timeout=3):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("fixture did not progress: " + str(owner.snapshot()))
            owner.wait(0.01)

    def test_hold_first_accept_second_and_return_reverse_order(self):
        owner = self.owner()
        first = self.submit(owner, 0, "hold")
        self.until(owner, lambda: owner.snapshot()["accepted_requests"] == 1)
        second = self.submit(owner, 1)
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        result = owner.poll()[0]
        self.assertEqual(result.request_id, second)
        self.assertEqual(result.result["call_id"], "q:1")
        self.assertEqual(result.result["prompt_tokens"], 11)
        self.assertEqual(owner.snapshot()["active_requests"], 1)
        self.assertTrue(owner.abort(first))
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        late = owner.poll()[0]
        self.assertEqual(late.request_id, first)
        self.assertTrue(late.engine_terminal_confirmed)
        self.assertEqual(owner.snapshot()["held_requests"], 0)

    def test_full_results_hold_capacity_until_consumption_then_recover(self):
        owner = self.owner()
        self.submit(owner, 0)
        self.submit(owner, 1)
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 2)
        self.assertEqual(owner.snapshot()["input_bytes"], 0)
        self.assertEqual(owner.snapshot()["reserved_result_bytes"], 8192)
        self.assertGreater(owner.snapshot()["buffered_result_bytes"], 0)
        self.assertLessEqual(owner.snapshot()["buffered_result_bytes"], 8192)
        denied = owner.submit(row(2), deadline_monotonic=time.monotonic() + 2)
        self.assertEqual(denied.status, "backpressure")
        owner.poll()
        self.submit(owner, 2)
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 2)
        owner.poll(2)
        self.assertEqual(owner.snapshot()["reserved_result_bytes"], 0)

    def test_close_and_drain_control_work_with_full_unconsumed_results(self):
        owner = self.owner()
        self.submit(owner, 0)
        self.submit(owner, 1)
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 2)
        self.assertEqual(owner.drain(1)["status"], "drained")
        report = owner.close(timeout=3)
        self.assertTrue(report["process_exit_confirmed"])
        self.assertTrue(report["io_thread_closed"])
        self.assertTrue(report["engine_api"]["engine_api_shutdown_confirmed"])
        self.assertIsNone(report["gpu_exit_confirmed"])
        self.assertEqual(sum(event.result["prompt_tokens"] for event in owner.poll(2)), 22)

    def test_aborted_unknown_retains_input_and_result_reservations(self):
        owner = self.owner()
        key = self.submit(owner, 0, "hold-unknown")
        self.until(owner, lambda: owner.snapshot()["accepted_requests"] == 1)
        self.assertTrue(owner.abort(key))
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        event = owner.poll()[0]
        self.assertFalse(event.engine_terminal_confirmed)
        self.assertEqual(event.result["prompt_tokens"], 11)
        self.assertEqual(owner.snapshot()["unknown_requests"], 1)
        self.assertGreater(owner.snapshot()["input_bytes"], 0)
        self.assertEqual(owner.drain(0.02)["status"], "unknown")
        rejected = owner.submit(row(1), deadline_monotonic=time.monotonic() + 2)
        self.assertEqual(rejected.status, "rejected")

    def test_child_death_retains_known_prefix_and_marks_only_missing_unknown(self):
        owner = self.owner()
        first = self.submit(owner, 0)
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        second = self.submit(owner, 1, "die")
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 2)
        events = owner.poll(2)
        self.assertEqual(events[0].request_id, first)
        self.assertTrue(events[0].engine_terminal_confirmed)
        self.assertEqual(events[0].result["prompt_tokens"], 11)
        self.assertEqual(events[1].request_id, second)
        self.assertFalse(events[1].engine_terminal_confirmed)
        self.assertIsNone(events[1].result)
        self.assertEqual(owner.snapshot()["held_requests"], 1)

    def test_query_identity_and_old_abort_do_not_leak_into_next_query(self):
        owner = self.owner()
        old = self.submit(owner, 0, query="first")
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        rejected = owner.submit(row(0, query="second"), deadline_monotonic=time.monotonic() + 2)
        self.assertEqual(rejected.status, "rejected")
        first = owner.poll()[0]
        current = self.submit(owner, 0, query="second")
        self.assertNotEqual(old, current)
        self.assertFalse(owner.abort(old))
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        second = owner.poll()[0]
        self.assertEqual(first.result["query_id"], "first")
        self.assertEqual(second.result["query_id"], "second")

    def test_large_output_retains_usage_in_small_failure_packet(self):
        owner = self.owner()
        self.submit(owner, 0, "large")
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        event = owner.poll()[0]
        self.assertEqual(event.result["status"], "failed")
        self.assertEqual(event.result["prompt_tokens"], 11)
        self.assertEqual(event.result["completion_tokens"], 1)
        self.assertEqual(event.result["error_type"], "ResultCapacityExceeded")
        self.assertTrue(event.engine_terminal_confirmed)

    def test_wrong_result_identity_becomes_unknown_and_keeps_accepted_reservation(self):
        owner = self.owner()
        key = self.submit(owner, 0, "wrong-call")
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        event = owner.poll()[0]
        self.assertEqual(event.request_id, key)
        self.assertEqual(event.identity["call_id"], "q:0")
        self.assertIsNone(event.result)
        self.assertFalse(event.engine_terminal_confirmed)
        self.assertGreater(owner.snapshot()["input_bytes"], 0)

    def test_oversize_input_and_expired_request_have_no_handoff(self):
        owner = self.owner()
        expired = owner.submit(row(), deadline_monotonic=time.monotonic() - 1)
        self.assertEqual(expired.status, "rejected")
        with self.assertRaisesRegex(ValueError, "byte capacity"):
            owner.submit(row(text="x" * owner.limits.item_input_bytes),
                         deadline_monotonic=time.monotonic() + 5)
        self.assertEqual(owner.snapshot()["held_requests"], 0)

    def test_raw_input_bytes_limit_returns_backpressure_without_spending_ticket(self):
        owner = self.owner(input_bytes=4096, item_input_bytes=4096)
        first = owner.submit(row(0, "hold" + "x" * 2200), deadline_monotonic=time.monotonic() + 3)
        self.assertEqual(first.status, "queued")
        denied = owner.submit(row(1, "hold" + "x" * 2200), deadline_monotonic=time.monotonic() + 3)
        self.assertEqual(denied.status, "backpressure")
        self.assertEqual(owner._sequence, 1)

    def test_cancel_before_child_sees_data_is_not_lost(self):
        owner = self.owner()
        # Both messages are queued under the same condition so the I/O loop
        # observes control first; the child's bounded early-abort set handles it.
        with owner._condition:
            key = self.submit(owner, 0, "hold")
            self.assertTrue(owner.abort(key))
        self.until(owner, lambda: owner.snapshot()["ready_results"] == 1)
        event = owner.poll()[0]
        self.assertTrue(event.engine_terminal_confirmed)
        self.assertEqual(owner.snapshot()["held_requests"], 0)

    def test_pending_native_abort_does_not_block_close_or_lose_known_usage(self):
        owner = self.owner()
        key = self.submit(owner, 0, "hold-stall-abort")
        self.until(owner, lambda: owner.snapshot()["accepted_requests"] == 1)
        self.assertTrue(owner.abort(key))
        self.assertEqual(owner.drain(0.03)["status"], "unknown")
        self.assertGreater(owner.snapshot()["input_bytes"], 0)
        report = owner.close(timeout=3)
        self.assertTrue(report["process_exit_confirmed"])
        self.assertTrue(report["engine_api"]["engine_api_shutdown_confirmed"])
        event = owner.poll()[0]
        self.assertTrue(event.engine_terminal_confirmed)
        self.assertEqual(event.result["prompt_tokens"], 11)

    def test_failed_child_launch_closes_all_created_socket_descriptors(self):
        made = []
        original = socket.socketpair

        def tracked_pair():
            pair = original()
            made.extend(pair)
            return pair

        with patch("src.serving.backends.async_engine_process.socket.socketpair", side_effect=tracked_pair), \
             patch("src.serving.backends.async_engine_process.subprocess.Popen", side_effect=OSError("fixture launch")):
            with self.assertRaisesRegex(OSError, "fixture launch"):
                self.owner()
        self.assertEqual(len(made), 6)
        self.assertTrue(all(sock.fileno() == -1 for sock in made))


if __name__ == "__main__":
    unittest.main()
