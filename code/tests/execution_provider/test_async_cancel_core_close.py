"""Real Core cancellation/close with real CPU IPC and explicit Ray/Daft fixtures."""

import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.execution_provider.adapters.lotus_records_async_actor import (
    AsyncCoreModelActor, AsyncRecordsActorOwner,
)
from src.execution_provider.adapters.lotus_records_core import SemLoomRecordsCore
from src.semantic_methods.lotus.records import FilterMethod, RecordsResources
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec
from tests.execution_provider.test_lotus_records_async_core import ThreadedRuntime
from tests.execution_provider.test_lotus_records_core import Source
from tests.serving.test_async_cancel_close_order import WitnessProcess


class CancelCoreCloseTests(unittest.TestCase):
    def test_query_cancel_followed_by_close_retains_unknown_and_records_close_owned_abort(self):
        rows = [dict(row_id="row-0", source_position=0, reviewId="r", id="taken_3",
                     reviewText="hold-unknown")]
        cancel, rpc_entered, release_rpc = (threading.Event() for _ in range(3))
        original_abort = AsyncCoreModelActor.abort_core
        original_close = AsyncCoreModelActor.close
        cancel_replies = []

        def delayed_abort(actor, key):
            rpc_entered.set()
            if not release_rpc.wait(4):
                raise TimeoutError("test did not release the delayed Ray cancel")
            reply = original_abort(actor, key)
            cancel_replies.append(reply)
            return reply

        def observed_close(actor, timeout):
            try:
                return original_close(actor, timeout)
            finally:
                release_rpc.set()

        with tempfile.TemporaryDirectory(prefix="async-cancel-core-") as temporary, \
                ThreadedRuntime(rows) as runtime, \
                patch.object(AsyncCoreModelActor, "_process_type", return_value=WitnessProcess), \
                patch.object(AsyncCoreModelActor, "abort_core", delayed_abort), \
                patch.object(AsyncCoreModelActor, "close", observed_close):
            method, generation = FilterMethod("fixture"), GenerationSpec()
            spec = EngineSpec(temporary, "/tmp/model-python", "/tmp/driver-python")
            resources = RecordsResources(prepare_batch_rows=1, model_batch_rows=1,
                max_active_batches=1, query_timeout_s=8, startup_timeout_s=5)
            actors = AsyncRecordsActorOwner(method, spec, generation, resources)
            owner = SemLoomRecordsCore(method, spec, generation, resources, actor_owner=actors)
            try:
                flow = owner.evaluate(Source(rows), query_id="cancel-close", cancelled=cancel.is_set)
                flow._fill()
                events_file = Path(temporary) / "events.jsonl"
                deadline = time.monotonic() + 3
                while not events_file.exists() or not events_file.read_text().strip():
                    flow.session.advance(1)
                    if time.monotonic() >= deadline:
                        self.fail("production Core did not reach fixture native admission")
                    owner.core.wake.wait(owner.core.wake.generation, 0.005)
                cancel.set()
                with self.assertRaises(InterruptedError):
                    next(flow)
                self.assertTrue(rpc_entered.wait(1))
                closed = owner.close(timeout=3)
                self.assertEqual(closed["status"], "unknown")
                self.assertEqual(closed["core_usage"]["held_tasks"], 1)
                self.assertEqual(closed["retained_remote_inputs"], 1)
                receipt = closed["last_query_report"]["unconsumed_call_results"][0]
                self.assertEqual(receipt["status"], "unknown")
                self.assertEqual(receipt["prompt_tokens"], 11)
                events = [json.loads(line) for line in events_file.read_text().splitlines()]
                order = [row["event"] for row in events]
                self.assertIn("fixture_native_abort_requested", order)
                self.assertLess(order.index("fixture_native_abort_requested"),
                                order.index("fixture_shutdown_entered"))
                handoff = closed["model_owner"]["engine_close_report"]["abort_handoff"]
                self.assertEqual(handoff["adopted_request_keys"], [[flow.session.session_id, 0]])
                self.assertEqual(handoff["scope"], "parent_control_queue_only")
                self.assertFalse(handoff["engine_terminal_confirmed"])
                deadline = time.monotonic() + 2
                while not cancel_replies and time.monotonic() < deadline:
                    time.sleep(0.005)
                self.assertTrue(cancel_replies)
                self.assertFalse(cancel_replies[0]["requested"])
                self.assertIsNone(closed["gpu_exit_confirmed"])
                self.assertEqual(len(runtime.async_actors), 1)
            finally:
                release_rpc.set()
                owner.close(timeout=2)


if __name__ == "__main__":
    unittest.main()
