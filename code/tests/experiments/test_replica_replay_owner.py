"""Persistent replay lifecycle and recording modes, using finite localhost calls."""

import json
from contextlib import nullcontext
from pathlib import Path
import sys
import tempfile
import time
import threading
import unittest
from unittest import mock
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "postgres"))
from test_semloom_model_replicas import Endpoint, directory
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.replica_model_replay import ReplicaReplayOwner, replay_batch
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.scheduling.endpoint_routing.replica_adaptation import ReplicaAdaptationConfig


class ReplicaReplayOwnerTests(unittest.TestCase):
    def test_final_trace_or_cleanup_evidence_failure_poisons_owner_without_http(self):
        for phase in ("trace", "evidence"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as root:
                root = Path(root)
                site = SimpleNamespace(identity="a", url="http://127.0.0.1:9/v1/chat/completions")
                config, replicas = FixedModelConfig(site.url, "fixture", 2000), directory((site,))
                owner = ReplicaReplayOwner(config, replicas, arm="round_robin", requests=1, max_tasks=1)
                budget = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget("record-failure", 1),
                    deadline_utc=time.time() + 30)
                budget.reserve_unit("cell", 1)
                unit = budget.claim_shared_unit("cell")
                from src.experiments.replica_model_replay import JsonEvents
                original_close = JsonEvents.close
                def fail_close(recorder):
                    original_close(recorder)
                    raise OSError("fixture final flush")
                patch = (mock.patch.object(JsonEvents, "close", fail_close) if phase == "trace"
                         else mock.patch.object(Path, "write_text", side_effect=OSError("fixture evidence")))
                try:
                    with patch, self.assertRaises(OSError):
                        replay_batch(config, replicas, (), arm="round_robin", requests=1, unit=unit,
                            output=root / "failed", stop_new_post_utc=time.time() + 10, owner=owner)
                    self.assertTrue(owner.failed)
                    self.assertIsNone(owner.observer)
                    self.assertFalse(owner.execution.engine.jobs.jobs or owner.execution.engine.capacity.records)
                    self.assertEqual(unit.attempts, 0)
                    self.assertEqual(budget.snapshot()["allocated_requests"], 1)
                finally:
                    self.assertTrue(owner.close())
                    budget.close_shared_unit("cell")

    def test_prepare_and_open_failures_always_detach_and_reap_without_http(self):
        for phase in ("prepare", "open", "deadline"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as root:
                root = Path(root)
                site = SimpleNamespace(identity="a", url="http://127.0.0.1:9/v1/chat/completions")
                config, replicas = FixedModelConfig(site.url, "fixture", 2000), directory((site,))
                budget = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget("setup-failure", 1),
                    deadline_utc=time.time() + 30)
                budget.reserve_unit("cell", 1)
                unit = budget.claim_shared_unit("cell")
                owner = ReplicaReplayOwner(config, replicas, arm="round_robin", requests=1, max_tasks=1)
                calls = (dict(source_sequence=0, source_row_id=0, source_call_id="original", body=b'{}'),)
                patch = (mock.patch("src.experiments.replica_model_replay.prepare_native_task", side_effect=ValueError("prepare fixture"))
                    if phase == "prepare" else mock.patch.object(owner.execution.engine, "open", side_effect=ValueError("open fixture"))
                    if phase == "open" else nullcontext())
                try:
                    with patch, self.assertRaises((ValueError, TimeoutError)):
                        replay_batch(config, replicas, calls, arm="round_robin", requests=1, unit=unit,
                            output=root / "failed", stop_new_post_utc=time.time() + 10, owner=owner,
                            batch_deadline_utc=time.time() - 1 if phase == "deadline" else time.time() + 5,
                            cleanup_deadline_utc=time.time() + 10)
                    self.assertTrue(owner.failed)
                    self.assertIsNone(owner.observer)
                    self.assertFalse(owner.execution.engine.jobs.jobs or owner.execution.engine.capacity.records)
                    cleanup = json.loads((root / "failed/cleanup.json").read_text())
                    self.assertEqual(cleanup["durable_reserved_attempts"], 0)
                    self.assertEqual(unit.attempts, 0)
                    self.assertEqual(budget.snapshot()["allocated_requests"], 1)
                finally:
                    self.assertTrue(owner.close())
                    budget.close_shared_unit("cell")

    def test_absolute_batch_deadline_drains_original_call_within_cleanup_window(self):
        with Endpoint("a", hold=True) as site, tempfile.TemporaryDirectory() as root:
            root = Path(root)
            budget = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget("deadline", 1),
                deadline_utc=time.time() + 30)
            budget.reserve_unit("cell", 1)
            unit = budget.claim_shared_unit("cell")
            config, replicas = FixedModelConfig(site.url, "fixture", 2000), directory((site,))
            owner = ReplicaReplayOwner(config, replicas, arm="round_robin", requests=1, max_tasks=1)
            calls = (dict(source_sequence=0, source_row_id=0, source_call_id="original",
                body=b'{"model":"fixture","messages":[{"role":"user","content":"row-0"}]}'),)
            release = threading.Timer(.15, site.released.set)
            start = time.time()
            release.start()
            try:
                with self.assertRaisesRegex(TimeoutError, "batch deadline"):
                    replay_batch(config, replicas, calls, arm="round_robin", requests=1, unit=unit,
                        output=root / "failed", stop_new_post_utc=start + .05, owner=owner,
                        batch_deadline_utc=start + .05, cleanup_deadline_utc=start + .5)
                self.assertLess(time.time() - start, 1)
                self.assertEqual(len(site.requests), 1)
                self.assertEqual(unit.attempts, 1)
                self.assertFalse(owner.execution.engine.jobs.jobs or owner.execution.engine.capacity.records)
                self.assertIsNone(owner.observer)
                self.assertTrue(owner.failed)
            finally:
                release.join(1)
                self.assertTrue(owner.close())
                budget.close_shared_unit("cell")

    def test_queries_reuse_feedback_and_owner_and_flush_buffered_trace(self):
        with Endpoint("a") as a, tempfile.TemporaryDirectory() as root:
            root = Path(root)
            budget = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget("persistent", 8),
                deadline_utc=time.time() + 30)
            config, replicas = FixedModelConfig(a.url, "fixture", 2000), directory((a,), capacities=((4, 4),))
            owner = ReplicaReplayOwner(config, replicas, arm="adaptive_route", requests=4,
                max_tasks=4, adaptation=ReplicaAdaptationConfig(sample_interval_s=.001,
                    min_rate_support_s=.001), flush_each=False)
            execution, feedback = owner.execution, owner.feedback
            calls = tuple(dict(source_sequence=i, source_row_id=i, source_call_id=f"fixture-{i}",
                body=json.dumps(dict(model="fixture", messages=[dict(role="user", content=f"row-{i}")])).encode()) for i in range(4))
            try:
                for repeat in range(2):
                    label = f"query-{repeat}"
                    budget.reserve_unit(label, 4)
                    unit = budget.claim_shared_unit(label)
                    try:
                        result = replay_batch(config, replicas, calls, arm="adaptive_route", requests=4,
                            unit=unit, output=root / label, stop_new_post_utc=time.time() + 20,
                            owner=owner, arrival_s=(0, 0, .02, .02))
                    finally:
                        budget.close_shared_unit(label)
                    self.assertIs(owner.execution, execution)
                    self.assertIs(owner.feedback, feedback)
                    self.assertEqual(result["feedback_before"]["a"]["successful_requests"], 4 * repeat)
                    self.assertEqual(result["feedback_after"]["a"]["successful_requests"], 4 * (repeat + 1))
                    self.assertFalse(owner.execution.engine.capacity.records or owner.execution.engine.jobs.jobs)
                    events = [json.loads(line) for line in (root / label / "events.jsonl").read_text().splitlines()]
                    self.assertEqual(events, json.loads(json.dumps(result["events"])))
                    self.assertFalse(result["trace_flush_each"])
                    self.assertEqual(unit.attempts, 4)
                self.assertEqual(len(a.requests), 8)
            finally:
                self.assertTrue(owner.close())

    def test_failed_start_poisons_owner_and_keeps_reserved_budget(self):
        with Endpoint("a") as a, tempfile.TemporaryDirectory() as root:
            root = Path(root)
            budget = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget("failure", 1),
                deadline_utc=time.time() + 30)
            budget.reserve_unit("fail", 1)
            unit = budget.claim_shared_unit("fail")
            config, replicas = FixedModelConfig(a.url, "fixture", 2000), directory((a,))
            owner = ReplicaReplayOwner(config, replicas, arm="round_robin", requests=1, max_tasks=1)
            calls = (dict(source_sequence=0, source_row_id=0, source_call_id="original", body=b'{}'),)
            def fail(_):
                raise ValueError("fixture hook")
            with self.assertRaisesRegex(ValueError, "fixture hook"):
                replay_batch(config, replicas, calls, arm="round_robin", requests=1, unit=unit,
                    output=root / "fail", stop_new_post_utc=time.time() + 20, owner=owner, query_started=fail)
            self.assertTrue(owner.failed)
            with self.assertRaisesRegex(ValueError, "owner differs"):
                replay_batch(config, replicas, calls, arm="round_robin", requests=1, unit=unit,
                    output=root / "again", stop_new_post_utc=time.time() + 20, owner=owner)
            self.assertEqual(len(a.requests), 0)
            self.assertEqual(budget.snapshot()["allocated_requests"], 1)
            budget.close_shared_unit("fail")


if __name__ == "__main__":
    unittest.main()
