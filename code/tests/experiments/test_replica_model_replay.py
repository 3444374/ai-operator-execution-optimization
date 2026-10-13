"""Real localhost calls verify the model replay harness, without model/GPU access."""

import hashlib
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "postgres"))
from test_semloom_model_replicas import Endpoint, directory
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.replica_model_replay import endpoint_rates, replay_batch
from src.execution_provider.adapters.model_config import FixedModelConfig


class ReplicaModelReplayTests(unittest.TestCase):
    def test_all_four_arms_preserve_bytes_complete_responses_and_cumulative_budget(self):
        with Endpoint("a") as a, Endpoint("b") as b, tempfile.TemporaryDirectory() as root:
            root = Path(root)
            budget = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget("fixture", 16),
                deadline_utc=time.time() + 60)
            calls = tuple(dict(source_sequence=i, source_call_id=f"source-{i}", source_row_id=f"db-row-{i}",
                body=json.dumps(dict(model="fixture", messages=[dict(role="user", content=f"row-{i}")],
                                     temperature=0), separators=(",", ":")).encode()) for i in range(4))
            rates = None
            config = FixedModelConfig(a.url, "fixture", 2000)
            for arm in ("round_robin", "fixed_rate", "adaptive_route", "adaptive_supply"):
                budget.reserve_unit(arm, 4)
                unit = budget.claim_shared_unit(arm)
                result = replay_batch(config, directory((a, b)), calls, arm=arm, requests=2, unit=unit,
                    output=root / arm, stop_new_post_utc=time.time() + 20, fixed_rates=rates)
                self.assertEqual(result["count"], 4)
                self.assertEqual(sorted(r["source_sequence"] for r in result["outcomes"]), list(range(4)))
                self.assertTrue(json.loads((root / arm / "cleanup.json").read_text())["clean"])
                snapshots = [e for e in result["events"] if e["event"] == "core_supply_snapshot"]
                self.assertEqual(snapshots[0]["queued"], 4)
                self.assertTrue(snapshots[0]["hard_dispatchable"])
                self.assertEqual(snapshots[-1]["usage"]["held_tasks"], 0)
                self.assertEqual(sum(e["event"] == "feedback_finished" for e in result["events"]), 4)
                if arm == "round_robin":
                    rates = endpoint_rates(result["events"], ("a", "b"))
                self.assertEqual(unit.attempts, 4)
                budget.close_shared_unit(arm)
            self.assertEqual(budget.snapshot()["allocated_requests"], 16)
            self.assertEqual(len(a.requests) + len(b.requests), 16)
            allowed = {hashlib.sha256(c["body"]).hexdigest() for c in calls}
            self.assertTrue(all(hashlib.sha256(r[1]).hexdigest() in allowed for r in a.requests + b.requests))


if __name__ == "__main__":
    unittest.main()
