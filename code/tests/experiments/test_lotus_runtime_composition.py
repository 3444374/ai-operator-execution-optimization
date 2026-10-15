"""Use complete LOTUS methods across native and SemLoom runtimes in one driver."""

from collections import Counter
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
import uuid

from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.postgresql.supplier_adapter_query import run_supplier_query
from tests.experiments.lotus_query_fixtures import fixture_server


@unittest.skipUnless(
    os.environ.get("SEMLOOM_LOTUS_RUNTIME_COMPOSITION"),
    "actual LOTUS, Daft, Ray and Arrow composition is explicitly opt-in",
)
class LotusRuntimeCompositionTests(unittest.TestCase):
    def test_native_then_semloom_preserves_the_same_full_calls_and_original_parser(self):
        root = Path(tempfile.mkdtemp(dir=os.environ.get("SEMLOOM_FIXTURE_ARTIFACT_ROOT")))
        arms = (
            "lotus-adapted-native",
            "lotus-method-native-daft",
            "lotus-method-native-ray-data",
            "lotus-method-semloom",
        )
        rows = [
            dict(
                row_id=str(i), text="complete duplicate input" if i < 2 else "中文\ncomplete input"
            )
            for i in range(4)
        ]
        ledger = CellBudgetLedger.create(
            root / "budget.sqlite",
            AttemptBudget("lotus-composition", 16),
            deadline_utc=time.time() + 300,
        )
        summaries = []
        canonical = None
        with fixture_server() as (endpoint, requests):
            for index, arm in enumerate(arms):
                case = root / str(index)
                summary = run_supplier_query(
                    arm,
                    load_source=lambda: iter(rows),
                    plan=SemanticMapPlan("Return ok.", "fixture", 16),
                    model=FixedModelConfig(endpoint, "fixture", 10000),
                    ledger=ledger,
                    unit_id="composition-" + str(index),
                    root=case,
                    options=NativeGraphOptions(concurrency=4, num_threads=8),
                    physical=RayMapConfig("resolved", 2, 2, 2097152, 8388608)
                    if arm == "lotus-method-semloom"
                    else None,
                    ray_temp_root=Path("/root/autodl-tmp/lc." + uuid.uuid4().hex[:8])
                    if arm.endswith("ray-data") or arm == "lotus-method-semloom"
                    else None,
                    query_timeout_s=120,
                    allowed_outputs=("ok",),
                    reference_outputs={r["row_id"]: "ok" for r in rows},
                )
                self.assertEqual(
                    (summary["status"], summary["actual_posts"], summary["quality"]["accuracy"]),
                    ("passed", 4, 1),
                )
                protocols = [
                    json.loads(line) for line in (case / "protocols.jsonl").read_text().splitlines()
                ]
                complete = Counter(
                    json.dumps(p["request"], sort_keys=True, ensure_ascii=False) for p in protocols
                )
                if canonical is None:
                    canonical = complete
                self.assertEqual(complete, canonical)
                if arm == "lotus-method-semloom":
                    self.assertFalse(any(summary["core_cleanup"]["final_core_usage"].values()))
                summaries.append(
                    dict(
                        arm=arm,
                        actual_posts=summary["actual_posts"],
                        runtime=summary.get("runtime"),
                        identity=summary["identity"],
                    )
                )
            self.assertEqual(len(requests), 16)
        (root / "composition-verification.json").write_text(
            json.dumps(
                dict(passed=True, real_model_requests=0, actual_posts=16, summaries=summaries),
                indent=2,
            )
            + "\n"
        )


if __name__ == "__main__":
    unittest.main()
