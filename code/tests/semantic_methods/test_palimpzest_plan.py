"""Fixed-plan contract tests; doubles here do not establish upstream-library qualification."""

import hashlib
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.semantic_methods.palimpzest import plan, source


class Operator:
    def __init__(self):
        self.instruction = "detect negative reviews"
        self.depends_on = ["text"]

    def get_op_params(self):
        return dict(desc=self.instruction, depends_on=self.depends_on)

    def get_id_params(self):
        return {"cached_id_can_stay_unchanged": "same"}


class PhysicalPlan:
    plan_id = "abcd"

    def __init__(self):
        self.operator, self.subplans = Operator(), []

    def __iter__(self):
        yield self.operator


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.source_check = patch.object(plan, "validate_source")
        self.source_check.start()
        self.addCleanup(self.source_check.stop)
        self.physical = PhysicalPlan()
        self.optimized, self.executed = [], []

        def optimize(dataset):
            self.optimized.append(dataset)
            return [self.physical]

        def execute_plan(*, plan):
            self.executed.append(plan)
            return ["original-record"], "original-stats"

        self.qp = SimpleNamespace(dataset="original-dataset", optimizer=SimpleNamespace(optimize=optimize),
                                  execution_strategy=SimpleNamespace(execute_plan=execute_plan))

    def test_select_once_and_execute_original_plan_without_optimizer(self):
        frozen = plan.freeze_best_plan(self.qp)
        self.assertEqual(self.optimized, ["original-dataset"])
        self.qp.optimizer.optimize = lambda _: self.fail("execution reselected a plan")
        self.assertEqual(plan.execute_frozen_plan(self.qp, frozen),
                         (["original-record"], "original-stats"))
        self.assertIs(self.executed[0], self.physical)
        self.assertIn("caller workload manifest", frozen.describe()["source_content_identity"])

    def test_changed_prompt_rejected_even_when_upstream_plan_id_is_cached(self):
        frozen = plan.freeze_best_plan(self.qp)
        self.physical.operator.instruction = "a different task"
        with self.assertRaisesRegex(ValueError, "parameters changed"):
            plan.execute_frozen_plan(self.qp, frozen)
        self.assertEqual(self.executed, [])

    def test_changed_dependency_is_rejected(self):
        frozen = plan.freeze_best_plan(self.qp)
        self.physical.operator.depends_on.append("another-field")
        with self.assertRaises(ValueError):
            plan.execute_frozen_plan(self.qp, frozen)

    def test_validator_pipeline_cannot_be_silently_bypassed(self):
        self.qp.validator = object()
        with self.assertRaisesRegex(ValueError, "cannot skip validator sampling"):
            plan.freeze_best_plan(self.qp)
        self.assertEqual(self.optimized, [])

    def test_udf_fingerprint_tracks_semantic_constants_without_source_line(self):
        original = lambda row: row["movieId"] == "taken_3"
        same = lambda row: row["movieId"] == "taken_3"
        different = lambda row: row["movieId"] == "taken_2"
        self.assertEqual(plan._function(original), plan._function(same))
        self.assertNotEqual(plan._function(original), plan._function(different))

    def test_mutable_udf_closure_is_rejected(self):
        values = ["taken_3"]
        with self.assertRaisesRegex(TypeError, "immutable"):
            plan._function(lambda row: row["movieId"] in values)

    def test_source_check_rejects_modified_file(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "seam.py").write_text("original")
            with patch.object(source, "import_module", return_value=SimpleNamespace(__file__=root / "__init__.py")), \
                    patch.object(source, "SOURCE_HASHES", {"seam.py": hashlib.sha256(b"original").hexdigest()}):
                self.assertEqual(source.validate_source(), root.resolve())
                (root / "seam.py").write_text("modified")
                with self.assertRaisesRegex(ValueError, "source differs"):
                    source.validate_source()


if __name__ == "__main__":
    unittest.main()
