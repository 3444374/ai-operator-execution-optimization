from __future__ import annotations

import importlib.util
import csv
import sys
import tempfile
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

SCRIPT = CODE_ROOT / "scripts" / "baselines" / "opening_database_e2e_matrix.py"
spec = importlib.util.spec_from_file_location("opening_database_e2e_matrix", SCRIPT)
matrix = importlib.util.module_from_spec(spec)
sys.modules["opening_database_e2e_matrix"] = matrix
spec.loader.exec_module(matrix)


class OpeningMatrixPureTests(unittest.TestCase):
    def test_arm_order_is_deterministic_and_complete(self) -> None:
        workload = matrix.Workload(
            label="w",
            name="workload",
            rows=2,
            max_tokens=64,
            manifest=Path("unused"),
            quality="completion_validity",
        )
        first = matrix._arm_order(20260807, workload, "formal", 1)
        second = matrix._arm_order(20260807, workload, "formal", 1)
        self.assertEqual(first, second)
        self.assertEqual(set(first), set(matrix.ARMS))

    def test_pair_digest_is_order_independent_and_content_sensitive(self) -> None:
        self.assertEqual(
            matrix._pairs_digest([(2, "b"), (1, "a")]),
            matrix._pairs_digest([(1, "a"), (2, "b")]),
        )
        self.assertNotEqual(
            matrix._pairs_digest([(1, "a")]),
            matrix._pairs_digest([(1, "changed")]),
        )

    def test_percentile(self) -> None:
        self.assertEqual(matrix._percentile([], 0.5), 0.0)
        self.assertEqual(matrix._percentile([1.0, 3.0], 0.5), 2.0)

    def test_request_csv_preserves_endpoint_zero(self) -> None:
        result = matrix.BaselineRequestResult(
            doc_id=1,
            endpoint_index=0,
            status="completed",
            error=None,
            submitted_at_s=1.0,
            started_at_s=1.1,
            completed_at_s=1.2,
            input_tokens=3,
            output_tokens=1,
            output_text="x",
            finish_reason="stop",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.csv"
            matrix._write_requests(path, (result,))
            with path.open(encoding="utf-8", newline="") as handle:
                row = next(csv.DictReader(handle))
        self.assertEqual(row["endpoint_index"], "0")


if __name__ == "__main__":
    unittest.main()
