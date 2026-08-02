from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling import ServiceQuantumSlice, slice_service_quanta  # noqa: E402


class ServiceQuantumTests(unittest.TestCase):
    def test_slices_rows_without_splitting_oversized_row(self) -> None:
        self.assertEqual(
            slice_service_quanta([6, 4, 7, 20, 3], 10),
            (
                ServiceQuantumSlice(0, 2, 10, False),
                ServiceQuantumSlice(2, 3, 7, False),
                ServiceQuantumSlice(3, 4, 20, True),
                ServiceQuantumSlice(4, 5, 3, False),
            ),
        )

    def test_empty_costs_produce_no_quanta(self) -> None:
        self.assertEqual(slice_service_quanta([], 10), ())

    def test_rejects_invalid_target(self) -> None:
        for target in (0, -1, True, 1.5):
            with self.subTest(target=target):
                with self.assertRaisesRegex(ValueError, "target_tokens"):
                    slice_service_quanta([1], target)  # type: ignore[arg-type]

    def test_rejects_invalid_row_cost(self) -> None:
        invalid_costs = ([1, -1], [1, True], [1, 1.5])
        for costs in invalid_costs:
            with self.subTest(costs=costs):
                with self.assertRaisesRegex(ValueError, "row costs"):
                    slice_service_quanta(costs, 10)  # type: ignore[arg-type]

    def test_zero_cost_rows_remain_in_order(self) -> None:
        self.assertEqual(
            slice_service_quanta([0, 0, 5, 5, 0], 5),
            (
                ServiceQuantumSlice(0, 3, 5, False),
                ServiceQuantumSlice(3, 5, 5, False),
            ),
        )


if __name__ == "__main__":
    unittest.main()
