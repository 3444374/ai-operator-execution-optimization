from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.packing import PackItem, best_fit_decreasing, summarize_packing


def items(costs: list[int]) -> list[PackItem]:
    return [
        PackItem(index, f"row-{index}", cost)
        for index, cost in enumerate(costs)
    ]


class PackingTests(unittest.TestCase):
    def test_canonical_best_fit_decreasing_membership(self) -> None:
        packed = best_fit_decreasing(
            items([6, 5, 4, 3, 2]),
            capacity=10,
            max_rows=3,
        )
        self.assertEqual(packed, ((0, 2), (1, 3, 4)))

    def test_ties_are_deterministic(self) -> None:
        source = [
            PackItem(2, "b", 5),
            PackItem(0, "a", 5),
            PackItem(1, "a", 5),
        ]
        expected = ((0, 1), (2,))
        for _ in range(5):
            self.assertEqual(
                best_fit_decreasing(
                    source,
                    capacity=10,
                    max_rows=2,
                ),
                expected,
            )

    def test_row_limit_and_oversized_rows_are_enforced(self) -> None:
        packed = best_fit_decreasing(
            items([12, 4, 3, 2]),
            capacity=10,
            max_rows=2,
        )
        self.assertEqual(packed, ((0,), (1, 2), (3,)))
        flattened = [index for group in packed for index in group]
        self.assertEqual(sorted(flattened), [0, 1, 2, 3])

    def test_empty_and_invalid_inputs(self) -> None:
        self.assertEqual(
            best_fit_decreasing([], capacity=10, max_rows=2),
            (),
        )
        with self.assertRaisesRegex(ValueError, "duplicate row_index"):
            best_fit_decreasing(
                [PackItem(0, "a", 1), PackItem(0, "b", 1)],
                capacity=10,
                max_rows=2,
            )
        for capacity, max_rows in ((0, 1), (1, 0), (-1, 1)):
            with self.subTest(capacity=capacity, max_rows=max_rows):
                with self.assertRaises(ValueError):
                    best_fit_decreasing(
                        [],
                        capacity=capacity,
                        max_rows=max_rows,
                    )

    def test_summary_excludes_oversized_batches_from_utilization(self) -> None:
        summary = summarize_packing(
            [10, 8, 12],
            [2, 2, 1],
            capacity=10,
        )
        self.assertEqual(summary.utilization_mean, 0.9)
        self.assertEqual(summary.utilization_p95, 1.0)
        self.assertEqual(summary.oversized_rows, 1)
        self.assertEqual(summary.input_rows, 5)
        self.assertEqual(summary.batch_count, 3)
        self.assertEqual(summary.cost_units_max, 12)

        fixed_rows = summarize_packing(
            [9, 7],
            [2, 2],
            capacity=0,
        )
        self.assertEqual(fixed_rows.utilization_mean, 0.0)
        self.assertEqual(fixed_rows.oversized_rows, 0)
        self.assertEqual(fixed_rows.input_rows, 4)


if __name__ == "__main__":
    unittest.main()
