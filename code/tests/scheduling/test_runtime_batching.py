from __future__ import annotations

import math
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

from src.scheduling.organization.batching import PendingBatchBuilder, RowArrival


def row(
    row_id: str,
    *,
    arrival_s: float = 1.0,
    prompt_tokens: int = 1,
    estimated_output_tokens: int = 0,
) -> RowArrival:
    return RowArrival(
        row_id=row_id,
        arrival_s=arrival_s,
        prompt_tokens=prompt_tokens,
        estimated_output_tokens=estimated_output_tokens,
        prefix_key="prefix",
        payload_ref=object(),
    )


class PendingBatchBuilderTests(unittest.TestCase):
    def test_none_removes_the_row_cap_without_disabling_the_token_budget(self) -> None:
        builder = PendingBatchBuilder(max_rows=None, token_budget=300)
        for index in range(300):
            self.assertEqual(builder.add(row(str(index))), index == 299)
        self.assertEqual(builder.close().row_count, 300)
        builder.set_token_budget(5)
        self.assertTrue(builder.add(row('oversize', prompt_tokens=6)))
        self.assertEqual(builder.close().row_count, 1)

    def test_uncapped_builder_rejects_missing_budget_and_zero_cost_requests(self) -> None:
        with self.assertRaisesRegex(ValueError, 'positive token budget'):
            PendingBatchBuilder(max_rows=None, token_budget=0)
        builder = PendingBatchBuilder(max_rows=None, token_budget=1)
        with self.assertRaisesRegex(ValueError, 'positive token budget'):
            builder.set_token_budget(0)
        with self.assertRaisesRegex(ValueError, 'positive request costs'):
            builder.add(row('zero', prompt_tokens=0))

    def test_close_preserves_row_order_and_aggregates_metadata(self) -> None:
        builder = PendingBatchBuilder(max_rows=2, token_budget=0)

        self.assertFalse(builder.add(row("r1", arrival_s=2.0, prompt_tokens=10)))
        self.assertTrue(builder.add(row("r2", arrival_s=3.0, prompt_tokens=20)))

        closed = builder.close()

        self.assertEqual([item.row_id for item in closed.rows], ["r1", "r2"])
        self.assertEqual(closed.prompt_tokens, 30)
        self.assertEqual(closed.oldest_arrival_s, 2.0)
        self.assertEqual(closed.row_count, 2)
        self.assertEqual(closed.estimated_total_tokens, 30)

    def test_token_budget_counts_prompt_and_estimated_output_tokens(self) -> None:
        builder = PendingBatchBuilder(max_rows=10, token_budget=10)

        self.assertFalse(builder.add(row("r1", prompt_tokens=4, estimated_output_tokens=5)))
        self.assertTrue(builder.add(row("r2", prompt_tokens=1, estimated_output_tokens=2)))

        closed = builder.close()

        self.assertEqual(closed.prompt_tokens, 5)
        self.assertEqual(closed.estimated_output_tokens, 7)
        self.assertEqual(closed.estimated_total_tokens, 12)

    def test_oversized_row_forms_a_complete_one_row_batch(self) -> None:
        builder = PendingBatchBuilder(max_rows=10, token_budget=10)

        self.assertTrue(builder.add(row("r1", prompt_tokens=8, estimated_output_tokens=3)))

        closed = builder.close()
        self.assertEqual(closed.row_count, 1)
        self.assertEqual(closed.rows[0].row_id, "r1")
        self.assertEqual(closed.estimated_total_tokens, 11)

    def test_invalid_or_nonfinite_metadata_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "row_id"):
            row("")
        with self.assertRaisesRegex(ValueError, "arrival_s"):
            row("r1", arrival_s=math.nan)
        with self.assertRaisesRegex(ValueError, "arrival_s"):
            row("r1", arrival_s=math.inf)
        with self.assertRaisesRegex(ValueError, "token"):
            row("r1", prompt_tokens=-1)
        with self.assertRaisesRegex(ValueError, "token"):
            row("r1", estimated_output_tokens=-1)
        with self.assertRaisesRegex(ValueError, "max_rows"):
            PendingBatchBuilder(max_rows=0, token_budget=1)
        with self.assertRaisesRegex(ValueError, "token_budget"):
            PendingBatchBuilder(max_rows=1, token_budget=-1)

    def test_close_rejects_an_empty_builder(self) -> None:
        with self.assertRaisesRegex(ValueError, "empty"):
            PendingBatchBuilder(max_rows=1, token_budget=1).close()

    def test_add_after_full_is_rejected_until_close_resets_builder(self) -> None:
        builder = PendingBatchBuilder(max_rows=1, token_budget=0)

        self.assertTrue(builder.add(row("r1")))
        with self.assertRaisesRegex(RuntimeError, "capacity"):
            builder.add(row("r2"))

        builder.close()
        self.assertTrue(builder.add(row("r2")))


if __name__ == "__main__":
    unittest.main()
