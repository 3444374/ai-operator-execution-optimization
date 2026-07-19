from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pyarrow as pa

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.organizers import OrganizerConfig, batch_metrics, make_organizer


def sample_table(row_count: int = 10) -> pa.Table:
    return pa.table(
        {
            "doc_id": list(range(row_count)),
            "prompt": [f"prompt {i}" for i in range(row_count)],
            "prompt_tokens": list(range(10, 10 + row_count)),
        }
    )


class OrganizerTests(unittest.TestCase):
    def test_arrow_organizer_splits_batches(self) -> None:
        organizer = make_organizer("arrow", OrganizerConfig(batch_size=4))
        result = organizer.organize(sample_table())

        self.assertEqual(result.metrics["organizer"], "arrow")
        self.assertEqual([batch.num_rows for batch in result.batches], [4, 4, 2])
        self.assertEqual(batch_metrics(result.batches)["output_rows"], 10)

    def test_daft_organizer_splits_batches_native(self) -> None:
        organizer = make_organizer("daft", OrganizerConfig(batch_size=4, runner="native"))
        result = organizer.organize(sample_table())

        self.assertEqual(result.metrics["organizer"], "daft")
        self.assertEqual(result.metrics["runner"], "native")
        self.assertEqual([batch.num_rows for batch in result.batches], [4, 4, 2])
        self.assertEqual(batch_metrics(result.batches)["output_rows"], 10)

    def test_daft_organizer_can_be_reused(self) -> None:
        organizer = make_organizer("daft", OrganizerConfig(batch_size=5, runner="native"))

        first = organizer.organize(sample_table())
        second = organizer.organize(sample_table())

        self.assertEqual([batch.num_rows for batch in first.batches], [5, 5])
        self.assertEqual([batch.num_rows for batch in second.batches], [5, 5])

    def test_arrow_organizer_supports_token_budget_batches(self) -> None:
        table = pa.table(
            {
                "doc_id": [1, 2, 3, 4],
                "prompt": ["a", "b", "c", "d"],
                "prompt_tokens": [100, 700, 300, 1200],
            }
        )
        organizer = make_organizer(
            "arrow",
            OrganizerConfig(
                batch_size=999,
                batching_policy="token_budget",
                token_budget=1000,
                completion_max_tokens=16,
            ),
        )
        result = organizer.organize(table)

        self.assertEqual([batch.num_rows for batch in result.batches], [2, 1, 1])
        self.assertEqual(batch_metrics(result.batches)["output_rows"], 4)

    def test_token_budget_requires_positive_budget(self) -> None:
        with self.assertRaisesRegex(ValueError, "token_budget must be positive"):
            make_organizer("arrow", OrganizerConfig(batch_size=4, batching_policy="token_budget"))

    def test_length_align_fixed_rows_reorders_by_prompt_tokens(self) -> None:
        table = pa.table(
            {
                "doc_id": [1, 2, 3, 4],
                "prompt": ["a", "b", "c", "d"],
                "prompt_tokens": [400, 10, 300, 20],
            }
        )
        organizer = make_organizer(
            "arrow",
            OrganizerConfig(batch_size=2, batching_policy="length_align_fixed_rows"),
        )
        result = organizer.organize(table)

        self.assertEqual(result.batches[0].column("doc_id").to_pylist(), [2, 4])
        self.assertEqual(result.batches[1].column("doc_id").to_pylist(), [3, 1])
        self.assertEqual(result.metrics["organization_policy_family"], "length_align")

    def test_prefix_aware_fixed_rows_groups_prefix_keys(self) -> None:
        table = pa.table(
            {
                "doc_id": [1, 2, 3, 4],
                "prompt": ["a", "b", "c", "d"],
                "prompt_tokens": [100, 100, 100, 100],
                "prefix_key": ["b", "a", "b", "a"],
            }
        )
        organizer = make_organizer(
            "arrow",
            OrganizerConfig(batch_size=2, batching_policy="prefix_aware_fixed_rows"),
        )
        result = organizer.organize(table)

        self.assertEqual(result.batches[0].column("prefix_key").to_pylist(), ["a", "a"])
        self.assertEqual(result.batches[1].column("prefix_key").to_pylist(), ["b", "b"])
        self.assertEqual(result.metrics["organization_policy_family"], "prefix_aware")
        self.assertEqual(result.metrics["prefix_group_ratio"], 1.0)


if __name__ == "__main__":
    unittest.main()
