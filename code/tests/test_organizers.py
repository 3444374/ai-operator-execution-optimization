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


def output_aware_table() -> pa.Table:
    return pa.table(
        {
            "doc_id": [10, 11, 12, 13, 14],
            "prompt": ["a", "b", "c", "d", "e"],
            "prompt_tokens": [6, 5, 4, 3, 2],
            "target_output_tokens": [0, 0, 0, 0, 0],
        }
    )


def row_cap_fragmentation_table() -> pa.Table:
    return pa.table(
        {
            "doc_id": list(range(6)),
            "prompt": [f"prompt {index}" for index in range(6)],
            "prompt_tokens": [1, 1, 2, 3, 3, 8],
        }
    )


def memberships(result) -> list[list[int]]:
    return [
        batch.column("doc_id").to_pylist()
        for batch in result.batches
    ]


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

    def test_sequential_token_budget_honors_same_row_cap_as_best_fit(self) -> None:
        table = pa.table(
            {
                "doc_id": [1, 2, 3, 4, 5],
                "prompt": ["a", "b", "c", "d", "e"],
                "prompt_tokens": [1, 1, 1, 1, 1],
            }
        )
        common = {
            "batch_size": 2,
            "token_budget": 100,
            "output_cost_mode": "prompt_only",
        }

        sequential = make_organizer(
            "arrow",
            OrganizerConfig(
                batching_policy="token_budget",
                **common,
            ),
        ).organize(table)
        best_fit = make_organizer(
            "arrow",
            OrganizerConfig(
                batching_policy="best_fit_token_budget",
                **common,
            ),
        ).organize(table)

        self.assertEqual(
            [batch.num_rows for batch in sequential.batches],
            [2, 2, 1],
        )
        self.assertEqual(
            [batch.num_rows for batch in best_fit.batches],
            [2, 2, 1],
        )

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

    def test_arrow_best_fit_uses_shared_deterministic_membership(self) -> None:
        result = make_organizer(
            "arrow",
            OrganizerConfig(
                batch_size=3,
                batching_policy="best_fit_token_budget",
                token_budget=10,
                output_cost_mode="prompt_only",
            ),
        ).organize(output_aware_table())

        self.assertEqual(memberships(result), [[10, 12], [11, 13, 14]])
        self.assertEqual(
            result.metrics["packing_algorithm"],
            "best_fit_decreasing",
        )
        self.assertEqual(result.metrics["packing_scope"], "organizer_input")
        self.assertEqual(result.metrics["packing_cost_unit"], "tokens")
        self.assertEqual(result.metrics["packing_input_rows"], 5)
        self.assertEqual(result.metrics["packing_batch_count"], 2)

    def test_arrow_and_daft_best_fit_membership_is_identical(self) -> None:
        config = OrganizerConfig(
            batch_size=3,
            runner="native",
            batching_policy="best_fit_token_budget",
            token_budget=10,
            output_cost_mode="prompt_only",
        )

        arrow = make_organizer("arrow", config).organize(
            output_aware_table()
        )
        daft = make_organizer("daft", config).organize(
            output_aware_table()
        )

        self.assertEqual(memberships(arrow), memberships(daft))
        self.assertEqual(arrow.batch_cost_units, daft.batch_cost_units)

    def test_arrow_and_daft_row_cap_aware_membership_is_identical(self) -> None:
        config = OrganizerConfig(
            batch_size=3,
            runner="native",
            batching_policy="row_cap_aware_token_budget",
            token_budget=10,
            output_cost_mode="prompt_only",
        )

        for organizer_name in ("arrow", "daft"):
            with self.subTest(organizer=organizer_name):
                result = make_organizer(
                    organizer_name,
                    config,
                ).organize(row_cap_fragmentation_table())

                self.assertEqual(
                    memberships(result),
                    [[5, 0, 1], [3, 4, 2]],
                )
                self.assertEqual(
                    result.metrics["packing_algorithm"],
                    "row_cap_aware_best_fit_decreasing",
                )
                self.assertEqual(
                    result.metrics["packing_scope"],
                    "organizer_input",
                )

    def test_trace_cost_changes_membership_without_changing_cap(self) -> None:
        table = pa.table(
            {
                "doc_id": [1, 2, 3],
                "prompt": ["a", "b", "c"],
                "prompt_tokens": [2, 2, 2],
                "target_output_tokens": [8, 0, 0],
            }
        )
        config = OrganizerConfig(
            batch_size=3,
            batching_policy="best_fit_token_budget",
            token_budget=10,
            completion_max_tokens=16,
            output_cost_mode="trace_target_output",
        )

        result = make_organizer("arrow", config).organize(table)

        self.assertEqual(memberships(result), [[1], [2, 3]])
        self.assertEqual(config.completion_max_tokens, 16)
        self.assertEqual(
            result.metrics["output_cost_source"],
            "burstgpt_unpaired_trace_metadata",
        )

    def test_trace_cost_requires_valid_prompt_and_target_tokens(self) -> None:
        invalid_tables = [
            pa.table(
                {
                    "doc_id": [1],
                    "prompt_tokens": [-1],
                    "target_output_tokens": [1],
                }
            ),
            pa.table(
                {
                    "doc_id": [1],
                    "prompt_tokens": [1],
                }
            ),
        ]
        config = OrganizerConfig(
            batch_size=2,
            batching_policy="best_fit_token_budget",
            token_budget=10,
            output_cost_mode="trace_target_output",
        )

        for table in invalid_tables:
            with self.subTest(columns=table.column_names):
                with self.assertRaises(ValueError):
                    make_organizer("arrow", config).organize(table)


if __name__ == "__main__":
    unittest.main()
