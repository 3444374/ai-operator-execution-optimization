from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.batching import (  # noqa: E402
    ArrivalReplayBatcher,
    PendingBatchBuilder,
    ReplayServiceObservation,
    RowArrival,
)
from src.scheduling.flush import (  # noqa: E402
    FixedTimeoutFlush,
    ImmediateFlush,
    QueueAdaptiveFlush,
)


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


class FakeReplayClock:
    def __init__(self, now_s: float = 100.0) -> None:
        self.current_s = now_s
        self.waited_until: list[float] = []

    def now(self) -> float:
        return self.current_s

    def wait_until(self, deadline_s: float) -> None:
        if self.waited_until and deadline_s < self.waited_until[-1]:
            raise AssertionError("replay deadlines must be monotonic")
        if deadline_s < self.current_s:
            raise AssertionError("already-due work must not wait")
        self.waited_until.append(deadline_s)
        self.current_s = max(self.current_s, deadline_s)

    def advance(self, seconds: float) -> None:
        self.current_s += seconds


def replay(
    rows: object,
    clock: FakeReplayClock,
    *,
    max_rows: int = 10,
    token_budget: int = 0,
    flush_policy: object | None = None,
    close_batch: object | None = None,
    service_observation: object | None = None,
) -> ArrivalReplayBatcher:
    return ArrivalReplayBatcher(
        rows=rows,
        builder_factory=lambda: PendingBatchBuilder(
            max_rows=max_rows,
            token_budget=token_budget,
        ),
        flush_policy=flush_policy or FixedTimeoutFlush(timeout_s=10.0),
        close_batch=close_batch
        or (lambda batch: tuple(item.row_id for item in batch.rows)),
        service_observation=service_observation
        or (
            lambda: ReplayServiceObservation(
                fresh=True,
                running=0,
                waiting=0,
                kv_usage=0.0,
            )
        ),
        clock=clock,
    )


def unsafe_arrival(value: object = 1.0, *, missing: bool = False) -> RowArrival:
    item = object.__new__(RowArrival)
    object.__setattr__(item, "row_id", "invalid")
    if not missing:
        object.__setattr__(item, "arrival_s", value)
    object.__setattr__(item, "prompt_tokens", 1)
    object.__setattr__(item, "estimated_output_tokens", 0)
    object.__setattr__(item, "prefix_key", "")
    object.__setattr__(item, "payload_ref", object())
    return item


class ArrivalReplayBatcherTests(unittest.TestCase):
    def test_first_arrival_is_normalized_and_later_gap_is_preserved(self) -> None:
        clock = FakeReplayClock()
        batcher = replay(
            [row("r1", arrival_s=7.0), row("r2", arrival_s=7.25)],
            clock,
        )

        self.assertEqual(list(batcher), [("r1", "r2")])
        self.assertEqual(clock.waited_until, [100.25])

    def test_fixed_timeout_closes_partial_batch_before_later_arrival(self) -> None:
        clock = FakeReplayClock()
        batcher = replay(
            [row("r1", arrival_s=5.0), row("r2", arrival_s=6.0)],
            clock,
            flush_policy=FixedTimeoutFlush(timeout_s=0.25),
        )

        self.assertEqual(list(batcher), [("r1",), ("r2",)])
        self.assertEqual(clock.waited_until, [100.25, 101.0])
        self.assertEqual(
            [event.reason for event in batcher.trace if event.action == "flush"],
            ["fixed_timeout", "end_of_input"],
        )

    def test_queue_congestion_flushes_at_hard_maximum(self) -> None:
        clock = FakeReplayClock()
        batcher = replay(
            [row("r1", arrival_s=2.0), row("r2", arrival_s=3.0)],
            clock,
            flush_policy=QueueAdaptiveFlush(max_wait_s=0.5),
            service_observation=lambda: ReplayServiceObservation(
                fresh=True,
                running=64,
                waiting=4,
                kv_usage=0.95,
            ),
        )

        self.assertEqual(list(batcher), [("r1",), ("r2",)])
        self.assertEqual(clock.waited_until, [100.5, 101.0])
        self.assertIn(
            "hard_max_wait",
            [event.reason for event in batcher.trace],
        )

    def test_rows_due_during_downstream_block_are_caught_up_without_waits(
        self,
    ) -> None:
        clock = FakeReplayClock()
        closed = 0

        def blocking_close(batch: object) -> tuple[str, ...]:
            nonlocal closed
            closed += 1
            if closed == 1:
                clock.advance(0.25)
            return tuple(item.row_id for item in batch.rows)

        batcher = replay(
            [
                row("r1", arrival_s=10.0),
                row("r2", arrival_s=10.1),
                row("r3", arrival_s=10.2),
            ],
            clock,
            flush_policy=ImmediateFlush(),
            close_batch=blocking_close,
        )

        iterator = iter(batcher)
        self.assertEqual(next(iterator), ("r1",))
        self.assertEqual(list(iterator), [("r2",), ("r3",)])
        self.assertEqual(clock.waited_until, [])

    def test_equal_arrivals_preserve_source_order(self) -> None:
        clock = FakeReplayClock()
        batcher = replay(
            [
                row("r2", arrival_s=8.0),
                row("r1", arrival_s=8.0),
                row("r3", arrival_s=8.0),
            ],
            clock,
        )

        self.assertEqual(list(batcher), [("r2", "r1", "r3")])

    def test_invalid_or_decreasing_replay_arrivals_are_rejected(self) -> None:
        invalid_rows = [
            unsafe_arrival(missing=True),
            unsafe_arrival(-1.0),
            unsafe_arrival(True),
            unsafe_arrival(math.nan),
            unsafe_arrival(math.inf),
        ]
        for invalid in invalid_rows:
            with self.subTest(arrival=getattr(invalid, "arrival_s", "missing")):
                with self.assertRaisesRegex(ValueError, "arrival_s"):
                    list(replay([invalid], FakeReplayClock()))

        with self.assertRaisesRegex(ValueError, "non-decreasing"):
            list(
                replay(
                    [
                        row("r1", arrival_s=2.0),
                        row("r2", arrival_s=1.0),
                    ],
                    FakeReplayClock(),
                )
            )

    def test_every_row_is_closed_exactly_once(self) -> None:
        clock = FakeReplayClock()
        batcher = replay(
            [
                row("r1", arrival_s=0.0),
                row("r2", arrival_s=0.1),
                row("r3", arrival_s=0.5),
                row("r4", arrival_s=0.5),
                row("r5", arrival_s=0.8),
            ],
            clock,
            max_rows=2,
            flush_policy=FixedTimeoutFlush(timeout_s=0.15),
        )

        flattened = [row_id for batch in batcher for row_id in batch]
        self.assertEqual(flattened, ["r1", "r2", "r3", "r4", "r5"])
        self.assertEqual(len(flattened), len(set(flattened)))

    def test_strict_token_membership_precloses_normal_rows_but_not_oversized_row(
        self,
    ) -> None:
        clock = FakeReplayClock()
        batcher = replay(
            [
                row("normal-1", arrival_s=0.0, prompt_tokens=6),
                row("normal-2", arrival_s=0.0, prompt_tokens=6),
                row("oversized", arrival_s=0.0, prompt_tokens=12),
            ],
            clock,
            token_budget=10,
        )

        closed = list(batcher)

        self.assertEqual(
            closed,
            [("normal-1",), ("normal-2",), ("oversized",)],
        )

    def test_replay_does_not_consume_rows_until_iteration_begins(self) -> None:
        clock = FakeReplayClock()
        consumed: list[str] = []

        def source() -> object:
            consumed.append("started")
            yield row("r1")

        batcher = replay(source(), clock)

        self.assertEqual(consumed, [])
        self.assertEqual(list(batcher), [("r1",)])
        self.assertEqual(consumed, ["started"])


class PendingBatchBuilderTests(unittest.TestCase):
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
