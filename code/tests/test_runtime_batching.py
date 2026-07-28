from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.batching import (  # noqa: E402
    ArrivalReplayBatcher,
    PendingBatchBuilder,
    ReplayServiceObservation,
    RowArrival,
    SystemReplayClock,
)
from src.scheduling.flush import (  # noqa: E402
    FixedTimeoutFlush,
    ImmediateFlush,
    QueueAdaptiveFlush,
    SloAwareEwmaFlush,
)
from src.scheduling.token_budget import (  # noqa: E402
    ServiceQuantumTokenBudgetController,
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
    arrival_time_scale: float = 1.0,
    token_budget_policy: object | None = None,
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
        arrival_time_scale=arrival_time_scale,
        token_budget_policy=token_budget_policy,
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


class SystemReplayClockTests(unittest.TestCase):
    def test_wait_until_retries_when_system_sleep_returns_early(self) -> None:
        timeline = {"now_s": 100.0}
        sleep_calls: list[float] = []

        def monotonic() -> float:
            return timeline["now_s"]

        def sleep(delay_s: float) -> None:
            sleep_calls.append(delay_s)
            if len(sleep_calls) == 1:
                timeline["now_s"] += delay_s / 2
            else:
                timeline["now_s"] += delay_s

        with (
            patch("src.scheduling.batching.time.monotonic", side_effect=monotonic),
            patch("src.scheduling.batching.time.sleep", side_effect=sleep),
        ):
            clock = SystemReplayClock()
            clock.wait_until(101.0)
            observed_now_s = clock.now()

        self.assertGreaterEqual(observed_now_s, 101.0)
        self.assertEqual(len(sleep_calls), 2)


class ArrivalReplayBatcherTests(unittest.TestCase):
    def test_arrival_time_scale_compresses_only_replay_offsets(self) -> None:
        clock = FakeReplayClock()
        batcher = replay(
            [row("r1", arrival_s=7.0), row("r2", arrival_s=107.0)],
            clock,
            arrival_time_scale=0.001,
        )

        self.assertEqual(list(batcher), [("r1", "r2")])
        self.assertEqual(clock.waited_until, [100.1])

    def test_arrival_time_scale_must_be_finite_and_positive(self) -> None:
        for scale in (0.0, -1.0, math.nan, math.inf):
            with self.subTest(scale=scale):
                with self.assertRaisesRegex(
                    ValueError,
                    "arrival_time_scale must be finite and positive",
                ):
                    replay([], FakeReplayClock(), arrival_time_scale=scale)

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

    def test_queue_pressure_selects_maximum_window(self) -> None:
        clock = FakeReplayClock()
        batcher = replay(
            [row("r1", arrival_s=2.0), row("r2", arrival_s=3.0)],
            clock,
            flush_policy=QueueAdaptiveFlush(
                min_wait_s=0.25,
                max_wait_s=0.5,
                pressure_running=8,
            ),
            service_observation=lambda: ReplayServiceObservation(
                fresh=True,
                running=8,
                waiting=4,
                kv_usage=0.95,
            ),
        )

        self.assertEqual(list(batcher), [("r1",), ("r2",)])
        self.assertEqual(clock.waited_until, [100.5, 101.0])
        self.assertIn(
            "queue_pressure",
            [event.reason for event in batcher.trace],
        )

    def test_row_before_selected_deadline_joins_after_observation_delay(
        self,
    ) -> None:
        policies = [
            FixedTimeoutFlush(timeout_s=0.1),
            QueueAdaptiveFlush(max_wait_s=0.1),
        ]
        for policy in policies:
            with self.subTest(policy=type(policy).__name__):
                clock = FakeReplayClock()

                def advancing_observation() -> ReplayServiceObservation:
                    clock.advance(0.11)
                    return ReplayServiceObservation(
                        fresh=True,
                        running=64,
                        waiting=4,
                        kv_usage=0.95,
                    )

                batcher = replay(
                    [
                        row("r1", arrival_s=0.0),
                        row("r2", arrival_s=0.05),
                    ],
                    clock,
                    flush_policy=policy,
                    service_observation=advancing_observation,
                )

                self.assertEqual(list(batcher), [("r1", "r2")])

    def test_due_before_selected_deadline_joins_after_downstream_delay(
        self,
    ) -> None:
        clock = FakeReplayClock()
        closed = 0

        def delayed_close(batch: object) -> tuple[str, ...]:
            nonlocal closed
            closed += 1
            if closed == 1:
                clock.advance(0.2)
            return tuple(item.row_id for item in batch.rows)

        batcher = replay(
            [
                row("first", arrival_s=0.0),
                row("second", arrival_s=1.0),
                row("third", arrival_s=1.02),
                row("after", arrival_s=1.06),
            ],
            clock,
            flush_policy=FixedTimeoutFlush(0.05),
            close_batch=delayed_close,
        )

        self.assertEqual(
            list(batcher),
            [("first",), ("second", "third"), ("after",)],
        )

    def test_adaptive_window_is_selected_once_per_pending_batch(self) -> None:
        clock = FakeReplayClock()
        observations = iter(
            [
                ReplayServiceObservation(True, 8, 1, 0.2),
                ReplayServiceObservation(True, 0, 0, 0.0),
            ]
        )
        batcher = replay(
            [
                row("r1", arrival_s=0.0),
                row("r2", arrival_s=0.04),
            ],
            clock,
            flush_policy=QueueAdaptiveFlush(
                min_wait_s=0.025,
                max_wait_s=0.05,
                pressure_running=8,
            ),
            service_observation=lambda: next(observations),
        )

        self.assertEqual(list(batcher), [("r1", "r2")])
        selected = [
            event
            for event in batcher.trace
            if event.window_reason == "queue_pressure"
        ]
        self.assertTrue(selected)
        self.assertTrue(all(event.selected_wait_s == 0.05 for event in selected))

    def test_dynamic_token_budget_is_selected_once_per_open_batch(self) -> None:
        clock = FakeReplayClock()
        controller = ServiceQuantumTokenBudgetController(
            (1024, 2048, 4096),
            fallback_budget=2048,
            target_service_s=1.0,
            max_fill_wait_s=1.0,
        )
        batcher = replay(
            [
                row("r1", arrival_s=0.0, prompt_tokens=1024),
                row("r2", arrival_s=0.5, prompt_tokens=1024),
                row("r3", arrival_s=1.0, prompt_tokens=1024),
            ],
            clock,
            token_budget=2048,
            flush_policy=FixedTimeoutFlush(0.1),
            token_budget_policy=controller,
            service_observation=lambda: ReplayServiceObservation(
                True,
                1,
                0,
                0.1,
                service_rate_tokens_s_per_endpoint=4096.0,
            ),
        )

        self.assertEqual(
            list(batcher),
            [("r1",), ("r2",), ("r3",)],
        )
        flush_events = [
            event for event in batcher.trace if event.action == "flush"
        ]
        self.assertEqual(
            [event.selected_token_budget for event in flush_events],
            [2048, 4096, 2048],
        )
        self.assertEqual(
            flush_events[1].token_budget_reason,
            "increase_one_step",
        )

    def test_low_load_base_window_excludes_row_after_25ms(self) -> None:
        clock = FakeReplayClock()
        batcher = replay(
            [
                row("r1", arrival_s=0.0),
                row("r2", arrival_s=0.03),
            ],
            clock,
            flush_policy=QueueAdaptiveFlush(
                min_wait_s=0.025,
                max_wait_s=0.05,
                pressure_running=8,
            ),
            service_observation=lambda: ReplayServiceObservation(
                True, 0, 0, 0.0
            ),
        )

        self.assertEqual(list(batcher), [("r1",), ("r2",)])

    def test_nonbinary_hard_deadline_flushes_before_later_arrival(self) -> None:
        policies = [
            FixedTimeoutFlush(timeout_s=0.1),
            QueueAdaptiveFlush(max_wait_s=0.1),
        ]
        for policy in policies:
            with self.subTest(policy=type(policy).__name__):
                clock = FakeReplayClock(now_s=100.0)
                batcher = replay(
                    [
                        row("r1", arrival_s=0.0),
                        row("r2", arrival_s=1.0),
                    ],
                    clock,
                    flush_policy=policy,
                    service_observation=lambda: ReplayServiceObservation(
                        fresh=True,
                        running=64,
                        waiting=4,
                        kv_usage=0.95,
                    ),
                )

                iterator = iter(batcher)
                self.assertEqual(next(iterator), ("r1",))
                self.assertEqual(clock.waited_until, [100.1])
                self.assertEqual(clock.now(), 100.1)
                self.assertEqual(list(iterator), [("r2",)])

    def test_missing_or_stale_metrics_use_fixed_fallback_window(self) -> None:
        observations = [
            ReplayServiceObservation(
                fresh=True,
                running=64,
                waiting=None,
                kv_usage=0.5,
            ),
            ReplayServiceObservation(
                fresh=False,
                running=64,
                waiting=0,
                kv_usage=0.5,
            ),
        ]
        for service in observations:
            with self.subTest(service=service):
                clock = FakeReplayClock(now_s=100.0)
                batcher = replay(
                    [
                        row("r1", arrival_s=0.0),
                        row("r2", arrival_s=1.0),
                    ],
                    clock,
                    flush_policy=QueueAdaptiveFlush(
                        min_wait_s=0.025,
                        max_wait_s=0.1,
                        pressure_running=8,
                    ),
                    service_observation=lambda: service,
                )

                iterator = iter(batcher)
                self.assertEqual(next(iterator), ("r1",))
                self.assertEqual(clock.waited_until, [100.025])
                self.assertIn(
                    "fixed_fallback",
                    [event.reason for event in batcher.trace],
                )
                self.assertEqual(list(iterator), [("r2",)])

    def test_slo_ewma_receives_rates_and_token_budget(self) -> None:
        clock = FakeReplayClock(now_s=100.0)
        batcher = replay(
            [
                row("r1", arrival_s=0.0, prompt_tokens=100),
                row("r2", arrival_s=0.04, prompt_tokens=100),
            ],
            clock,
            token_budget=1000,
            flush_policy=SloAwareEwmaFlush(
                min_wait_s=0.025,
                max_wait_s=0.050,
                request_slo_s=1.0,
                ewma_alpha=1.0,
            ),
            service_observation=lambda: ReplayServiceObservation(
                fresh=True,
                running=4,
                waiting=0,
                kv_usage=0.2,
                service_rate_tokens_s_per_endpoint=2_000.0,
            ),
        )

        self.assertEqual(list(batcher), [("r1", "r2")])
        initial = batcher.trace[0]
        self.assertEqual(initial.selected_wait_s, 0.050)
        self.assertEqual(initial.window_reason, "fixed_fallback")
        self.assertIsNone(initial.arrival_rate_tokens_s)
        self.assertEqual(
            initial.service_rate_tokens_s_per_endpoint,
            2_000.0,
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

    def test_empty_input_produces_no_batches_or_waits(self) -> None:
        clock = FakeReplayClock()
        batcher = replay([], clock)

        self.assertEqual(list(batcher), [])
        self.assertEqual(clock.waited_until, [])
        self.assertEqual(batcher.trace, ())


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
