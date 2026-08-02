from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling.core.lifecycle import (  # noqa: E402
    MonotonicEpochClock,
    RequestLifecycleSeed,
    SubmissionServiceTiming,
    build_request_trace_rows,
)
from src.scheduling.core.models import SubmissionLifecycleEvent  # noqa: E402


def seed(
    request_id: str,
    doc_id: str,
    arrival_epoch_s: float,
) -> RequestLifecycleSeed:
    return RequestLifecycleSeed(
        request_id=request_id,
        submission_id="job:batch:0",
        doc_id=doc_id,
        prompt_tokens=10 if doc_id == "1" else 20,
        estimated_output_tokens=4,
        prefix_key="p",
        arrival_epoch_s=arrival_epoch_s,
        flush_epoch_s=100.025,
        request_time_origin="replayed_arrival",
    )


def completed_event() -> SubmissionLifecycleEvent:
    return SubmissionLifecycleEvent(
        submission_id="job:batch:0",
        pool_id="default",
        endpoint_id="task-0",
        gpu_id="0",
        submit_epoch_s=100.030,
        completion_epoch_s=100.300,
        status="completed",
    )


def service_timing(
    start_epoch_s: float | None = 100.040,
    end_epoch_s: float | None = 100.290,
) -> SubmissionServiceTiming:
    return SubmissionServiceTiming(
        submission_id="job:batch:0",
        service_start_epoch_s=start_epoch_s,
        service_end_epoch_s=end_epoch_s,
    )


class RequestLifecycleTests(unittest.TestCase):
    def test_monotonic_epoch_clock_ignores_later_wall_clock_adjustments(
        self,
    ) -> None:
        monotonic_values = iter([10.0, 10.1, 10.2])
        wall_clock_calls = []

        def wall_clock() -> float:
            wall_clock_calls.append(True)
            return 1_000.0

        clock = MonotonicEpochClock(
            monotonic_clock=lambda: next(monotonic_values),
            epoch_clock=wall_clock,
        )

        self.assertAlmostEqual(clock(), 1_000.1)
        self.assertAlmostEqual(clock(), 1_000.2)
        self.assertEqual(len(wall_clock_calls), 1)

    def test_join_preserves_arrival_and_shared_submission_timing(self) -> None:
        rows = build_request_trace_rows(
            seeds=[
                seed("job:row:1", "1", 100.000),
                seed("job:row:2", "2", 100.010),
            ],
            submission_events=[completed_event()],
            service_by_submission_id={"job:batch:0": service_timing()},
            client_estimated_output_tokens_by_doc_id={"1": 2, "2": 1},
            actual_output_tokens_by_doc_id={},
            slo_target_s=0.250,
        )

        self.assertEqual([row.doc_id for row in rows], ["1", "2"])
        self.assertEqual(
            [row.client_estimated_output_tokens for row in rows],
            [2, 1],
        )
        self.assertEqual([row.actual_output_tokens for row in rows], [None, None])
        self.assertEqual([row.total_tokens for row in rows], [None, None])
        self.assertEqual(
            {row.output_token_source for row in rows},
            {"submission_aggregate_unavailable"},
        )
        for actual, expected in zip(
            (row.buffer_s for row in rows),
            (0.025, 0.015),
        ):
            self.assertAlmostEqual(actual, expected)
        for actual, expected in zip(
            (row.e2e_s for row in rows),
            (0.300, 0.290),
        ):
            self.assertAlmostEqual(actual, expected)
        self.assertEqual([row.slo_met for row in rows], [False, False])
        self.assertTrue(
            all(row.latency_granularity == "submission" for row in rows)
        )
        self.assertTrue(
            all(row.request_time_origin == "replayed_arrival" for row in rows)
        )

    def test_request_trace_preserves_offline_time_origin(self) -> None:
        offline_seed = RequestLifecycleSeed(
            request_id="job:row:1",
            submission_id="job:batch:0",
            doc_id="1",
            prompt_tokens=4,
            estimated_output_tokens=2,
            prefix_key="",
            arrival_epoch_s=100.0,
            flush_epoch_s=101.0,
            request_time_origin="offline_job_start",
        )
        rows = build_request_trace_rows(
            [offline_seed],
            [
                SubmissionLifecycleEvent(
                    submission_id="job:batch:0",
                    pool_id="default",
                    endpoint_id="task-0",
                    gpu_id="0",
                    submit_epoch_s=101.1,
                    completion_epoch_s=102.0,
                    status="completed",
                )
            ],
            {
                "job:batch:0": SubmissionServiceTiming(
                    submission_id="job:batch:0",
                    service_start_epoch_s=101.2,
                    service_end_epoch_s=101.9,
                )
            },
            {"1": 1},
            {},
            slo_target_s=None,
        )

        self.assertEqual(rows[0].request_time_origin, "offline_job_start")
        self.assertEqual(rows[0].latency_granularity, "submission")

        self.assertEqual(rows[0].e2e_s, 2.0)

    def test_request_lifecycle_seed_rejects_unknown_time_origin(self) -> None:
        with self.assertRaisesRegex(ValueError, "request_time_origin"):
            RequestLifecycleSeed(
                request_id="job:row:1",
                submission_id="job:batch:0",
                doc_id="1",
                prompt_tokens=4,
                estimated_output_tokens=2,
                prefix_key="",
                arrival_epoch_s=100.0,
                flush_epoch_s=101.0,
                request_time_origin="unknown",
            )

    def test_join_uses_only_genuine_per_request_actual_tokens(self) -> None:
        row = build_request_trace_rows(
            seeds=[seed("job:row:1", "1", 100.000)],
            submission_events=[completed_event()],
            service_by_submission_id={"job:batch:0": service_timing()},
            client_estimated_output_tokens_by_doc_id={"1": 2},
            actual_output_tokens_by_doc_id={"1": 3},
            finish_reason_by_doc_id={"1": "stop"},
            slo_target_s=None,
        )[0]

        self.assertEqual(row.actual_output_tokens, 3)
        self.assertEqual(row.total_tokens, 13)
        self.assertEqual(row.output_token_source, "endpoint_request")
        self.assertEqual(row.finish_reason, "stop")
        self.assertIsNone(row.slo_met)

    def test_join_rejects_duplicate_or_incomplete_identity_maps(self) -> None:
        common = {
            "submission_events": [completed_event()],
            "service_by_submission_id": {"job:batch:0": service_timing()},
            "client_estimated_output_tokens_by_doc_id": {"1": 2},
            "actual_output_tokens_by_doc_id": {},
            "slo_target_s": None,
        }
        with self.assertRaisesRegex(ValueError, "duplicate request_id"):
            build_request_trace_rows(
                seeds=[
                    seed("job:row:1", "1", 100.000),
                    seed("job:row:1", "2", 100.010),
                ],
                **common,
            )
        with self.assertRaisesRegex(ValueError, "client-estimated output token"):
            build_request_trace_rows(
                seeds=[seed("job:row:1", "1", 100.000)],
                submission_events=[completed_event()],
                service_by_submission_id={"job:batch:0": service_timing()},
                client_estimated_output_tokens_by_doc_id={},
                actual_output_tokens_by_doc_id={},
                slo_target_s=None,
            )
        with self.assertRaisesRegex(ValueError, "service timing"):
            build_request_trace_rows(
                seeds=[seed("job:row:1", "1", 100.000)],
                submission_events=[completed_event()],
                service_by_submission_id={},
                client_estimated_output_tokens_by_doc_id={"1": 2},
                actual_output_tokens_by_doc_id={},
                slo_target_s=None,
            )

    def test_join_rejects_invalid_timestamp_order(self) -> None:
        event = SubmissionLifecycleEvent(
            submission_id="job:batch:0",
            pool_id="default",
            endpoint_id="task-0",
            gpu_id="0",
            submit_epoch_s=100.020,
            completion_epoch_s=100.300,
            status="completed",
        )
        with self.assertRaisesRegex(ValueError, "timestamp order"):
            build_request_trace_rows(
                seeds=[seed("job:row:1", "1", 100.000)],
                submission_events=[event],
                service_by_submission_id={
                    "job:batch:0": service_timing(100.040, 100.290)
                },
                client_estimated_output_tokens_by_doc_id={"1": 2},
                actual_output_tokens_by_doc_id={},
                slo_target_s=None,
            )

    def test_backend_clock_skew_does_not_fabricate_submit_to_service(self) -> None:
        row = build_request_trace_rows(
            seeds=[seed("job:row:1", "1", 100.000)],
            submission_events=[completed_event()],
            service_by_submission_id={
                "job:batch:0": service_timing(99.900, 100.200)
            },
            client_estimated_output_tokens_by_doc_id={"1": 2},
            actual_output_tokens_by_doc_id={},
            slo_target_s=None,
        )[0]

        self.assertIsNone(row.submit_to_service_s)
        self.assertAlmostEqual(row.service_s, 0.300)
        self.assertEqual(row.service_clock_domain, "backend")

    def test_failed_submission_has_no_fabricated_service_timing(self) -> None:
        event = SubmissionLifecycleEvent(
            submission_id="job:batch:0",
            pool_id="default",
            endpoint_id="task-0",
            gpu_id="0",
            submit_epoch_s=100.030,
            completion_epoch_s=100.100,
            status="failed",
            error="backend unavailable",
        )

        row = build_request_trace_rows(
            seeds=[seed("job:row:1", "1", 100.000)],
            submission_events=[event],
            service_by_submission_id={
                "job:batch:0": service_timing(None, None)
            },
            client_estimated_output_tokens_by_doc_id={},
            actual_output_tokens_by_doc_id={},
            slo_target_s=0.250,
        )[0]

        self.assertEqual(row.status, "failed")
        self.assertEqual(row.error_type, "backend unavailable")
        self.assertIsNone(row.service_start_epoch_s)
        self.assertIsNone(row.service_s)
        self.assertAlmostEqual(row.e2e_s, 0.100)
        self.assertFalse(row.slo_met)


if __name__ == "__main__":
    unittest.main()
