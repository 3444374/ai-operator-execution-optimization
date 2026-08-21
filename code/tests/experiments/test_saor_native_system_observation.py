from __future__ import annotations

import unittest

from src.experiments.saor.native_system_observation import (
    JobObservationContract,
    build_system_observation,
    summarize_gateway_rows,
)


def _row(
    job_id: str,
    sequence: int,
    arrived: float,
    completed: float,
    work: int,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "gateway_request_id": sequence,
        "job_id": job_id,
        "endpoint_id": f"endpoint-{sequence % 2}",
        "received_epoch_s": arrived,
        "upstream_start_epoch_s": arrived + 0.001,
        "upstream_response_epoch_s": completed,
        "response_completed_epoch_s": completed + 0.001,
        "dispatch_delay_s": 0.001,
        "upstream_status": 200,
        "retry_count": 0,
        "request_body_sha256": f"body-{sequence}",
        "forwarded_body_sha256": f"body-{sequence}",
        "actual_prompt_tokens": work - 2,
        "actual_output_tokens": 2,
        "actual_total_tokens": work,
        "status": "completed",
    }


class NativeSystemObservationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.contracts = (
            JobObservationContract("job0", "bulk", 1.0, 30.0, None),
            JobObservationContract("job1", "foreground", 1.0, 30.0, 6.0),
        )

    def test_common_backlog_fairness_uses_actual_token_work(self) -> None:
        rows = [
            _row("job0", 0, 10.0, 12.0, 10),
            _row("job0", 1, 10.5, 15.0, 20),
            _row("job1", 2, 11.0, 13.0, 30),
            _row("job1", 3, 11.5, 13.5, 40),
        ]
        summary = summarize_gateway_rows(rows, self.contracts)

        self.assertEqual(summary["status"], "passed")
        self.assertAlmostEqual(
            summary["service_fairness"]["common_backlog_duration_s"],
            2.5,
        )
        self.assertEqual(
            summary["service_fairness"]["completed_work_by_job"],
            {"job0": 10, "job1": 70},
        )
        self.assertAlmostEqual(
            summary["service_fairness"]["weighted_jain_fairness"],
            0.64,
        )
        self.assertAlmostEqual(
            summary["service_fairness"]["longest_no_service_by_job_s"]["job0"],
            1.5,
        )

    def test_system_timeline_uses_release_before_source_and_result_visibility(self) -> None:
        gateway = summarize_gateway_rows(
            [
                _row("job0", 0, 102.0, 108.0, 10),
                _row("job1", 1, 107.0, 111.0, 20),
            ],
            self.contracts,
        )
        system = build_system_observation(
            gateway,
            t0_by_job={"job0": 100.0, "job1": 105.0},
            t1_by_job={"job0": 101.0, "job1": 106.0},
            t4_by_job={"job0": 109.0, "job1": 112.0},
        )

        self.assertEqual(system["status"], "passed")
        self.assertEqual(system["jobs"]["job0"]["jct_s"], 9.0)
        self.assertEqual(system["jobs"]["job0"]["source_s"], 1.0)
        self.assertEqual(system["jobs"]["job0"]["execution_s"], 8.0)
        self.assertEqual(system["jobs"]["job0"]["service_span_s"], 6.0)
        self.assertEqual(system["group_jct_s"], 12.0)
        self.assertEqual(system["correct_throughput_tokens_per_s"], 2.5)
        self.assertEqual(
            system["jobs"]["job0"]["job_jct_slo_status"], "unavailable"
        )
        self.assertTrue(system["jobs"]["job1"]["job_jct_slo_violation"])
        self.assertEqual(
            system["isolation_observation"]["counterfactual_scope"],
            "within_run_only_not_full_solo_slowdown",
        )

    def test_retry_or_body_mutation_fails_closed(self) -> None:
        row = _row("job0", 0, 1.0, 2.0, 10)
        row["retry_count"] = 1
        with self.assertRaisesRegex(ValueError, "retry"):
            summarize_gateway_rows([row], self.contracts[:1])

        row["retry_count"] = 0
        row["forwarded_body_sha256"] = "mutated"
        with self.assertRaisesRegex(ValueError, "body"):
            summarize_gateway_rows([row], self.contracts[:1])

    def test_invalid_timeline_order_fails_closed(self) -> None:
        gateway = summarize_gateway_rows(
            [_row("job0", 0, 2.0, 3.0, 10)], self.contracts[:1]
        )
        with self.assertRaisesRegex(ValueError, "T0<=T1<=T2<=T3<=T4"):
            build_system_observation(
                gateway,
                t0_by_job={"job0": 2.5},
                t1_by_job={"job0": 2.6},
                t4_by_job={"job0": 4.0},
            )


if __name__ == "__main__":
    unittest.main()
