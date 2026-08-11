"""Unit tests for phase-change action-sequence auditing."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parents[2] / "scripts" / "analysis"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import audit_phase_change as audit  # noqa: E402


SEGMENTS = (
    {"start_s": 0.0, "end_s": 60.0, "job_b_active": False},
    {"start_s": 60.0, "end_s": 120.0, "job_b_active": True},
    {"start_s": 120.0, "end_s": 180.0, "job_b_active": False},
    {"start_s": 180.0, "end_s": 240.0, "job_b_active": True},
)


def _row(
    endpoint: str,
    elapsed: float,
    *,
    action: str = "hold",
    reason: str = "hysteresis_or_deadband",
    applied: int = 160,
    kv: float = 0.2,
    waiting: int = 0,
    active: int = 128,
) -> dict[str, str]:
    return {
        "endpoint_id": endpoint,
        "observed_epoch_s": str(1000.0 + elapsed),
        "control_action": action,
        "control_reason": reason,
        "control_applied_request_limit": str(applied),
        "vllm_kv_usage": str(kv),
        "vllm_waiting": str(waiting),
        "active_requests": str(active),
    }


def _valid_rows() -> list[dict[str, str]]:
    rows = []
    for endpoint in ("endpoint-0", "endpoint-1"):
        rows.extend(
            [
                _row(
                    endpoint,
                    10,
                    action="increase",
                    reason="ready_backlog_below_target",
                    applied=160,
                ),
                _row(endpoint, 15, applied=160, active=145),
                _row(endpoint, 20, applied=160, active=145),
                _row(endpoint, 66, kv=0.9),
                _row(
                    endpoint,
                    70,
                    action="decrease",
                    reason="persistent_service_queue",
                    applied=128,
                    kv=0.9,
                ),
                _row(endpoint, 75, applied=128, kv=0.7),
                _row(endpoint, 80, applied=128, kv=0.7),
                _row(
                    endpoint,
                    130,
                    action="increase",
                    reason="ready_backlog_rate_bootstrap",
                    applied=160,
                ),
                _row(endpoint, 135, applied=160, active=145),
                _row(endpoint, 140, applied=160, active=145),
                _row(endpoint, 186, kv=0.9),
                _row(
                    endpoint,
                    190,
                    action="decrease",
                    reason="persistent_service_queue",
                    applied=128,
                    kv=0.9,
                ),
                _row(endpoint, 195, applied=128, kv=0.7),
                _row(endpoint, 200, applied=128, kv=0.7),
            ]
        )
    return rows


class TestAuditPhaseChange(unittest.TestCase):
    def test_off_phase_safety_allows_isolated_boundary_drain(self) -> None:
        self.assertTrue(
            audit._phase_is_safe(
                {
                    "waiting_max": 7.0,
                    "waiting_p95": 0.0,
                    "kv_max": 0.91,
                    "kv_p95": 0.72,
                }
            )
        )
        self.assertFalse(
            audit._phase_is_safe(
                {
                    "waiting_max": 7.0,
                    "waiting_p95": 1.0,
                    "kv_max": 0.91,
                    "kv_p95": 0.86,
                }
            )
        )

    def test_admission_lag_uses_replayed_arrival_to_submit_boundary(self) -> None:
        rows = [
            {
                "endpoint_id": "endpoint-0",
                "request_time_origin": "replayed_arrival",
                "arrival_epoch_s": str(float(index)),
                "submit_epoch_s": str(float(index) + lag),
            }
            for index, lag in enumerate((0.1, 0.2, 1.5, 2.0))
        ]

        result = audit._admission_lag_summary(rows, "endpoint-0")

        self.assertEqual(result["admission_lag_p50_s"], 0.85)
        self.assertEqual(result["admission_lag_p95_s"], 2.0)
        self.assertEqual(result["admission_lag_max_s"], 2.0)

    def test_admission_lag_rejects_non_replayed_clock_origin(self) -> None:
        rows = [
            {
                "endpoint_id": "endpoint-0",
                "request_time_origin": "offline_job_start",
                "arrival_epoch_s": "1.0",
                "submit_epoch_s": "2.0",
            }
        ]
        with self.assertRaisesRegex(ValueError, "replayed arrivals"):
            audit._admission_lag_summary(rows, "endpoint-0")

    def test_accepts_ordered_bidirectional_actions_with_relief(self) -> None:
        result = audit._audit_actions(
            _valid_rows(),
            {"start_epoch_s": 1000.0},
            SEGMENTS,
            128,
            160,
        )
        self.assertEqual(
            [item["action"] for item in result["endpoint-0"]],
            ["increase", "decrease", "increase", "decrease"],
        )
        self.assertEqual(
            result["endpoint-0"][0]["post_increase_active_p50"],
            145,
        )

    def test_rejects_upshift_that_remains_clamped_at_lower_arm(self) -> None:
        rows = _valid_rows()
        for row in rows:
            if float(row["observed_epoch_s"]) in {1015.0, 1020.0}:
                row["active_requests"] = "128"
        with self.assertRaisesRegex(ValueError, "above the lower arm"):
            audit._audit_actions(
                rows,
                {"start_epoch_s": 1000.0},
                SEGMENTS,
                128,
                160,
            )

    def test_rejects_missing_second_downshift(self) -> None:
        rows = [
            row
            for row in _valid_rows()
            if not (
                row["control_action"] == "decrease"
                and float(row["observed_epoch_s"]) == 1190.0
            )
        ]
        with self.assertRaisesRegex(ValueError, "lacks decrease in phase 3"):
            audit._audit_actions(
                rows,
                {"start_epoch_s": 1000.0},
                SEGMENTS,
                128,
                160,
            )


if __name__ == "__main__":
    unittest.main()
