from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.experiments.shared_vllm.runtime import (  # noqa: E402
    EndpointServiceRateTracker,
    build_observe_only_text_state_rows,
)
from src.experiments.shared_vllm.runner import _apply_state_control  # noqa: E402
from src.scheduling.submission_control.capacity import (  # noqa: E402
    BoundedCapacityController,
    CapacityArm,
)


class SharedVllmRuntimeStateTests(unittest.TestCase):
    def test_builds_observe_only_staged_state(self) -> None:
        rows = build_observe_only_text_state_rows(
            [
                {
                    "endpoint_id": "endpoint-0",
                    "observed_epoch_s": 10.0,
                    "elapsed_s": 2.0,
                    "request_limit": 128,
                    "work_limit": 131072,
                    "active_requests": 64,
                    "active_work": 32768,
                    "waiting_work": 8192,
                    "oldest_waiting_age_s": 0.5,
                }
            ],
            [
                {
                    "endpoint_index": 0,
                    "running": 60.0,
                    "waiting": 2.0,
                    "kv_usage": 0.25,
                }
            ],
            endpoint_ids=("endpoint-0",),
            calibration_signature="sig",
            service_rates={"endpoint-0": 7000.0},
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["runtime_state_mode"], "observe_only")
        self.assertEqual(rows[0]["organizer_queued_work"], 8192)
        self.assertEqual(rows[0]["model_queued_work_estimated"], 1024)
        self.assertEqual(rows[0]["model_capacity_work"], 131072)
        self.assertEqual(rows[0]["service_rate_tokens_s"], 7000.0)

    def test_service_rate_tracker_uses_cumulative_token_delta(self) -> None:
        tracker = EndpointServiceRateTracker(alpha=1.0)
        first = tracker.update(
            [
                {
                    "endpoint_index": 0,
                    "observed_epoch_s": 10.0,
                    "completed_tokens_total": 100.0,
                }
            ],
            endpoint_ids=("endpoint-0",),
        )
        second = tracker.update(
            [
                {
                    "endpoint_index": 0,
                    "observed_epoch_s": 12.0,
                    "completed_tokens_total": 300.0,
                }
            ],
            endpoint_ids=("endpoint-0",),
        )

        self.assertEqual(first, {})
        self.assertEqual(second, {"endpoint-0": 100.0})

    def test_zero_completion_rate_remains_unavailable_in_typed_state(self) -> None:
        rows = build_observe_only_text_state_rows(
            [
                {
                    "endpoint_id": "endpoint-0",
                    "observed_epoch_s": 10.0,
                    "elapsed_s": 2.0,
                    "request_limit": 96,
                    "work_limit": 98304,
                    "active_requests": 0,
                    "active_work": 0,
                    "waiting_work": 0,
                    "oldest_waiting_age_s": 0.0,
                }
            ],
            [
                {
                    "endpoint_index": 0,
                    "running": 0.0,
                    "waiting": 0.0,
                    "kv_usage": 0.0,
                }
            ],
            endpoint_ids=("endpoint-0",),
            calibration_signature="sig",
            service_rates={"endpoint-0": 0.0},
        )

        self.assertEqual(rows[0]["service_rate_tokens_s"], "")

    def test_actuator_applies_bounded_capacity_decision(self) -> None:
        class Observer:
            def __init__(self) -> None:
                self.updates = []

            def update_capacity(self, endpoint_id, **kwargs):
                self.updates.append((endpoint_id, kwargs))
                return {"request_limit": kwargs["request_limit"]}

        controller = BoundedCapacityController(
            (CapacityArm(128, 131072), CapacityArm(160, 131072)),
            fallback=CapacityArm(128, 131072),
            target_service_rate_tokens_s=7600.0,
            consecutive_samples=1,
            cooldown_samples=0,
        )
        rows = build_observe_only_text_state_rows(
            [
                {
                    "endpoint_id": "endpoint-0",
                    "observed_epoch_s": 10.0,
                    "elapsed_s": 2.0,
                    "request_limit": 128,
                    "work_limit": 131072,
                    "active_requests": 128,
                    "active_work": 65536,
                    "waiting_work": 8192,
                    "oldest_waiting_age_s": 0.5,
                }
            ],
            [
                {
                    "endpoint_index": 0,
                    "running": 120.0,
                    "waiting": 0.0,
                    "kv_usage": 0.25,
                }
            ],
            endpoint_ids=("endpoint-0",),
            calibration_signature="sig",
            service_rates={"endpoint-0": 6000.0},
        )
        observer = Observer()

        controlled = _apply_state_control(
            rows,
            controllers={"endpoint-0": controller},
            observer=observer,
            calibration_signature="sig",
            max_state_age_s=10_000_000_000.0,
        )

        self.assertEqual(controlled[0]["control_action"], "increase")
        self.assertEqual(controlled[0]["control_applied_request_limit"], 160)
        self.assertEqual(observer.updates[0][1]["work_limit"], 131072)


if __name__ == "__main__":
    unittest.main()
