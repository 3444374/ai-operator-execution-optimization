from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.modalities.text.contracts import build_text_runtime_snapshot  # noqa: E402
from src.scheduling.submission_control.capacity import (  # noqa: E402
    BoundedCapacityController,
    CapacityArm,
)


def _snapshot(*, queued: int, service_waiting: int = 0, signature: str = "sig"):
    return build_text_runtime_snapshot(
        active_work=65536,
        upstream_queued_work=queued,
        service_waiting_requests=service_waiting,
        active_requests=128,
        oldest_upstream_age_s=1.0,
        observed_at_s=10.0,
        capacity_work=131072,
        calibration_signature=signature,
    )


class BoundedCapacityControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.controller = BoundedCapacityController(
            (
                CapacityArm(96, 131072),
                CapacityArm(128, 131072),
                CapacityArm(160, 131072),
            ),
            fallback=CapacityArm(128, 131072),
            target_service_rate_tokens_s=7600.0,
            consecutive_samples=2,
            cooldown_samples=0,
        )

    def test_increases_after_persistent_feed_limited_samples(self) -> None:
        decisions = [
            self.controller.select(
                _snapshot(queued=1024),
                active_requests=128,
                service_waiting_requests=0,
                service_rate_tokens_s=6000.0,
                kv_usage=0.2,
                now_s=10.1,
                max_age_s=1.0,
                calibration_signature="sig",
            )
            for _ in range(2)
        ]
        self.assertEqual(decisions[-1].action, "increase")
        self.assertEqual(decisions[-1].arm.request_limit, 160)

    def test_holds_without_real_upstream_demand(self) -> None:
        decisions = [
            self.controller.select(
                _snapshot(queued=0),
                active_requests=128,
                service_waiting_requests=0,
                service_rate_tokens_s=1000.0,
                kv_usage=0.2,
                now_s=10.1,
                max_age_s=1.0,
                calibration_signature="sig",
            )
            for _ in range(3)
        ]
        self.assertTrue(all(item.action == "hold" for item in decisions))
        self.assertEqual(self.controller.current_arm.request_limit, 128)

    def test_decreases_after_persistent_service_queue(self) -> None:
        decisions = [
            self.controller.select(
                _snapshot(queued=1024, service_waiting=8),
                active_requests=128,
                service_waiting_requests=8,
                service_rate_tokens_s=7600.0,
                kv_usage=0.2,
                now_s=10.1,
                max_age_s=1.0,
                calibration_signature="sig",
            )
            for _ in range(2)
        ]
        self.assertEqual(decisions[-1].action, "decrease")
        self.assertEqual(decisions[-1].arm.request_limit, 96)

    def test_kv_pressure_is_a_congestion_guard(self) -> None:
        decisions = [
            self.controller.select(
                _snapshot(queued=1024),
                active_requests=128,
                service_waiting_requests=0,
                service_rate_tokens_s=7600.0,
                kv_usage=0.9,
                now_s=10.1,
                max_age_s=1.0,
                calibration_signature="sig",
            )
            for _ in range(2)
        ]

        self.assertEqual(decisions[-1].action, "decrease")

    def test_stale_state_falls_back(self) -> None:
        decision = self.controller.select(
            _snapshot(queued=1024, signature="other"),
            active_requests=128,
            service_waiting_requests=0,
            service_rate_tokens_s=6000.0,
            kv_usage=0.2,
            now_s=10.1,
            max_age_s=1.0,
            calibration_signature="sig",
        )
        self.assertEqual((decision.action, decision.arm.request_limit), ("fallback", 128))

    def test_calibrated_initial_arm_can_ramp_to_safe_fallback(self) -> None:
        controller = BoundedCapacityController(
            (CapacityArm(96, 131072), CapacityArm(128, 131072)),
            fallback=CapacityArm(128, 131072),
            initial=CapacityArm(96, 131072),
            target_service_rate_tokens_s=7600.0,
            consecutive_samples=1,
            cooldown_samples=0,
        )

        decision = controller.select(
            _snapshot(queued=1024),
            active_requests=96,
            service_waiting_requests=0,
            service_rate_tokens_s=6000.0,
            kv_usage=0.2,
            now_s=10.1,
            max_age_s=1.0,
            calibration_signature="sig",
        )

        self.assertEqual((decision.action, decision.arm.request_limit), ("increase", 128))


if __name__ == "__main__":
    unittest.main()
