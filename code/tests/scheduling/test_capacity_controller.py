from __future__ import annotations

import sys
import unittest
from dataclasses import replace
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


def _snapshot(*, queued: int, service_waiting: int | None = 0, signature: str = "sig"):
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

    def test_bootstraps_one_calibrated_arm_before_rate_is_available(self) -> None:
        controller = BoundedCapacityController(
            (CapacityArm(96, 131072), CapacityArm(128, 131072)),
            fallback=CapacityArm(128, 131072),
            initial=CapacityArm(96, 131072),
            target_service_rate_tokens_s=7600.0,
            consecutive_samples=2,
            cooldown_samples=0,
        )

        decisions = [
            controller.select(
                _snapshot(queued=1024),
                active_requests=96,
                service_waiting_requests=0,
                service_rate_tokens_s=None,
                kv_usage=0.2,
                now_s=10.1,
                max_age_s=1.0,
                calibration_signature="sig",
            )
            for _ in range(2)
        ]

        self.assertEqual(decisions[-1].action, "increase")
        self.assertEqual(decisions[-1].reason, "ready_backlog_rate_bootstrap")
        self.assertEqual(decisions[-1].arm.request_limit, 128)

    def test_missing_rate_never_bootstraps_without_ready_backlog(self) -> None:
        decisions = [
            self.controller.select(
                _snapshot(queued=0),
                active_requests=128,
                service_waiting_requests=0,
                service_rate_tokens_s=None,
                kv_usage=0.2,
                now_s=10.1,
                max_age_s=1.0,
                calibration_signature="sig",
            )
            for _ in range(3)
        ]

        self.assertTrue(all(item.action == "hold" for item in decisions))
        self.assertEqual(self.controller.current_arm.request_limit, 128)

    def test_upshift_can_use_shorter_hysteresis_than_downshift(self) -> None:
        controller = BoundedCapacityController(
            (CapacityArm(96, 131072), CapacityArm(128, 131072)),
            fallback=CapacityArm(96, 131072),
            initial=CapacityArm(96, 131072),
            target_service_rate_tokens_s=7600.0,
            consecutive_samples=8,
            increase_consecutive_samples=2,
            cooldown_samples=0,
        )

        decisions = [
            controller.select(
                _snapshot(queued=1024),
                active_requests=96,
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

    def test_unknown_waiting_restores_calibrated_supply_from_either_side(self) -> None:
        for initial in (96, 160):
            with self.subTest(initial=initial):
                controller = BoundedCapacityController(
                    self.controller.candidates,
                    fallback=self.controller.fallback,
                    initial=CapacityArm(initial, 131072),
                    target_service_rate_tokens_s=7600,
                    cooldown_samples=0,
                )
                decision = controller.select(
                    _snapshot(queued=1024, service_waiting=None), active_requests=initial,
                    service_waiting_requests=None, service_rate_tokens_s=1000,
                    kv_usage=.2, now_s=10.1, max_age_s=1, calibration_signature="sig",
                )
                self.assertEqual(
                    (decision.action, decision.reason), ("fallback", "missing_service_waiting"),
                )
                self.assertEqual(decision.arm, CapacityArm(128, 131072))

    def test_unknown_waiting_resets_hysteresis_and_clear_queue_can_resume(self) -> None:
        controller = self.controller
        def select(waiting):
            return controller.select(
                _snapshot(queued=1024, service_waiting=waiting), active_requests=128,
                service_waiting_requests=waiting, service_rate_tokens_s=1000,
                kv_usage=.2, now_s=10.1, max_age_s=1, calibration_signature="sig",
            )
        self.assertEqual(select(0).action, "hold")
        self.assertEqual(select(None).reason, "missing_service_waiting")
        self.assertEqual(select(0).action, "hold")
        self.assertEqual(select(0).action, "increase")

    def test_valid_pressure_still_decreases_with_missing_waiting_or_rate(self) -> None:
        for waiting, kv in ((None, .9), (0, .9), (1, .2)):
            for rate in (1000, None):
                with self.subTest(waiting=waiting, kv=kv, rate=rate):
                    controller = BoundedCapacityController(
                        self.controller.candidates, fallback=self.controller.fallback,
                        target_service_rate_tokens_s=7600,
                        consecutive_samples=2, increase_consecutive_samples=1,
                        cooldown_samples=0,
                    )
                    decisions = [controller.select(
                        _snapshot(queued=1024, service_waiting=waiting), active_requests=128,
                        service_waiting_requests=waiting, service_rate_tokens_s=rate,
                        kv_usage=kv, now_s=10.1, max_age_s=1, calibration_signature="sig",
                    ) for _ in range(2)]
                    self.assertTrue(all(item.action != "increase" for item in decisions))
                    self.assertEqual(
                        (decisions[-1].action, decisions[-1].arm.request_limit), ("decrease", 96),
                    )

    def test_unknown_waiting_keeps_stale_signature_and_kv_guards(self) -> None:
        for signature, now, kv, reason in (
            ("other", 10.1, .2, "stale_or_signature_mismatch"),
            ("sig", 12, .2, "stale_or_signature_mismatch"),
            ("sig", 10.1, None, "missing_or_invalid_kv_usage"),
        ):
            with self.subTest(signature=signature, now=now, kv=kv):
                decision = self.controller.select(
                    _snapshot(queued=1024, service_waiting=None, signature=signature),
                    active_requests=128, service_waiting_requests=None,
                    service_rate_tokens_s=1000, kv_usage=kv, now_s=now,
                    max_age_s=1, calibration_signature="sig",
                )
                self.assertEqual((decision.action, decision.reason), ("fallback", reason))

    def test_unknown_upstream_queue_cannot_become_ready_demand(self) -> None:
        snapshot = _snapshot(queued=1024)
        snapshot = replace(
            snapshot,
            stages=(replace(snapshot.stages[0], queued_work=None), snapshot.stages[1]),
        )
        decision = self.controller.select(
            snapshot, active_requests=128, service_waiting_requests=0,
            service_rate_tokens_s=1000, kv_usage=.2, now_s=10.1,
            max_age_s=1, calibration_signature="sig",
        )
        self.assertEqual((decision.action, decision.reason), ("fallback", "missing_upstream_queue"))


if __name__ == "__main__":
    unittest.main()
