from __future__ import annotations

import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.planning.work import (  # noqa: E402
    RuntimeStateSnapshot,
    StageStateSnapshot,
)
from src.scheduling.submission_control.stage_work import (  # noqa: E402
    BoundedStageWorkController,
)


def _state(
    *,
    model_active: int,
    model_queued: int,
    model_age_s: float,
    upstream_queued: int,
    signature: str = "sig",
) -> RuntimeStateSnapshot:
    return RuntimeStateSnapshot(
        stages=(
            StageStateSnapshot(
                stage="organizer",
                active_work=0,
                queued_work=upstream_queued,
                service_rate_units_s=None,
                oldest_queue_age_s=0.0,
                observed_at_s=10.0,
            ),
            StageStateSnapshot(
                stage="model",
                active_work=model_active,
                queued_work=model_queued,
                service_rate_units_s=100.0,
                oldest_queue_age_s=model_age_s,
                observed_at_s=10.0,
                capacity_work=100,
            ),
        ),
        observed_at_s=10.0,
        calibration_signature=signature,
    )


class BoundedStageWorkControllerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.controller = BoundedStageWorkController(
            (50, 75, 100),
            fallback_work=75,
            controlled_stage="model",
            upstream_stage="organizer",
            congestion_queue_fraction=0.1,
            congestion_age_s=1.0,
        )

    def test_increases_one_step_when_work_waits_upstream(self) -> None:
        decision = self.controller.select(
            _state(
                model_active=50,
                model_queued=0,
                model_age_s=0.0,
                upstream_queued=20,
            ),
            now_s=10.1,
            max_age_s=1.0,
            calibration_signature="sig",
        )

        self.assertEqual((decision.work_limit, decision.action), (100, "increase"))

    def test_decreases_one_step_on_aged_service_queue(self) -> None:
        decision = self.controller.select(
            _state(
                model_active=100,
                model_queued=20,
                model_age_s=2.0,
                upstream_queued=20,
            ),
            now_s=10.1,
            max_age_s=1.0,
            calibration_signature="sig",
        )

        self.assertEqual((decision.work_limit, decision.action), (50, "decrease"))

    def test_stale_or_wrong_signature_falls_back_to_static(self) -> None:
        first = self.controller.select(
            _state(
                model_active=50,
                model_queued=0,
                model_age_s=0.0,
                upstream_queued=20,
            ),
            now_s=10.1,
            max_age_s=1.0,
            calibration_signature="sig",
        )
        self.assertEqual(first.work_limit, 100)

        fallback = self.controller.select(
            _state(
                model_active=50,
                model_queued=0,
                model_age_s=0.0,
                upstream_queued=20,
                signature="other",
            ),
            now_s=10.1,
            max_age_s=1.0,
            calibration_signature="sig",
        )
        self.assertEqual((fallback.work_limit, fallback.action), (75, "fallback"))


if __name__ == "__main__":
    unittest.main()
