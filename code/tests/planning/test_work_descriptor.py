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
    StageWork,
    WorkDescriptor,
)


class WorkDescriptorTest(unittest.TestCase):
    def test_exposes_primary_and_stage_specific_work(self) -> None:
        descriptor = WorkDescriptor(
            stages=(
                StageWork("prepare", 4096, "encoded_bytes"),
                StageWork("model", 2 * 224 * 224, "pixels"),
                StageWork("result", 2 * 512 * 4, "bytes"),
            ),
            primary_stage="model",
            calibration_signature="host:model:processor:protocol:workload",
            locality_key="shape-224",
            deadline_s=12.5,
            lower_primary_units=90_000,
            upper_primary_units=110_000,
        )

        self.assertEqual(descriptor.primary.units, 100_352)
        self.assertEqual(descriptor.for_stage("prepare").units, 4096)
        self.assertEqual(descriptor.deadline_s, 12.5)
        self.assertIsNone(descriptor.for_stage("sink"))

    def test_rejects_duplicate_or_invalid_primary_stage(self) -> None:
        with self.assertRaisesRegex(ValueError, "unique"):
            WorkDescriptor(
                stages=(
                    StageWork("model", 1, "tokens"),
                    StageWork("model", 2, "tokens"),
                ),
                primary_stage="model",
                calibration_signature="sig",
            )
        with self.assertRaisesRegex(ValueError, "primary_stage"):
            WorkDescriptor(
                stages=(StageWork("model", 1, "tokens"),),
                primary_stage="prepare",
                calibration_signature="sig",
            )

    def test_runtime_state_freshness_is_explicit(self) -> None:
        snapshot = RuntimeStateSnapshot(
            stages=(
                StageStateSnapshot(
                    stage="model",
                    active_work=64,
                    queued_work=8,
                    service_rate_units_s=32.0,
                    oldest_queue_age_s=0.25,
                    observed_at_s=10.0,
                    capacity_work=128,
                ),
            ),
            observed_at_s=10.0,
            calibration_signature="sig",
        )

        self.assertTrue(snapshot.is_fresh(now_s=10.5, max_age_s=1.0))
        self.assertFalse(snapshot.is_fresh(now_s=11.1, max_age_s=1.0))


if __name__ == "__main__":
    unittest.main()
