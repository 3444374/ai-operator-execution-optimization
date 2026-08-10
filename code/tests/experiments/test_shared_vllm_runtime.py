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
    build_observe_only_text_state_rows,
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
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["runtime_state_mode"], "observe_only")
        self.assertEqual(rows[0]["organizer_queued_work"], 8192)
        self.assertEqual(rows[0]["model_queued_work_estimated"], 1024)
        self.assertEqual(rows[0]["model_capacity_work"], 131072)


if __name__ == "__main__":
    unittest.main()
