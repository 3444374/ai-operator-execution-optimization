from __future__ import annotations

import math
import sys
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path

CODE_ROOT = Path(__file__).resolve().parents[1]
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.scheduling import RayWorkerOptions  # noqa: E402


class RayWorkerOptionsTests(unittest.TestCase):
    def test_http_worker_options_never_reserve_gpu_or_retry(self) -> None:
        options = RayWorkerOptions(num_cpus=0.25, actor_max_concurrency=4)

        self.assertEqual(
            options.task_options(),
            {
                "num_cpus": 0.25,
                "num_gpus": 0,
                "max_retries": 0,
            },
        )
        self.assertEqual(
            options.actor_options(),
            {
                "num_cpus": 0.25,
                "num_gpus": 0,
                "max_concurrency": 4,
                "max_restarts": 0,
                "max_task_retries": 0,
            },
        )

    def test_worker_options_require_positive_finite_cpu(self) -> None:
        for num_cpus in (0.0, -0.1, math.inf, -math.inf, math.nan):
            with self.subTest(num_cpus=num_cpus):
                with self.assertRaisesRegex(
                    ValueError,
                    "num_cpus must be positive and finite",
                ):
                    RayWorkerOptions(
                        num_cpus=num_cpus,
                        actor_max_concurrency=1,
                    )

    def test_actor_options_require_positive_concurrency(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "actor_max_concurrency must be positive",
        ):
            RayWorkerOptions(num_cpus=0.25, actor_max_concurrency=0)

    def test_worker_options_are_immutable(self) -> None:
        options = RayWorkerOptions(num_cpus=0.25)

        with self.assertRaises(FrozenInstanceError):
            options.num_cpus = 1.0


if __name__ == "__main__":
    unittest.main()
