from __future__ import annotations

import sys
import unittest
from pathlib import Path


CODE_ROOT = next(
    parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

from src.infrastructure.ray_runtime_preflight import validate_ray_worker_nofile


class RayRuntimePreflightTests(unittest.TestCase):
    def test_deduplicates_addresses_and_records_worker_limits(self) -> None:
        calls: list[str] = []

        def probe(address: str) -> tuple[int, int]:
            calls.append(address)
            return 65_536, 1_048_576

        result = validate_ray_worker_nofile(
            ("127.0.0.1:6380", "127.0.0.1:6380"),
            probe=probe,
        )

        self.assertEqual(calls, ["127.0.0.1:6380"])
        self.assertEqual(result["127.0.0.1:6380"]["soft"], 65_536)

    def test_rejects_the_1024_limit_that_exhausts_daft_ray_workers(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "soft=1024"):
            validate_ray_worker_nofile(
                ("127.0.0.1:6380",),
                probe=lambda _address: (1_024, 1_048_576),
            )


if __name__ == "__main__":
    unittest.main()
