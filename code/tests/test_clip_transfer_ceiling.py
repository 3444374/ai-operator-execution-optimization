from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "profile_clip_transfer_ceiling.py"
SPEC = importlib.util.spec_from_file_location("profile_clip_transfer_ceiling", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ClipTransferCeilingTests(unittest.TestCase):
    def test_csv_keeps_every_repeat_and_mode(self) -> None:
        rows = [
            {
                "mode": mode,
                "batch_size": 16,
                "repeat_index": 1,
                "wall_s": 0.1,
                "host_ownership_copy_s": 0.0,
                "h2d_cuda_s": 0.01,
                "forward_cuda_s": 0.09,
                "images_per_s": 160.0,
                "host_input_bytes": 1,
                "device_input_bytes": 1,
                "logical_h2d_gbps": 0.1,
                "output_sum": 1.0,
                "output_norm_error": 0.0,
            }
            for mode in MODULE.MODES
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raw.csv"
            MODULE._write_csv(path, rows)
            lines = path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), 4)
        self.assertIn("host_ownership_copy_s", lines[0])


if __name__ == "__main__":
    unittest.main()
