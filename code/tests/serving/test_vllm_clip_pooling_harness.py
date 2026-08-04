from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
SCRIPT = CODE_ROOT / "scripts" / "profiling" / "run_vllm_clip_pooling_gate.py"
SPEC = importlib.util.spec_from_file_location("run_vllm_clip_pooling_gate", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class VllmClipPoolingHarnessTests(unittest.TestCase):
    def test_run_process_records_normal_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            exit_code, timed_out = MODULE.run_process(
                [sys.executable, "-c", "print('ok')"],
                timeout_seconds=5,
                stdout_path=root / "stdout.log",
                stderr_path=root / "stderr.log",
            )

            self.assertEqual(exit_code, 0)
            self.assertFalse(timed_out)
            self.assertEqual((root / "stdout.log").read_text().strip(), "ok")

    def test_run_process_returns_124_on_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            exit_code, timed_out = MODULE.run_process(
                [sys.executable, "-c", "import time; time.sleep(5)"],
                timeout_seconds=1,
                stdout_path=root / "stdout.log",
                stderr_path=root / "stderr.log",
            )

            self.assertEqual(exit_code, 124)
            self.assertTrue(timed_out)


if __name__ == "__main__":
    unittest.main()
