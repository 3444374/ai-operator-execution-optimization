"""The new CLI redacts propagated failures before a supervisor writes stderr."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


CODE = Path(__file__).resolve().parents[2]
CLI = CODE / "scripts/experiments/run_sembench_mmqa.py"
MARKER = "synthetic-mmqa-sensitive-value"


def command(setup):
    script = """import runpy,sys
from unittest.mock import patch
""" + setup + """
with patch('src.experiments.sembench_mmqa.main', side_effect=failure):
    runpy.run_path(sys.argv[1], run_name='__main__')
"""
    return [sys.executable, "-B", "-c", script, str(CLI)]


class MmqaCLIErrorTests(unittest.TestCase):
    def run_failure(self, setup):
        env = dict(os.environ, PYTHONPATH=str(CODE), CUDA_VISIBLE_DEVICES="",
                   LITELLM_LOCAL_MODEL_COST_MAP="True", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1")
        return subprocess.run(command(setup), env=env, capture_output=True, text=True, timeout=20)

    def test_exception_chain_and_notes_are_redacted_without_losing_failure_identity(self):
        result = self.run_failure(
            "marker=" + repr(MARKER) + "\n"
            "cause=ValueError('password='+marker)\n"
            "failure=RuntimeError('postgresql://fixture:'+marker+'@localhost/example')\n"
            "failure.__cause__=cause\n"
            "failure.add_note('auth_token='+marker)\n")
        self.assertEqual(result.returncode, 1)
        self.assertNotIn(MARKER, result.stderr)
        for expected in ("Traceback", "ValueError", "RuntimeError", "password=***",
                         "fixture:***@localhost", "auth_token=***"):
            self.assertIn(expected, result.stderr)

    def test_explicit_exit_codes_are_preserved_and_text_exit_is_redacted(self):
        for code, expected in ((None, 0), (37, 37), ("password=" + MARKER, 1)):
            with self.subTest(code=code):
                result = self.run_failure("failure=SystemExit(" + repr(code) + ")\n")
                self.assertEqual(result.returncode, expected)
                self.assertNotIn(MARKER, result.stderr)
                if isinstance(code, str):
                    self.assertIn("password=***", result.stderr)

    def test_keyboard_interrupt_retains_interrupt_exit_status(self):
        result = self.run_failure("failure=KeyboardInterrupt('password='+" + repr(MARKER) + ")\n")
        self.assertEqual(result.returncode, 130)
        self.assertIn("KeyboardInterrupt", result.stderr)
        self.assertNotIn(MARKER, result.stderr)

    def test_supervisor_persists_only_the_redacted_worker_traceback(self):
        from src.experiments.postgresql.query_supervisor import supervise
        setup = "failure=ValueError('password='+" + repr(MARKER) + ")\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "worker failed"):
                supervise(command(setup), root, lambda: None,
                          query_timeout_s=20, preparation_s=20, grace_s=.1, max_duration_s=20)
            recorded = (root / "worker.log").read_text()
            self.assertNotIn(MARKER, recorded)
            self.assertIn("ValueError", recorded)
            self.assertIn("password=***", recorded)
            report = json.loads((root / "supervisor.json").read_text())
            self.assertEqual((report["status"], report["worker_exit"]), ("failed", 1))
            self.assertEqual(report["remaining_owned_pids"], [])


if __name__ == "__main__":
    unittest.main()
