"""CLI contract tests for the SemMap resource runner entry point.

Pins the registered exit-code contract (0 = all required cases
valid+passed; 1 = valid measurement, qualification failed; 2 = invalid
or inconclusive; 3 = runner/preflight/internal failure) and the
summary-always-written guarantee across those paths.
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.experiments.postgresql import semmap_resource_runner as runner


def _case(measurement, qualification, **extra):
    result = {"measurement_status": measurement,
              "qualification_status": qualification}
    result.update(extra)
    return result


class ExitCodeContractTests(unittest.TestCase):
    def test_all_pass_exits_zero(self):
        summary = {"cases": {
            "stress": _case("valid", "passed"),
            "cancel": _case("valid", "passed"),
            "disconnect": _case("valid", "passed"),
            "exit": _case("valid", "passed")}}
        self.assertEqual(runner._exit_code(summary), runner.EXIT_ALL_PASS)
        self.assertEqual(runner.EXIT_ALL_PASS, 0)

    def test_valid_failed_exits_one(self):
        summary = {"cases": {
            "stress": _case("valid", "failed"),
            "cancel": _case("valid", "passed")}}
        self.assertEqual(runner._exit_code(summary), runner.EXIT_VALID_FAILED)
        self.assertEqual(runner.EXIT_VALID_FAILED, 1)

    def test_inconclusive_exits_two(self):
        summary = {"cases": {
            "stress": _case("inconclusive", "not_evaluated")}}
        self.assertEqual(runner._exit_code(summary), runner.EXIT_NOT_EVALUATED)
        self.assertEqual(runner.EXIT_NOT_EVALUATED, 2)

    def test_invalid_exits_two(self):
        summary = {"cases": {
            "stress": _case("invalid", "not_evaluated")}}
        self.assertEqual(runner._exit_code(summary), runner.EXIT_NOT_EVALUATED)

    def test_invalid_outranks_failed_across_cases(self):
        # Precedence: an invalid anywhere forces exit 2 even if another
        # case recorded a valid failed verdict.
        summary = {"cases": {
            "stress": _case("valid", "failed"),
            "cancel": _case("invalid", "not_evaluated")}}
        self.assertEqual(runner._exit_code(summary), runner.EXIT_NOT_EVALUATED)

    def test_run_failure_constant_is_three(self):
        self.assertEqual(runner.EXIT_RUNNER_FAILURE, 3)


class SummaryAlwaysWrittenTests(unittest.TestCase):
    def test_preflight_failure_writes_summary_and_exits_three(self):
        with tempfile.TemporaryDirectory() as directory:
            args = type("Args", (), {
                "root": Path(directory), "repo": Path(directory),
                "prefix": Path(directory), "client": None,
                "commit": "x" * 40})()
            with mock.patch.object(
                    runner.subprocess, "check_output",
                    side_effect=FileNotFoundError("no pg_config")):
                code = runner.run(args)
            self.assertEqual(code, runner.EXIT_RUNNER_FAILURE)
            summary = json.loads(
                (args.root / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "runner_failure")
            self.assertIn("reason", summary)

    def test_case_crash_writes_summary_and_exits_three(self):
        # A post-preflight runtime exception must not collide with the
        # valid-failed exit code (the pre-fix behaviour: Python's
        # default exit 1 with a 'failed' status).
        with tempfile.TemporaryDirectory() as directory:
            args = type("Args", (), {
                "root": Path(directory), "repo": Path(directory),
                "prefix": Path(directory), "client": None,
                "commit": "x" * 40})()

            def fake_check_output(argv, text=True):
                argv = list(argv)
                if "--version" in argv:
                    return "PostgreSQL 18.3\n"
                if "rev-parse" in argv:
                    return "x" * 40 + "\n"      # match args.commit
                if "status" in argv:
                    return ""                    # clean tree
                raise AssertionError(f"unexpected argv {argv}")

            with mock.patch.object(
                    runner.subprocess, "check_output",
                    side_effect=fake_check_output), \
                 mock.patch.object(runner, "build_client"), \
                 mock.patch.object(
                    runner, "pwd",
                    type("PwdModule", (), {"getpwnam": staticmethod(
                        lambda name: type("User", (), {"pw_uid": 999})())})), \
                 mock.patch.object(runner, "_chown", side_effect=None), \
                 mock.patch.object(
                    runner, "fixture",
                    side_effect=RuntimeError("sentinel crash")):
                code = runner.run(args)
            self.assertEqual(code, runner.EXIT_RUNNER_FAILURE)
            summary = json.loads(
                (args.root / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "runner_failure")
            self.assertIn("RuntimeError", summary["reason"])


class EntryModuleTests(unittest.TestCase):
    def test_entry_raises_system_exit_with_main(self):
        source = Path(runner.__file__).read_text(encoding="utf-8")
        self.assertIn('raise SystemExit(main())', source)

    def test_diagnostic_flag_forces_not_evaluated(self):
        source = Path(runner.__file__).read_text(encoding="utf-8")
        self.assertIn('"--diagnostic"', source)
        self.assertIn('case["qualification_status"] = "not_evaluated"',
                      source)

    def test_diagnostic_mode_reduces_workload(self):
        source = Path(runner.__file__).read_text(encoding="utf-8")
        self.assertIn("ROWS_PER_ROUND = 100", source)
        self.assertIn("ROUNDS = 1", source)


if __name__ == "__main__":
    unittest.main()
