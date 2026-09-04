"""Runner state-machine tests: exit codes, no hardcoded verdicts, phases."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.experiments.postgresql import semmap_resource_runner as runner


class Args:
    def __init__(self, root: Path):
        self.root = root
        self.repo = root
        self.prefix = root
        self.client = root / "client"
        self.commit = "x" * 40


class ExitCodeTests(unittest.TestCase):
    def test_all_pass_exits_zero(self):
        summary = {"cases": {
            "stress": {"measurement_status": "valid", "qualification_status": "passed"},
            "cancel": {"measurement_status": "valid", "qualification_status": "passed"}}}
        self.assertEqual(runner._exit_code(summary), 0)

    def test_valid_failed_exits_one(self):
        summary = {"cases": {
            "stress": {"measurement_status": "valid", "qualification_status": "failed"}}}
        self.assertEqual(runner._exit_code(summary), 1)

    def test_inconclusive_exits_two(self):
        summary = {"cases": {
            "stress": {"measurement_status": "inconclusive",
                       "qualification_status": "not_evaluated"}}}
        self.assertEqual(runner._exit_code(summary), 2)

    def test_invalid_exits_two(self):
        summary = {"cases": {
            "stress": {"measurement_status": "invalid",
                       "qualification_status": "not_evaluated"}}}
        self.assertEqual(runner._exit_code(summary), 2)

    def test_not_run_exits_two(self):
        summary = {"cases": {
            "stress": {"measurement_status": "valid", "qualification_status": "passed"},
            "cancel": {"measurement_status": "not_run",
                       "qualification_status": "not_evaluated"}}}
        self.assertEqual(runner._exit_code(summary), 2)


class HardcodedVerdictTests(unittest.TestCase):
    def test_runner_source_has_no_unsupported_hardcoded_verdicts(self):
        source = Path(runner.__file__).read_text(encoding="utf-8")
        forbidden = (
            '"measurement_status": "valid",\n'
            '                      "qualification_status": "passed",\n'
            '                  })',
        )
        for needle in forbidden:
            self.assertNotIn(needle, source)

    def test_fault_case_functions_evaluate_policies(self):
        source = Path(runner.__file__).read_text(encoding="utf-8")
        for function in ("run_cancel_case", "run_disconnect_case", "run_exit_case"):
            self.assertIn(f"def {function}", source)
            self.assertIn("_evaluate_case", source)


class CaseIndependenceTests(unittest.TestCase):
    def test_cleaned_peak_failure_does_not_block_fault_cases(self):
        stress = {"cleanup_policy": [
            {"metric": "provider_uds_session_fd_peak_delta_combined",
             "observed": 3, "limit": 2}]}
        self.assertFalse(runner._is_unsafe(stress))

    def test_unrecovered_total_fd_blocks_later_cases(self):
        for metric in ("total_fd_end_delta", "thread_end_delta",
                       "provider_uds_session_fd_end_delta_combined"):
            self.assertTrue(runner._is_unsafe(
                {"cleanup_policy": [{"metric": metric, "observed": 1, "limit": 0}]}),
                metric)


class SqlstateContractTests(unittest.TestCase):
    def test_registered_contracts_are_single_values(self):
        # The accepted SQLSTATE per case must stay the registered single
        # value; widening the set here would hide production mapping bugs.
        self.assertEqual(runner.CANCEL_SQLSTATE, "57014")
        self.assertEqual(runner.DISCONNECT_SQLSTATE, "08006")
        self.assertEqual(runner.GATEWAY_EXIT_SQLSTATE, "08006")

    def test_exit_case_records_contract_mismatch_as_failure(self):
        source = Path(runner.__file__).read_text(encoding="utf-8")
        self.assertIn("sqlstate_contract", source)
        self.assertIn('report["qualification_status"] = "failed"', source)


class SummaryAlwaysWrittenTests(unittest.TestCase):
    def test_run_writes_summary_even_on_preflight_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            args = Args(Path(directory))
            with mock.patch.object(
                    runner.subprocess, "check_output",
                    side_effect=FileNotFoundError("no pg_config")):
                code = runner.run(args)
            self.assertEqual(code, runner.EXIT_RUNNER_FAILURE)
            summary = json.loads(
                (args.root / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "runner_failure")
            self.assertEqual(summary["reason"], "preflight_error:FileNotFoundError")


class OneArtifactTests(unittest.TestCase):
    def test_no_measurements_attempt_files_are_written(self):
        source = Path(runner.__file__).read_text(encoding="utf-8")
        self.assertNotIn("measurements-attempt", source)
        self.assertIn("cleanup", source)


if __name__ == "__main__":
    unittest.main()
