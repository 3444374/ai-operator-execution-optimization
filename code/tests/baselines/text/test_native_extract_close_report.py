"""Keep native phase evidence when closing an owned phase raises."""

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace
import unittest

from src.baselines.text.frameworks.lotus_chain_native_relation import NativeExtractRelationRows


class NativeExtractCloseReportTests(unittest.TestCase):
    def stream(self, phases):
        temp = TemporaryDirectory(prefix="native-close-report-")
        self.addCleanup(temp.cleanup)
        control = Path(temp.name) / "control"
        control.mkdir()
        chain = SimpleNamespace(stages=("extract", "map"),
                                relation_segments=lambda: ("prefix", "suffix"))
        stream = NativeExtractRelationRows(chain, ("source",), None, control,
            time.monotonic() + 10, None, scheduler_owner="declared-native-fixture")
        stream._phases = phases
        return stream

    def phase(self, *, api=True, raises=True):
        model = dict(engine_ready=dict(engine_uid="declared-fixture-engine"),
                     engine_api=dict(engine_api_shutdown_confirmed=api),
                     gpu_exit_confirmed=None)
        report = dict(status="closed" if api else "unknown", cleanup_errors=[],
                      author_physical_state=dict(resources_confirmed=True),
                      engine_reports=[model], model=model)
        phase = SimpleNamespace(last_close_report=report, received={"source"},
            calls=[dict(call_id="known-call", total_tokens=7)], batch_stage_stats=[],
            raw_result_batches={}, unassociated_results=[])
        def close():
            if raises:
                raise RuntimeError("declared native phase close failure")
            return report
        phase.close = close
        return phase

    def test_close_exception_preserves_known_model_and_physical_evidence_without_mutating_phase(self):
        phase = self.phase()
        original = deepcopy(phase.last_close_report)
        stream = self.stream([phase])
        for _ in range(2):
            report = stream.close()
            self.assertEqual(report["status"], "unknown")
            self.assertFalse(report["query_completed"])
            self.assertEqual(report["registered_engine_report_count"], 1)
            self.assertEqual(report["engine_reports"], original["engine_reports"])
            saved = report["phase_reports"][0]
            self.assertEqual(saved["status"], "unknown")
            self.assertEqual(saved["close_error_type"], "RuntimeError")
            self.assertTrue(saved["author_physical_state"]["resources_confirmed"])
            self.assertIsNone(report["gpu_exit_confirmed"])
            self.assertTrue(stream.control.is_dir())
            self.assertEqual(stream.calls, phase.calls)
            self.assertEqual(phase.last_close_report, original)

    def test_unknown_model_api_literal_survives_failed_phase_close(self):
        phase = self.phase(api=False)
        report = self.stream([phase]).close()
        self.assertEqual(report["status"], "unknown")
        self.assertIs(report["engine_reports"][0]["engine_api"]["engine_api_shutdown_confirmed"], False)
        self.assertTrue(report["phase_reports"][0]["author_physical_state"]["resources_confirmed"])

    def test_unreadable_failed_phase_report_does_not_skip_other_phase_cleanup(self):
        calls = []
        class UnreadablePhase:
            received = set()
            batch_stage_stats = []
            raw_result_batches = {}
            unassociated_results = []
            calls = []
            def close(self):
                calls.append("first")
                raise RuntimeError("first phase failed")
            @property
            def last_close_report(self):
                raise OSError("phase evidence unavailable")
        second = self.phase(raises=False)
        close = second.close
        def close_second():
            calls.append("second")
            return close()
        second.close = close_second
        report = self.stream([UnreadablePhase(), second]).close()
        self.assertEqual(calls, ["first", "second"])
        self.assertEqual(report["status"], "unknown")
        self.assertEqual(report["registered_engine_report_count"], 1)
        self.assertTrue(any("phase_0_close_report: OSError" == error for error in report["cleanup_errors"]))

    def test_malformed_phase_metadata_does_not_skip_remaining_cleanup(self):
        first = self.phase(raises=False)
        first.last_close_report["engine_reports"] = None
        second = self.phase(raises=False)
        report = self.stream([first, second]).close()
        self.assertEqual(report["status"], "unknown")
        self.assertEqual(report["registered_engine_report_count"], 1)
        self.assertEqual(len(report["phase_reports"]), 2)
        self.assertIn("phase_0_close_report: TypeError", report["cleanup_errors"])


if __name__ == "__main__":
    unittest.main()
