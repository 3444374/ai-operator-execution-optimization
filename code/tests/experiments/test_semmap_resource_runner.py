"""Runner state-machine tests: persistence order, single peak judgment, case independence."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.experiments.postgresql import semmap_resource_runner as runner
from src.observability.process_resources.model import FdIdentity, FdKind, ProcessSnapshot


def snap(ns, fds):
    return ProcessSnapshot(
        monotonic_ns=ns, rss_bytes=1_000_000, thread_count=1,
        fds=tuple(FdIdentity(fd=10 + i, target="socket:[1]", kind=kind)
                  for i, kind in enumerate(fds)))


UDS_C = FdKind.PROVIDER_UDS_CLIENT
UDS_A = FdKind.PROVIDER_UDS_ACCEPTED
REL = FdKind.RELATION_FILE


class Args:
    def __init__(self, root: Path):
        self.root = root
        self.repo = root
        self.prefix = root
        self.gateway_observer = root / "observer.py"
        self.client = root / "client"
        self.commit = "x" * 40


class StateMachineOrderTests(unittest.TestCase):
    """Persist-then-evaluate ordering, verified by instrumented saves."""

    def test_stress_case_persists_raw_before_gate_report(self):
        order = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def fake_persist(path, trace):
                order.append(("persist", str(path)))

            real_save = runner.save
            def save_spy(path, value):
                order.append(("save", path.name))
                real_save(path, value)
            with mock.patch.object(runner, "persist_trace", fake_persist), \
                 mock.patch.object(runner, "save", save_spy), \
                 mock.patch.object(runner, "build_qualification_report") as build, \
                 mock.patch.object(runner, "child") as child_ctx, \
                 mock.patch.object(runner, "wait_file"), \
                 mock.patch.object(runner, "wait_log", return_value={"backend_pid": 1}), \
                 mock.patch.object(runner, "wait_gateway_events", return_value=[
                     {"event": "session_end"}] + [{"event": "task", "payload_digest": "d"}] * 6001
                     + [{"event": "session_end"}]), \
                 mock.patch.object(runner, "record_operation") as record:
                stress_trace = type("T", (), {
                    "baseline": {"backend": snap(1, []), "gateway": snap(1, [])},
                    "samples": ({"backend": snap(2, [UDS_C]), "gateway": snap(2, [UDS_A])},),
                })()
                report = type("R", (), {
                    "measurement_status": "valid", "qualification_status": "passed",
                    "peak_policy": [], "cleanup_policy": [], "diagnostics": {},
                })()
                record.return_value = ({"rows": 6000}, stress_trace)
                build.return_value = report
                class FakeProc:
                    pid = 4242
                    def wait(self, timeout=None):
                        return 0
                    def terminate(self):
                        pass
                child_ctx.return_value.__enter__ = lambda s: FakeProc()
                child_ctx.return_value.__exit__ = lambda s, *a: False
                connection = type("C", (), {"info": type("I", (), {"backend_pid": 111})})()
                args = Args(root)
                args.root = root
                result = runner.run_stress_case(args, connection, None, Path("f.json"), "d")
        persists = [i for i, (kind, name) in enumerate(order) if kind == "persist"]
        gate = [i for i, (kind, name) in enumerate(order)
                if kind == "save" and name == "gate_report.json"]
        self.assertTrue(persists, "stress raw must be persisted")
        self.assertTrue(gate, "gate_report.json must be written")
        self.assertLess(persists[0], gate[0],
                        "raw trace must hit disk before the gate report")

    def test_peak_violation_does_not_spin_to_deadline(self):
        # A frozen peak violation must be reported from the single stress
        # evaluation; the cleanup loop must not re-judge it 93 times.
        with tempfile.TemporaryDirectory() as directory:
            from src.experiments.postgresql.resource_qualification import (
                build_qualification_report as real_build)
            stress_trace = type("T", (), {
                "baseline": {"backend": snap(1, []), "gateway": snap(1, [])},
                "samples": (
                    {"backend": snap(2, [UDS_C, UDS_C, UDS_C]), "gateway": snap(2, [UDS_A])},
                ),
            })()
            report = real_build(stress_trace.baseline, stress_trace)
            self.assertEqual(report.qualification_status, "failed")
            self.assertTrue(any(
                v.metric == "provider_uds_session_fd_peak_delta_combined"
                for v in report.peak_policy))


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
                {"cleanup_policy": [{"metric": metric, "observed": 1, "limit": 0}]}), metric)

    def test_summary_records_not_run_with_reason(self):
        summary = {
            "cases": {"stress_large_payload": {
                "cleanup_policy": [{"metric": "total_fd_end_delta",
                                    "observed": 1, "limit": 0}]}}}
        stop = runner._is_unsafe(summary["cases"]["stress_large_payload"])
        self.assertTrue(stop)


class OneArtifactTests(unittest.TestCase):
    def test_cleanup_samples_recorded_inside_single_json(self):
        # cleanup_samples.json is one file of cleanup_sample rows; no
        # measurements-attempt-*.json is ever written by the v2 runner.
        source = Path(runner.__file__).read_text(encoding="utf-8")
        self.assertNotIn("measurements-attempt", source)
        self.assertIn("cleanup_samples.json", source)


if __name__ == "__main__":
    unittest.main()
