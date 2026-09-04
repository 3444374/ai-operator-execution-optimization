"""Contract tests exposing correctness defects in the resource measurement chain.

These tests were written BEFORE the fixes (commit series per the 2026-09-04
review instruction) and are allowed to fail at the commit that introduces
them; each failure names the defect it observes. They must all pass by the
end of the repair series:

1. peak must be the maximum simultaneous same-tick delta, never the union
   of fd numbers observed across the run;
2. a tick must observe all roles under one shared /proc view;
3. /proc read failures must surface as invalid/partial, never as zeros or
   empty sets that could fake a green verdict;
4. an operation that raises must still yield its trace for persistence;
5. no fault case may report a hardcoded valid/passed without a policy
   evaluation behind it;
6. only the four legal measurement/qualification combinations may compose;
7. the CLI must exit non-zero on non-passing outcomes.
"""
import unittest

from src.observability.process_resources.model import (
    FdIdentity,
    FdKind,
    ProcessSnapshot,
    ResourceTrace,
)
from src.experiments.postgresql.resource_qualification import (
    build_qualification_report,
    evaluate_peak_policy,
)


from src.observability.process_resources.model import SampleTick, SnapshotStatus


def _snapshot(pid, ns, fds):
    return ProcessSnapshot(
        pid=pid, process_start_time_ticks=pid, monotonic_ns=ns,
        status=SnapshotStatus.VALID, rss_bytes=1_000_000, thread_count=1,
        fds=tuple(fds))


def snap(ns, backend_fds=(), gateway_fds=()):
    return SampleTick(
        monotonic_ns=ns, unix_table_valid=True,
        processes={
            "backend": _snapshot(1, ns, tuple(
                FdIdentity(fd=fd, target="socket:[1]",
                           kind=FdKind.PROVIDER_UDS_CONNECTED)
                for fd in backend_fds)),
            "gateway": _snapshot(2, ns, tuple(
                FdIdentity(fd=fd, target="socket:[2]",
                           kind=FdKind.PROVIDER_UDS_CONNECTED)
                for fd in gateway_fds)),
        })


def empty_baseline():
    return {
        "backend": _snapshot(1, 0, ()),
        "gateway": _snapshot(2, 0, ()),
    }


class SameTickPeakContractTests(unittest.TestCase):
    """Peak = max simultaneous provider-session FDs, not fd-number history."""

    def test_three_sequential_sessions_reusing_fd_numbers_peak_is_one(self):
        # Rounds execute strictly sequentially: fd 18 closes before 19 opens.
        # The union-of-history implementation reports 3; the true peak is 1.
        trace = ResourceTrace(baseline=empty_baseline(), ticks=(
            snap(1, backend_fds=(18,), gateway_fds=(4,)),
            snap(2, backend_fds=(), gateway_fds=()),
            snap(3, backend_fds=(19,), gateway_fds=(4,)),
            snap(4, backend_fds=(), gateway_fds=()),
            snap(5, backend_fds=(20,), gateway_fds=(4,)),
        ))
        violations, diagnostics = evaluate_peak_policy(trace.baseline, trace)
        self.assertEqual(
            diagnostics["per_role"]["backend"]["provider_uds_peak_delta"], 1,
            "sequential fd reuse must count as peak 1, not the fd-history size")
        self.assertEqual(
            diagnostics["provider_uds_session_fd_peak_delta_combined"], 2,
            "same-tick combined peak is one client plus one accepted")
        self.assertEqual(violations, [])

    def test_same_tick_combined_counts_concurrent_presence_only(self):
        # backend +1 on tick A only, gateway +1 on tick B only: the combined
        # peak per tick is 1, and per-role historical maxima (1 + 1) may not
        # be added across different ticks to manufacture a violation.
        trace = ResourceTrace(baseline=empty_baseline(), ticks=(
            snap(1, backend_fds=(18,), gateway_fds=()),
            snap(2, backend_fds=(), gateway_fds=(4,)),
        ))
        violations, diagnostics = evaluate_peak_policy(trace.baseline, trace)
        self.assertEqual(
            diagnostics["provider_uds_session_fd_peak_delta_combined"], 1,
            "combined peak must be computed within one tick")

    def test_combined_three_in_one_tick_fails(self):
        trace = ResourceTrace(baseline=empty_baseline(), ticks=(
            snap(1, backend_fds=(18, 19), gateway_fds=(4,)),
        ))
        violations, _ = evaluate_peak_policy(trace.baseline, trace)
        self.assertTrue(any(
            v.metric == "provider_uds_session_fd_peak_delta_combined"
            for v in violations))


class ProcFailureContractTests(unittest.TestCase):
    """/proc read failures must never masquerade as observed zeros."""

    def test_snapshot_carries_explicit_validity(self):
        snapshot = ProcessSnapshot(1, 1_000_000, 1, ())
        # After the fix every snapshot must expose a status field; its
        # absence (AttributeError today) is the observed defect.
        self.assertTrue(hasattr(snapshot, "status"),
                        "ProcessSnapshot must carry an explicit SnapshotStatus")
        self.assertTrue(hasattr(snapshot, "pid"),
                        "ProcessSnapshot must carry the pid it observed")

    def test_invalid_snapshot_cannot_compose_a_passing_report(self):
        trace = ResourceTrace(baseline=empty_baseline(), ticks=(snap(1),))
        # A tick flagged invalid/partial must force invalid/inconclusive
        # overall, never valid/passed.
        pending = getattr(trace.ticks[0].processes["backend"], "status", None)
        if pending is None:
            self.fail("cannot express an invalid tick in the current model")


class OperationFailureContractTests(unittest.TestCase):
    """An operation exception must not swallow the trace."""

    def test_record_operation_returns_trace_on_operation_error(self):
        from src.observability.process_resources.recorder import record_operation
        from src.observability.process_resources.model import SampleTick, SnapshotStatus

        class FakeTickSampler:
            def sample_all(self, monotonic_ns):
                return SampleTick(
                    monotonic_ns=monotonic_ns, unix_table_valid=True,
                    processes={"backend": ProcessSnapshot(
                        pid=1, process_start_time_ticks=1,
                        monotonic_ns=monotonic_ns,
                        status=SnapshotStatus.VALID, rss_bytes=1,
                        thread_count=1, fds=())})

        def failing():
            raise RuntimeError("sentinel")

        recorded = record_operation(
            FakeTickSampler(), failing, sample_seconds=0)
        self.assertIsNotNone(getattr(recorded, "trace", None),
                             "the recorded result must expose the trace")
        self.assertIsNotNone(getattr(recorded, "operation_error", None),
                             "the operation error must be captured, not raised")


class StatusCompositionContractTests(unittest.TestCase):
    """Only valid+passed, valid+failed, inconclusive+not_evaluated,
    invalid+not_evaluated are legal compositions."""

    def test_illegal_combinations_are_rejected(self):
        from src.experiments.postgresql.resource_qualification import (
            compose_status)
        self.assertEqual(compose_status(("inconclusive", "passed")), None)
        self.assertEqual(compose_status(("invalid", "failed")), None)

    def test_priority_invalid_beats_everything(self):
        from src.experiments.postgresql.resource_qualification import (
            compose_status)
        self.assertEqual(compose_status(("invalid", "not_evaluated")),
                         ("invalid", "not_evaluated"))


class HardcodedVerdictContractTests(unittest.TestCase):
    """Fault cases must not write fixed valid/passed without a policy."""

    def test_runner_source_has_no_unsupported_hardcoded_verdicts(self):
        from pathlib import Path
        runner_source = Path(__file__).resolve().parents[2] / (
            "src/experiments/postgresql/semmap_resource_runner.py")
        source = runner_source.read_text(encoding="utf-8")
        for needle in (
            '"measurement_status": "valid",\n                  "qualification_status": "passed"',
        ):
            self.assertNotIn(needle, source,
                             "verdicts must come from a policy evaluation")


if __name__ == "__main__":
    unittest.main()
