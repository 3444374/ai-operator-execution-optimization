"""Contract tests for the post-6e66cc11 audit findings.

Each test here pins a defect found in the second review round of the
semmap-resource-v2 measurement chain. Written first (failing), per the
repository's characterization discipline: a fix without a failing test
that observes the defect is not verifiable.
"""
import unittest

from src.experiments.postgresql import semmap_resource_runner as runner
from src.experiments.postgresql.resource_qualification import compose_status
from src.observability.process_resources.model import (
    FdKind,
    FdIdentity,
    ProcessSnapshot,
    ResourceTrace,
    SampleTick,
    SnapshotStatus,
)
from src.experiments.postgresql.provider_session_attribution import (
    SessionWindow,
    attribute_provider_sessions,
    reclassify_clients,
)

import time


def _snapshot(pid, fds, kind=FdKind.UNBOUND_UNIX_SOCKET):
    return ProcessSnapshot(
        pid=pid, process_start_time_ticks=100, monotonic_ns=0,
        status=SnapshotStatus.VALID, errors=(), rss_bytes=1000,
        thread_count=1,
        fds=tuple(FdIdentity(fd=f, target=f"socket:[{f}]", kind=kind,
                             inode=f) for f in fds))


def _trace(ticks):
    return ResourceTrace(baseline={}, ticks=tuple(ticks),
                         fd_correlation_evidence=None)


class StressCaseComposabilityTests(unittest.TestCase):
    """The stress case must be able to compose valid+passed once a real
    cleanup trace exists. Defect: _evaluate_case(cleanup_trace=None)
    forces inconclusive and the post-hoc attachment block only ever
    worsens the measurement, so a formal stress run can never be valid
    (permanent exit 2 regardless of evidence quality)."""

    def test_perfect_inputs_compose_valid_passed(self):
        now = time.monotonic_ns()

        def snap(pid, fds, kind=FdKind.UNBOUND_UNIX_SOCKET):
            return ProcessSnapshot(
                pid=pid, process_start_time_ticks=100, monotonic_ns=now,
                status=SnapshotStatus.VALID, errors=(), rss_bytes=1000,
                thread_count=1,
                fds=tuple(FdIdentity(fd=f, target=f"socket:[{f}]",
                                     kind=kind, inode=f) for f in fds))

        gateway_base = ProcessSnapshot(
            pid=200, process_start_time_ticks=100, monotonic_ns=now,
            status=SnapshotStatus.VALID, errors=(), rss_bytes=2000,
            thread_count=2,
            fds=(FdIdentity(fd=4, target="socket:[400]",
                            kind=FdKind.PROVIDER_UDS_CONNECTED,
                            inode=400, unix_path="/p/sock"),))
        baseline = {"backend": snap(100, (0, 1, 2)), "gateway": gateway_base}
        stress = ResourceTrace(
            baseline=baseline,
            ticks=(SampleTick(now + 1, True, {
                "backend": snap(100, (0, 1, 2, 15)),
                "gateway": gateway_base}),),
            fd_correlation_evidence=None)
        cleanup = ResourceTrace(
            baseline=baseline,
            ticks=(SampleTick(now + 2, True, {
                "backend": snap(100, (0, 1, 2)),
                "gateway": gateway_base}),),
            fd_correlation_evidence=None)

        class FakeRecorded:
            result = {"rows": 6000}
            operation_error = None
            sampling_error = None
            trace = stress

        class FakeBaselineCapture:
            def __init__(self):
                self.baseline = baseline

        class FakeWindow:
            session_id = 2
            start_ns = now + 1
            end_ns = now + 1
            peer_pid = 100
            accepted_inode = 400

        import pathlib
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            report = runner._evaluate_case(
                case_name="stress_large_payload",
                root=pathlib.Path(td),
                baseline_capture=FakeBaselineCapture(),
                recorded=FakeRecorded(),
                windows=[FakeWindow()],
                cleanup_trace=cleanup)          # the real cleanup trace
        self.assertEqual(report["measurement_status"], "valid")
        self.assertEqual(report["qualification_status"], "passed")


class ComposeStatusUsedInRunnerTests(unittest.TestCase):
    """The registered four-pair vocabulary must be enforced on the
    production composition path, not only by the dead-code pure function.
    Defects: run_disconnect_case writes qualification=failed over an
    inconclusive/invalid measurement; run_exit_case does the same;
    compose_status itself has no production caller."""

    def test_runner_uses_compose_status(self):
        import inspect
        source = inspect.getsource(runner)
        self.assertIn("compose_status(", source,
                      "the runner must compose verdicts through the "
                      "registered compose_status vocabulary")

    def test_disconnect_top_level_cannot_emit_illegal_pair(self):
        # Reproduce the shape of run_disconnect_case's tail: recovery
        # not_evaluated (inconclusive measurement) must NOT combine with
        # a top-level qualification of failed.
        composed = compose_status(("inconclusive", "failed"))
        self.assertIsNone(composed)

    def test_compose_status_rejects_valid_not_evaluated(self):
        self.assertIsNone(compose_status(("valid", "not_evaluated")))
        self.assertIsNone(compose_status(("valid", "not_run")))


class TicklessAttributionTests(unittest.TestCase):
    """All-tickless windows must fail closed. Defect: when every session
    window contains no sampled tick, attribute_provider_sessions returns
    a non-None attribution with zero problems — the provider gates then
    run on zero observation evidence and can pass."""

    def test_all_windows_tickless_returns_none(self):
        window = SessionWindow(session_id=1, start_ns=100, end_ns=200,
                               peer_pid=100, accepted_fd=7,
                               accepted_inode=400)
        trace = _trace([
            SampleTick(5, True, {"backend": _snapshot(100, (0, 1, 2))})])
        baseline = {"backend": _snapshot(100, (0, 1, 2))}
        result = attribute_provider_sessions(
            backend_pid=100, baseline=baseline, trace=trace,
            windows=[window])
        self.assertIsNone(result.attribution,
                          "zero observed windows must not yield an "
                          "attribution the policy can pass on")
        self.assertTrue(any("tick" in p for p in result.problems))


class ReclassifyScopeTests(unittest.TestCase):
    """reclassify_clients must rewrite only within the session windows
    and only the exact (fd, inode) pairs the attribution identified.
    Defect: it rewrites by fd number across ALL ticks, so an fd number
    reused by an unrelated socket after the session closes is labelled
    a provider client (inflated peaks, polluted diagnostics)."""

    def test_out_of_window_reuse_is_not_relabelled(self):
        now = time.monotonic_ns()
        window = SessionWindow(session_id=1, start_ns=now + 1,
                               end_ns=now + 2, peer_pid=100,
                               accepted_fd=7, accepted_inode=400)
        attribution = {"sessions": [{
            "session_id": 1, "candidate_fds": [18], "attributed": {
                "fd": 18, "inode": 18, "accepted_inode": 400,
                "peer_pid": 100}}], "problems": []}
        # tick 1: fd 18 is the attributed socket (in window);
        # tick 2: fd 18 was closed and reused by a different socket
        #         (different inode) AFTER the session ended.
        from src.observability.process_resources.model import (
            ProcessSnapshot as PS)
        tick1 = SampleTick(now + 1, True, {"backend": _snapshot(
            100, (0, 1, 2, 18))})
        reused = tuple(
            (item if item.fd != 18 else
             FdIdentity(fd=18, target="socket:[9900]",
                        kind=FdKind.UNBOUND_UNIX_SOCKET, inode=9900))
            for item in _snapshot(100, (0, 1, 2, 18)).fds)
        tick2 = SampleTick(now + 5, True, {
            "backend": PS(pid=100, process_start_time_ticks=100,
                          monotonic_ns=now + 5,
                          status=SnapshotStatus.VALID, errors=(),
                          rss_bytes=1000, thread_count=1, fds=reused)})
        trace = _trace([tick1, tick2])
        rewritten = reclassify_clients(trace, attribution, [window])
        kinds = {}
        for tick in rewritten.ticks:
            for item in tick.processes["backend"].fds:
                if item.fd == 18:
                    kinds[("in-window" if tick is rewritten.ticks[0]
                           else "post-window", item.inode)] = item.kind
        self.assertIs(
            kinds[("in-window", 18)], FdKind.PROVIDER_UDS_CONNECTED)
        self.assertIs(
            kinds[("post-window", 9900)], FdKind.UNBOUND_UNIX_SOCKET,
            "an fd number reused by a different inode outside the "
            "session window must not be relabelled a provider client")


class RunnerExceptionExitCodeTests(unittest.TestCase):
    """A runtime exception inside run()'s case loop must surface as
    runner_failure (exit 3) with a written summary, not exit 1 (the
    valid-failed code) with a misleading 'failed' status."""

    def test_run_catches_runtime_exceptions(self):
        import inspect
        source = inspect.getsource(runner.run)
        self.assertIn("except Exception", source,
                      "run() must catch runtime exceptions after "
                      "preflight and record runner_failure")

    def test_not_run_vocabulary(self):
        # 'not_run' is outside the registered four-value measurement
        # vocabulary; skipped safety cases must still record one of
        # valid/inconclusive/invalid (as not_evaluated).
        import inspect
        source = inspect.getsource(runner)
        self.assertNotIn('"not_run"', source)


if __name__ == "__main__":
    unittest.main()
