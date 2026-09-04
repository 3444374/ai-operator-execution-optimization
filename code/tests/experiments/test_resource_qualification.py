"""Metric schema v2 policy tests: same-tick peaks, exact cleanup, statuses."""
import unittest

from src.observability.process_resources.model import (
    FdIdentity, FdKind, ProcessSnapshot, ResourceTrace, SampleTick, SnapshotStatus)
from src.experiments.postgresql.resource_qualification import (
    METRIC_SCHEMA, build_qualification_report, compose_status,
    evaluate_cleanup_policy, evaluate_peak_policy)

UDS = FdKind.PROVIDER_UDS_CONNECTED
REL = FdKind.RELATION_FILE
UNK = FdKind.UNKNOWN
EPOLL = FdKind.EVENTFD_OR_ANON_INODE


def _snap(pid, ns, fds, rss=1_000_000, threads=1):
    return ProcessSnapshot(
        pid=pid, process_start_time_ticks=pid, monotonic_ns=ns,
        status=SnapshotStatus.VALID, rss_bytes=rss, thread_count=threads,
        fds=tuple(fds))


def _ids(specs):
    return tuple(FdIdentity(fd=fd, target=target, kind=kind)
                 for fd, target, kind in specs)


def tick(ns, backend=(), gateway=(), rss=1_000_000, threads=1):
    return SampleTick(
        monotonic_ns=ns, unix_table_valid=True,
        processes={
            "backend": _snap(1, ns, backend, rss, threads),
            "gateway": _snap(2, ns, gateway, rss, threads),
        })


def base(backend=(), gateway=(), threads=1):
    return {"backend": _snap(1, 0, backend, threads=threads),
            "gateway": _snap(2, 0, gateway, threads=threads)}


def trace(baseline, ticks):
    return ResourceTrace(baseline=baseline, ticks=tuple(ticks))


def uds(fd):
    return (fd, "socket:[9]", UDS)


class PeakPolicyTests(unittest.TestCase):
    def test_one_uds_each_same_tick_passes(self):
        run = trace(base(), [tick(1, backend=_ids([uds(17)]),
                                      gateway=_ids([uds(5)])),
                             tick(2)])
        report = build_qualification_report(run.baseline, run, phase="stress")
        self.assertEqual((report.measurement_status, report.qualification_status),
                         ("valid", "passed"))
        self.assertEqual(
            report.diagnostics["peak"]["provider_uds_session_fd_peak_delta_combined"], 2)

    def test_combined_three_in_one_tick_fails(self):
        run = trace(base(), [tick(1, backend=_ids([uds(17), uds(18)]),
                                      gateway=_ids([uds(5)]))])
        report = build_qualification_report(run.baseline, run, phase="stress")
        self.assertEqual(report.qualification_status, "failed")
        metrics = {(v.scope, v.metric) for v in report.peak_policy}
        self.assertIn(("backend", "provider_uds_client_fd_peak_delta"), metrics)
        self.assertIn(("backend+gateway", "provider_uds_session_fd_peak_delta_combined"), metrics)

    def test_uds_two_plus_regular_file_passes_uds_gate(self):
        run = trace(base(), [tick(1, backend=_ids([uds(17), (18, "/pg/base/1/2", REL)]),
                                      gateway=_ids([uds(5)])),
                             tick(2)])
        report = build_qualification_report(run.baseline, run, phase="stress")
        self.assertEqual(report.qualification_status, "passed")
        self.assertEqual(
            report.diagnostics["peak"]["per_role"]["backend"]["diagnostic_peak_deltas"]["relation_file"], 1)

    def test_unknown_fd_peak_is_inconclusive(self):
        run = trace(base(), [tick(1, backend=_ids([uds(17), (18, "", UNK)]),
                                      gateway=_ids([uds(5)]))])
        report = build_qualification_report(run.baseline, run, phase="stress")
        self.assertEqual((report.measurement_status, report.qualification_status),
                         ("inconclusive", "not_evaluated"))

    def test_rss_peak_over_limit_fails(self):
        run = trace(base(), [tick(1, backend=_ids([uds(17)]),
                                      gateway=_ids([uds(5)]),
                                      rss=64 * 1024 * 1024)])
        report = build_qualification_report(run.baseline, run, phase="stress")
        self.assertEqual(report.qualification_status, "failed")

    def test_sequential_fd_reuse_peak_is_one(self):
        run = trace(base(), [
            tick(1, backend=_ids([uds(18)]), gateway=_ids([uds(4)])),
            tick(2),
            tick(3, backend=_ids([uds(19)]), gateway=_ids([uds(4)])),
            tick(4),
            tick(5, backend=_ids([uds(20)]), gateway=_ids([uds(4)])),
        ])
        report = build_qualification_report(run.baseline, run, phase="stress")
        self.assertEqual(report.qualification_status, "passed")


class CleanupPolicyTests(unittest.TestCase):
    def test_total_fd_end_one_fails(self):
        run = trace(base(), [
            tick(1, backend=_ids([uds(17)]), gateway=_ids([uds(5)])),
            tick(2, backend=_ids([(18, "/pg/base/1/2", REL)]))])
        report = build_qualification_report(run.baseline, run, phase="cleanup")
        self.assertEqual(report.qualification_status, "failed")
        self.assertIn("total_fd_end_delta",
                      [v.metric for v in report.cleanup_policy])

    def test_negative_thread_delta_also_fails(self):
        baseline = {"backend": _snap(1, 0, ()),
                    "gateway": _snap(2, 0, (), threads=3)}
        run = ResourceTrace(baseline=baseline, ticks=(tick(1),))
        report = build_qualification_report(run.baseline, run, phase="cleanup")
        self.assertEqual(report.qualification_status, "failed")
        self.assertIn("thread_end_delta", [v.metric for v in report.cleanup_policy])

    def test_uds_end_nonzero_fails(self):
        run = trace(base(), [
            tick(1, backend=_ids([uds(17)]), gateway=_ids([uds(5)])),
            tick(2, backend=_ids([uds(17)]))])
        report = build_qualification_report(run.baseline, run, phase="cleanup")
        self.assertEqual(report.qualification_status, "failed")
        self.assertIn("provider_uds_session_fd_end_delta_combined",
                      [v.metric for v in report.cleanup_policy])

    def test_empty_ticks_is_invalid(self):
        run = ResourceTrace(baseline=base(), ticks=())
        report = build_qualification_report(run.baseline, run, phase="stress")
        self.assertEqual((report.measurement_status, report.qualification_status),
                         ("invalid", "not_evaluated"))

    def test_invalid_snapshot_is_invalid(self):
        broken = SampleTick(monotonic_ns=1, unix_table_valid=True, processes={
            "backend": ProcessSnapshot(pid=1, process_start_time_ticks=None,
                monotonic_ns=1, status=SnapshotStatus.INVALID),
            "gateway": _snap(2, 1, ())})
        run = ResourceTrace(baseline=base(), ticks=(broken,))
        report = build_qualification_report(run.baseline, run, phase="stress")
        self.assertEqual(report.measurement_status, "invalid")


class PhaseSeparationTests(unittest.TestCase):
    def test_stress_phase_never_judges_cleanup(self):
        # Session still open at stress end must not produce a cleanup verdict.
        run = trace(base(), [tick(1, backend=_ids([uds(17)]),
                                      gateway=_ids([uds(5)]))])
        report = build_qualification_report(run.baseline, run, phase="stress")
        self.assertEqual(report.cleanup_policy, [])
        self.assertEqual(report.qualification_status, "passed")

    def test_combined_phase_judges_both(self):
        run = trace(base(), [
            tick(1, backend=_ids([uds(17)]), gateway=_ids([uds(5)])),
            tick(2)])
        report = build_qualification_report(run.baseline, run, phase="combined")
        self.assertEqual(report.qualification_status, "passed")


class StatusCompositionTests(unittest.TestCase):
    def test_all_legal_combinations(self):
        self.assertEqual(compose_status(("valid", "passed"), ("valid", "passed")),
                         ("valid", "passed"))
        self.assertEqual(compose_status(("valid", "passed"), ("valid", "failed")),
                         ("valid", "failed"))
        self.assertEqual(compose_status(("valid", "passed"), ("inconclusive", "not_evaluated")),
                         ("inconclusive", "not_evaluated"))
        self.assertEqual(compose_status(("valid", "failed"), ("invalid", "not_evaluated")),
                         ("invalid", "not_evaluated"))

    def test_illegal_rejected(self):
        self.assertIsNone(compose_status(("inconclusive", "passed")))
        self.assertIsNone(compose_status(("invalid", "failed")))


if __name__ == "__main__":
    unittest.main()
