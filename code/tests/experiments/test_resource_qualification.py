"""Metric schema v2 policy tests: the cases registered in contract §8.4.2."""
import unittest

from src.observability.process_resources.model import FdIdentity, FdKind, ProcessSnapshot, ResourceTrace
from src.experiments.postgresql.resource_qualification import (
    METRIC_SCHEMA,
    build_qualification_report,
)


def snap(ns, fds, rss=1_000_000, threads=1):
    # Real traces give UNKNOWN only when the target could not be resolved
    # (empty string); sockets always carry a resolvable "socket:[inode]"
    # target. Keep that distinction so session correlation cannot swallow
    # genuinely unclassifiable descriptors in tests either.
    targets = {
        FdKind.UNKNOWN: "",
        FdKind.SOCKET_OTHER: "socket:[7]",
        FdKind.RELATION_FILE: "/pgdata/base/16384/16388",
        FdKind.TOAST_RELATION_FILE: "/pgdata/base/16384/16388_toast",
        FdKind.POSTGRES_TEMP_FILE: "/pgdata/base/pgsql_tmp/123.0",
        FdKind.REGULAR_FILE_OTHER: "/etc/hostname",
        FdKind.PIPE: "pipe:[9]",
        FdKind.EVENTFD_OR_ANON_INODE: "anon_inode:[eventpoll]",
    }
    return ProcessSnapshot(
        monotonic_ns=ns,
        rss_bytes=rss,
        thread_count=threads,
        fds=tuple(FdIdentity(fd=10 + i, target=targets.get(kind, "socket:[1]"), kind=kind)
                  for i, kind in enumerate(fds)),
    )


def trace(baseline_fds, sample_fds_list, end_fds=None, rss_list=None, threads_end=None):
    base = {
        "backend": snap(1, baseline_fds.get("backend", [])),
        "gateway": snap(1, baseline_fds.get("gateway", [])),
    }
    samples = []
    for index, sample_fds in enumerate(sample_fds_list):
        rss = rss_list[index] if rss_list else 1_000_000
        samples.append({
            "backend": snap(2 + index, sample_fds.get("backend", []), rss=rss),
            "gateway": snap(2 + index, sample_fds.get("gateway", []), rss=rss),
        })
    if end_fds is not None:
        samples.append({
            "backend": snap(999, end_fds.get("backend", []),
                           threads=threads_end or 1),
            "gateway": snap(999, end_fds.get("gateway", []),
                            threads=threads_end or 1),
        })
    return ResourceTrace(baseline=base, samples=tuple(samples))


UDS_C = FdKind.PROVIDER_UDS_CLIENT
UDS_A = FdKind.PROVIDER_UDS_ACCEPTED
REL = FdKind.RELATION_FILE
UNK = FdKind.UNKNOWN


class PeakPolicyTests(unittest.TestCase):
    def test_one_uds_each_end_clean_passes(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C], "gateway": [UDS_A, REL]}],
            end_fds={})
        report = build_qualification_report(run.baseline, run)
        self.assertEqual(report.measurement_status, "valid")
        self.assertEqual(report.qualification_status, "passed")
        self.assertEqual(report.metric_schema, METRIC_SCHEMA)
        self.assertEqual(
            report.diagnostics["peak"]["provider_uds_session_fd_peak_delta_combined"], 2)

    def test_provider_uds_combined_three_fails(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C, UDS_C], "gateway": [UDS_A]}])
        report = build_qualification_report(run.baseline, run)
        self.assertEqual(report.qualification_status, "failed")
        metrics = {(v.scope, v.metric) for v in report.peak_policy}
        self.assertIn(("backend", "provider_uds_client_fd_peak_delta"), metrics)
        self.assertIn(("backend+gateway", "provider_uds_session_fd_peak_delta_combined"), metrics)

    def test_uds_two_plus_regular_file_passes_uds_gate_records_total(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C, REL], "gateway": [UDS_A]}],
            end_fds={})
        report = build_qualification_report(run.baseline, run)
        self.assertEqual(report.qualification_status, "passed")
        self.assertEqual(
            report.diagnostics["peak"]["per_role"]["backend"]["diagnostic_peak_fds"]["relation_file"]["delta"], 1)

    def test_unknown_fd_peak_is_inconclusive_not_evaluated(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C, UNK], "gateway": [UDS_A]}])
        report = build_qualification_report(run.baseline, run)
        self.assertEqual(report.measurement_status, "inconclusive")
        self.assertEqual(report.qualification_status, "not_evaluated")
        self.assertFalse(report.passed)

    def test_rss_peak_over_limit_fails(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C], "gateway": [UDS_A]}],
            rss_list=[64 * 1024 * 1024])
        report = build_qualification_report(run.baseline, run)
        self.assertEqual(report.qualification_status, "failed")
        self.assertTrue(any(v.metric == "rss_peak_delta" for v in report.peak_policy))


class CleanupPolicyTests(unittest.TestCase):
    def test_total_fd_end_one_fails_even_if_uds_clean(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C], "gateway": [UDS_A]}],
            end_fds={"backend": [REL], "gateway": []})
        report = build_qualification_report(run.baseline, run)
        self.assertEqual(report.qualification_status, "failed")
        self.assertIn("total_fd_end_delta",
                      [v.metric for v in report.cleanup_policy])

    def test_thread_end_delta_fails(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C], "gateway": [UDS_A]}],
            end_fds={"backend": [], "gateway": []},
            threads_end=2)
        report = build_qualification_report(run.baseline, run)
        self.assertEqual(report.qualification_status, "failed")
        self.assertIn("thread_end_delta",
                      [v.metric for v in report.cleanup_policy])

    def test_uds_end_nonzero_fails(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C], "gateway": [UDS_A]}],
            end_fds={"backend": [UDS_C], "gateway": []})
        report = build_qualification_report(run.baseline, run)
        self.assertEqual(report.qualification_status, "failed")
        self.assertIn("provider_uds_session_fd_end_delta_combined",
                      [v.metric for v in report.cleanup_policy])

    def test_empty_samples_is_invalid(self):
        run = ResourceTrace(
            baseline={"backend": snap(1, []), "gateway": snap(1, [])},
            samples=())
        report = build_qualification_report(run.baseline, run)
        self.assertEqual(report.measurement_status, "invalid")
        self.assertEqual(report.qualification_status, "not_evaluated")


class SessionCorrelationTests(unittest.TestCase):
    """v2.1: client-side UDS reclassification by gateway session co-occurrence."""

    def test_correlated_client_socket_counts_as_uds_client(self):
        from src.experiments.postgresql.resource_qualification import (
            correlate_client_uds)
        # backend fd 17: new unbound socket while gateway holds an accepted session
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UNK], "gateway": [UDS_A]}],
            end_fds={})
        run.samples[0]["backend"].fds[0].__new__  # noqa: B018 - just proving tuple immutability
        # build with a socket-like target: unknown kind but socket target
        from src.observability.process_resources.model import FdIdentity, ProcessSnapshot
        rebuilt = ResourceTrace(
            baseline=run.baseline,
            samples=({
                "backend": ProcessSnapshot(1, 1_000_000, 1, (
                    FdIdentity(fd=17, target="socket:[999]", kind=FdKind.SOCKET_OTHER),)),
                "gateway": ProcessSnapshot(1, 1_000_000, 1, (
                    FdIdentity(fd=4, target="socket:[1]", kind=FdKind.PROVIDER_UDS_ACCEPTED),)),
            },))
        correlated = correlate_client_uds(rebuilt)
        kinds = [f.kind for f in correlated.samples[0]["backend"].fds]
        self.assertEqual(kinds, [FdKind.PROVIDER_UDS_CLIENT])
        self.assertEqual(correlated.fd_correlation_evidence[17]["original_kind"],
                         "socket_other")
        self.assertEqual(correlated.fd_correlation_evidence[17]["rule"],
                         "session-correlation")

    def test_baseline_socket_is_not_reclassified(self):
        from src.experiments.postgresql.resource_qualification import (
            correlate_client_uds)
        from src.observability.process_resources.model import FdIdentity, ProcessSnapshot
        base_socket = FdIdentity(fd=8, target="socket:[42]", kind=FdKind.SOCKET_OTHER)
        run = ResourceTrace(
            baseline={"backend": ProcessSnapshot(0, 1, 1, (base_socket,)),
                      "gateway": ProcessSnapshot(0, 1, 1, ())},
            samples=({
                "backend": ProcessSnapshot(1, 1_000_000, 1, (base_socket,)),
                "gateway": ProcessSnapshot(1, 1_000_000, 1, (
                    FdIdentity(fd=4, target="socket:[1]", kind=FdKind.PROVIDER_UDS_ACCEPTED),)),
            },))
        correlated = correlate_client_uds(run)
        self.assertEqual(correlated.samples[0]["backend"].fds[0].kind,
                         FdKind.SOCKET_OTHER)
        self.assertFalse(correlated.fd_correlation_evidence)

    def test_no_gateway_session_means_no_reclassification(self):
        from src.experiments.postgresql.resource_qualification import (
            correlate_client_uds)
        from src.observability.process_resources.model import FdIdentity, ProcessSnapshot
        run = ResourceTrace(
            baseline={"backend": ProcessSnapshot(0, 1, 1, ()),
                      "gateway": ProcessSnapshot(0, 1, 1, ())},
            samples=({
                "backend": ProcessSnapshot(1, 1_000_000, 1, (
                    FdIdentity(fd=17, target="socket:[999]", kind=FdKind.SOCKET_OTHER),)),
                "gateway": ProcessSnapshot(1, 1_000_000, 1, ()),
            },))
        correlated = correlate_client_uds(run)
        self.assertEqual(correlated.samples[0]["backend"].fds[0].kind,
                         FdKind.SOCKET_OTHER)

    def test_run7_shape_now_valid_passed_with_correlation(self):
        # The archived diagnostic shape: new backend socket + accepted gateway
        # session + an eventpoll fd; under correlation this maps to the
        # pass verdict instead of inconclusive.
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UNK], "gateway": [UDS_A]}],
            end_fds={})
        # give backend fd the socket target so correlation applies
        from src.observability.process_resources.model import FdIdentity, ProcessSnapshot
        EPOLL = FdKind.EVENTFD_OR_ANON_INODE
        rebuilt = ResourceTrace(
            baseline={"backend": ProcessSnapshot(0, 1_000_000, 1, ()),
                      "gateway": ProcessSnapshot(0, 1_000_000, 1, ())},
            samples=({
                "backend": ProcessSnapshot(1, 1_000_000, 1, (
                    FdIdentity(fd=17, target="socket:[999]", kind=FdKind.SOCKET_OTHER),
                    FdIdentity(fd=18, target="anon_inode:[eventpoll]", kind=EPOLL))),
                "gateway": ProcessSnapshot(1, 1_000_000, 1, (
                    FdIdentity(fd=5, target="socket:[1]", kind=FdKind.PROVIDER_UDS_ACCEPTED),)),
            }, {
                "backend": ProcessSnapshot(2, 1_000_000, 1, ()),
                "gateway": ProcessSnapshot(2, 1_000_000, 1, ()),
            }))
        report = build_qualification_report(rebuilt.baseline, rebuilt)
        self.assertEqual(report.measurement_status, "valid")
        self.assertEqual(report.qualification_status, "passed")
        self.assertIn(17, report.diagnostics["fd_correlation"])


class V1ReplayShapeTests(unittest.TestCase):
    """The archived v1 shape (+1 UDS each, +1 transient file) maps to a pass
    if and only if the transient file classifies; UNKNOWN stays inconclusive."""

    def test_classified_transient_passes_v2(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C, REL], "gateway": [UDS_A]}],
            end_fds={})
        report = build_qualification_report(run.baseline, run)
        self.assertEqual((report.measurement_status, report.qualification_status),
                         ("valid", "passed"))

    def test_unclassified_transient_is_inconclusive(self):
        run = trace(
            baseline_fds={},
            sample_fds_list=[{"backend": [UDS_C, UNK], "gateway": [UDS_A]}],
            end_fds={})
        report = build_qualification_report(run.baseline, run)
        self.assertEqual((report.measurement_status, report.qualification_status),
                         ("inconclusive", "not_evaluated"))


if __name__ == "__main__":
    unittest.main()
