"""Metric schema v2 policy tests: the cases registered in contract §8.4.2."""
import unittest

from src.observability.process_resources.model import FdIdentity, FdKind, ProcessSnapshot, ResourceTrace
from src.experiments.postgresql.resource_qualification import (
    METRIC_SCHEMA,
    build_qualification_report,
)


def snap(ns, fds, rss=1_000_000, threads=1):
    return ProcessSnapshot(
        monotonic_ns=ns,
        rss_bytes=rss,
        thread_count=threads,
        fds=tuple(FdIdentity(fd=10 + i, target="socket:[1]", kind=kind)
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
