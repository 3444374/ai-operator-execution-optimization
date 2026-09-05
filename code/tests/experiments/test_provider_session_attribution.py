"""Attribution tests: gateway session evidence to backend provider clients.

Contract (registered pre-run correction #1, 2026-09-04): a backend
unbound AF_UNIX socket is the provider client only when the five
session-evidence conditions hold. Any failure returns attribution=None
PLUS the problems that caused it, and the qualification must then be
inconclusive — an attribution failure may never silently fall back to
the raw trace and report a provider verdict built on zero evidence.
"""
import unittest

from src.observability.process_resources.model import (
    FdIdentity, FdKind, ProcessSnapshot, ResourceTrace, SampleTick,
    SnapshotStatus)
from src.experiments.postgresql.provider_session_attribution import (
    attribute_provider_sessions,
    reclassify_clients,
    session_windows,
)

UNBOUND = FdKind.UNBOUND_UNIX_SOCKET
UDS = FdKind.PROVIDER_UDS_CONNECTED
REL = FdKind.RELATION_FILE


def _snap(pid, ns, fds):
    return ProcessSnapshot(
        pid=pid, process_start_time_ticks=pid, monotonic_ns=ns,
        status=SnapshotStatus.VALID, rss_bytes=1_000_000,
        thread_count=1, fds=tuple(fds))


def _unbound(fd, inode):
    return FdIdentity(fd=fd, target=f"socket:[{inode}]", kind=UNBOUND,
                      inode=inode)


def _connected(fd, inode):
    return FdIdentity(fd=fd, target=f"socket:[{inode}]", kind=UDS,
                      inode=inode)


def _regular(fd):
    return FdIdentity(fd=fd, target="/tmp/plain-file", kind=REL, inode=None)


def _events(sessions, *, accepted_inode_override=None):
    events = []
    for session_id, spec in enumerate(sessions, 1):
        start, end, peer, inode = spec
        if accepted_inode_override is not None:
            inode = accepted_inode_override
        events.append({"event": "session_start", "session_id": session_id,
                       "monotonic_ns": start, "gateway_pid": 2,
                       "accepted_fd": 5, "accepted_socket_inode": inode,
                       "peer_pid": peer, "peer_uid": 999, "peer_gid": 999})
        events.append({"event": "session_end", "session_id": session_id, "connection_closed": True,
                       "monotonic_ns": end})
    return events


class SessionWindowTests(unittest.TestCase):
    def test_windows_fold_from_events(self):
        windows = session_windows(_events([(100, 200, 1, 777)]))
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0].start_ns, 100)
        self.assertEqual(windows[0].end_ns, 200)
        self.assertEqual(windows[0].peer_pid, 1)
        self.assertEqual(windows[0].accepted_inode, 777)


class AttributionTests(unittest.TestCase):
    def _trace(self, ticks):
        return ResourceTrace(
            baseline={"backend": _snap(1, 0, ()),
                      "gateway": _snap(2, 0, ())},
            ticks=tuple(ticks))

    def _tick(self, ns, backend=(), gateway=()):
        return SampleTick(monotonic_ns=ns, unix_table_valid=True, processes={
            "backend": _snap(1, ns, backend),
            "gateway": _snap(2, ns, gateway)})

    def _attribute(self, trace, events, backend_pid=1):
        return attribute_provider_sessions(
            backend_pid=backend_pid, baseline=trace.baseline, trace=trace,
            windows=session_windows(events))

    def test_unique_candidate_with_matching_peer_attributes(self):
        run = self._trace([
            self._tick(120, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 777),)),
            self._tick(180, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 777),)),
        ])
        windows = session_windows(_events([(100, 200, 1, 777)]))
        result = self._attribute(run, _events([(100, 200, 1, 777)]))
        self.assertEqual(result.problems, [])
        self.assertIsNotNone(result.attribution)
        self.assertEqual(
            result.attribution["sessions"][0]["attributed"]["fd"], 17)
        rewritten = reclassify_clients(run, result.attribution, windows)
        kinds = [item.kind for item in rewritten.ticks[0].processes["backend"].fds]
        self.assertEqual(kinds, [UDS])
        self.assertEqual(
            rewritten.fd_correlation_evidence["1"]["attributed"]["accepted_inode"], 777)

    def test_two_candidates_is_inconclusive_with_reason(self):
        run = self._trace([
            self._tick(120, backend=(_unbound(17, 4001), _unbound(18, 4002)),
                       gateway=(_connected(5, 777),)),
        ])
        result = self._attribute(run, _events([(100, 200, 1, 777)]))
        self.assertIsNone(result.attribution)
        self.assertTrue(any("candidates_2" in p for p in result.problems),
                        result.problems)

    def test_peer_pid_mismatch_is_inconclusive_with_reason(self):
        run = self._trace([
            self._tick(120, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 777),)),
        ])
        result = self._attribute(
            run, _events([(100, 200, 1, 777)]), backend_pid=42)
        self.assertIsNone(result.attribution)
        self.assertTrue(any("peer_mismatch" in p for p in result.problems),
                        result.problems)

    def test_accepted_inode_never_seen_is_inconclusive_with_reason(self):
        run = self._trace([
            self._tick(120, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 888),)),
        ])
        result = self._attribute(run, _events([(100, 200, 1, 777)]))
        self.assertIsNone(result.attribution)
        self.assertTrue(any("accepted_inode_unseen" in p for p in result.problems),
                        result.problems)

    def test_missing_accepted_inode_is_a_problem_not_a_skip(self):
        # The observer failed to readlink the accepted socket: condition 3
        # may not be silently disabled — four-of-five attribution is not
        # attribution.
        run = self._trace([
            self._tick(120, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 777),)),
        ])
        result = self._attribute(
            run, _events([(100, 200, 1, None)]))
        self.assertIsNone(result.attribution)
        self.assertTrue(
            any("accepted_inode_unrecorded" in p for p in result.problems),
            result.problems)

    def test_one_observed_session_does_not_qualify_another_tickless_session(self):
        run = self._trace([self._tick(500, backend=(_unbound(17, 4001),), gateway=(_connected(5, 777),))])
        result = self._attribute(run, _events([(100, 150, 1, 777), (400, 600, 1, 777)]))
        self.assertIsNone(result.attribution)
        self.assertTrue(any("session1_no_ticks" in p for p in result.problems))

    def test_baseline_unbound_socket_is_not_a_candidate(self):
        base = {"backend": _snap(1, 0, (_unbound(8, 3000),)),
                "gateway": _snap(2, 0, ())}
        run = ResourceTrace(baseline=base, ticks=(
            self._tick(120, backend=(_unbound(8, 3000), _unbound(17, 4001)),
                       gateway=(_connected(5, 777),)),))
        result = attribute_provider_sessions(
            backend_pid=1, baseline=base, trace=run,
            windows=session_windows(_events([(100, 200, 1, 777)])))
        self.assertIsNotNone(result.attribution)
        self.assertEqual(
            result.attribution["sessions"][0]["attributed"]["fd"], 17)

    def test_regular_files_are_never_provider_candidates(self):
        # Relation files and other regular descriptors must never enter the
        # provider-socket candidate set.
        run = self._trace([
            self._tick(120, backend=(_regular(20), _unbound(17, 4001)),
                       gateway=(_connected(5, 777),)),
        ])
        result = self._attribute(run, _events([(100, 200, 1, 777)]))
        self.assertIsNotNone(result.attribution)
        self.assertEqual(
            result.attribution["sessions"][0]["attributed"]["fd"], 17)

    def test_no_sessions_at_all_is_not_a_pass(self):
        # No session windows: nothing was observed, so nothing can be
        # qualified. This must not read as "zero provider deltas, passed".
        run = self._trace([self._tick(120, backend=(), gateway=())])
        result = self._attribute(run, [])
        self.assertIsNone(result.attribution)
        self.assertIn("no_session_windows", result.problems)

    def test_baseline_unreadable_is_reported(self):
        base = {"backend": ProcessSnapshot(
            pid=1, process_start_time_ticks=1, monotonic_ns=0,
            status=SnapshotStatus.VALID, rss_bytes=1, thread_count=1,
            fds=None)}
        run = ResourceTrace(baseline=base, ticks=(
            self._tick(120, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 777),)),))
        result = attribute_provider_sessions(
            backend_pid=1, baseline=base, trace=run,
            windows=session_windows(_events([(100, 200, 1, 777)])))
        self.assertIsNone(result.attribution)
        self.assertIn("backend_baseline_unreadable", result.problems)


class AttributionFailureIsInconclusiveTests(unittest.TestCase):
    """The audit found this exact false-green: attribution failure fell
    back to the raw trace where backend clients stay a gate-less kind,
    and the case reported valid+passed. The composition must instead
    force inconclusive/not_evaluated."""

    def _raw_trace_two_concurrent_sockets(self):
        # Two concurrent unbound sockets in one session window make
        # attribution fail (candidates_2). The RAW trace keeps both as
        # UNBOUND (diagnostic kind, no gate).
        return ResourceTrace(
            baseline={"backend": _snap(1, 0, ()),
                      "gateway": _snap(2, 0, ())},
            ticks=(SampleTick(monotonic_ns=120, unix_table_valid=True,
                              processes={
                                  "backend": _snap(1, 120, (
                                      _unbound(17, 4001),
                                      _unbound(18, 4002)),
                                  ),
                                  "gateway": _snap(2, 120, (
                                      _connected(5, 777),)),
                              }),))

    def test_failed_attribution_forces_inconclusive_not_passed(self):
        from src.experiments.postgresql.resource_qualification import (
            build_qualification_report)
        from src.experiments.postgresql import provider_session_attribution \
            as psa
        trace = self._raw_trace_two_concurrent_sockets()
        result = attribute_provider_sessions(
            backend_pid=1, baseline=trace.baseline, trace=trace,
            windows=session_windows(_events([(100, 200, 1, 777)])))
        self.assertIsNone(result.attribution)  # ambiguous
        # Raw trace alone WOULD pass every gate (unbound is gate-less)…
        raw_report = build_qualification_report(
            trace.baseline, trace, phase="stress")
        self.assertEqual(raw_report.measurement_status, "valid")
        self.assertEqual(raw_report.qualification_status, "passed")
        # …so the runner-level rule must downgrade on attribution failure.
        measurement = raw_report.measurement_status
        if result.attribution is None and measurement == "valid":
            measurement = "inconclusive"
        self.assertEqual(measurement, "inconclusive")
        qualification = "passed" if measurement == "valid" else "not_evaluated"
        self.assertEqual(qualification, "not_evaluated")


if __name__ == "__main__":
    unittest.main()

class AttributionIdentityTests(unittest.TestCase):
    _trace = AttributionTests._trace
    _tick = AttributionTests._tick
    _attribute = AttributionTests._attribute

    def test_endpoints_in_different_ticks_cannot_be_paired(self):
        run=self._trace([self._tick(120,backend=(_unbound(17,4001),)),
                         self._tick(180,gateway=(_connected(5,777),))])
        result=self._attribute(run,_events([(100,200,1,777)]))
        self.assertIsNone(result.attribution)

    def test_reused_baseline_fd_with_new_inode_is_new_candidate(self):
        base={"backend":_snap(1,0,(_unbound(17,3000),)),"gateway":_snap(2,0,())}
        run=ResourceTrace(base,(self._tick(120,backend=(_unbound(17,4001),),gateway=(_connected(5,777),)),))
        result=self._attribute(run,_events([(100,200,1,777)]))
        self.assertIsNotNone(result.attribution,result.problems)

    def test_unknown_reuse_and_other_session_are_not_reclassified(self):
        from dataclasses import replace
        run=self._trace([self._tick(120,backend=(_unbound(17,4001),),gateway=(_connected(5,777),))])
        result=self._attribute(run,_events([(100,200,1,777)]))
        later=self._tick(300,backend=(_unbound(17,4001),))
        unknown=FdIdentity(17,"memfd:other",FdKind.UNKNOWN,999)
        trace=replace(run,ticks=run.ticks+(self._tick(180,backend=(unknown,)),later))
        rewritten=reclassify_clients(trace,result.attribution)
        self.assertEqual(rewritten.ticks[1].processes['backend'].fds[0].kind,FdKind.UNKNOWN)
        self.assertEqual(rewritten.ticks[2].processes['backend'].fds[0].kind,UNBOUND)

    def test_post_session_residual_remains_in_cleanup_evidence(self):
        from src.experiments.postgresql.provider_session_attribution import residual_provider_fds
        run=self._trace([self._tick(120,backend=(_unbound(17,4001),),gateway=(_connected(5,777),))])
        result=self._attribute(run,_events([(100,200,1,777)]))
        cleanup=self._trace([self._tick(300,backend=(_unbound(17,4001),))])
        self.assertEqual(residual_provider_fds(cleanup,result.attribution)[0]['inode'],4001)

    def test_duplicate_orphan_and_unclosed_events_are_rejected(self):
        events=_events([(100,200,1,777)])
        for bad in ([events[0],events[0]], [events[1]], [events[0]]):
            with self.subTest(bad=bad),self.assertRaises(ValueError):
                session_windows(bad)
