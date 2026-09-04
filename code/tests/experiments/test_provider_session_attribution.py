"""Attribution tests: gateway session evidence to backend provider clients."""
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


def _events(sessions):
    events = []
    for session_id, (start, end, peer, inode) in enumerate(sessions, 1):
        events.append({"event": "session_start", "session_id": session_id,
                       "monotonic_ns": start, "gateway_pid": 2,
                       "accepted_fd": 5, "accepted_socket_inode": inode,
                       "peer_pid": peer, "peer_uid": 999, "peer_gid": 999})
        events.append({"event": "session_end", "session_id": session_id,
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

    def test_unique_candidate_with_matching_peer_attributes(self):
        run = self._trace([
            self._tick(120, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 777),)),
            self._tick(180, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 777),)),
        ])
        attribution = attribute_provider_sessions(
            backend_pid=1, baseline=run.baseline, trace=run,
            windows=session_windows(_events([(100, 200, 1, 777)])))
        self.assertIsNotNone(attribution)
        self.assertEqual(attribution["sessions"][0]["attributed"]["fd"], 17)
        rewritten = reclassify_clients(run, attribution)
        kinds = [item.kind for item in rewritten.ticks[0].processes["backend"].fds]
        self.assertEqual(kinds, [UDS])
        self.assertEqual(
            rewritten.fd_correlation_evidence[17]["accepted_inode"], 777)

    def test_two_candidates_is_inconclusive(self):
        run = self._trace([
            self._tick(120, backend=(_unbound(17, 4001), _unbound(18, 4002)),
                       gateway=(_connected(5, 777),)),
        ])
        attribution = attribute_provider_sessions(
            backend_pid=1, baseline=run.baseline, trace=run,
            windows=session_windows(_events([(100, 200, 1, 777)])))
        self.assertIsNone(attribution)

    def test_peer_pid_mismatch_is_inconclusive(self):
        run = self._trace([
            self._tick(120, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 777),)),
        ])
        attribution = attribute_provider_sessions(
            backend_pid=42, baseline=run.baseline, trace=run,
            windows=session_windows(_events([(100, 200, 1, 777)])))
        self.assertIsNone(attribution)

    def test_accepted_inode_never_seen_is_inconclusive(self):
        run = self._trace([
            self._tick(120, backend=(_unbound(17, 4001),),
                       gateway=(_connected(5, 888),)),
        ])
        attribution = attribute_provider_sessions(
            backend_pid=1, baseline=run.baseline, trace=run,
            windows=session_windows(_events([(100, 200, 1, 777)])))
        self.assertIsNone(attribution)

    def test_baseline_unbound_socket_is_not_a_candidate(self):
        base = {"backend": _snap(1, 0, (_unbound(8, 3000),)),
                "gateway": _snap(2, 0, ())}
        run = ResourceTrace(baseline=base, ticks=(
            self._tick(120, backend=(_unbound(8, 3000), _unbound(17, 4001)),
                       gateway=(_connected(5, 777),)),))
        attribution = attribute_provider_sessions(
            backend_pid=1, baseline=base, trace=run,
            windows=session_windows(_events([(100, 200, 1, 777)])))
        self.assertIsNotNone(attribution)
        self.assertEqual(attribution["sessions"][0]["attributed"]["fd"], 17)


if __name__ == "__main__":
    unittest.main()
