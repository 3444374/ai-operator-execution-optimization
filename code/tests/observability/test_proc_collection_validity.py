"""Collection-validity contract tests: /proc failures are explicit.

The instruction's section-6 coverage (unreadable fd dir, unreadable
/proc/net/unix, unreadable statm/status, readlink race retry, persistently
failed consistency) was entirely absent — every test constructed VALID
snapshots by hand, so the failure paths in snapshot_process and
sample_tick had no observable contract. These tests drive the real
collector logic with patched low-level readers (they run on any OS; the
Linux integration file covers the live /proc path).
"""
import unittest
from unittest import mock

from src.observability.process_resources import linux_procfs as lp
from src.observability.process_resources.model import (
    FdKind, SnapshotStatus)


def _readers(fds=None, targets=None, start_time=100, rss=(4096 * 100, 3)):
    """Patch the low-level readers with controlled behavior.

    fds:        return value of _list_fds (None = unreadable)
    targets:    dict fd -> readlink result (None = readlink failure)
    start_time: value of process_start_time_ticks (None = unreadable)
    rss:        (rss_bytes, threads) or None-values for unreadable
    """
    rss_bytes, threads = rss
    return {
        "_list_fds": mock.Mock(side_effect=(
            (lambda _calls=[]: (fds() if callable(fds) else fds)))),
        "_read_link": mock.Mock(
            side_effect=lambda _pid, fd: (targets or {}).get(fd, "pipe:[1]")),
        "process_start_time_ticks": mock.Mock(return_value=start_time),
        "_read_rss_threads": mock.Mock(return_value=(rss_bytes, threads)),
        "unix_socket_table": mock.Mock(
            return_value={777: "/tmp/prov.sock"}),
    }


class SnapshotValidityTests(unittest.TestCase):
    def _snapshot(self, patches, pid=1):
        with mock.patch.multiple(lp, **patches):
            return lp.snapshot_process(
                pid, monotonic_ns=1,
                unix_paths_by_inode={777: "/tmp/prov.sock"},
                provider_socket_path="/tmp/prov.sock")

    def test_fd_directory_unreadable_is_invalid_not_empty(self):
        snap = self._snapshot(_readers(fds=None))
        self.assertEqual(snap.status, SnapshotStatus.INVALID)
        self.assertIn("fd_list_unreadable", snap.errors)
        self.assertIsNone(snap.fds)   # never () masquerading as "read"

    def test_statm_unreadable_leaves_rss_none_with_error(self):
        snap = self._snapshot(_readers(rss=(None, 3)))
        self.assertIsNone(snap.rss_bytes)          # never 0
        self.assertIn("statm_unreadable", snap.errors)
        self.assertEqual(snap.thread_count, 3)

    def test_status_unreadable_leaves_threads_none_with_error(self):
        snap = self._snapshot(_readers(rss=(4096, None)))
        self.assertIsNone(snap.thread_count)       # never 0
        self.assertIn("status_unreadable", snap.errors)

    def test_pid_start_time_unreadable_is_invalid(self):
        snap = self._snapshot(_readers(start_time=None))
        self.assertEqual(snap.status, SnapshotStatus.INVALID)
        self.assertIn("stat_unreadable", snap.errors)

    def test_pid_replaced_is_invalid(self):
        readers = _readers(start_time=999)
        with mock.patch.multiple(lp, **readers):
            snap = lp.snapshot_process(
                1, monotonic_ns=1, unix_paths_by_inode={},
                expected_start_time_ticks=100)
        self.assertEqual(snap.status, SnapshotStatus.INVALID)
        self.assertIn("process_replaced", snap.errors)


class ConsistentSnapshotTests(unittest.TestCase):
    def _snapshot_with_list(self, list_side_effect, targets=None):
        readers = _readers(targets=targets)
        readers["_list_fds"] = mock.Mock(side_effect=list_side_effect)
        with mock.patch.multiple(lp, **readers):
            return lp.snapshot_process(
                1, monotonic_ns=1, unix_paths_by_inode={})

    def test_race_then_agreement_is_partial_with_recovery_note(self):
        # First pair of listings disagree (fd opened between them); the
        # retry pair agrees. The snapshot is usable evidence but stays
        # PARTIAL with the churn error retained — a tick that raced once
        # may not silently re-badge itself VALID.
        calls = iter([[0, 1], [0, 1, 2], [0, 1, 2], [0, 1, 2]])
        snap = self._snapshot_with_list(lambda _pid: next(calls))
        self.assertEqual(snap.status, SnapshotStatus.PARTIAL)
        self.assertIn("fd_set_changed_during_read", snap.errors)
        self.assertEqual(len(snap.fds), 3)   # the agreed set IS recorded

    def test_persistent_churn_is_partial_with_errors(self):
        # The fd set never stops changing within the retry budget.
        counter = iter(range(100))

        def churning(_pid):
            return [0, 1, next(counter)]

        snap = self._snapshot_with_list(churning)
        self.assertEqual(snap.status, SnapshotStatus.INVALID)
        self.assertIn("fd_set_changed_during_read", snap.errors)
        self.assertIsNone(snap.fds)

    def test_single_unreadable_fd_is_a_partial_marker_not_invalid(self):
        # One fd whose readlink keeps failing stays as an UNKNOWN
        # placeholder in a PARTIAL snapshot, and the error NAMES the fd —
        # the other fds remain observable evidence and the artifact says
        # exactly which descriptor is missing.
        targets = {0: None}
        snap = self._snapshot_with_list(lambda _pid: [0, 1], targets=targets)
        self.assertEqual(snap.status, SnapshotStatus.PARTIAL)
        self.assertTrue(any(e.startswith("fd_readlink_unreadable:0")
                            for e in snap.errors), snap.errors)
        placeholders = [f for f in snap.fds if f.fd == 0]
        self.assertEqual(len(placeholders), 1)
        self.assertEqual(placeholders[0].kind, FdKind.UNKNOWN)


class UnixTableValidityTests(unittest.TestCase):
    def test_unix_table_unreadable_makes_tick_inconclusive(self):
        # validate_measurement flags unix_table_valid=False ticks; the
        # collector must therefore mark the tick, not silently pass an
        # empty dict as if no sockets existed.
        readers = _readers(fds=[0, 1])
        readers["unix_socket_table"] = mock.Mock(return_value={})
        with mock.patch.multiple(lp, **readers):
            tick = lp.sample_tick({"backend": 1}, monotonic_ns=1)
        self.assertFalse(tick.unix_table_valid)


if __name__ == "__main__":
    unittest.main()
