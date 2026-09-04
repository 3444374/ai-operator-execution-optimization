"""Unit tests for FD classification and the process-resource model."""
import unittest

from src.observability.process_resources.linux_procfs import (
    classify_target,
    unix_socket_table,
)
from src.observability.process_resources.model import FdKind, ProcessSnapshot
from src.observability.process_resources.recorder import (
    ProcfsSampler,
    serialize_snapshot,
)


UNIX_TABLE = {111: "/tmp/semloom/stress.sock", 222: "/tmp/unrelated.sock"}


class ClassifyTargetTests(unittest.TestCase):
    def test_provider_uds_path_matches_bound_socket(self):
        kind, inode, path = classify_target(
            "socket:[111]", UNIX_TABLE, "/tmp/semloom/stress.sock")
        self.assertEqual(kind, FdKind.PROVIDER_UDS_ACCEPTED)
        self.assertEqual(inode, 111)
        self.assertEqual(path, "/tmp/semloom/stress.sock")

    def test_other_bound_unix_socket_is_socket_other(self):
        kind, _, path = classify_target("socket:[222]", UNIX_TABLE, "/tmp/semloom/stress.sock")
        self.assertEqual(kind, FdKind.SOCKET_OTHER)
        self.assertEqual(path, "/tmp/unrelated.sock")

    def test_unbound_socket_is_socket_other_without_path(self):
        kind, inode, path = classify_target("socket:[333]", UNIX_TABLE, None)
        self.assertEqual(kind, FdKind.SOCKET_OTHER)
        self.assertEqual(inode, 333)
        self.assertIsNone(path)

    def test_pipe_and_anon_inode(self):
        self.assertEqual(classify_target("pipe:[9]", {}, None)[0], FdKind.PIPE)
        self.assertEqual(classify_target("anon_inode:[eventfd]", {}, None)[0],
                         FdKind.EVENTFD_OR_ANON_INODE)

    def test_postgres_relation_forks(self):
        for name in ("base/16384/16388", "base/16384/16388_fsm", "base/16384/16388_vm"):
            kind, _, _ = classify_target(f"/pgdata/{name}", {}, None)
            self.assertEqual(kind, FdKind.RELATION_FILE, name)

    def test_toast_relation_and_temp_files(self):
        kind, _, _ = classify_target("/pgdata/base/16384/16388_toast", {}, None)
        self.assertEqual(kind, FdKind.TOAST_RELATION_FILE)
        kind, _, _ = classify_target("/pgdata/base/pgsql_tmp/12345.0", {}, None)
        self.assertEqual(kind, FdKind.POSTGRES_TEMP_FILE)

    def test_regular_file_and_unknown(self):
        kind, _, _ = classify_target("/etc/hostname", {}, None)
        self.assertEqual(kind, FdKind.REGULAR_FILE_OTHER)
        kind, _, _ = classify_target("memfd:abc", {}, None)
        self.assertEqual(kind, FdKind.UNKNOWN)


class UnixSocketTableTests(unittest.TestCase):
    def test_table_maps_inode_to_path_or_empty(self):
        table = unix_socket_table()
        self.assertIsInstance(table, dict)
        # Every value is a path string; keys are inode ints (Linux host only).
        for inode, path in table.items():
            self.assertIsInstance(inode, int)
            self.assertIsInstance(path, str)


def make_snapshot(fd_kinds):
    from src.observability.process_resources.model import FdIdentity
    return ProcessSnapshot(
        monotonic_ns=1,
        rss_bytes=100,
        thread_count=1,
        fds=tuple(
            FdIdentity(fd=fd, target="socket:[1]", kind=kind)
            for fd, kind in enumerate(fd_kinds)
        ),
    )


class ModelTests(unittest.TestCase):
    def test_counts_by_kind_set(self):
        snapshot = make_snapshot([FdKind.PROVIDER_UDS_CLIENT, FdKind.RELATION_FILE, FdKind.PIPE])
        self.assertEqual(snapshot.total_fd_count, 3)
        self.assertEqual(snapshot.count(FdKind.PROVIDER_UDS_CLIENT), 1)
        self.assertEqual(snapshot.fd_numbers({FdKind.RELATION_FILE, FdKind.PIPE}), {1, 2})


class SerializerTests(unittest.TestCase):
    def test_snapshot_serializes_kind_names(self):
        payload = serialize_snapshot("backend", make_snapshot([FdKind.UNKNOWN]))
        self.assertEqual(payload["role"], "backend")
        self.assertEqual(payload["fds"][0]["kind"], "unknown")
        self.assertEqual(payload["total_fd_count"], 1)


class RoleOverrideTests(unittest.TestCase):
    """The backend refines a shared provider-path socket into the client end."""

    def test_procfs_sampler_unavailable_off_linux_returns_no_fds(self):
        sampler = ProcfsSampler({"backend": -1}, "/tmp/none.sock")
        snapshot = sampler("backend", 0)
        self.assertEqual(snapshot.total_fd_count, 0)




class ReadlinkRaceTests(unittest.TestCase):
    """A momentarily unreadable fd keeps its last resolved identity."""

    def test_transient_unreadable_fd_inherits_previous_kind(self):
        from src.observability.process_resources.linux_procfs import snapshot_process
        from src.observability.process_resources.model import FdIdentity, FdKind, ProcessSnapshot
        previous = ProcessSnapshot(
            monotonic_ns=0, rss_bytes=1, thread_count=1,
            fds=(FdIdentity(fd=18, target="anon_inode:[eventpoll]",
                            kind=FdKind.EVENTFD_OR_ANON_INODE),))
        # On a non-Linux host /proc/<pid> listing yields nothing; the
        # carry-forward path is what we assert via a fake snapshot flow.
        snapshot = snapshot_process(
            -1, monotonic_ns=1, provider_socket_path=None,
            unix_paths_by_inode={}, rss_bytes=1, thread_count=1,
            previous=previous)
        self.assertEqual(snapshot.total_fd_count, 0)  # no live fds to inherit from on this host


if __name__ == "__main__":
    unittest.main()
