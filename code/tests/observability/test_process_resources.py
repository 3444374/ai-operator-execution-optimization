"""Unit tests for FD classification and the process-resource model."""
import unittest

from src.observability.process_resources.linux_procfs import (
    classify_target,
    unix_socket_table,
)
from src.observability.process_resources.model import (
    FdKind,
    PgFileClassificationContext,
    ProcessSnapshot,
    SnapshotStatus,
)


UNIX_TABLE = {111: "/tmp/semloom/stress.sock", 222: "/tmp/unrelated.sock",
              333: None}


class ClassifyTargetTests(unittest.TestCase):
    def test_provider_path_socket_is_provider_connected(self):
        kind, inode, path = classify_target(
            "socket:[111]", UNIX_TABLE, "/tmp/semloom/stress.sock")
        self.assertEqual(kind, FdKind.PROVIDER_UDS_CONNECTED)
        self.assertEqual(inode, 111)
        self.assertEqual(path, "/tmp/semloom/stress.sock")

    def test_other_bound_unix_socket_is_socket_other(self):
        kind, _, path = classify_target("socket:[222]", UNIX_TABLE, "/tmp/semloom/stress.sock")
        self.assertEqual(kind, FdKind.SOCKET_OTHER)
        self.assertEqual(path, "/tmp/unrelated.sock")

    def test_unbound_unix_socket_is_kept_distinct(self):
        kind, inode, path = classify_target("socket:[333]", UNIX_TABLE, None)
        self.assertEqual(kind, FdKind.UNBOUND_UNIX_SOCKET)
        self.assertEqual(inode, 333)
        self.assertIsNone(path)

    def test_socket_absent_from_table_is_socket_other(self):
        kind, _, _ = classify_target("socket:[999]", UNIX_TABLE, None)
        self.assertEqual(kind, FdKind.SOCKET_OTHER)

    def test_pipe_and_anon_inode(self):
        self.assertEqual(classify_target("pipe:[9]", {}, None)[0], FdKind.PIPE)
        self.assertEqual(classify_target("anon_inode:[eventfd]", {}, None)[0],
                         FdKind.EVENTFD_OR_ANON_INODE)

    def test_filenode_context_classifies_exact_relations(self):
        context = PgFileClassificationContext(
            data_directory="/pgdata",
            relation_filenodes=frozenset({16388}),
            toast_filenodes=frozenset({16390}))
        for name in ("base/16384/16388", "base/16384/16388_fsm",
                     "base/16384/16388_vm", "base/16384/16388.1"):
            kind, _, _ = classify_target(f"/pgdata/{name}", {}, None, context)
            self.assertEqual(kind, FdKind.RELATION_FILE, name)
        kind, _, _ = classify_target("/pgdata/base/16384/16390", {}, None, context)
        self.assertEqual(kind, FdKind.TOAST_RELATION_FILE)

    def test_unknown_filenode_under_pgdata_is_unknown_not_guessed(self):
        # A numeric basename the run never learned from the catalog is NOT
        # relation evidence: classifying it by name-shape alone was the
        # v1-era guessing this schema exists to remove.
        context = PgFileClassificationContext(
            data_directory="/pgdata",
            relation_filenodes=frozenset({16388}),
            toast_filenodes=frozenset({16390}))
        kind, _, _ = classify_target("/pgdata/base/16384/99999", {}, None, context)
        self.assertEqual(kind, FdKind.UNKNOWN)
        # Outside the data directory entirely, a numeric name is likewise
        # never a relation file.
        kind, _, _ = classify_target("/tmp/12345", {}, None, context)
        self.assertEqual(kind, FdKind.REGULAR_FILE_OTHER)

    def test_postgres_temp_file(self):
        kind, _, _ = classify_target("/pgdata/base/pgsql_tmp/123.0", {}, None)
        self.assertEqual(kind, FdKind.POSTGRES_TEMP_FILE)

    def test_regular_file_and_unknown(self):
        kind, _, _ = classify_target("/etc/hostname", {}, None)
        self.assertEqual(kind, FdKind.REGULAR_FILE_OTHER)
        kind, _, _ = classify_target("memfd:abc", {}, None)
        self.assertEqual(kind, FdKind.UNKNOWN)


class UnixSocketTableTests(unittest.TestCase):
    def test_table_maps_inode_to_path_or_none(self):
        table = unix_socket_table()
        self.assertIsInstance(table, dict)
        for inode, path in table.items():
            self.assertIsInstance(inode, int)
            self.assertTrue(path is None or isinstance(path, str))


class ModelTests(unittest.TestCase):
    def test_snapshot_unreadable_fields_stay_none(self):
        snapshot = ProcessSnapshot(
            pid=1, process_start_time_ticks=None, monotonic_ns=1,
            status=SnapshotStatus.INVALID)
        self.assertIsNone(snapshot.rss_bytes)
        self.assertIsNone(snapshot.thread_count)
        self.assertIsNone(snapshot.fds)
        self.assertIsNone(snapshot.total_fd_count)

    def test_counts_by_kind_set(self):
        from src.observability.process_resources.model import FdIdentity
        snapshot = ProcessSnapshot(
            pid=1, process_start_time_ticks=1, monotonic_ns=1,
            status=SnapshotStatus.VALID, rss_bytes=1, thread_count=1,
            fds=tuple(FdIdentity(fd=fd, target="socket:[1]", kind=kind)
                      for fd, kind in enumerate(
                          (FdKind.PROVIDER_UDS_CONNECTED, FdKind.RELATION_FILE, FdKind.PIPE))))
        self.assertEqual(snapshot.total_fd_count, 3)
        self.assertEqual(snapshot.count(FdKind.PROVIDER_UDS_CONNECTED), 1)
        self.assertEqual(
            snapshot.fd_numbers({FdKind.RELATION_FILE, FdKind.PIPE}), {1, 2})


if __name__ == "__main__":
    unittest.main()
