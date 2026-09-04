"""Minimal Linux integration: classify a real UDS pair and a regular file.

Skipped on non-Linux hosts so the suite stays runnable on Windows checkouts;
on Linux it verifies end-to-end that classification identifies the provider
socket pair by path and keeps unrelated files out of provider metrics.
"""
import os
import socket
import tempfile
import unittest

from src.observability.process_resources.linux_procfs import (
    snapshot_process,
    unix_socket_table,
)
from src.observability.process_resources.model import FdKind, PROVIDER_UDS_KINDS


@unittest.skipUnless(os.path.exists("/proc/net/unix"), "Linux /proc required")
class UnixPairIntegration(unittest.TestCase):
    def test_provider_pair_and_regular_file_classified(self):
        with tempfile.TemporaryDirectory(prefix="semloom-fdtest-") as directory:
            path = os.path.join(directory, "probe.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            server.listen(1)
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(path)
            accepted, _ = server.accept()
            try:
                table = unix_socket_table()
                snapshot = snapshot_process(
                    os.getpid(),
                    monotonic_ns=0,
                    provider_socket_path=path,
                    unix_paths_by_inode=table,
                    rss_bytes=4096,
                    thread_count=1,
                )
                provider_fds = snapshot.fd_numbers(PROVIDER_UDS_KINDS)
                # listener + accepted on the server side (this process holds both)
                self.assertEqual(len(provider_fds), 2, [f.target for f in snapshot.fds])
                for fd in provider_fds:
                    identity = next(f for f in snapshot.fds if f.fd == fd)
                    self.assertEqual(identity.unix_path, path)

                with tempfile.NamedTemporaryFile(dir=directory, suffix=".txt") as handle:
                    handle.write(b"x")
                    handle.flush()
                    other = snapshot_process(
                        os.getpid(),
                        monotonic_ns=0,
                        provider_socket_path=path,
                        unix_paths_by_inode=table,
                        rss_bytes=4096,
                        thread_count=1,
                    )
                    regular = [f for f in other.fds
                               if f.kind in (FdKind.REGULAR_FILE_OTHER, FdKind.UNKNOWN)
                               and f.target == handle.name]
                    self.assertTrue(regular, "temp regular file must stay visible")
                    self.assertNotIn(FdKind.PROVIDER_UDS_CLIENT,
                                     {f.kind for f in other.fds if f.target == handle.name})
            finally:
                accepted.close()
                client.close()
                server.close()
                if os.path.exists(path):
                    os.unlink(path)

    def test_closed_pair_leaves_zero_provider_delta(self):
        with tempfile.TemporaryDirectory(prefix="semloom-fdclose-") as directory:
            path = os.path.join(directory, "gone.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            server.listen(1)
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(path)
            accepted, _ = server.accept()
            accepted.close(); client.close(); server.close()
            os.unlink(path)
            snapshot = snapshot_process(
                os.getpid(),
                monotonic_ns=0,
                provider_socket_path=path,
                rss_bytes=4096,
                thread_count=1,
            )
            self.assertEqual(snapshot.count(PROVIDER_UDS_KINDS), 0)


if __name__ == "__main__":
    unittest.main()
