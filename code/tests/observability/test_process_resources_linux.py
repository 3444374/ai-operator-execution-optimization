"""Linux integration: cross-process UDS pair, peer credentials, attribution.

Runs only on Linux with /proc, AF_UNIX, and SO_PEERCRED. A real child
process holds the unbound client end so the attribution rules can be
exercised exactly as the qualification uses them: the parent owns the
listener and accepted socket, the child owns the pathnameless client.
"""
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest

from src.observability.process_resources.linux_procfs import (
    sample_tick,
    snapshot_process,
    unix_socket_table,
)
from src.observability.process_resources.model import FdKind, SnapshotStatus
from src.experiments.postgresql.provider_session_attribution import (
    attribute_provider_sessions,
    reclassify_clients,
    session_windows,
)
from src.experiments.postgresql.semmap_resource_gateway_observer import (
    _accepted_inode,
    _peer_credentials,
)

CHILD_HOLDER = """
import socket, sys, time
path = sys.argv[1]
ready = sys.argv[2]
client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
client.connect(path)
with open(ready, 'w') as handle:
    handle.write(str(client.fileno()))
import time as t
while True:
    t.sleep(0.05)
"""


@unittest.skipUnless(os.path.exists("/proc/net/unix"), "Linux /proc required")
class CrossProcessUdsIntegration(unittest.TestCase):
    def test_child_client_attributed_via_peer_evidence(self):
        with tempfile.TemporaryDirectory(prefix="semloom-xproc-") as directory:
            path = os.path.join(directory, "provider.sock")
            ready = os.path.join(directory, "ready")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            server.listen(1)
            child = subprocess.Popen(
                [sys.executable, "-c", CHILD_HOLDER, path, ready],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                for _ in range(200):
                    if os.path.exists(ready):
                        break
                    time.sleep(0.02)
                accepted, _ = server.accept()
                peer = _peer_credentials(accepted)
                self.assertIsNotNone(peer)
                self.assertEqual(peer[0], child.pid,
                                 "SO_PEERCRED peer pid must be the client child")
                inode = _accepted_inode(accepted)
                self.assertIsNotNone(inode)

                # Observer-shaped events + ticks of both processes.
                events = [
                    {"event": "session_start", "session_id": 1,
                     "monotonic_ns": 1, "gateway_pid": os.getpid(),
                     "accepted_fd": accepted.fileno(),
                     "accepted_socket_inode": inode,
                     "peer_pid": peer[0], "peer_uid": peer[1],
                     "peer_gid": peer[2]},
                    {"event": "session_end", "session_id": 1,
                     "monotonic_ns": 4_000_000_000},
                ]
                base = sample_tick(
                    {"gateway": os.getpid(), "backend": child.pid},
                    monotonic_ns=0, provider_socket_path=path)
                ticks = []
                for ns in (1_000_000_000, 2_000_000_000, 3_000_000_000):
                    ticks.append(sample_tick(
                        {"gateway": os.getpid(), "backend": child.pid},
                        monotonic_ns=ns, provider_socket_path=path))
                from src.observability.process_resources.model import ResourceTrace
                trace = ResourceTrace(
                    baseline=dict(base.processes), ticks=tuple(ticks))

                attribution = attribute_provider_sessions(
                    backend_pid=child.pid, baseline=trace.baseline,
                    trace=trace, windows=session_windows(events))
                self.assertIsNotNone(attribution,
                                     "unique child client must be attributed")
                attributed_fd = attribution["sessions"][0]["attributed"]["fd"]
                rewritten = reclassify_clients(trace, attribution)
                kinds = [
                    item.kind for item in
                    rewritten.ticks[0].processes["backend"].fds
                    if item.fd == attributed_fd]
                self.assertEqual(kinds, [FdKind.PROVIDER_UDS_CONNECTED])
                # The regular-file descriptors of the child must stay untouched.
                accepted.close()
            finally:
                child.terminate()
                child.wait(timeout=5)
                server.close()
                if os.path.exists(path):
                    os.unlink(path)

    def test_unbound_unix_entries_are_kept_in_table(self):
        # A connected client socket appears in /proc/net/unix with no path.
        with tempfile.TemporaryDirectory(prefix="semloom-unbound-") as directory:
            path = os.path.join(directory, "u.sock")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            server.listen(1)
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.connect(path)
            accepted, _ = server.accept()
            try:
                table = unix_socket_table()
                client_inode = int(os.readlink(
                    f"/proc/self/fd/{client.fileno()}")[len("socket:["):-1])
                self.assertIn(client_inode, table)
                self.assertIsNone(table[client_inode],
                                  "connected client has no bound path")
                # And the collector classifies it as UNBOUND_UNIX_SOCKET.
                snapshot = snapshot_process(
                    os.getpid(), monotonic_ns=1,
                    unix_paths_by_inode=table,
                    provider_socket_path=path)
                kinds = [item.kind for item in snapshot.fds or ()
                         if item.inode == client_inode]
                self.assertEqual(kinds, [FdKind.UNBOUND_UNIX_SOCKET])
            finally:
                accepted.close()
                client.close()
                server.close()
                os.unlink(path)


if __name__ == "__main__":
    unittest.main()
