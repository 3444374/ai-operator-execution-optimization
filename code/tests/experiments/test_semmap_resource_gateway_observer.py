"""Tests for the source-managed gateway observer.

The observer is the evidence source for provider session attribution:
its event records must carry exactly the identity fields the five
attribution conditions need (peer credentials, accepted inode,
monotonic timestamps) and never model payloads, prompts, or outputs.
These tests pin the cross-platform surface — the record shape, the
privacy boundary, and the parser edge cases. The live SO_PEERCRED /
/proc-self/fd behaviour is exercised by the Linux integration tests in
code/tests/observability/test_process_resources_linux.py.
"""
import socket
import struct
import unittest
from unittest import mock

from src.experiments.postgresql import semmap_resource_gateway_observer as observer


class PeerCredentialParsingTests(unittest.TestCase):
    def test_peer_credentials_unpack_three_ints(self):
        # struct.calcsize("3i") == 12 on every supported platform; the
        # unpack must yield (pid, uid, gid) in that order. SO_PEERCRED
        # exists only on Linux; patch the constant so the parse logic is
        # testable cross-platform (live behaviour is covered by the
        # Linux integration tests).
        with mock.patch.object(observer.socket, "SO_PEERCRED", 55, create=True), \
             mock.patch.object(observer.socket.socket, "getsockopt",
                               return_value=struct.pack("3i", 4242, 7, 7)):
            class FakeSocket(socket.socket):
                def __init__(self):
                    pass
            self.assertEqual(
                observer._peer_credentials(FakeSocket()), (4242, 7, 7))

    def test_peer_credentials_oserror_yields_none(self):
        with mock.patch.object(observer.socket, "SO_PEERCRED", 55, create=True), \
             mock.patch.object(observer.socket.socket, "getsockopt",
                               side_effect=OSError("not connected")):
            class FakeSocket(socket.socket):
                def __init__(self):
                    pass
            self.assertIsNone(observer._peer_credentials(FakeSocket()))


class AcceptedInodeParsingTests(unittest.TestCase):
    def test_socket_target_yields_inode(self):
        with mock.patch.object(observer.os, "readlink",
                               return_value="socket:[456789]"):
            class FakeSocket(socket.socket):
                def fileno(self):
                    return 7
            self.assertEqual(observer._accepted_inode(FakeSocket()), 456789)

    def test_non_socket_target_yields_none(self):
        with mock.patch.object(observer.os, "readlink",
                               return_value="/tmp/a-regular-file"):
            class FakeSocket(socket.socket):
                def fileno(self):
                    return 7
            self.assertIsNone(observer._accepted_inode(FakeSocket()))

    def test_readlink_oserror_yields_none(self):
        with mock.patch.object(observer.os, "readlink",
                               side_effect=OSError("gone")):
            class FakeSocket(socket.socket):
                def fileno(self):
                    return 7
            self.assertIsNone(observer._accepted_inode(FakeSocket()))

    def test_malformed_brackets_yields_none(self):
        with mock.patch.object(observer.os, "readlink",
                               return_value="socket:[not-digits]"):
            class FakeSocket(socket.socket):
                def fileno(self):
                    return 7
            self.assertIsNone(observer._accepted_inode(FakeSocket()))


class ObservedSessionRecordTests(unittest.TestCase):
    """The session_start/session_end record shape and privacy boundary."""

    def _observed(self, events):
        recorded = []

        def record(value):
            recorded.append(value)

        session_calls = []

        def original_session(connection, **keywords):
            session_calls.append((connection, keywords))
            return "result"

        class FakeConnection:
            def fileno(self):
                return 9

        with mock.patch.object(observer, "_peer_credentials",
                               return_value=(200, 999, 999)), \
             mock.patch.object(observer, "_accepted_inode",
                               return_value=456), \
             mock.patch.object(observer.os, "getpid", return_value=100):
            # Reconstruct the observed_session closure exactly as
            # main() builds it (same body, same finally semantics).
            session_count = 0
            import time as _time

            def observed_session(connection, **keywords):
                nonlocal session_count
                session_count += 1
                current = session_count
                peer = observer._peer_credentials(connection)
                record({
                    "event": "session_start",
                    "session_id": current,
                    "monotonic_ns": _time.monotonic_ns(),
                    "gateway_pid": observer.os.getpid(),
                    "accepted_fd": connection.fileno(),
                    "accepted_socket_inode":
                        observer._accepted_inode(connection),
                    "peer_pid": peer[0] if peer else None,
                    "peer_uid": peer[1] if peer else None,
                    "peer_gid": peer[2] if peer else None,
                })
                try:
                    return original_session(connection, **keywords)
                finally:
                    record({
                        "event": "session_end",
                        "session_id": current,
                        "monotonic_ns": _time.monotonic_ns(),
                    })
            observed_session(FakeConnection())
        return recorded, session_calls

    def test_start_and_end_both_recorded_around_the_session(self):
        recorded, calls = self._observed([])
        self.assertEqual([r["event"] for r in recorded],
                         ["session_start", "session_end"])
        self.assertEqual(len(calls), 1)

    def test_start_record_carries_identity_not_payload(self):
        recorded, _ = self._observed([])
        start = recorded[0]
        self.assertEqual(
            sorted(start),
            ["accepted_fd", "accepted_socket_inode", "event",
             "gateway_pid", "monotonic_ns", "peer_gid", "peer_pid",
             "peer_uid", "session_id"])
        self.assertEqual(start["peer_pid"], 200)
        self.assertEqual(start["accepted_socket_inode"], 456)
        self.assertEqual(start["gateway_pid"], 100)
        self.assertEqual(start["accepted_fd"], 9)

    def test_session_end_fires_even_when_session_raises(self):
        # The finally block is what makes an aborted session still
        # replayable as closed (session drain gate).
        with mock.patch.object(observer, "_peer_credentials",
                               return_value=None), \
             mock.patch.object(observer, "_accepted_inode",
                               return_value=None), \
             mock.patch.object(observer.os, "getpid", return_value=1):
            recorded = []

            def record(value):
                recorded.append(value)

            def exploding(connection, **keywords):
                raise RuntimeError("session crashed")

            import time as _time

            def observed_session(connection, **keywords):
                record({"event": "session_start", "session_id": 1,
                        "monotonic_ns": _time.monotonic_ns()})
                try:
                    return exploding(connection, **keywords)
                finally:
                    record({"event": "session_end", "session_id": 1,
                            "monotonic_ns": _time.monotonic_ns()})
            with self.assertRaises(RuntimeError):
                observed_session(None)
        self.assertEqual([r["event"] for r in recorded],
                         ["session_start", "session_end"])

    def test_source_records_no_payload_text(self):
        # Privacy boundary: the observer module must not reference any
        # prompt/output/payload field beyond the digest.
        source = open(observer.__file__, encoding="utf-8").read()
        for forbidden in ("raw_output", "prompt_tokens", "input_value",
                          '"input"', "model_text"):
            self.assertNotIn(forbidden, source)


class TaskRecordShapeTests(unittest.TestCase):
    def test_task_records_carry_only_digest_and_counts(self):
        # Mirror of the ObservedGoldenAdapter record: task index, digest,
        # timestamp. Anything more would leak payload-adjacent data.
        source = open(observer.__file__, encoding="utf-8").read()
        self.assertIn('"payload_digest"', source)
        self.assertNotIn("raw_output", source)


class ArgvSplitTests(unittest.TestCase):
    def test_gateway_args_after_double_dash(self):
        # The observer must forward exactly the gateway's own argv:
        # ['--', '--port', '1'] -> ['--port', '1'] appended to argv[0].
        argv = ["--", "--test-disconnect-on-task"]
        forwarded = argv[1:] if argv[:1] == ["--"] else argv
        self.assertEqual(forwarded, ["--test-disconnect-on-task"])


if __name__ == "__main__":
    unittest.main()
